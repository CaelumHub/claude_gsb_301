"""并发调度：构建池 + 用例池 + 阶段流水线 + 定时触发循环。

这是平台「测试调度与并发」难点的核心。一次构建要并发执行大量用例，多场
构建又要并行推进，同时定时任务到点还要自动触发新构建。三层并发：

1. **构建级并发**：一个线程池（``build_pool``）承载多场同时进行的构建，
   用 ``max_build_workers`` 限制并发构建数，避免磁盘/CPU 被打满；
2. **用例级并发**：每场构建内部再用一个线程池（``case_pool``）并发跑
   用例，用 ``max_case_workers`` 限制单构建内的并发度；结果通过
   :meth:`storage.buildstore.BuildStore.record_result` 在文件锁保护下
   并发安全地收集与聚合；
3. **定时触发**：一个后台循环线程按 ``tick`` 间隔扫描启用的定时计划，
   命中 cron 且本分钟尚未触发过就提交新构建，防止同一分钟重复触发。

阶段化流水线
------------
一条流水线（pipeline）由若干**有序阶段**组成（静态检查 → 单元测试 →
接口回归 → 报告生成），每个阶段绑定一套用例或一组检查：

- 前一阶段通过才进入下一阶段；
- 阶段失败可配置 ``on_fail=abort``（直接中止后续阶段）或
  ``on_fail=continue``（继续跑但结果标记失败）；
- 用例类阶段可设置 ``env_scope=pipeline``，按流水线绑定的环境顺序依次
  执行（开发环境先跑、预发后跑），每次执行是阶段下的一次「尝试」；
  固定环境（``fixed``）只跑一次，报告阶段（``once``）整条流水线只跑一次。

实现上，每场流水线运行是一场 **父构建**（``kind=pipeline``），每个阶段
在某个环境上的一次执行是一场 **子构建**（``kind=stage``）。子构建复用
普通构建的用例并发执行与结果收集逻辑，父构建负责顺序编排、阶段闸门、
状态聚合与最终收尾。阶段运行态（状态/耗时/多环境尝试）实时写在父构建
的 ``stages`` 字段，供页面可视化。

取消：每个构建持有一个 ``threading.Event``，流水线父构建与各子构建共用
同一个事件；取消后在跑的用例尽快中止、未跑的阶段/尝试不再启动，未执行
的用例与阶段分别标记 ``skipped``，最终构建标为 ``cancelled``。
"""

from __future__ import annotations

import datetime
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from .cron import cron_matches, parse_cron
from .models import new_id


class Scheduler:
    """测试并发调度器（普通构建 + 阶段流水线）。"""

    def __init__(self, registry, build_registry, executor, env_manager,
                 report_gen, coverage_analyzer, defect_manager, notify_manager,
                 static_checker=None,
                 max_build_workers: int = 4, max_case_workers: int = 8,
                 tick_seconds: float = 20.0):
        self.registry = registry
        self.builds = build_registry
        self.executor = executor
        self.env_manager = env_manager
        self.report_gen = report_gen
        self.coverage = coverage_analyzer
        self.defects = defect_manager
        self.notify = notify_manager
        # 静态检查器可选注入（旧测试/旧调用方不注入时延迟创建）
        self.static_checker = static_checker

        self.max_build_workers = max_build_workers
        self.max_case_workers = max_case_workers
        self.tick_seconds = tick_seconds

        self._build_pool = ThreadPoolExecutor(
            max_workers=max_build_workers, thread_name_prefix="build")
        self._running: dict[str, dict] = {}
        self._running_lock = threading.Lock()

        self._stop_event = threading.Event()
        self._tick_thread: Optional[threading.Thread] = None
        self._scan_lock = threading.Lock()

    def _checker(self):
        if self.static_checker is None:
            from .staticcheck import StaticChecker
            self.static_checker = StaticChecker(self.registry)
        return self.static_checker

    # ------------------------------------------------------------------ 启动
    def start(self) -> None:
        if self._tick_thread is None:
            self._tick_thread = threading.Thread(
                target=self._tick_loop, name="scheduler-tick", daemon=True)
            self._tick_thread.start()

    def shutdown(self) -> None:
        self._stop_event.set()
        self._build_pool.shutdown(wait=False, cancel_futures=True)

    # ------------------------------------------------------------------ 触发：普通构建
    def submit_build(self, project_id: str, suite_id: str,
                     env_id: Optional[str] = None, trigger: str = "manual",
                     schedule_id: Optional[str] = None) -> dict:
        """提交一场普通套件构建，立即返回构建元信息（后台线程池运行）。"""
        suites = self.registry.store("suites")
        cases_store = self.registry.store("cases")

        suite = suites.get(suite_id)
        if suite is None:
            return {"error": "测试套件不存在"}

        env_id = env_id or suite.get("env_id")
        if not env_id:
            envs = self.env_manager.list(project_id)
            if not envs:
                return {"error": "项目还没有可用环境，请先创建环境"}
            env_id = envs[0]["id"]
        if self.env_manager.get(env_id) is None:
            return {"error": "环境不存在"}

        case_ids = suite.get("case_ids") or []
        cases = cases_store.get_many(case_ids)
        if not cases:
            return {"error": "套件内没有用例"}

        build_id = new_id("build")
        build = self.builds.for_project(project_id).create(
            build_id,
            suite_id=suite_id,
            env_id=env_id,
            name=suite.get("name", ""),
            trigger=trigger,
        )

        cancel_event = threading.Event()
        with self._running_lock:
            self._running[build_id] = {
                "cancel": cancel_event,
                "project_id": project_id,
                "suite_id": suite_id,
            }

        self._build_pool.submit(
            self._run_build, project_id, build_id, cases, env_id, cancel_event)

        if schedule_id:
            self._record_schedule_run(schedule_id, project_id, build_id)

        return build

    # --------------------------------------------------------- 触发：阶段流水线
    def submit_pipeline(self, project_id: str, pipeline_id: str,
                        trigger: str = "manual",
                        schedule_id: Optional[str] = None) -> dict:
        """提交一场流水线运行（父构建），阶段在后台按顺序编排执行。"""
        pipeline = self.registry.store("pipelines").get(pipeline_id)
        if pipeline is None:
            return {"error": "流水线不存在"}
        if not pipeline.get("enabled", True):
            return {"error": "流水线已停用"}

        stages = pipeline.get("stages") or []
        if not stages:
            return {"error": "流水线还没有配置阶段"}

        # 校验流水线环境（pipeline 级阶段按这个顺序跑：开发先跑、预发后跑）
        env_ids = pipeline.get("env_ids") or []
        envs = [self.env_manager.get(eid) for eid in env_ids]
        envs = [e for e in envs if e]
        if not envs:
            available = self.env_manager.list(project_id)
            if not available:
                return {"error": "项目还没有可用环境，请先创建环境"}
            envs = available

        # 固定环境阶段也校验一下环境存在性
        for st in stages:
            if st.get("env_scope") == "fixed":
                if self.env_manager.get(st.get("env_id")) is None:
                    return {"error": f"阶段「{st.get('name')}」绑定的环境不存在"}

        build_id = new_id("pipe")
        stage_snapshot = [self._stage_snapshot(st) for st in stages]
        env_names = [e.get("name", e["id"]) for e in envs]
        build = self.builds.for_project(project_id).create(
            build_id,
            name=pipeline.get("name", "未命名流水线"),
            trigger=trigger,
            kind="pipeline",
            pipeline_id=pipeline_id,
            pipeline_run_id=build_id,
            stages=stage_snapshot,
            env_name=" → ".join(env_names),
        )

        cancel_event = threading.Event()
        with self._running_lock:
            self._running[build_id] = {
                "cancel": cancel_event,
                "project_id": project_id,
                "pipeline_id": pipeline_id,
            }

        self._build_pool.submit(
            self._run_pipeline, project_id, build_id, pipeline, envs, cancel_event)

        if schedule_id:
            self._record_schedule_run(schedule_id, project_id, build_id)

        return build

    @staticmethod
    def _stage_snapshot(st: dict) -> dict:
        return {
            "stage_id": st["id"],
            "name": st.get("name", "未命名阶段"),
            "type": st.get("type", "api"),
            "on_fail": st.get("on_fail", "abort"),
            "env_scope": st.get("env_scope", "pipeline"),
            "suite_id": st.get("suite_id"),
            "tags_any": st.get("tags_any") or [],
            "env_id": st.get("env_id"),
            "status": "pending",
            "attempts": [],
            "total": 0, "passed": 0, "failed": 0, "error": 0,
            "timeout": 0, "skipped": 0, "duration": 0.0,
            "started_at": None, "finished_at": None,
        }

    def cancel_build(self, build_id: str) -> dict:
        handle = self._running.get(build_id)
        if handle is None:
            return {"error": "构建不在运行中或不存在"}
        handle["cancel"].set()
        return {"ok": True, "build_id": build_id}

    def running(self) -> list[dict]:
        out = []
        with self._running_lock:
            for build_id, handle in list(self._running.items()):
                build = self.builds.for_project(handle["project_id"]).get(build_id)
                out.append({
                    "build_id": build_id,
                    "project_id": handle["project_id"],
                    "suite_id": handle.get("suite_id"),
                    "pipeline_id": handle.get("pipeline_id"),
                    "status": build.get("status") if build else "running",
                    "total": build.get("total", 0) if build else 0,
                    "passed": build.get("passed", 0) if build else 0,
                    "failed": build.get("failed", 0) if build else 0,
                    "started_at": build.get("started_at") if build else None,
                })
        return out

    # ------------------------------------------------------------------ 普通构建执行
    def _run_build(self, project_id: str, build_id: str, cases: list,
                   env_id: str, cancel_event: threading.Event,
                   finalize: bool = True) -> dict:
        """执行一场普通套件构建。

        ``finalize=False`` 时跳过收尾（报告/通知/缺陷），供流水线在全部
        阶段结束后统一对父构建收尾，避免每个阶段子构建都发一次通知。
        返回结束后的构建摘要。
        """
        store = self.builds.for_project(project_id)
        env_config = self.env_manager.to_executor_config(env_id)
        store.set_total(build_id, len(cases))
        store.append_log(build_id, f"构建 {build_id} 开始，共 {len(cases)} 个用例，"
                                   f"环境 {env_id}")

        case_workers = max(1, min(self.max_case_workers, len(cases)))
        try:
            with ThreadPoolExecutor(max_workers=case_workers,
                                    thread_name_prefix=f"case-{build_id[:6]}") as pool:
                self._run_case_pool(store, build_id, cases, env_config, env_id,
                                    cancel_event, pool, log_prefix="")
        except Exception as exc:  # noqa: BLE001
            store.append_log(build_id, f"构建执行异常: {exc}")

        status = self._terminal_status(store, build_id, cancel_event)
        store.finish(build_id, status)
        build = store.get(build_id)
        store.append_log(build_id, f"构建结束: {status}（通过 {build.get('passed', 0)}"
                                   f"/{build.get('total', 0)}）")

        if finalize:
            self._finalize(project_id, build_id)
        return build

    @staticmethod
    def _terminal_status(store, build_id, cancel_event) -> str:
        build = store.get(build_id)
        if cancel_event.is_set():
            return "cancelled"
        if (build.get("failed", 0) + build.get("error", 0) + build.get("timeout", 0)) == 0:
            return "passed"
        return "failed"

    def _run_case_pool(self, store, build_id, cases, env_config, env_id,
                       cancel_event, pool, log_prefix="",
                       mirror_build_id: Optional[str] = None,
                       static_project_id: Optional[str] = None,
                       env_name: Optional[str] = None) -> None:
        """在给定用例线程池里并发执行一批用例，结果落盘并聚合。

        - ``mirror_build_id``：流水线父构建 id，提供时每条结果同时镜像到
          父构建，使父构建聚合计数覆盖全部阶段；
        - ``static_project_id``：提供时这批「用例」走静态检查而非执行；
        - ``env_name``：环境显示名，写入结果便于父构建按环境区分。
        """
        parent_store = (self.builds.for_project(static_project_id or store.project_id)
                        if mirror_build_id else None)
        futures = {}
        for i, case in enumerate(cases):
            if cancel_event.is_set():
                break
            if static_project_id is not None:
                fut = pool.submit(self._run_static_one, static_project_id,
                                  case, cancel_event)
            else:
                fut = pool.submit(
                    self._run_one, case, env_config, env_id, i, cancel_event,
                    env_name)
            futures[fut] = case

        for future in as_completed(futures):
            case = futures[future]
            try:
                results = future.result()
            except Exception as exc:  # noqa: BLE001
                results = [{
                    "case_id": case.get("id"),
                    "case_name": case.get("name", "未命名用例"),
                    "group": (case.get("tags") or ["默认"])[0],
                    "priority": case.get("priority", "P3"),
                    "status": "error",
                    "duration": 0.0,
                    "steps": [], "assertions": [],
                    "logs": [f"用例执行异常: {exc}"],
                }]
            if not isinstance(results, list):
                results = [results]
            for result in results:
                self._persist_result(store, build_id, case, result)
                if mirror_build_id:
                    # 镜像到父构建：只更新聚合计数，不重复写单用例日志文件
                    parent_store.record_result(mirror_build_id, result)

        # 未提交的用例（被取消跳过）记为 skipped（子 + 父各一份）
        submitted = {c.get("id") for c in futures.values()}
        for case in cases:
            if case.get("id") not in submitted:
                skipped = {
                    "case_id": case.get("id"),
                    "case_name": case.get("name", "未命名用例"),
                    "group": case.get("group")
                             or ((case.get("tags") or ["默认"])[0]),
                    "priority": case.get("priority", "P3"),
                    "status": "skipped",
                    "duration": 0.0,
                    "steps": [], "assertions": [],
                    "logs": ["因取消而未执行"],
                }
                self._persist_result(store, build_id, case, skipped)
                if mirror_build_id:
                    parent_store.record_result(mirror_build_id, skipped)

    def _run_one(self, case: dict, env_config: dict, env_id: str,
                 index: int, cancel_event: threading.Event,
                 env_name: Optional[str] = None):
        result = self.executor.execute_case(
            case, env_config, cancel_event=cancel_event,
            timeout=case.get("timeout", 60))
        result["env_id"] = env_id
        result["env_name"] = env_name
        result["order"] = index
        return [result]

    def _run_static_one(self, project_id: str, case: dict,
                        cancel_event: threading.Event):
        return self._checker().run(project_id, [case], cancel_event=cancel_event)

    def _persist_result(self, store, build_id: str, case: dict, result: dict) -> None:
        store.record_result(build_id, result)
        case_id = case.get("id")
        if case_id and result.get("stage_kind") != "static":
            log_text = "\n".join(result.get("logs", []))
            store.write_case_log(build_id, case_id, log_text)

    # ------------------------------------------------------------ 流水线编排
    def _run_pipeline(self, project_id: str, parent_id: str, pipeline: dict,
                      envs: list[dict], cancel_event: threading.Event) -> None:
        """顺序编排一条流水线的全部阶段。"""
        store = self.builds.for_project(project_id)
        store.mark_running(parent_id)
        store.append_log(
            parent_id,
            f"流水线「{pipeline.get('name')}」开始，共 {len(pipeline.get('stages') or [])} "
            f"个阶段，环境顺序: {' → '.join(e.get('name', e['id']) for e in envs)}")

        gate_failed = False     # 前序阶段失败且策略为 abort
        cancelled = False

        for st_index, stage_def in enumerate(pipeline.get("stages") or []):
            stage_id = stage_def["id"]
            store.patch_stage_state(parent_id, stage_id, {"started_at": time.time()})

            if cancel_event.is_set():
                cancelled = True
                # 报告阶段是收尾步骤：即使已取消，也生成一份反映已完成部分
                # 的报告；其余未执行阶段标记跳过。
                if stage_def.get("type") == "report":
                    self._run_report_stage(store, parent_id, stage_def, cancel_event)
                else:
                    self._skip_stage(store, parent_id, stage_def, "流水线已取消",
                                     cancel_event)
                continue

            if gate_failed:
                # 报告阶段是收尾步骤，即使前面中止也照常生成；其余阶段跳过
                if stage_def.get("type") == "report":
                    self._run_report_stage(store, parent_id, stage_def, cancel_event)
                else:
                    self._skip_stage(store, parent_id, stage_def,
                                     "前序阶段失败且策略为直接中止，本阶段跳过",
                                     cancel_event)
                continue

            if stage_def.get("type") == "report":
                self._run_report_stage(store, parent_id, stage_def, cancel_event)
                continue

            # 解析阶段要跑的用例
            cases = self._resolve_stage_cases(project_id, stage_def)
            scope = stage_def.get("env_scope", "pipeline")
            if scope == "once":
                stage_envs = [envs[0]]
            elif scope == "fixed":
                fixed = self.env_manager.get(stage_def.get("env_id"))
                stage_envs = [fixed] if fixed else []
            else:
                stage_envs = envs

            if not cases or not stage_envs:
                self._run_empty_stage(store, parent_id, stage_def, stage_envs,
                                      cancel_event,
                                      reason=("阶段没有可执行的环境" if stage_envs == []
                                              else "阶段未绑定用例或标签"))
                stage_state = store.get_stage_state(parent_id, stage_def["id"])
                stage_failed_hard = bool(stage_state and stage_state.get("status")
                                         in ("failed", "error", "timeout"))
            else:
                stage_failed_hard = self._run_case_stage(
                    project_id, store, parent_id, stage_def, cases, stage_envs,
                    cancel_event)
            if stage_failed_hard and stage_def.get("on_fail", "abort") == "abort":
                gate_failed = True
                store.append_log(
                    parent_id,
                    f"✖ 阶段「{stage_def.get('name')}」失败策略为「直接中止」，"
                    f"后续阶段不再执行（报告阶段除外）")

            store.patch_stage_state(parent_id, stage_id, {"finished_at": time.time()})
            if cancel_event.is_set():
                cancelled = True

        # 父构建终态
        if cancel_event.is_set():
            status = "cancelled"
        else:
            status = self._terminal_status(store, parent_id, cancel_event)
        store.finish(parent_id, status)
        store.append_log(parent_id, f"流水线结束: {status}")
        self._finalize(project_id, parent_id)

        with self._running_lock:
            self._running.pop(parent_id, None)

    def _resolve_stage_cases(self, project_id: str, stage_def: dict) -> list[dict]:
        """按阶段绑定解析出用例，保持稳定顺序。

        - 指定套件：套件内用例；
        - 指定标签集合：命中任一标签的用例；
        - 都没指定：静态检查阶段默认扫描项目下**全部用例**（lint 的自然
          语义），其他类型返回空（交由空阶段失败处理）。
        """
        cases_store = self.registry.store("cases")
        suite_id = stage_def.get("suite_id")
        if suite_id:
            suite = self.registry.store("suites").get(suite_id)
            if suite is None:
                return []
            return cases_store.get_many(suite.get("case_ids") or [])
        tags = stage_def.get("tags_any") or []
        if tags:
            all_cases = cases_store.query(where=[("project_id", "eq", project_id)],
                                         order_by="created_at", order="asc")
            return [c for c in all_cases
                    if set(c.get("tags") or []) & set(tags)]
        if stage_def.get("type") == "static":
            return cases_store.query(where=[("project_id", "eq", project_id)],
                                     order_by="created_at", order="asc")
        return []

    def _run_case_stage(self, project_id, store, parent_id, stage_def, cases,
                        stage_envs, cancel_event) -> bool:
        """执行一个用例类阶段（可跨多环境顺序执行）。

        返回阶段是否「硬失败」（存在 failed/error/timeout 的尝试）。
        """
        stage_id = stage_def["id"]
        is_static = stage_def.get("type") == "static"
        stage_failed = False

        for env_index, env in enumerate(stage_envs):
            env_id = env["id"]
            env_name = env.get("name", env_id)
            if cancel_event.is_set():
                self._skip_attempt(store, parent_id, stage_def, env,
                                   "流水线已取消")
                continue

            attempt_id = new_id("att")
            store.add_total(parent_id, len(cases))
            child_id = new_id("stage")
            child = store.create(
                child_id,
                name=f"{stage_def.get('name')} · {env_name}",
                env_id=env_id,
                env_name=env_name,
                kind="stage",
                parent_build_id=parent_id,
                pipeline_id=store.get(parent_id).get("pipeline_id"),
                stage_id=stage_id,
                stage_attempt_id=attempt_id,
                trigger="pipeline",
            )
            store.upsert_stage_attempt(parent_id, stage_id, {
                "attempt_id": attempt_id,
                "build_id": child_id,
                "env_id": env_id,
                "env_name": env_name,
                "status": "running",
                "total": len(cases), "passed": 0, "failed": 0, "error": 0,
                "timeout": 0, "skipped": 0, "duration": 0.0,
                "started_at": time.time(),
            })
            store.patch_stage_state(parent_id, stage_id, {"status": "running"})
            store.append_log(
                parent_id,
                f"▶ 阶段「{stage_def.get('name')}」在环境 {env_name} 开始，"
                f"{len(cases)} 个{'静态检查对象' if is_static else '用例'}")

            # 子构建执行（环境之间严格顺序：开发跑完才跑预发）
            child_store = store
            child_store.set_total(child_id, len(cases))
            env_config = self.env_manager.to_executor_config(env_id)
            case_workers = max(1, min(self.max_case_workers, len(cases)))
            child_started = time.time()
            stage_error = None
            try:
                with ThreadPoolExecutor(
                        max_workers=case_workers,
                        thread_name_prefix=f"stg-{child_id[:6]}") as pool:
                    if is_static:
                        self._run_case_pool(
                            child_store, child_id, cases, env_config, env_id,
                            cancel_event, pool,
                            mirror_build_id=parent_id,
                            static_project_id=project_id,
                            env_name=env_name)
                    else:
                        self._run_case_pool(
                            child_store, child_id, cases, env_config, env_id,
                            cancel_event, pool,
                            mirror_build_id=parent_id,
                            env_name=env_name)
            except Exception as exc:  # noqa: BLE001
                stage_error = exc
                child_store.append_log(child_id, f"阶段执行异常: {exc}")

            cstatus = self._terminal_status(child_store, child_id, cancel_event)
            if stage_error is not None and cstatus == "passed":
                cstatus = "error"
            child_store.finish(child_id, cstatus)
            cb = child_store.get(child_id)
            summary = {
                "attempt_id": attempt_id,
                "build_id": child_id,
                "env_id": env_id,
                "env_name": env_name,
                "status": cstatus,
                "total": cb.get("total", 0),
                "passed": cb.get("passed", 0),
                "failed": cb.get("failed", 0),
                "error": cb.get("error", 0),
                "timeout": cb.get("timeout", 0),
                "skipped": cb.get("skipped", 0),
                "duration": cb.get("duration", round(time.time() - child_started, 3)),
                "finished_at": time.time(),
            }
            store.upsert_stage_attempt(parent_id, stage_id, summary)
            store.append_log(
                parent_id,
                f"■ 阶段「{stage_def.get('name')}」环境 {env_name} {cstatus}："
                f"通过 {summary['passed']}/{summary['total']}，耗时 {summary['duration']}s")

            if cstatus in ("failed", "error", "timeout"):
                stage_failed = True
                if stage_def.get("on_fail", "abort") == "abort":                    # 该阶段剩余环境不再执行，补 skipped 尝试以完整可视化
                    for rest_env in stage_envs[env_index + 1:]:
                        self._skip_attempt(
                            store, parent_id, stage_def, rest_env,
                            "前一环境失败且策略为直接中止")
                    break  # 该阶段剩余环境也不再跑

        return stage_failed

    def _run_empty_stage(self, store, parent_id, stage_def, stage_envs,
                         cancel_event, reason: str = "阶段未绑定用例或标签") -> None:
        """阶段没有可跑用例：每个应跑环境补一条失败记录并计入闸门。"""
        stage_id = stage_def["id"]
        for env in stage_envs or [{"id": None, "name": "-"}]:
            attempt_id = new_id("att")
            result = {
                "case_id": f"stage-empty-{stage_id}-{attempt_id}",
                "case_name": f"阶段「{stage_def.get('name')}」无可执行用例",
                "group": "stage",
                "priority": "P3",
                "status": "failed",
                "duration": 0.0,
                "steps": [{"index": 0, "action": "stage", "name": "阶段配置校验",
                           "status": "failed", "message": reason, "duration": 0.0}],
                "assertions": [],
                "logs": [reason],
                "message": reason,
            }
            store.add_total(parent_id, 1)
            store.record_result(parent_id, result)
            store.upsert_stage_attempt(parent_id, stage_id, {
                "attempt_id": attempt_id,
                "build_id": None,
                "env_id": env.get("id"),
                "env_name": env.get("name", "-"),
                "status": "failed",
                "total": 1, "passed": 0, "failed": 1, "error": 0,
                "timeout": 0, "skipped": 0, "duration": 0.0,
                "started_at": time.time(), "finished_at": time.time(),
            })
            store.append_log(parent_id, f"✖ 阶段「{stage_def.get('name')}」: {reason}")
        store.patch_stage_state(parent_id, stage_id, {"finished_at": time.time()})

    def _skip_attempt(self, store, parent_id, stage_def, env, reason: str) -> None:
        """多环境阶段中，剩余环境因取消/中止而未执行，补 skipped 尝试。"""
        attempt_id = new_id("att")
        store.upsert_stage_attempt(parent_id, stage_def["id"], {
            "attempt_id": attempt_id,
            "build_id": None,
            "env_id": env.get("id"),
            "env_name": env.get("name", env.get("id")),
            "status": "skipped",
            "total": 0, "passed": 0, "failed": 0, "error": 0,
            "timeout": 0, "skipped": 0, "duration": 0.0,
            "started_at": time.time(), "finished_at": time.time(),
            "message": reason,
        })

    def _skip_stage(self, store, parent_id, stage_def, reason: str,
                    cancel_event) -> None:
        """整个阶段因闸门关闭/取消而跳过。"""
        stage_id = stage_def["id"]
        now_ts = time.time()
        attempt_id = new_id("att")
        # 补一条 skipped 伪结果，让父构建进度与阶段状态一致
        pseudo = {
            "case_id": f"stage-skip-{stage_id}",
            "case_name": f"阶段「{stage_def.get('name')}」已跳过",
            "group": "stage",
            "priority": "P3",
            "status": "skipped",
            "duration": 0.0,
            "steps": [{"index": 0, "action": "stage", "name": "阶段跳过",
                       "status": "skipped", "message": reason, "duration": 0.0}],
            "assertions": [],
            "logs": [reason],
            "message": reason,
        }
        store.add_total(parent_id, 1)
        store.record_result(parent_id, pseudo)
        store.put_stage_state(parent_id, {
            **self._stage_snapshot(stage_def),
            "status": "skipped",
            "started_at": now_ts,
            "finished_at": now_ts,
            "attempts": [{
                "attempt_id": attempt_id,
                "build_id": None,
                "env_id": None,
                "env_name": "-",
                "status": "skipped",
                "total": 1, "passed": 0, "failed": 0, "error": 0,
                "timeout": 0, "skipped": 1, "duration": 0.0,
                "started_at": now_ts, "finished_at": now_ts,
                "message": reason,
            }],
        })
        store.append_log(parent_id, f"⏭ 阶段「{stage_def.get('name')}」跳过：{reason}")

    def _run_report_stage(self, store, parent_id, stage_def,
                          cancel_event) -> None:
        """报告生成阶段：整条流水线只跑一次，生成报告并产出一条阶段结果。"""
        stage_id = stage_def["id"]
        now_ts = time.time()
        attempt_id = new_id("att")
        store.patch_stage_state(parent_id, stage_id,
                                {"started_at": now_ts, "status": "running"})
        started = time.time()
        project_id = store.project_id
        ok = True
        message = "流水线汇总报告已生成"
        try:
            self.report_gen.build_report(project_id, parent_id, force=True)
            parent = store.get(parent_id)
            total = parent.get("total", 0)
            passed = parent.get("passed", 0)
            self.coverage.generate(
                project_id, parent_id,
                (passed / total) if total else 1.0)
        except Exception as exc:  # noqa: BLE001
            ok = False
            message = f"报告生成失败: {exc}"
            store.append_log(parent_id, message)

        result_status = "passed" if ok else "error"
        store.add_total(parent_id, 1)
        store.record_result(parent_id, {
            "case_id": f"stage-report-{stage_id}",
            "case_name": "生成测试报告",
            "group": "stage",
            "priority": "P3",
            "status": result_status,
            "duration": round(max(time.time() - started, 0.001), 3),
            "steps": [{"index": 0, "action": "report", "name": "生成报告",
                       "status": result_status, "message": message,
                       "duration": round(time.time() - started, 3)}],
            "assertions": [],
            "logs": [message],
            "message": message,
        })
        store.upsert_stage_attempt(parent_id, stage_id, {
            "attempt_id": attempt_id,
            "build_id": parent_id,
            "env_id": None,
            "env_name": "汇总",
            "status": result_status,
            "total": 1,
            "passed": 1 if ok else 0,
            "failed": 0,
            "error": 0 if ok else 1,
            "timeout": 0, "skipped": 0,
            "duration": round(time.time() - started, 3),
            "started_at": started,
            "finished_at": time.time(),
        })
        store.patch_stage_state(parent_id, stage_id, {"finished_at": time.time()})
        store.append_log(parent_id, f"📄 报告阶段结束：{message}")

    # ------------------------------------------------------------------ 收尾
    def _finalize(self, project_id: str, build_id: str) -> None:
        store = self.builds.for_project(project_id)
        build = store.get(build_id)
        if build is None:
            return
        total = build.get("total", 0)
        passed = build.get("passed", 0)
        passed_ratio = (passed / total) if total else 1.0

        # 报告 / 覆盖率：流水线父构建在报告阶段已生成，这里幂等兜底
        try:
            self.report_gen.build_report(project_id, build_id, force=True)
        except Exception:  # noqa: BLE001
            pass
        try:
            self.coverage.generate(project_id, build_id, passed_ratio)
        except Exception:  # noqa: BLE001
            pass

        # 通知
        is_pipeline = build.get("kind") == "pipeline"
        subject = "流水线" if is_pipeline else "构建"
        event = "build.passed" if build["status"] == "passed" else "build.failed"
        payload = {
            "build_id": build_id,
            "project_id": project_id,
            "status": build["status"],
            "passed": passed,
            "total": total,
            "pass_rate": round(passed_ratio * 100, 1),
            "duration": build.get("duration", 0.0),
            "kind": build.get("kind", "build"),
            "pipeline_id": build.get("pipeline_id"),
            "subject": subject,
        }
        self.notify.fire(project_id, "build.finished", payload)
        self.notify.fire(project_id, event, payload)

        # 自动缺陷（项目配置开启时，把失败用例转成缺陷）。
        # 排除阶段自身的伪结果（group=stage：空阶段配置错误、报告生成失败），
        # 它们是流水线配置/平台问题，不是被测用例缺陷；静态检查命中的失败
        # 因为关联真实用例，仍会生成缺陷。
        project = self.registry.store("projects").get(project_id)
        if project and project.get("auto_create_defects"):
            failures = store.results(
                build_id, where=[("status", "in", ["failed", "error", "timeout"])])
            case_failures = [fr for fr in failures if fr.get("group") != "stage"]
            for fr in case_failures[:20]:
                self.defects.create_from_case(project_id, fr, build_id)

    # ------------------------------------------------------------------ 定时循环
    def _tick_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._scan_schedules()
            except Exception:  # noqa: BLE001
                pass
            self._stop_event.wait(self.tick_seconds)

    def _scan_schedules(self) -> None:
        # 串行化扫描，避免多个线程（后台 tick + 手动触发）同时读到
        # 「本分钟尚未触发」而重复触发同一计划。
        with self._scan_lock:
            self._scan_schedules_locked()

    def _scan_schedules_locked(self) -> None:
        now = datetime.datetime.now()
        minute_key = now.strftime("%Y-%m-%d %H:%M")
        schedules_store = self.registry.store("schedules")
        for schedule in schedules_store.all():
            if not schedule.get("enabled", True):
                continue
            if schedule.get("last_fired_minute") == minute_key:
                continue  # 本分钟已触发过，防止同一分钟重复
            try:
                if cron_matches(schedule.get("cron", "* * * * *"), now):
                    schedule["last_fired_minute"] = minute_key
                    schedules_store.update(schedule["id"], {"last_fired_minute": minute_key})
                    if schedule.get("pipeline_id"):
                        self.submit_pipeline(
                            schedule.get("project_id"),
                            schedule["pipeline_id"],
                            trigger="schedule",
                            schedule_id=schedule["id"],
                        )
                    else:
                        self.submit_build(
                            schedule.get("project_id"),
                            schedule.get("suite_id"),
                            env_id=schedule.get("env_id"),
                            trigger="schedule",
                            schedule_id=schedule["id"],
                        )
            except ValueError:
                continue

    def _record_schedule_run(self, schedule_id: str, project_id: str,
                             build_id: str) -> None:
        self.registry.store("schedule_runs").insert({
            "id": new_id("schrun"),
            "schedule_id": schedule_id,
            "project_id": project_id,
            "build_id": build_id,
            "fired_at": time.time(),
            "status": "submitted",
        })

    def describe_cron(self, expr: str) -> str:
        """把 cron 表达式转成人话（供前端展示）。"""
        try:
            sched = parse_cron(expr)
        except ValueError:
            return "无效表达式"
        parts = []
        if sched.minute == list(range(0, 60)):
            parts.append("每分钟")
        else:
            parts.append(f"第 {','.join(map(str, sched.minute[:6]))} 分" + ("…" if len(sched.minute) > 6 else ""))
        if sched.hour != list(range(0, 24)):
            parts.append(f"{','.join(map(str, sched.hour[:6]))} 时" + ("…" if len(sched.hour) > 6 else ""))
        if sched.day != list(range(1, 32)):
            parts.append(f"每月 {','.join(map(str, sched.day[:8]))} 日" + ("…" if len(sched.day) > 8 else ""))
        if sched.weekday != list(range(0, 7)):
            parts.append(f"周 {','.join(map(str, sched.weekday))}")
        return " · ".join(parts) if parts else "每分钟"
