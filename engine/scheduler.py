"""并发调度：构建池 + 用例池 + 定时触发循环。

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

取消：每个构建持有一个 ``threading.Event``，用例执行器在步骤之间检查它，
取消后已在跑或用例尽快中止、未跑的不再启动，最终构建标为 ``cancelled``。

4. **流水线（阶段化验证）**：一条流水线运行同样占用一个 build worker，
   但内部按阶段**严格串行**推进——静态检查 → 单元测试 → 接口回归 → 报告，
   前一阶段通过（或失败策略为 ``continue``）才放行下一阶段；``abort``
   阶段失败后，后续阶段直接标 ``skipped``。每个执行阶段仍落成一场真实构建
   （复用用例池与 BuildStore），因此监控 / 报告 / 覆盖率页面无需特判即可
   展示阶段明细；各阶段可绑定不同环境，实现 dev 先跑、staging 后跑。
"""

from __future__ import annotations

import datetime
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from .cron import cron_matches, parse_cron
from .models import new_id
from .staticcheck import build_static_cases


class Scheduler:
    """测试并发调度器。"""

    def __init__(self, registry, build_registry, executor, env_manager,
                 report_gen, coverage_analyzer, defect_manager, notify_manager,
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

        self.max_build_workers = max_build_workers
        self.max_case_workers = max_case_workers
        self.tick_seconds = tick_seconds

        self._build_pool = ThreadPoolExecutor(
            max_workers=max_build_workers, thread_name_prefix="build")
        self._running: dict[str, dict] = {}
        # 流水线运行 id -> {"cancel": Event, "project_id", "current_stage"}
        self._running_pipelines: dict[str, dict] = {}
        self._running_lock = threading.Lock()

        self._stop_event = threading.Event()
        self._tick_thread: Optional[threading.Thread] = None
        self._scan_lock = threading.Lock()

    # ------------------------------------------------------------------ 启动
    def start(self) -> None:
        if self._tick_thread is None:
            self._tick_thread = threading.Thread(
                target=self._tick_loop, name="scheduler-tick", daemon=True)
            self._tick_thread.start()

    def shutdown(self) -> None:
        self._stop_event.set()
        self._build_pool.shutdown(wait=False, cancel_futures=True)
        # 让已提交但尚未开始的流水线也能尽快感知取消
        for handle in list(self._running_pipelines.values()):
            handle["cancel"].set()

    # ------------------------------------------------------------------ 触发
    def submit_build(self, project_id: str, suite_id: str,
                     env_id: Optional[str] = None, trigger: str = "manual",
                     schedule_id: Optional[str] = None) -> dict:
        """提交一场构建，立即返回构建元信息（构建在后台线程池运行）。"""
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

        # 若是定时触发，记录一次计划运行历史
        if schedule_id:
            self._record_schedule_run(schedule_id, project_id, build_id)

        return build

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
                    "suite_id": handle["suite_id"],
                    "status": build.get("status") if build else "running",
                    "total": build.get("total", 0) if build else 0,
                    "passed": build.get("passed", 0) if build else 0,
                    "failed": build.get("failed", 0) if build else 0,
                    "started_at": build.get("started_at") if build else None,
                })
        return out

    # ------------------------------------------------------------------ 流水线
    def submit_pipeline(self, pipeline: dict, trigger: str = "manual",
                        env_overrides: Optional[dict] = None) -> dict:
        """提交一次流水线运行，立即返回运行记录（在后台构建池中串行推进）。

        :param pipeline: 流水线定义（含 project_id 与有序 stages）
        :param env_overrides: {stage_id: env_id}，触发时临时替换阶段环境
        """
        project_id = pipeline["project_id"]
        stages = pipeline.get("stages") or []
        if not stages:
            return {"error": "流水线还没有配置任何阶段"}
        if self.registry.store("projects").get(project_id) is None:
            return {"error": "项目不存在"}

        run_id = new_id("run")
        now = time.time()
        run_stages = []
        for idx, stage in enumerate(stages):
            env_id = (env_overrides or {}).get(stage["id"]) or stage.get("env_id")
            run_stages.append({
                "id": stage["id"],
                "name": stage.get("name", f"阶段 {idx + 1}"),
                "type": stage.get("type", "suite"),
                "env_id": env_id,
                "suite_id": stage.get("suite_id"),
                "checks": stage.get("checks") or [],
                "on_failure": stage.get("on_failure", "abort"),
                "order": idx,
                "status": "pending",
                "build_id": None,
                "started_at": None,
                "finished_at": None,
                "duration": 0.0,
                "passed": 0, "failed": 0, "total": 0,
                "message": "",
            })
        run = {
            "id": run_id,
            "pipeline_id": pipeline["id"],
            "pipeline_name": pipeline.get("name", ""),
            "project_id": project_id,
            "trigger": trigger,
            "status": "pending",
            "stages": run_stages,
            "current_stage": None,
            "started_at": None,
            "finished_at": None,
            "duration": 0.0,
            "passed_stages": 0,
            "failed_stages": 0,
            "skipped_stages": 0,
            "logs": [],
            "created_at": now,
        }
        self.registry.store("pipeline_runs").insert(run)

        cancel_event = threading.Event()
        with self._running_lock:
            self._running_pipelines[run_id] = {
                "cancel": cancel_event, "project_id": project_id,
                "current_stage": None, "stage_event": None,
            }
        self._build_pool.submit(self._run_pipeline, run_id, project_id, cancel_event)
        return run

    def cancel_pipeline(self, run_id: str) -> dict:
        with self._running_lock:
            handle = self._running_pipelines.get(run_id)
            if handle is None:
                return {"error": "流水线不在运行中或不存在"}
            handle["cancel"].set()
            # 若当前正卡在某个阶段构建里，一并置位它的取消事件，
            # 让用例执行（含分片睡眠）能尽快停下。
            stage_event = handle.get("stage_event")
            if stage_event is not None:
                stage_event.set()
        return {"ok": True, "run_id": run_id}

    def running_pipelines(self) -> list[dict]:
        runs_store = self.registry.store("pipeline_runs")
        with self._running_lock:
            ids = list(self._running_pipelines.keys())
        out = []
        for run_id in ids:
            run = runs_store.get(run_id)
            if run:
                out.append({
                    "run_id": run_id,
                    "project_id": run.get("project_id"),
                    "pipeline_name": run.get("pipeline_name"),
                    "status": run.get("status"),
                    "current_stage": run.get("current_stage"),
                })
        return out

    def _update_run(self, run_id: str, patch: dict, log: Optional[str] = None,
                    run: Optional[dict] = None) -> dict:
        """更新流水线运行记录（分片存储，锁内读-改-写）。

        传入 ``run`` 时直接以内存中的最新对象为准（阶段状态刚在它上面改过，
        若再从磁盘重读会把这些改动覆盖掉，导致阶段永远停在 pending）。
        """
        store = self.registry.store("pipeline_runs")
        run = run if run is not None else store.get(run_id)
        if run is None:
            return {}
        run.update(patch)
        if log is not None:
            line = f"[{time.strftime('%H:%M:%S')}] {log}"
            run["logs"] = (run.get("logs") or []) + [line]
            run["logs"] = run["logs"][-500:]  # 控制日志体积
        return store.update(run_id, run)

    @staticmethod
    def _update_stage(run: dict, stage_id: str, patch: dict) -> dict:
        for stage in run.get("stages", []):
            if stage["id"] == stage_id:
                stage.update(patch)
                return stage
        return {}

    def _run_pipeline(self, run_id: str, project_id: str,
                      cancel_event: threading.Event) -> None:
        """串行推进一条流水线：前一阶段通过（或失败策略为 continue）才进入下一阶段。"""
        runs_store = self.registry.store("pipeline_runs")
        build_store = self.builds.for_project(project_id)
        started = time.time()
        self._update_run(run_id, {"status": "running", "started_at": started},
                         log=f"流水线 {run_id} 开始")

        # 闸门：上一阶段是否放行。abort 阶段失败会把它置 False，后续阶段跳过。
        gate_open = True
        try:
            while True:
                run = runs_store.get(run_id)
                if run is None:
                    return
                stages = run["stages"]
                current = next((s for s in stages if s["status"] == "running"), None)
                if current is None:
                    current = next((s for s in stages if s["status"] == "pending"), None)
                if current is None:
                    break  # 全部阶段都已落终态

                if cancel_event.is_set():
                    self._update_stage(run, current["id"], {
                        "status": "cancelled", "message": "流水线被取消",
                        "finished_at": time.time(),
                    })
                    self._update_run(run_id, {"stages": run["stages"]},
                                     log=f"阶段「{current['name']}」取消", run=run)
                    continue

                if not gate_open:
                    self._update_stage(run, current["id"], {
                        "status": "skipped", "message": "前序阶段失败且策略为直接中止",
                        "finished_at": time.time(),
                    })
                    self._update_run(run_id, {"stages": run["stages"]},
                                     log=f"阶段「{current['name']}」跳过（闸门关闭）",
                                     run=run)
                    continue

                self._update_stage(run, current["id"], {"status": "running",
                                                        "started_at": time.time()})
                self._update_run(run_id, {
                    "stages": run["stages"], "current_stage": current["id"],
                }, log=f"进入阶段「{current['name']}」（{current['type']}）", run=run)

                ok, message = self._run_pipeline_stage(
                    run_id, project_id, current, build_store, cancel_event)

                run = runs_store.get(run_id)
                # 取消优先：阶段即便有失败结果，也以 cancelled 落账
                stage_started = current.get("started_at") or time.time()
                stage_status = "cancelled" if cancel_event.is_set() else \
                    ("passed" if ok else "failed")
                stage = self._update_stage(run, current["id"], {
                    "status": stage_status,
                    "finished_at": time.time(),
                    "duration": round(time.time() - stage_started, 3),
                    "message": ("流水线被取消" if stage_status == "cancelled" else message),
                })
                # 回填该阶段构建的聚合计数
                if stage.get("build_id"):
                    b = build_store.get(stage["build_id"])
                    if b:
                        stage.update(total=b.get("total", 0), passed=b.get("passed", 0),
                                     failed=(b.get("failed", 0) + b.get("error", 0)
                                             + b.get("timeout", 0)))
                gate_open = ok or current.get("on_failure") == "continue"
                self._update_run(run_id, {"stages": run["stages"]},
                                 log=f"阶段「{current['name']}」"
                                     f"{'通过' if ok else '失败'}：{message}",
                                 run=run)
        except Exception as exc:  # noqa: BLE001
            self._update_run(run_id, {}, log=f"流水线执行异常: {exc}")

        # 终态聚合
        run = runs_store.get(run_id)
        if cancel_event.is_set():
            final_status = "cancelled"
        else:
            stages = run["stages"]
            hard_fail = any(s["status"] == "failed" and s.get("on_failure") == "abort"
                            for s in stages)
            soft_fail = any(s["status"] == "failed" for s in stages)
            if hard_fail or any(s["status"] == "cancelled" for s in stages):
                final_status = "failed" if soft_fail else "cancelled"
            elif soft_fail:
                final_status = "failed"  # continue 的失败仍要在结果上标记
            else:
                final_status = "passed"
        finished = time.time()
        self._update_run(run_id, {
            "status": final_status,
            "finished_at": finished,
            "duration": round(finished - started, 3),
            "current_stage": None,
            "passed_stages": sum(1 for s in run["stages"] if s["status"] == "passed"),
            "failed_stages": sum(1 for s in run["stages"] if s["status"] == "failed"),
            "skipped_stages": sum(1 for s in run["stages"] if s["status"] == "skipped"),
        }, log=f"流水线结束: {final_status}，总耗时 {round(finished - started, 2)}s")

        self._fire_pipeline_notify(project_id, run_id, final_status)

        with self._running_lock:
            self._running_pipelines.pop(run_id, None)

    def _run_pipeline_stage(self, run_id: str, project_id: str, stage: dict,
                            build_store, cancel_event: threading.Event
                            ) -> tuple[bool, str]:
        """执行单个阶段，返回 (是否通过, 说明)。

        static / suite 阶段都落成一场真实构建（监控页可看日志与明细）；
        report 阶段只做汇总，不产生构建。
        """
        stype = stage.get("type", "suite")

        if stype == "report":
            ok, msg = self._run_report_stage(run_id, project_id, build_store)
            return ok, msg

        # 解析环境
        env_id = stage.get("env_id")
        if not env_id:
            envs = self.env_manager.list(project_id)
            if not envs:
                return False, "项目没有可用环境"
            env_id = envs[0]["id"]
        if self.env_manager.get(env_id) is None:
            return False, f"环境不存在: {env_id}"

        # 解析本阶段要跑的用例
        if stype == "static":
            cases = self._static_stage_cases(project_id, stage, run_id)
            if not cases:
                return False, "项目内没有可供静态检查的内容"
            kind = "static"
            build_name = f"[静态检查] {stage.get('name')}"
            suite_id = None
        else:
            suite = self.registry.store("suites").get(stage.get("suite_id"))
            if suite is None:
                return False, "绑定的套件不存在或已被删除"
            cases = self.registry.store("cases").get_many(suite.get("case_ids") or [])
            if not cases:
                return False, "套件内没有用例"
            kind = "suite"
            build_name = f"[{stage.get('name')}] {suite.get('name', '')}"
            suite_id = suite["id"]

        build_id = new_id("build")
        build_store.create(
            build_id, suite_id=suite_id, env_id=env_id, name=build_name,
            trigger="pipeline", kind=kind,
            pipeline_run_id=run_id, pipeline_stage_id=stage["id"],
        )
        # 阶段记录挂上构建 id，前端可跳监控页看明细
        self._link_stage_build(run_id, stage["id"], build_id)

        # 阶段构建与流水线共用同一个取消事件：取消流水线时，
        # 正在跑的阶段用例会在下一个步骤 / 睡眠分片立即停下。
        with self._running_lock:
            self._running[build_id] = {
                "cancel": cancel_event, "project_id": project_id,
                "suite_id": suite_id,
            }
            self._running_pipelines[run_id]["stage_event"] = cancel_event
            self._running_pipelines[run_id]["current_stage"] = build_id
        try:
            env_config = self.env_manager.to_executor_config(env_id)
            build_store.set_total(build_id, len(cases))
            build_store.append_log(
                build_id,
                f"流水线阶段 {stage.get('name')} 开始：{len(cases)} 个检查/用例，环境 {env_id}")
            self._execute_case_batch(build_store, build_id, cases, env_config,
                                     env_id, cancel_event)
            status = self._terminal_status(build_store, build_id, cancel_event)
            build_store.finish(build_id, status)
            # 阶段构建仍生成报告 / 覆盖率，但不单独发通知、不自动建缺陷，
            # 避免一条流水线刷出十几条通知；统一在流水线结束时通知一次。
            self._finalize(project_id, build_id, notify=False, auto_defects=False)
        finally:
            with self._running_lock:
                self._running.pop(build_id, None)
                self._running_pipelines[run_id]["current_stage"] = None
                self._running_pipelines[run_id]["stage_event"] = None

        build = build_store.get(build_id)
        if status == "cancelled":
            return False, "阶段被取消"
        passed, total = build.get("passed", 0), build.get("total", 0)
        failed = build.get("failed", 0) + build.get("error", 0) + build.get("timeout", 0)
        if status == "passed":
            return True, f"全部通过（{passed}/{total}），耗时 {build.get('duration', 0)}s"
        return False, f"{failed} 项失败（通过 {passed}/{total}），耗时 {build.get('duration', 0)}s"

    def _link_stage_build(self, run_id: str, stage_id: str, build_id: str) -> None:
        store = self.registry.store("pipeline_runs")
        run = store.get(run_id)
        self._update_stage(run, stage_id, {"build_id": build_id})
        store.update(run_id, {"stages": run["stages"]})

    def _static_stage_cases(self, project_id: str, stage: dict,
                            run_id: str) -> list[dict]:
        cases = self.registry.store("cases").query(
            where=[("project_id", "eq", project_id)])
        suites = self.registry.store("suites").query(
            where=[("project_id", "eq", project_id)])
        # 环境管理器按项目列出，保证多项目之间检查不串台
        environments = self.env_manager.list(project_id)
        return build_static_cases(cases, suites, environments,
                                  selected=stage.get("checks") or None,
                                  id_prefix=f"static_{run_id[-6:]}")

    def _run_report_stage(self, run_id: str, project_id: str,
                          build_store) -> tuple[bool, str]:
        """报告阶段：强制刷新流水线此前各阶段构建的报告缓存。"""
        run = self.registry.store("pipeline_runs").get(run_id)
        count = 0
        if run:
            for s in run["stages"]:
                if s.get("build_id"):
                    try:
                        self.report_gen.build_report(
                            project_id, s["build_id"], force=True)
                        count += 1
                    except Exception:  # noqa: BLE001
                        pass
        return True, f"已汇总生成 {count} 份阶段报告"

    def _fire_pipeline_notify(self, project_id: str, run_id: str,
                              status: str) -> None:
        run = self.registry.store("pipeline_runs").get(run_id)
        if run is None:
            return
        payload = {
            "run_id": run_id,
            "pipeline_id": run.get("pipeline_id"),
            "pipeline_name": run.get("pipeline_name"),
            "project_id": project_id,
            "status": status,
            "passed_stages": run.get("passed_stages", 0),
            "failed_stages": run.get("failed_stages", 0),
            "skipped_stages": run.get("skipped_stages", 0),
            "duration": run.get("duration", 0.0),
        }
        self.notify.fire(project_id, "pipeline.finished", payload)
        self.notify.fire(project_id,
                         "pipeline.passed" if status == "passed" else "pipeline.failed",
                         payload)

    # ------------------------------------------------------------------ 构建执行
    def _run_build(self, project_id: str, build_id: str, cases: list,
                   env_id: str, cancel_event: threading.Event,
                   notify: bool = True) -> None:
        """执行一场普通构建（用例池并发 + 收尾）。

        流水线阶段构建走 :meth:`_execute_case_batch`，因为它要在「阶段之间」
        插入闸门逻辑，不能在每个用例批次结束后就做整体收尾。
        """
        store = self.builds.for_project(project_id)
        env_config = self.env_manager.to_executor_config(env_id)
        store.set_total(build_id, len(cases))
        store.append_log(build_id, f"构建 {build_id} 开始，共 {len(cases)} 个用例，"
                                   f"环境 {env_id}")

        try:
            self._execute_case_batch(store, build_id, cases, env_config,
                                     env_id, cancel_event)
        except Exception as exc:  # noqa: BLE001
            store.append_log(build_id, f"构建执行异常: {exc}")

        # 终态判定
        status = self._terminal_status(store, build_id, cancel_event)
        store.finish(build_id, status)
        build = store.get(build_id)
        store.append_log(build_id, f"构建结束: {status}（通过 {build.get('passed', 0)}"
                                   f"/{build.get('total', 0)}）")

        # 收尾：报告 + 覆盖率 + 通知 + 自动缺陷
        self._finalize(project_id, build_id, notify=notify)

        with self._running_lock:
            self._running.pop(build_id, None)

    @staticmethod
    def _terminal_status(store, build_id: str,
                         cancel_event: threading.Event) -> str:
        build = store.get(build_id)
        if cancel_event.is_set():
            return "cancelled"
        if (build.get("failed", 0) + build.get("error", 0) + build.get("timeout", 0)) == 0:
            return "passed"
        return "failed"

    def _execute_case_batch(self, store, build_id: str, cases: list,
                            env_config: dict, env_id: str,
                            cancel_event: threading.Event) -> None:
        """并发执行一批用例并把结果落盘。普通构建与流水线阶段共用。

        取消时未提交的用例统一记为 skipped。这里**不**做终态判定与收尾，
        交由调用方决定（流水线还要继续下一阶段）。
        """
        case_workers = max(1, min(self.max_case_workers, len(cases)))
        with ThreadPoolExecutor(max_workers=case_workers,
                                thread_name_prefix=f"case-{build_id[:6]}") as pool:
            futures = {}
            for i, case in enumerate(cases):
                if cancel_event.is_set():
                    break
                futures[pool.submit(
                    self._run_one, case, env_config, env_id, i,
                    cancel_event)] = case

            for future in as_completed(futures):
                case = futures[future]
                try:
                    result = future.result()
                except Exception as exc:  # noqa: BLE001
                    result = {
                        "case_id": case.get("id"),
                        "case_name": case.get("name", "未命名用例"),
                        "group": (case.get("tags") or ["默认"])[0],
                        "priority": case.get("priority", "P3"),
                        "status": "error",
                        "duration": 0.0,
                        "steps": [],
                        "assertions": [],
                        "logs": [f"用例执行异常: {exc}"],
                    }
                self._persist_result(store, build_id, case, result)
            # 未提交的用例（被取消跳过）记为 skipped
            submitted = {case.get("id") for case in futures.values()}
            for case in cases:
                if case.get("id") not in submitted:
                    skipped = {
                        "case_id": case.get("id"),
                        "case_name": case.get("name", "未命名用例"),
                        "group": (case.get("tags") or ["默认"])[0],
                        "priority": case.get("priority", "P3"),
                        "status": "skipped",
                        "duration": 0.0,
                        "steps": [], "assertions": [],
                        "logs": ["因取消而未执行"],
                    }
                    self._persist_result(store, build_id, case, skipped)

    def _run_one(self, case: dict, env_config: dict, env_id: str,
                 index: int, cancel_event: threading.Event) -> dict:
        result = self.executor.execute_case(
            case, env_config, cancel_event=cancel_event,
            timeout=case.get("timeout", 60))
        result["env_id"] = env_id
        result["order"] = index
        return result

    def _persist_result(self, store, build_id: str, case: dict, result: dict) -> None:
        store.record_result(build_id, result)
        case_id = case.get("id")
        if case_id:
            log_text = "\n".join(result.get("logs", []))
            store.write_case_log(build_id, case_id, log_text)

    def _finalize(self, project_id: str, build_id: str, notify: bool = True,
                  auto_defects: bool = True) -> None:
        store = self.builds.for_project(project_id)
        build = store.get(build_id)
        if build is None:
            return
        total = build.get("total", 0)
        passed = build.get("passed", 0)
        passed_ratio = (passed / total) if total else 1.0

        try:
            self.report_gen.build_report(project_id, build_id, force=True)
        except Exception:  # noqa: BLE001
            pass
        try:
            self.coverage.generate(project_id, build_id, passed_ratio)
        except Exception:  # noqa: BLE001
            pass

        if notify:
            # 通知
            event = "build.passed" if build["status"] == "passed" else "build.failed"
            payload = {
                "build_id": build_id,
                "project_id": project_id,
                "status": build["status"],
                "passed": passed,
                "total": total,
                "pass_rate": round(passed_ratio * 100, 1),
                "duration": build.get("duration", 0.0),
            }
            self.notify.fire(project_id, "build.finished", payload)
            self.notify.fire(project_id, event, payload)

        # 自动缺陷（项目配置开启时，把失败用例转成缺陷）
        if auto_defects:
            project = self.registry.store("projects").get(project_id)
            if project and project.get("auto_create_defects"):
                failures = store.results(build_id, where=[("status", "in", ["failed", "error", "timeout"])])
                for fr in failures[:20]:
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
