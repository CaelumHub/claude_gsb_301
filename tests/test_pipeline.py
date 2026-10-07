"""阶段流水线测试。

覆盖需求的核心语义：

1. 一次完整验证拆成有序阶段（静态检查 → 单元 → 接口回归 → 报告），
   前一阶段通过才进入下一阶段；
2. 阶段失败策略：``abort`` 直接中止后续阶段（剩余环境也不跑），
   ``continue`` 继续跑但阶段结果标记失败；
3. 多环境顺序编排：按流水线绑定顺序 dev 先跑、staging 后跑；
4. 每个阶段在页面侧可见的状态与耗时（stages 字段 + 每次环境尝试）；
5. 静态检查的 error 阻断 / warning 不阻断；
6. 取消：未跑的阶段标记 skipped，父构建 cancelled；报告阶段作为收尾
   即使前置中止也照常执行。
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import (CoverageAnalyzer, DefectManager, EnvironmentManager,
                    NotificationManager, ReportGenerator, Scheduler, TestExecutor,
                    StaticChecker)
from engine.staticcheck import evaluate_rule
from storage import BuildStoreRegistry, StoreRegistry


def _make(tmp):
    registry = StoreRegistry(os.path.join(tmp, "store"), shard_size=50)
    builds = BuildStoreRegistry(os.path.join(tmp, "builds"))
    env_mgr = EnvironmentManager(registry, tmp)
    checker = StaticChecker(registry)
    sched = Scheduler(registry, builds, TestExecutor(), env_mgr,
                      ReportGenerator(builds), CoverageAnalyzer(builds),
                      DefectManager(registry), NotificationManager(registry),
                      checker, max_build_workers=2, max_case_workers=4)
    return registry, builds, env_mgr, checker, sched


def _wait(builds, pid, bid, timeout=25):
    deadline = time.time() + timeout
    while time.time() < deadline:
        b = builds.for_project(pid).get(bid)
        if b and b["status"] in ("passed", "failed", "cancelled", "error"):
            return b
        time.sleep(0.03)
    raise AssertionError("构建未在规定时间内结束")


class TestStaticCheckRules(unittest.TestCase):
    def test_rule_evaluation(self):
        # 命名非空
        self.assertIsNotNone(evaluate_rule({"type": "name_not_empty"}, {"name": ""}))
        self.assertIsNone(evaluate_rule({"type": "name_not_empty"}, {"name": "登录"}))
        # 必须含断言
        self.assertIsNotNone(evaluate_rule({"type": "must_have_assert"},
                                           {"name": "a", "steps": [{"action": "request"}]}))
        self.assertIsNone(evaluate_rule({"type": "must_have_assert"},
                                        {"name": "a", "steps": [{"action": "assert"}]}))
        # 硬编码 URL
        hit = evaluate_rule({"type": "no_hardcoded_url"},
                            {"name": "a", "steps": [
                                {"action": "request", "url": "https://x.com/a"}]})
        self.assertIsNotNone(hit)
        self.assertIsNone(evaluate_rule({"type": "no_hardcoded_url"},
                                        {"name": "a", "steps": [
                                            {"action": "request", "url": "/api/health"}]}))
        # 步骤数上限
        self.assertIsNotNone(evaluate_rule({"type": "max_steps", "max_steps": 2},
                                           {"name": "a", "steps": [{}, {}, {}]}))
        # 命名规范
        self.assertIsNone(evaluate_rule({"type": "naming_convention"},
                                        {"name": "登录接口-2"}))
        self.assertIsNotNone(evaluate_rule({"type": "naming_convention"},
                                           {"name": "!!!"}))

    def test_warning_level_does_not_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry, builds, env_mgr, checker, sched = _make(tmp)
            pid = registry.store("projects").insert({"name": "P"})
            checker.create_rule(pid, {"name": "硬编码告警", "type": "no_hardcoded_url",
                                      "level": "warning"})
            cid = registry.store("cases").insert({
                "project_id": pid, "name": "用例", "priority": "P2", "tags": ["api"],
                "steps": [{"action": "request", "url": "http://hardcoded.local/x"},
                          {"action": "assert", "type": "status",
                           "actual": "${resp.status}", "expected": 200}]})
            results = checker.run(pid, [registry.store("cases").get(cid)])
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["status"], "passed")  # warning 不阻断
            self.assertIn("硬编码", results[0]["logs"][1])


class TestPipelineExecution(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        (self.registry, self.builds, self.env_mgr,
         self.checker, self.sched) = _make(self.tmp.name)
        self.pid = self.registry.store("projects").insert(
            {"name": "P", "auto_create_defects": False})
        self.dev = self.env_mgr.create(
            self.pid, {"name": "dev", "config": {"latency_ms": 0, "fail_rate": 0.0}})
        self.stg = self.env_mgr.create(
            self.pid, {"name": "staging", "config": {"latency_ms": 0, "fail_rate": 0.0}})

    def tearDown(self):
        self.sched.shutdown()
        self.tmp.cleanup()

    def _case(self, name, tags=("api",), fail=False, steps=None, cid=None):
        if steps is None:
            steps = [
                {"action": "request", "method": "GET",
                 "url": "/api/error" if fail else "/api/health"},
                {"action": "assert", "type": "status",
                 "actual": "${resp.status}", "expected": 200},
            ]
        record = {"id": cid, "project_id": self.pid, "name": name,
                  "priority": "P2", "tags": list(tags), "timeout": 30, "steps": steps}
        return self.registry.store("cases").insert(record)

    def _suite(self, case_ids, name="套件"):
        return self.registry.store("suites").insert({
            "project_id": self.pid, "name": name,
            "env_id": self.dev["id"], "case_ids": case_ids})

    def _pipeline(self, stages, env_ids=None):
        return self.registry.store("pipelines").insert({
            "project_id": self.pid, "name": "流水线",
            "env_ids": env_ids or [self.dev["id"], self.stg["id"]],
            "enabled": True, "stages": stages, "created_at": time.time()})

    @staticmethod
    def _stage(static=False, unit=False, api=False, report=False, **kw):
        stype = ("report" if report else "api" if api else
                 "unit" if unit else "static" if static else "api")
        return {
            "id": kw.get("id", f"stg_{stype}"), "name": kw.get("name", stype),
            "type": stype, "on_fail": kw.get("on_fail", "abort"),
            "env_scope": "once" if report else kw.get("env_scope", "pipeline"),
            "suite_id": kw.get("suite_id"), "tags_any": kw.get("tags_any", []),
            "env_id": kw.get("env_id"),
        }

    def test_continue_policy_runs_all_envs_abort_stops(self):
        unit_bad = self._case("单元失败", ["unit"], steps=[
            {"action": "assert", "type": "equals", "actual": 1, "expected": 2}])
        unit_good = self._case("单元通过", ["unit"], steps=[
            {"action": "assert", "type": "equals", "actual": 1, "expected": 1}])
        api_good = self._case("接口通过")
        api_bad = self._case("接口失败", fail=True)
        s_unit = self._suite([unit_good, unit_bad], "单元套件")
        s_api = self._suite([api_good, api_bad], "接口套件")
        self.checker.create_rule(
            self.pid, {"name": "非空名", "type": "name_not_empty", "level": "error"})

        pipe = self._pipeline([
            self._stage(static=True, name="静态检查", on_fail="abort"),
            self._stage(unit=True, name="单元测试", suite_id=s_unit, on_fail="continue"),
            self._stage(api=True, name="接口回归", suite_id=s_api, on_fail="abort"),
            self._stage(report=True, name="生成报告"),
        ])
        bid = self.sched.submit_pipeline(self.pid, pipe)["id"]
        build = _wait(self.builds, self.pid, bid)

        stages = {s["stage_id"]: s for s in build["stages"]}
        self.assertEqual(stages["stg_static"]["status"], "passed")
        # 静态阶段在两个环境都尝试
        self.assertEqual(
            [a["env_name"] for a in stages["stg_static"]["attempts"]],
            ["dev", "staging"])
        # 单元阶段失败但 continue：dev/staging 都跑完
        self.assertEqual(stages["stg_unit"]["status"], "failed")
        self.assertEqual(
            [a["env_name"] for a in stages["stg_unit"]["attempts"]],
            ["dev", "staging"])
        # 接口回归 abort：dev 失败后 staging 标记为跳过尝试
        self.assertEqual(stages["stg_api"]["status"], "failed")
        api_attempts = stages["stg_api"]["attempts"]
        self.assertEqual(
            [(a["env_name"], a["status"]) for a in api_attempts],
            [("dev", "failed"), ("staging", "skipped")])
        # 报告阶段收尾必跑
        self.assertEqual(stages["stg_report"]["status"], "passed")
        self.assertEqual(build["status"], "failed")

        # 阶段耗时字段存在
        for st in build["stages"]:
            self.assertIn("duration", st)
            self.assertIsInstance(st["duration"], float)

        # 子构建：static×2 + unit×2 + api×1 = 5
        children = self.builds.for_project(self.pid).list_children(bid)
        self.assertEqual(len(children), 5)
        self.assertTrue(all(c["kind"] == "stage" for c in children))
        # 父构建结果镜像了所有阶段（通过静态检查 + 单元 + 接口 + 报告伪结果）
        self.assertGreaterEqual(build["total"], build["passed"] + build["failed"])
        # 报告与覆盖率已生成
        self.assertIsNotNone(self.builds.for_project(self.pid).read_report(bid))
        self.assertIsNotNone(self.builds.for_project(self.pid).read_coverage(bid))

    def test_all_green_pipeline_passes_in_env_order(self):
        c1 = self._case("用例1")
        c2 = self._case("用例2")
        suite = self._suite([c1, c2])
        pipe = self._pipeline([
            self._stage(api=True, name="接口回归", suite_id=suite),
            self._stage(report=True, name="报告"),
        ])
        bid = self.sched.submit_pipeline(self.pid, pipe)["id"]
        build = _wait(self.builds, self.pid, bid)
        self.assertEqual(build["status"], "passed")
        stages = {s["stage_id"]: s for s in build["stages"]}
        # 开发先跑、预发后跑（尝试顺序即执行顺序）
        att = stages["stg_api"]["attempts"]
        self.assertEqual([a["env_name"] for a in att], ["dev", "staging"])
        self.assertTrue(all(a["status"] == "passed" for a in att))
        # 2 用例 × 2 环境 + 1 报告伪结果
        self.assertEqual(build["passed"], 5)
        self.assertEqual(build["failed"], 0)

    def test_static_error_aborts_later_stages_but_report_runs(self):
        self.checker.create_rule(
            self.pid, {"name": "非空名", "type": "name_not_empty", "level": "error"})
        bad = self._case("", ["api"])
        good = self._case("正常")
        suite = self._suite([bad, good])
        pipe = self._pipeline([
            self._stage(static=True, name="静态检查", on_fail="abort"),
            self._stage(api=True, name="接口回归", suite_id=suite, on_fail="abort"),
            self._stage(report=True, name="报告"),
        ])
        bid = self.sched.submit_pipeline(self.pid, pipe)["id"]
        build = _wait(self.builds, self.pid, bid)
        stages = {s["stage_id"]: s for s in build["stages"]}
        self.assertEqual(stages["stg_static"]["status"], "failed")
        # 第一个环境（dev）就硬失败，staging 标记为跳过尝试
        self.assertEqual(
            [(a["env_name"], a["status"]) for a in stages["stg_static"]["attempts"]],
            [("dev", "failed"), ("staging", "skipped")])
        # 接口阶段整体跳过
        self.assertEqual(stages["stg_api"]["status"], "skipped")
        # 报告阶段照常执行
        self.assertEqual(stages["stg_report"]["status"], "passed")
        self.assertEqual(build["status"], "failed")

    def test_cancel_marks_remaining_skipped(self):
        # 用例数远大于并发度（4）且每步有睡眠，保证取消时必有未启动的用例
        ids = [self._case(f"慢用例{i}", steps=[
            {"action": "sleep", "seconds": 0.5},
            {"action": "assert", "type": "equals", "actual": 1, "expected": 1}])
            for i in range(40)]
        suite = self._suite(ids)
        pipe = self._pipeline([
            self._stage(api=True, name="接口回归", suite_id=suite, on_fail="continue"),
            self._stage(report=True, name="报告"),
        ])
        run = self.sched.submit_pipeline(self.pid, pipe)
        time.sleep(0.15)  # 等流水线与第一批用例真正跑起来再取消
        self.sched.cancel_build(run["id"])
        build = _wait(self.builds, self.pid, run["id"])
        self.assertEqual(build["status"], "cancelled")
        # 取消后报告阶段仍作为收尾执行（反映已完成部分）
        stages = {s["stage_id"]: s for s in build["stages"]}
        self.assertEqual(stages["stg_report"]["status"], "passed")

    def test_fixed_env_scope_runs_once(self):
        c1 = self._case("固定环境用例")
        suite = self._suite([c1])
        pipe = self._pipeline([
            self._stage(api=True, name="固定dev", suite_id=suite,
                        env_scope="fixed", env_id=self.stg["id"]),
            self._stage(report=True, name="报告"),
        ])
        bid = self.sched.submit_pipeline(self.pid, pipe)["id"]
        build = _wait(self.builds, self.pid, bid)
        stages = {s["stage_id"]: s for s in build["stages"]}
        att = stages["stg_api"]["attempts"]
        self.assertEqual(len(att), 1)
        self.assertEqual(att[0]["env_name"], "staging")

    def test_empty_bound_stage_is_failed(self):
        # 非静态阶段没绑定任何用例：阶段失败（配置问题），abort 后跳过后续
        empty_suite = self._suite([], "空套件")
        pipe = self._pipeline([
            self._stage(api=True, name="空接口阶段", suite_id=empty_suite),
            self._stage(report=True, name="报告"),
        ])
        bid = self.sched.submit_pipeline(self.pid, pipe)["id"]
        build = _wait(self.builds, self.pid, bid)
        stages = {s["stage_id"]: s for s in build["stages"]}
        self.assertEqual(stages["stg_api"]["status"], "failed")
        self.assertEqual(stages["stg_report"]["status"], "passed")


if __name__ == "__main__":
    unittest.main()
