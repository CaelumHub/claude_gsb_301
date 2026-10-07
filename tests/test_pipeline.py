"""流水线（阶段化验证）测试。

覆盖：
- 静态检查器对各类「必崩配置」的识别（禁用用例 / 空步骤 / 危险脚本 /
  未定义变量 / 空套件 / 悬挂引用 / 环境配置）；
- 流水线阶段严格串行、前阶段通过才进入下一阶段；
- 失败策略 abort（后续阶段 skipped，流水线 failed）；
- 失败策略 continue（后续阶段照常执行，流水线仍标记 failed）；
- 多环境编排：每个阶段可绑定不同环境；
- 报告阶段聚合；取消流水线。
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import (CoverageAnalyzer, DefectManager, EnvironmentManager,
                    NotificationManager, ReportGenerator, Scheduler, TestExecutor)
from engine.staticcheck import build_static_cases
from storage import BuildStoreRegistry, StoreRegistry


def _make_scheduler(data_root):
    registry = StoreRegistry(os.path.join(data_root, "store"), shard_size=50)
    builds = BuildStoreRegistry(os.path.join(data_root, "builds"))
    executor = TestExecutor()
    env_mgr = EnvironmentManager(registry, data_root)
    coverage = CoverageAnalyzer(builds)
    report = ReportGenerator(builds)
    defects = DefectManager(registry)
    notify = NotificationManager(registry)
    sched = Scheduler(registry, builds, executor, env_mgr, report, coverage,
                      defects, notify, max_build_workers=2, max_case_workers=4,
                      tick_seconds=60)
    return registry, builds, env_mgr, sched


def _pass_case(name, tag, pid=None):
    return {
        "project_id": pid, "name": name, "priority": "P1", "tags": [tag],
        "timeout": 30, "enabled": True,
        "steps": [
            {"action": "request", "method": "GET", "url": "/api/health"},
            {"action": "assert", "type": "status", "actual": "${resp.status}",
             "expected": 200},
        ],
    }


def _wait_run(sched, run_id, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = sched.registry.store("pipeline_runs").get(run_id)
        if run and run["status"] not in ("pending", "running"):
            return run
        time.sleep(0.03)
    raise AssertionError("流水线运行超时未结束")


class TestStaticChecker(unittest.TestCase):
    def test_clean_project_all_pass(self):
        cases = [{"id": "c1", **_pass_case("健康检查", "api")}]
        suites = [{"id": "s1", "name": "套件", "case_ids": ["c1"]}]
        envs = [{"id": "e1", "name": "dev",
                 "config": {"base_url": "http://x", "fail_rate": 0.0},
                 "dependencies": [{"name": "requests", "constraint": ">=2.28"}]}]
        synth = build_static_cases(cases, suites, envs)
        self.assertTrue(synth)
        # 全部合成用例都应是「通过」形态（首步 truthy 断言）
        self.assertTrue(all(c["steps"][0]["action"] == "assert" and
                            c["steps"][0]["type"] == "truthy" for c in synth))

    def test_detects_disabled_empty_dangerous_and_bad_var(self):
        cases = [
            {"id": "c1", "name": "禁用", "enabled": False, "timeout": 30, "steps": []},
            {"id": "c2", "name": "空步骤", "enabled": True, "timeout": 30, "steps": []},
            {"id": "c3", "name": "危险脚本", "enabled": True, "timeout": 30,
             "steps": [{"action": "script", "expr": "__import__('os')"}]},
            {"id": "c4", "name": "未定义变量", "enabled": True, "timeout": 30,
             "steps": [{"action": "assert", "type": "equals",
                        "actual": "${undefined_var}", "expected": 1}]},
            {"id": "c5", "name": "超时非法", "enabled": True, "timeout": 99999,
             "steps": [{"action": "assert", "type": "truthy", "actual": True}]},
        ]
        synth = build_static_cases(cases, [], [])
        failing = [c for c in synth
                   if not (c["steps"][0]["action"] == "assert"
                           and c["steps"][0]["type"] == "truthy")]
        messages = " ".join(c["name"] for c in failing)
        for needle in ("禁用", "空步骤", "危险脚本", "未定义变量", "超时配置非法"):
            self.assertIn(needle, messages)

    def test_suite_and_env_checks(self):
        cases = [{"id": "c1", **_pass_case("正常", "api")}]
        suites = [
            {"id": "s1", "name": "空套件", "case_ids": []},
            {"id": "s2", "name": "悬挂套件", "case_ids": ["ghost"]},
        ]
        envs = [{"id": "e1", "name": "坏环境",
                 "config": {"base_url": "", "fail_rate": 5},
                 "dependencies": [{"name": "requests", "constraint": ""}]}]
        synth = build_static_cases(cases, suites, envs)
        messages = " ".join(c["name"] for c in synth)
        self.assertIn("为空", messages)
        self.assertIn("已删除用例引用", messages)
        self.assertIn("缺少 base_url", messages)


class TestPipelineExecution(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.registry, self.builds, self.env_mgr, self.sched = _make_scheduler(self.tmp.name)
        self.pid = self._project_with_cases()

    def tearDown(self):
        self.sched.shutdown()
        self.tmp.cleanup()

    def _project_with_cases(self):
        pid = self.registry.store("projects").insert({"name": "P"})
        self.env_ok = self.env_mgr.create(pid, {
            "name": "dev", "config": {"base_url": "http://dev.mock.local",
                                      "latency_ms": 0, "fail_rate": 0.0}})
        self.env_stage = self.env_mgr.create(pid, {
            "name": "staging", "config": {"base_url": "http://st.mock.local",
                                          "latency_ms": 0, "fail_rate": 0.0}})
        cs = self.registry.store("cases")
        self.case_ok = cs.insert({"id": "case_ok", **_pass_case("正常用例", "api", pid)})
        # 请求 /api/error 必返回 500 的失败用例
        self.case_bad = cs.insert({
            "id": "case_bad", "project_id": pid, "name": "失败用例", "priority": "P1",
            "tags": ["api"], "timeout": 30, "enabled": True,
            "steps": [
                {"action": "request", "method": "GET", "url": "/api/error"},
                {"action": "assert", "type": "status", "actual": "${resp.status}",
                 "expected": 200},
            ],
        })
        self.suite_ok = self.registry.store("suites").insert({
            "id": "suite_ok", "project_id": pid, "name": "通过集",
            "env_id": self.env_ok["id"], "case_ids": [self.case_ok]})
        self.suite_bad = self.registry.store("suites").insert({
            "id": "suite_bad", "project_id": pid, "name": "失败集",
            "env_id": self.env_ok["id"], "case_ids": [self.case_bad]})
        return pid

    def _pipeline(self, stages):
        return {
            "id": "pipe_test", "project_id": self.pid, "name": "测试流水线",
            "stages": stages,
        }

    def test_happy_path_stages_pass_in_order(self):
        pipe = self._pipeline([
            {"id": "st_static", "name": "静态检查", "type": "static",
             "env_id": self.env_ok["id"], "on_failure": "abort", "checks": []},
            {"id": "st_unit", "name": "单元测试", "type": "suite",
             "suite_id": self.suite_ok, "env_id": self.env_ok["id"],
             "on_failure": "abort"},
            {"id": "st_report", "name": "报告", "type": "report",
             "on_failure": "continue"},
        ])
        run = self.sched.submit_pipeline(pipe)
        final = _wait_run(self.sched, run["id"])

        self.assertEqual(final["status"], "passed")
        statuses = [s["status"] for s in final["stages"]]
        self.assertEqual(statuses, ["passed", "passed", "passed"])
        # 前两个阶段产生了真实构建，报告阶段没有构建
        self.assertTrue(final["stages"][0]["build_id"])
        self.assertTrue(final["stages"][1]["build_id"])
        self.assertIsNone(final["stages"][2]["build_id"])
        # 每个执行阶段都有耗时
        for st in final["stages"]:
            self.assertGreaterEqual(st["duration"], 0.0)
        # 阶段构建属于 static / suite 两种 kind，且带流水线关联字段
        b1 = self.builds.for_project(self.pid).get(final["stages"][0]["build_id"])
        self.assertEqual(b1["kind"], "static")
        self.assertEqual(b1["pipeline_run_id"], run["id"])
        b2 = self.builds.for_project(self.pid).get(final["stages"][1]["build_id"])
        self.assertEqual(b2["kind"], "suite")
        self.assertEqual(b2["passed"], 1)

    def test_abort_policy_skips_later_stages(self):
        pipe = self._pipeline([
            {"id": "st1", "name": "失败阶段", "type": "suite",
             "suite_id": self.suite_bad, "env_id": self.env_ok["id"],
             "on_failure": "abort"},
            {"id": "st2", "name": "不该跑", "type": "suite",
             "suite_id": self.suite_ok, "env_id": self.env_ok["id"],
             "on_failure": "abort"},
            {"id": "st3", "name": "报告", "type": "report",
             "on_failure": "continue"},
        ])
        run = self.sched.submit_pipeline(pipe)
        final = _wait_run(self.sched, run["id"])

        self.assertEqual(final["status"], "failed")
        self.assertEqual([s["status"] for s in final["stages"]],
                         ["failed", "skipped", "skipped"])
        self.assertEqual(final["failed_stages"], 1)
        self.assertEqual(final["skipped_stages"], 2)
        # 被闸门跳过的阶段不应产生构建
        self.assertIsNone(final["stages"][1]["build_id"])

    def test_continue_policy_runs_all_and_marks_failed(self):
        pipe = self._pipeline([
            {"id": "st1", "name": "失败但继续", "type": "suite",
             "suite_id": self.suite_bad, "env_id": self.env_ok["id"],
             "on_failure": "continue"},
            {"id": "st2", "name": "照样执行", "type": "suite",
             "suite_id": self.suite_ok, "env_id": self.env_stage["id"],
             "on_failure": "abort"},
        ])
        run = self.sched.submit_pipeline(pipe)
        final = _wait_run(self.sched, run["id"])

        # 两阶段都真实执行；一败一成，流水线整体仍标记失败
        self.assertEqual([s["status"] for s in final["stages"]],
                         ["failed", "passed"])
        self.assertEqual(final["status"], "failed")
        # 第二阶段确实跑在 staging 环境上
        b2 = self.builds.for_project(self.pid).get(final["stages"][1]["build_id"])
        self.assertEqual(b2["env_id"], self.env_stage["id"])

    def test_static_failure_aborts_before_any_test(self):
        # 放一个禁用用例到通过套件里，静态检查必挂
        self.registry.store("cases").insert({
            "id": "case_disabled", "project_id": self.pid, "name": "禁用用例",
            "enabled": False, "timeout": 30, "tags": ["api"],
            "steps": _pass_case("x", "api")["steps"],
        })
        pipe = self._pipeline([
            {"id": "st_static", "name": "静态检查", "type": "static",
             "env_id": self.env_ok["id"], "on_failure": "abort", "checks": []},
            {"id": "st_unit", "name": "单元测试", "type": "suite",
             "suite_id": self.suite_ok, "env_id": self.env_ok["id"],
             "on_failure": "abort"},
        ])
        run = self.sched.submit_pipeline(pipe)
        final = _wait_run(self.sched, run["id"])
        self.assertEqual(final["stages"][0]["status"], "failed")
        self.assertEqual(final["stages"][1]["status"], "skipped")

    def test_cancel_pipeline(self):
        # 用 30 个用例 + 稍高延迟保证流水线还在第一个阶段时就能取消
        self.env_mgr.update(self.env_ok["id"],
                            {"config": {"latency_ms": 40, "fail_rate": 0.0}})
        cs = self.registry.store("cases")
        ids = [cs.insert({
            "id": f"case_slow_{i}", **_pass_case(f"慢用例{i}", "api", self.pid)})
            for i in range(30)]
        suite = self.registry.store("suites").insert({
            "id": "suite_slow", "project_id": self.pid, "name": "慢集",
            "env_id": self.env_ok["id"], "case_ids": ids})
        pipe = self._pipeline([
            {"id": "st1", "name": "慢阶段", "type": "suite",
             "suite_id": suite, "env_id": self.env_ok["id"],
             "on_failure": "abort"},
            {"id": "st2", "name": "后续", "type": "suite",
             "suite_id": self.suite_ok, "env_id": self.env_ok["id"],
             "on_failure": "abort"},
        ])
        run = self.sched.submit_pipeline(pipe)
        time.sleep(0.15)
        result = self.sched.cancel_pipeline(run["id"])
        self.assertEqual(result.get("ok"), True)
        final = _wait_run(self.sched, run["id"])
        self.assertIn(final["status"], ("cancelled", "failed"))
        self.assertEqual(final["stages"][1]["status"], "cancelled")

    def test_empty_pipeline_rejected(self):
        result = self.sched.submit_pipeline(
            {"id": "p", "project_id": self.pid, "name": "空", "stages": []})
        self.assertIn("error", result)


if __name__ == "__main__":
    unittest.main()
