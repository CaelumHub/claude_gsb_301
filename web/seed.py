"""演示 / 初始数据生成。

应用启动时，若数据目录里还没有任何项目，会自动调用 :func:`seed_demo_data`
生成一份演示数据（项目 + 用例 + 套件 + 环境 + 计划 + 集成），让各个页面
一打开就有内容可点、可测。HTTP 接口 ``POST /api/seed/demo`` 也复用这里，
供前端「生成演示项目」按钮调用。
"""

from __future__ import annotations

import time

from engine import new_id


def seed_demo_data(registry, env_mgr, notify_mgr) -> dict:
    """生成演示项目，返回 ``{"project": ..., "env_id": ..., "suite_id": ...}``。"""
    proj = {
        "id": new_id("proj"),
        "name": "演示项目 · 测试与CI",
        "description": "内置示例用例、套件、环境与通知集成的演示项目。",
        "repo_url": "https://example.com/demo",
        "auto_create_defects": True,
        "created_at": time.time(),
    }
    registry.store("projects").insert(proj)
    pid = proj["id"]

    env = env_mgr.create(pid, {
        "name": "dev 开发环境",
        "python_version": "3.11",
        "base_image": "python:3.11-slim",
        "variables": {"BASE_URL": "http://dev.mock.local", "REGION": "dev"},
        "config": {"base_url": "http://dev.mock.local", "latency_ms": 15, "fail_rate": 0.0},
        "dependencies": [
            {"name": "requests", "constraint": ">=2.28"},
            {"name": "pytest", "constraint": ">=7.0"},
            {"name": "flask", "constraint": ">=3.0"},
        ],
    })
    env2 = env_mgr.create(pid, {
        "name": "staging 预发环境",
        "python_version": "3.12",
        "base_image": "python:3.12-slim",
        "variables": {"BASE_URL": "http://staging.mock.local", "REGION": "staging"},
        "config": {"base_url": "http://staging.mock.local", "latency_ms": 45, "fail_rate": 0.15},
        "dependencies": [
            {"name": "requests", "constraint": ">=2.30"},
            {"name": "django", "constraint": ">=4.2"},
            {"name": "numpy", "constraint": ">=1.24"},
        ],
    })

    def _case(name, priority, tags, steps):
        return registry.store("cases").insert({
            "id": new_id("case"),
            "project_id": pid,
            "name": name,
            "description": "演示用例",
            "priority": priority,
            "tags": tags,
            "timeout": 60,
            "enabled": True,
            "steps": steps,
            "created_at": time.time(),
        })

    c1 = _case("健康检查接口", "P0", ["smoke", "api"], [
        {"action": "request", "method": "GET", "url": "/api/health", "name": "请求健康检查"},
        {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200, "name": "状态码 200"},
        {"action": "assert", "type": "truthy", "actual": "${resp.body.ok}", "expected": True, "name": "返回 ok"},
    ])
    c2 = _case("登录接口", "P0", ["smoke", "auth"], [
        {"action": "set", "key": "user", "value": "admin", "name": "准备用户名"},
        {"action": "request", "method": "POST", "url": "/api/login", "name": "请求登录"},
        {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200, "name": "登录成功"},
        {"action": "assert", "type": "contains", "actual": "${resp.body}", "expected": "ok", "name": "返回体含 ok"},
    ])
    c3 = _case("用户列表查询", "P1", ["api", "users"], [
        {"action": "request", "method": "GET", "url": "/api/users", "name": "查询用户列表"},
        {"action": "script", "expr": "len([1,2,3])", "save_as": "count", "name": "计算数量"},
        {"action": "assert", "type": "gte", "actual": "${count}", "expected": 3, "name": "数量 >= 3"},
    ])
    c4 = _case("创建项目", "P1", ["api", "projects"], [
        {"action": "request", "method": "POST", "url": "/api/projects", "name": "创建项目"},
        {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200, "name": "状态码 200"},
    ])
    c5 = _case("慢接口（性能）", "P2", ["perf"], [
        {"action": "request", "method": "GET", "url": "/api/slow", "name": "请求慢接口"},
        {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200, "name": "状态码 200"},
    ])
    c6 = _case("失败注入接口", "P2", ["chaos"], [
        {"action": "request", "method": "GET", "url": "/api/error", "name": "请求失败接口"},
        {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200, "name": "期望 200"},
    ])
    c7 = _case("字符串断言", "P2", ["unit"], [
        {"action": "script", "expr": "2 + 3 * 4", "save_as": "result", "name": "算术"},
        {"action": "assert", "type": "equals", "actual": "${result}", "expected": 14, "name": "结果等于 14"},
        {"action": "assert", "type": "between", "actual": "${result}", "expected": [10, 20], "name": "结果在 10~20"},
    ])
    c8 = _case("正则断言", "P3", ["unit"], [
        {"action": "set", "key": "text", "value": "release-2.31.0", "name": "设置文本"},
        {"action": "assert", "type": "regex", "actual": "${text}", "expected": r"^\d+\.\d+", "name": "匹配版本号"},
    ])

    suite = {
        "id": new_id("suite"),
        "project_id": pid,
        "name": "冒烟测试套件",
        "description": "核心链路冒烟",
        "group": "smoke",
        "env_id": env["id"],
        "case_ids": [c1, c2, c3, c4, c5, c6, c7, c8],
        "created_at": time.time(),
    }
    registry.store("suites").insert(suite)

    # 单元测试套件：纯脚本/断言类用例（静态、单元阶段绑定它）
    suite_unit = {
        "id": new_id("suite"),
        "project_id": pid,
        "name": "单元测试套件",
        "description": "脚本与断言类单元用例",
        "group": "unit",
        "env_id": env["id"],
        "case_ids": [c7, c8],
        "created_at": time.time(),
    }
    registry.store("suites").insert(suite_unit)

    # -- 静态检查规则（流水线第一阶段使用） -------------------------------
    from engine.staticcheck import DEFAULT_NAME_PATTERN
    rules_store = registry.store("static_rules")

    notify_mgr.create(pid, {
        "type": "webhook",
        "name": "CI Webhook",
        "config": {"url": "https://example.com/hooks/ci"},
        "events": ["build.finished", "build.failed"],
    })
    notify_mgr.create(pid, {
        "type": "email",
        "name": "团队邮件",
        "config": {"address": "qa@example.com"},
        "events": ["build.failed"],
    })

    # -- 静态检查规则（流水线第一阶段使用） -------------------------------
    from engine.staticcheck import DEFAULT_NAME_PATTERN
    rules_store = registry.store("static_rules")
    rule_specs = [
        ("用例命名非空", "name_not_empty", "error", {}),
        ("至少包含一个步骤", "steps_not_empty", "error", {}),
        ("必须包含断言", "must_have_assert", "error", {}),
        ("命名规范（中英文/数字/连接符）", "naming_convention", "warning",
         {"pattern": DEFAULT_NAME_PATTERN}),
        ("禁止硬编码域名", "no_hardcoded_url", "warning", {}),
        ("步骤数不超过 20", "max_steps", "warning", {"max_steps": 20}),
    ]
    for rname, rtype, level, extra in rule_specs:
        rules_store.insert({
            "id": new_id("rule"),
            "project_id": pid,
            "name": rname,
            "description": "演示内置静态规则",
            "type": rtype,
            "level": level,
            "pattern": extra.get("pattern", ""),
            "max_steps": extra.get("max_steps", 20),
            "enabled": True,
            "builtin": True,
            "created_at": time.time(),
        })

    # -- 标准四阶段流水线：静态检查 → 单元测试 → 接口回归 → 报告生成 -------
    # 环境编排：开发环境（dev）先跑，预发环境（staging）后跑。
    # - 静态检查 / 单元测试：失败后「继续跑但标记结果」（continue）；
    # - 接口回归：失败「直接中止」（abort），冒烟套件内含失败注入用例，
    #   dev 就会失败并中止，能直观看到预发不再执行；
    # - 报告生成：收尾阶段，即使中止也照常产出汇总报告。
    pipeline = {
        "id": new_id("pipe"),
        "project_id": pid,
        "name": "标准回归流水线",
        "description": "静态检查 → 单元测试 → 接口回归（dev→staging）→ 报告",
        "env_ids": [env["id"], env2["id"]],
        "enabled": True,
        "stages": [
            {
                "id": new_id("stg"),
                "name": "静态检查",
                "type": "static",
                "on_fail": "continue",
                "env_scope": "pipeline",
                "suite_id": None,
                "tags_any": [],
                "env_id": None,
            },
            {
                "id": new_id("stg"),
                "name": "单元测试",
                "type": "unit",
                "on_fail": "continue",
                "env_scope": "pipeline",
                "suite_id": suite_unit["id"],
                "tags_any": [],
                "env_id": None,
            },
            {
                "id": new_id("stg"),
                "name": "接口回归",
                "type": "api",
                "on_fail": "abort",
                "env_scope": "pipeline",
                "suite_id": suite["id"],
                "tags_any": [],
                "env_id": None,
            },
            {
                "id": new_id("stg"),
                "name": "生成报告",
                "type": "report",
                "on_fail": "continue",
                "env_scope": "once",
                "suite_id": None,
                "tags_any": [],
                "env_id": None,
            },
        ],
        "created_at": time.time(),
    }
    registry.store("pipelines").insert(pipeline)

    registry.store("schedules").insert({
        "id": new_id("sch"),
        "project_id": pid,
        "name": "每 10 分钟跑一次阶段流水线",
        "cron": "*/10 * * * *",
        "pipeline_id": pipeline["id"],
        "suite_id": None,
        "env_id": None,
        "enabled": False,
        "last_fired_minute": None,
        "created_at": time.time(),
    })

    return {"project": proj, "env_id": env["id"], "suite_id": suite["id"],
            "pipeline_id": pipeline["id"]}
