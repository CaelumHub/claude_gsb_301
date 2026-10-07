"""静态检查引擎：不执行用例，只对用例定义做规则校验。

流水线的第一个阶段通常是静态检查——在真正跑测试之前，先用一组规则
扫描用例定义，把「名字为空、没有断言步骤、硬编码了线上域名、步骤过多」
这类低级问题拦在最前面。静态检查几乎没有运行成本，先跑它可以让一次
回归在最早的时间点暴露问题，避免「点一下跑到底，等完全部才知道前面
早就崩了」。

一条规则命中一条问题，产出一条与用例结果同构的记录（case_id / status /
steps / assertions / logs），因此阶段结果可以直接复用构建结果存储与
报告聚合：

- 规则级别为 ``error``   -> 结果 ``failed``，阶段判失败（可配置中止/继续）；
- 规则级别为 ``warning`` -> 结果 ``passed``，问题只写进日志，不阻断。

规则实体存在通用分片存储 ``static_rules`` 中，字段::

    id / project_id / name / type / level / pattern / max_steps / enabled

- ``type``        规则类型，见 :data:`engine.models.STATIC_RULE_TYPES`
- ``level``       error / warning
- ``pattern``     naming_convention 用的正则（默认 ^[一-龥A-Za-z0-9（）()\-_ ·/]+$）
- ``max_steps``   max_steps 规则的上限（默认 20）
"""

from __future__ import annotations

import re
import time
from typing import Any, Optional

from .models import new_id

# 默认命名规范：中英文、数字与常见连接符，不允许纯符号/控制字符
DEFAULT_NAME_PATTERN = r"^[一-龥A-Za-z0-9（）()\-_ ·/]+$"

# request 步骤里被视为「硬编码域名」的 URL 前缀
_ABSOLUTE_URL_RE = re.compile(r"^https?://", re.IGNORECASE)


def _default_max_steps() -> int:
    return 20


def evaluate_rule(rule: dict, case: dict) -> Optional[str]:
    """对单个用例求值单条规则；命中返回问题描述，未命中返回 None。"""
    rtype = rule.get("type", "name_not_empty")
    name = (case.get("name") or "").strip()
    steps = case.get("steps") or []

    if rtype == "name_not_empty":
        if not name:
            return "用例名称为空"
        return None

    if rtype == "steps_not_empty":
        if not steps:
            return "用例没有任何步骤"
        return None

    if rtype == "must_have_assert":
        if not any(s.get("action") == "assert" for s in steps):
            return "用例不包含断言步骤（assert）"
        return None

    if rtype == "naming_convention":
        pattern = rule.get("pattern") or DEFAULT_NAME_PATTERN
        if not name:
            return None  # 空名交给 name_not_empty 规则报
        try:
            if not re.search(pattern, name):
                return f"用例名称不符合命名规范 /{pattern}/"
        except re.error as exc:
            return f"命名规则的正则无效: {exc}"
        return None

    if rtype == "no_hardcoded_url":
        for idx, step in enumerate(steps):
            if step.get("action") != "request":
                continue
            url = str(step.get("url") or "")
            if _ABSOLUTE_URL_RE.match(url):
                return f"第 {idx + 1} 步硬编码了绝对地址 {url}，应使用相对路径走环境 base_url"
        return None

    if rtype == "max_steps":
        limit = int(rule.get("max_steps") or _default_max_steps())
        if len(steps) > limit:
            return f"步骤数 {len(steps)} 超过上限 {limit}"
        return None

    return f"未知规则类型 {rtype!r}"


class StaticChecker:
    """静态检查器：加载规则、扫描用例、产出结果记录。"""

    def __init__(self, registry):
        self.registry = registry
        self._store = registry.store("static_rules")

    # -- 规则 CRUD --------------------------------------------------------
    def create_rule(self, project_id: str, payload: dict) -> dict:
        rule = {
            "id": new_id("rule"),
            "project_id": project_id,
            "name": (payload.get("name") or "").strip() or "未命名规则",
            "description": payload.get("description", ""),
            "type": payload.get("type", "name_not_empty"),
            "level": payload.get("level", "error"),
            "pattern": payload.get("pattern") or "",
            "max_steps": int(payload.get("max_steps") or _default_max_steps()),
            "enabled": bool(payload.get("enabled", True)),
            "builtin": bool(payload.get("builtin", False)),
            "created_at": time.time(),
        }
        self._store.insert(rule)
        return rule

    def list_rules(self, project_id: str, enabled_only: bool = False) -> list[dict]:
        rules = self._store.query(where=[("project_id", "eq", project_id)],
                                  order_by="created_at", order="asc")
        if enabled_only:
            rules = [r for r in rules if r.get("enabled", True)]
        return rules

    def update_rule(self, rule_id: str, patch: dict) -> Optional[dict]:
        allowed = {k: patch[k] for k in
                   ("name", "description", "type", "level", "pattern",
                    "max_steps", "enabled") if k in patch}
        return self._store.update(rule_id, allowed)

    def delete_rule(self, rule_id: str) -> bool:
        return self._store.delete(rule_id)

    # -- 执行检查 ---------------------------------------------------------
    def check_case(self, case: dict, rules: list[dict]) -> list[dict]:
        """对一个用例跑全部启用规则，返回命中的问题记录（每条一条）。"""
        started = time.time()
        case_id = case.get("id")
        case_name = case.get("name") or "未命名用例"
        findings: list[dict] = []
        for idx, rule in enumerate(rules):
            message = evaluate_rule(rule, case)
            if message is None:
                continue
            is_error = rule.get("level", "error") == "error"
            full_msg = f"[{rule.get('name', rule.get('type'))}] {message}"
            findings.append({
                "case_id": case_id,
                "case_name": case_name,
                "group": "static",
                "priority": case.get("priority", "P3"),
                # warning 不计失败：结果记通过，问题留在日志里
                "status": "failed" if is_error else "passed",
                "duration": round(max(time.time() - started, 0.001), 3),
                "steps": [{
                    "index": 0, "action": "static",
                    "name": rule.get("name", rule.get("type", "static")),
                    "status": "failed" if is_error else "passed",
                    "message": full_msg,
                    "duration": 0.001,
                }],
                "assertions": [{
                    "name": rule.get("name", rule.get("type", "static")),
                    "type": "static",
                    "expected": "无问题",
                    "actual": message,
                    "ok": not is_error,
                    "message": message,
                }],
                "logs": [
                    f"静态检查用例 {case_name} (id={case_id})",
                    f"  规则 {rule.get('type')} 级别 {rule.get('level', 'error')}: {message}",
                ],
                "message": full_msg,
                "stage_kind": "static",
                "check_index": idx,
            })
        return findings

    def run(self, project_id: str, cases: list[dict],
            cancel_event=None) -> list[dict]:
        """对一批用例执行启用的静态规则，返回结果记录。

        **每个被检查用例恰好产出一条结果**（与构建按用例计数的 total 口径
        一致）：命中的全部规则聚合成一条——只要有 error 级别命中即为
        ``failed``，否则（只有 warning 或全通过）为 ``passed``；每条规则
        的明细保留在 steps / assertions / logs 中，报告页仍可下钻。
        """
        rules = self.list_rules(project_id, enabled_only=True)
        results: list[dict] = []
        for case in cases:
            if cancel_event is not None and cancel_event.is_set():
                break
            findings = self.check_case(case, rules)
            if not findings:
                results.append({
                    "case_id": case.get("id"),
                    "case_name": case.get("name") or "未命名用例",
                    "group": "static",
                    "priority": case.get("priority", "P3"),
                    "status": "passed",
                    "duration": 0.001,
                    "steps": [{
                        "index": 0, "action": "static", "name": "静态规则扫描",
                        "status": "passed",
                        "message": f"{len(rules)} 条规则全部通过",
                        "duration": 0.001,
                    }],
                    "assertions": [],
                    "logs": [f"静态检查通过: {case.get('name')}（{len(rules)} 条规则）"],
                    "message": "",
                    "stage_kind": "static",
                })
                continue

            has_error = any(f["status"] == "failed" for f in findings)
            warning_count = sum(1 for f in findings if f["status"] == "passed")
            steps = [f["steps"][0] for f in findings]
            for i, st in enumerate(steps):
                st["index"] = i
            assertions = [a for f in findings for a in f["assertions"]]
            detail_lines = [line for f in findings for line in f["logs"][1:]]
            logs = ([f"静态检查用例 {case.get('name')} (id={case.get('id')})"]
                    + detail_lines)
            messages = [f["message"] for f in findings]
            results.append({
                "case_id": case.get("id"),
                "case_name": case.get("name") or "未命名用例",
                "group": "static",
                "priority": case.get("priority", "P3"),
                "status": "failed" if has_error else "passed",
                "duration": 0.001,
                "steps": steps,
                "assertions": assertions,
                "logs": logs,
                "message": "；".join(messages),
                "warning_count": warning_count,
                "stage_kind": "static",
            })
        return results
