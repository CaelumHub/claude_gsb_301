"""静态检查：在真正执行用例之前做一轮「不发请求」的规则校验。

与单元 / 接口阶段不同，静态检查不依赖环境、不发起模拟请求，只扫描项目内
的用例 / 套件 / 环境实体本身，尽早发现「配置层面就必崩」的问题，例如：

- 用例被禁用、用例一个步骤都没有；
- 套件为空、套件引用了已删除的用例；
- 用例步骤里出现危险脚本（``import`` / ``__`` / ``open`` 等）；
- 用例超时时间非法（<=0 或大得离谱）；
- 环境缺 base_url / fail_rate 越界 / 依赖没有版本约束；
- ``${...}`` 变量引用拼写错误（引用了前面从未赋值的变量）。

为了让静态检查阶段与用例阶段**共用同一套执行 / 结果收集 / 报告链路**，
每条检查结果都被表达成一个「合成用例」：

- 检查通过 -> 一个只含通过断言的用例；
- 检查失败 -> 一个 ``equals`` 断言必然失败的用例，失败说明写进 expected/actual。

这样 BuildStore、执行监控页、测试报告页无需任何特判就能展示静态检查明细。
"""

from __future__ import annotations

import re

# 全部可用检查（key 为稳定标识，前端 / 流水线定义按 key 勾选）
CHECKS = [
    {"key": "case_enabled",    "name": "禁用用例检查",   "target": "case",
     "desc": "套件引用了被禁用的用例"},
    {"key": "case_empty",      "name": "空步骤检查",     "target": "case",
     "desc": "用例没有任何步骤，跑了也没有意义"},
    {"key": "case_timeout",    "name": "超时配置检查",   "target": "case",
     "desc": "用例超时时间非法（应在 1~3600 秒之间）"},
    {"key": "case_dangerous",  "name": "危险脚本检查",   "target": "case",
     "desc": "script 步骤含 import / open / __ 等受限内容"},
    {"key": "case_variable",   "name": "变量引用检查",   "target": "case",
     "desc": "${...} 引用了前面步骤从未赋值的变量"},
    {"key": "suite_empty",     "name": "空套件检查",     "target": "suite",
     "desc": "套件内一个用例都没有"},
    {"key": "suite_dangling",  "name": "悬挂引用检查",   "target": "suite",
     "desc": "套件引用了不存在的用例 id"},
    {"key": "env_config",      "name": "环境配置检查",   "target": "env",
     "desc": "环境缺 base_url、fail_rate 越界或依赖缺版本约束"},
]

CHECK_MAP = {c["key"]: c for c in CHECKS}
DEFAULT_CHECKS = [c["key"] for c in CHECKS]

_FORBIDDEN = ("import", "__", "open", "eval", "exec", "globals", "locals",
              "getattr", "setattr", "compile", "os", "sys", "subprocess")

# ${resp.status} 这类引用；resp/save_as 之外的内建变量视为白名单
_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_.]*)\}")
# 请求步骤默认保存响应到 resp（与 executor 的 save_as 默认值一致）
_BUILTIN_VARS = {"resp", "case"}


def _synthetic_case(case_id: str, name: str, group: str, ok: bool,
                    message: str, detail: str = "") -> dict:
    """把一条检查结果表达成合成用例（结构与 cases 实体一致）。"""
    if ok:
        steps = [
            {"action": "assert", "type": "truthy", "actual": True,
             "name": "检查通过"},
        ]
    else:
        steps = [
            {"action": "set", "key": "_actual", "value": detail or "不通过",
             "name": "实际情况"},
            {"action": "assert", "type": "equals", "actual": "${_actual}",
             "expected": message, "name": "期望符合规范"},
        ]
    return {
        "id": case_id,
        "name": name,
        "description": "静态检查合成用例",
        "priority": "P1",
        "tags": ["static-check", group],
        "timeout": 30,
        "enabled": True,
        "steps": steps,
        "_synthetic": True,
    }


def _find_dangerous(case: dict) -> str:
    for i, step in enumerate(case.get("steps") or []):
        if step.get("action") != "script":
            continue
        expr = str(step.get("expr", ""))
        for word in _FORBIDDEN:
            if word in expr.lower():
                return f"第 {i + 1} 步脚本含受限内容「{word}」"
    return ""


def _find_bad_variables(case: dict) -> str:
    defined = set(_BUILTIN_VARS)
    # 环境变量在执行时才注入，静态阶段无法知道名字；${ENV.xxx} 之类按通过处理
    for i, step in enumerate(case.get("steps") or []):
        for field in ("url", "params", "headers", "body", "actual", "expected"):
            value = step.get(field)
            if isinstance(value, dict):
                value = str(value)
            if not isinstance(value, str):
                continue
            for ref in _VAR_RE.findall(value):
                root = ref.split(".")[0]
                if root not in defined:
                    return f"第 {i + 1} 步引用了未定义变量 ${{{ref}}}"
        # 本步定义的变量，供后续步骤使用
        save_as = step.get("save_as")
        if save_as:
            defined.add(save_as)
        if step.get("action") == "set" and step.get("key"):
            defined.add(step["key"])
    return ""


def _check_case(case: dict) -> list[tuple[str, bool, str, str]]:
    """返回该用例的检查结果列表 ``[(check_key, ok, message, detail)]``。

    只产出**失败**项；某个检查 key 在所有用例上都没有失败项，即视为通过。
    """
    findings: list[tuple[str, bool, str, str]] = []
    name = case.get("name", case.get("id", "未命名用例"))

    def _fail(key: str, message: str, detail: str) -> None:
        findings.append((key, False, message, detail))

    if not case.get("enabled", True):
        _fail("case_enabled", f"用例「{name}」已被禁用", "用例 enabled=false")

    if not (case.get("steps") or []):
        _fail("case_empty", f"用例「{name}」没有步骤", "steps 为空")

    timeout = case.get("timeout", 60)
    if not isinstance(timeout, (int, float)) or timeout <= 0 or timeout > 3600:
        _fail("case_timeout", f"用例「{name}」超时配置非法: {timeout!r}",
              "timeout 应在 1~3600 秒之间")

    danger = _find_dangerous(case)
    if danger:
        _fail("case_dangerous", f"用例「{name}」{danger}", "危险脚本")

    bad_var = _find_bad_variables(case)
    if bad_var:
        _fail("case_variable", f"用例「{name}」{bad_var}", "未定义变量")

    return findings


def build_static_cases(cases: list[dict], suites: list[dict],
                       environments: list[dict],
                       selected: list[str] | None = None,
                       id_prefix: str = "static") -> list[dict]:
    """根据项目实体生成静态检查合成用例列表。

    :param cases:       项目下全部用例
    :param suites:      项目下全部套件（只需含 id/name/case_ids）
    :param environments:项目下全部环境
    :param selected:   启用的检查 key 列表；None 表示全选
    :param id_prefix:  合成用例 id 前缀（保证一次流水线内不与真实用例撞 id）
    """
    selected = set(selected or DEFAULT_CHECKS)
    out: list[dict] = []
    seq = 0

    def _next_id() -> str:
        nonlocal seq
        seq += 1
        return f"{id_prefix}_chk_{seq:03d}"

    case_ids = {c.get("id") for c in cases}

    # -- 用例级检查（每个检查 key：有失败就逐条列出，全过则一条通过明细）--
    case_checks = [k for k in ("case_enabled", "case_empty", "case_timeout",
                               "case_dangerous", "case_variable") if k in selected]
    failures_by_key: dict[str, list[tuple[str, str]]] = {k: [] for k in case_checks}
    for case in cases:
        for key, _ok, message, detail in _check_case(case):
            if key in failures_by_key:
                failures_by_key[key].append((message, detail))
    for key in case_checks:
        failures = failures_by_key[key]
        if failures:
            for message, detail in failures:
                out.append(_synthetic_case(_next_id(), message,
                                           "static-case", False, message, detail))
        else:
            out.append(_synthetic_case(
                _next_id(), f"{CHECK_MAP[key]['name']}：全部用例通过",
                "static-case", True, ""))

    # -- 套件级检查 -------------------------------------------------------
    if "suite_empty" in selected:
        for suite in suites:
            ok = bool(suite.get("case_ids"))
            out.append(_synthetic_case(
                _next_id(),
                f"套件「{suite.get('name', suite.get('id'))}」"
                + ("至少包含一个用例" if ok else "为空，没有任何用例"),
                "static-suite", ok,
                "" if ok else "套件 case_ids 为空",
                "空套件" if not ok else ""))

    if "suite_dangling" in selected:
        for suite in suites:
            dangling = [cid for cid in (suite.get("case_ids") or [])
                        if cid not in case_ids]
            ok = not dangling
            out.append(_synthetic_case(
                _next_id(),
                f"套件「{suite.get('name', suite.get('id'))}」"
                + ("引用完整" if ok else
                   f"存在 {len(dangling)} 个已删除用例引用"),
                "static-suite", ok,
                "" if ok else "悬挂 id: " + ", ".join(dangling[:5]),
                "悬挂引用" if not ok else ""))

    # -- 环境级检查 -------------------------------------------------------
    if "env_config" in selected:
        for env in environments:
            problems: list[str] = []
            cfg = env.get("config") or {}
            if not cfg.get("base_url"):
                problems.append("缺少 base_url")
            try:
                rate = float(cfg.get("fail_rate", 0.0))
                if not 0.0 <= rate <= 1.0:
                    problems.append(f"fail_rate={rate} 超出 [0,1]")
            except (TypeError, ValueError):
                problems.append("fail_rate 不是数字")
            for dep in env.get("dependencies") or []:
                if not str(dep.get("constraint", "")).strip():
                    problems.append(f"依赖 {dep.get('name')} 缺少版本约束")
            ok = not problems
            out.append(_synthetic_case(
                _next_id(),
                f"环境「{env.get('name', env.get('id'))}」"
                + ("配置合法" if ok else "：" + "；".join(problems[:3])),
                "static-env", ok,
                "" if ok else "；".join(problems),
                "环境配置问题" if not ok else ""))

    return out
