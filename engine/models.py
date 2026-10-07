"""领域模型：枚举常量、id 生成与通用工具。"""

from __future__ import annotations

import time
import uuid

# 用例优先级
PRIORITIES = ["P0", "P1", "P2", "P3"]

# 用例结果状态（单条）
CASE_STATUSES = ["passed", "failed", "error", "skipped", "timeout"]

# 构建状态（一次执行）
BUILD_STATUSES = ["pending", "running", "passed", "failed", "cancelled", "error"]

# 阶段状态（与构建状态共用一套终态，语义完全对应）
STAGE_STATUSES = ["pending", "running", "passed", "failed", "cancelled", "skipped"]

# 阶段类型：静态检查 / 单元测试 / 接口回归 / 报告生成
STAGE_TYPES = ["static", "unit", "api", "report"]

# 阶段失败策略：abort=直接中止后续阶段；continue=继续跑但标记失败
STAGE_FAIL_POLICIES = ["abort", "continue"]

# 阶段的环境编排方式：
#   once    不依赖环境（如「报告生成」，整条流水线只跑一次）
#   pipeline 按流水线绑定的环境顺序依次跑（开发先跑、预发后跑）
#   fixed   固定绑定一个环境
STAGE_ENV_SCOPES = ["once", "pipeline", "fixed"]

# 缺陷严重级别与状态流
SEVERITIES = ["blocker", "critical", "major", "minor", "trivial"]
DEFECT_STATUSES = ["open", "in_progress", "fixed", "verified", "closed", "reopened"]

# 通知集成类型
INTEGRATION_TYPES = ["webhook", "slack", "email", "dingtalk"]

# 触发来源
TRIGGER_TYPES = ["manual", "schedule", "webhook", "ci"]

# 静态检查规则级别：error 命中即阶段失败；warning 只提示不阻断
STATIC_RULE_LEVELS = ["error", "warning"]

# 内置静态检查规则类型
STATIC_RULE_TYPES = [
    "name_not_empty",     # 用例必须有名称
    "steps_not_empty",    # 用例至少有一个步骤
    "must_have_assert",   # 用例必须包含断言步骤
    "naming_convention",  # 命名规范（默认正则）
    "no_hardcoded_url",   # 禁止硬编码域名（应走环境 base_url）
    "max_steps",          # 步骤数上限
]


def new_id(prefix: str) -> str:
    """生成带前缀的唯一 id（时间戳 + 随机后缀，便于阅读与排查）。"""
    return f"{prefix}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}"


def now() -> float:
    return time.time()
