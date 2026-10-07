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

# 流水线状态（一次流水线运行）
PIPELINE_RUN_STATUSES = ["pending", "running", "passed", "failed", "cancelled"]

# 流水线阶段状态（含阶段间闸门语义）
STAGE_STATUSES = ["pending", "running", "passed", "failed", "skipped", "cancelled"]

# 流水线阶段类型：静态检查 / 用例套件（单元、接口等都映射到它）/ 报告生成
STAGE_TYPES = ["static", "suite", "report"]

# 阶段失败策略：abort=直接中止后续阶段；continue=继续跑但在结果上标记
STAGE_FAILURE_POLICIES = ["abort", "continue"]

# 构建类型：普通套件构建 / 静态检查构建 / 流水线下属阶段构建
BUILD_KINDS = ["suite", "static"]

# 流水线相关通知事件
PIPELINE_EVENTS = ["pipeline.finished", "pipeline.passed", "pipeline.failed"]

# 缺陷严重级别与状态流
SEVERITIES = ["blocker", "critical", "major", "minor", "trivial"]
DEFECT_STATUSES = ["open", "in_progress", "fixed", "verified", "closed", "reopened"]

# 通知集成类型
INTEGRATION_TYPES = ["webhook", "slack", "email", "dingtalk"]

# 触发来源
TRIGGER_TYPES = ["manual", "schedule", "webhook", "ci"]


def new_id(prefix: str) -> str:
    """生成带前缀的唯一 id（时间戳 + 随机后缀，便于阅读与排查）。"""
    return f"{prefix}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}"


def now() -> float:
    return time.time()
