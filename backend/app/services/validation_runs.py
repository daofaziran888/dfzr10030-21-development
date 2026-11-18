"""采集结果校验运行包：按校验字段、采样比例和阻断级别执行校验。

这里是校验服务的数据流核心：
1. 读取采集结果样本与校验规则（规则按数据版本快照）；
2. 按采样比例抽取本次采样范围，逐字段执行校验；
3. 固化「校验运行包」与「异常样本清单」，每个异常样本都能回查到采样范围与阻断规则；
4. 采集监控服务再消费运行包，生成待办台账、当日告警并回写规则差异。

仓储与项目其它模块保持一致，使用内存结构；区别是运行包不列入 store 的业务表，
避免影响各模块列表的示例数据条数与运营概览口径。
"""
from __future__ import annotations

import hashlib
import json
import re
import threading
from datetime import date, datetime
from typing import Any

from app.store import store

# ---------------------------------------------------------------------------
# 采集结果样本（真实项目里由采集服务写入样本库，此处提供可复现的样本池）
# ---------------------------------------------------------------------------

COLLECTION_SAMPLES: list[dict[str, Any]] = [
    {"样本编号": "SAMP-0001", "采集批次": "B20261118-01", "来源": "网页采集",
     "商品标题": "无线蓝牙耳机", "商品价格": "199.00", "库存数量": "120"},
    {"样本编号": "SAMP-0002", "采集批次": "B20261118-01", "来源": "网页采集",
     "商品标题": "机械键盘 87 键", "商品价格": "329.00", "库存数量": "-12"},
    {"样本编号": "SAMP-0003", "采集批次": "B20261118-01", "来源": "网页采集",
     "商品标题": "", "商品价格": "89.90", "库存数量": "8"},
    {"样本编号": "SAMP-0004", "采集批次": "B20261118-01", "来源": "API接入",
     "商品标题": "便携显示器", "商品价格": "暂无报价", "库存数量": "18"},
    {"样本编号": "SAMP-0005", "采集批次": "B20261118-01", "来源": "API接入",
     "商品标题": "桌面收纳盒", "商品价格": "面议", "库存数量": "40"},
    {"样本编号": "SAMP-0006", "采集批次": "B20261118-01", "来源": "API接入",
     "商品标题": "USB-C 扩展坞", "商品价格": "259.00", "库存数量": "-2"},
    {"样本编号": "SAMP-0007", "采集批次": "B20261118-01", "来源": "增量采集",
     "商品标题": "降噪头戴耳机", "商品价格": "899.00", "库存数量": "-5"},
    {"样本编号": "SAMP-0008", "采集批次": "B20261118-01", "来源": "增量采集",
     "商品标题": "铝合金支架", "商品价格": "79.00", "库存数量": "300"},
    {"样本编号": "SAMP-0009", "采集批次": "B20261118-01", "来源": "增量采集",
     "商品标题": "专业采集卡", "商品价格": "128000", "库存数量": "3"},
    {"样本编号": "SAMP-0010", "采集批次": "B20261118-01", "来源": "RPA",
     "商品标题": "静音鼠标", "商品价格": "69.00", "库存数量": "210"},
    {"样本编号": "SAMP-0011", "采集批次": "B20261118-01", "来源": "RPA",
     "商品标题": "摄像头保护盖", "商品价格": "9.90", "库存数量": "1000"},
    {"样本编号": "SAMP-0012", "采集批次": "B20261118-01", "来源": "RPA",
     "商品标题": "氮化镓充电器", "商品价格": "149.00", "库存数量": "3.5"},
]

# ---------------------------------------------------------------------------
# 内存仓储与并发保护
# ---------------------------------------------------------------------------

LOCK = threading.RLock()
RUNS: dict[int, dict[str, Any]] = {}
RUN_SEQ = 0
TODO_LEDGER: list[dict[str, Any]] = []
TODO_KEYS: set[str] = set()
TODO_SEQ = 0
MONITOR_RECORDS: dict[str, dict[str, Any]] = {}
RECORD_SEQ = 0
RELAYED_RUNS: dict[int, dict[str, Any]] = {}
IDEMPOTENCY: dict[str, int] = {}
RUN_FINGERPRINTS: dict[str, int] = {}

# 规则数据版本：每次规则登记或状态流转都会推进，运行包固定记录执行时的版本。
RULE_REVISION = 1
SAMPLE_VERSION = 1

# 采集监控服务版本：校验页通过运行包的「监控回查」记录定位到该版本。
MONITOR_SERVICE_VERSION = "collect-monitor 1.2.0"
VALIDATOR_SERVICE_VERSION = "validator 1.1.0"
DATA_FLOW = [
    "采集结果样本", "校验规则（数据版本快照）", "按采样比例抽取采样范围",
    "字段校验与阻断判定", "校验运行包", "异常样本清单",
    "采集监控服务（待办台账 / 当日告警 / 规则差异回写）",
]

BLOCK_LEVEL = "阻断"
WARN_LEVEL = "告警"
NOTICE_LEVEL = "提示"
CONCLUSION_PASS = "通过"
CONCLUSION_ALERT = "告警"
CONCLUSION_BLOCK = "阻断"
CONCLUSION_UNKNOWN = "无法判定"


def bump_rule_revision() -> int:
    """规则发生变化时推进数据版本；旧运行包仍保留原版本供回查。"""
    global RULE_REVISION
    with LOCK:
        RULE_REVISION += 1
        return RULE_REVISION


def current_rule_revision() -> int:
    return RULE_REVISION


def _now() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")


def _today() -> str:
    return date.today().isoformat()


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _as_list(value: Any) -> list[str]:
    """把「规则编号/样本编号」的多种提交形式统一成字符串列表。"""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [_text(item) for item in value if _text(item)]
    text = _text(value)
    if not text:
        return []
    return [part for part in re.split(r"[,，、;；\s]+", text) if part]


def _digest(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# 规则口径归一化：新口径直接使用，旧口径给出安全默认值并保留提示
# ---------------------------------------------------------------------------

def normalize_ratio(value: Any) -> tuple[float | None, bool]:
    """采样比例归一化为 0~1；返回 (比例, 是否兼容旧口径)。"""
    if value is None:
        return None, False
    text = _text(value)
    if not text:
        return None, False
    if text in {"按规则配置", "沿用规则", "rule"}:
        return None, False
    percent = text.endswith("%")
    try:
        number = float(text.rstrip("%").strip())
    except ValueError:
        return None, True
    ratio = number / 100.0 if percent or number > 1 else number
    if ratio < 0 or ratio > 1:
        return None, True
    return ratio, False


def normalize_level(value: Any) -> tuple[str, bool]:
    """阻断级别归一化；无法识别时按阻断处理，避免把未校验数据当成通过。"""
    text = _text(value)
    if not text:
        return BLOCK_LEVEL, True
    lowered = text.casefold()
    if any(token in text for token in ("不阻断", "非阻断", "无阻断")) or lowered in {"none", "off"}:
        return NOTICE_LEVEL, False
    if "强阻断" in text or text == BLOCK_LEVEL or "阻断" in text or lowered in {"block", "blocking", "high"}:
        return BLOCK_LEVEL, False
    if "告警" in text or "警告" in text or lowered in {"warn", "warning", "medium"}:
        return WARN_LEVEL, False
    if "提示" in text or "提醒" in text or lowered in {"notice", "info", "low"}:
        return NOTICE_LEVEL, False
    return BLOCK_LEVEL, True


def normalize_type(value: Any) -> tuple[str, bool]:
    text = _text(value)
    mapping = {
        "非空": "非空", "not_empty": "非空", "notempty": "非空", "required": "非空",
        "数值范围": "数值范围", "数值区间": "数值范围", "range": "数值范围",
        "整数": "整数", "integer": "整数", "int": "整数",
        "正则": "正则", "regex": "正则", "pattern": "正则",
        "枚举": "枚举", "enum": "枚举", "候选值": "枚举",
        "等于": "等于", "equals": "等于", "精确匹配": "等于",
        "包含": "包含", "contains": "包含",
        "数值": "数值", "number": "数值", "numeric": "数值",
    }
    key = text.casefold()
    if key in mapping:
        return mapping[key], False
    return "非空", True


def _tolerance(value: Any) -> float:
    text = _text(value)
    if not text:
        return 0.0
    try:
        return abs(float(text.rstrip("%").strip()))
    except ValueError:
        return 0.0


def _parse_range(expected: str) -> tuple[float | None, float | None] | None:
    """解析期望值里常见的范围写法；解析不了返回 None。"""
    text = expected.strip().replace("，", ",")
    match = re.fullmatch(r"(-?\d+(?:\.\d+)?)\s*(?:-|~|至|,)\s*(-?\d+(?:\.\d+)?)", text)
    if match:
        return float(match.group(1)), float(match.group(2))
    match = re.fullmatch(r"(>=|>|<=|<)\s*(-?\d+(?:\.\d+)?)", text)
    if match:
        op, number = match.group(1), float(match.group(2))
        if op == ">=":
            return number, None
        if op == ">":
            return number, None
        if op == "<=":
            return None, number
        return None, number
    return None


def _rule_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    ratio, ratio_legacy = normalize_ratio(row.get("采样比例"))
    level, level_legacy = normalize_level(row.get("阻断级别"))
    check_type, type_legacy = normalize_type(row.get("校验类型"))
    rule = {
        "id": int(row.get("id", 0)),
        "规则编号": _text(row.get("规则编号")),
        "校验字段": _text(row.get("校验字段")),
        "校验类型": check_type,
        "原始校验类型": _text(row.get("校验类型")),
        "期望值": _text(row.get("期望值")),
        "容错范围": _text(row.get("容错范围")),
        "采样比例": (_text(row.get("采样比例")) or "100%"),
        "采样比例值": 1.0 if ratio is None else ratio,
        "阻断级别": level,
        "规则状态": _text(row.get("status") or row.get("规则状态")),
        "旧口径兼容": bool(ratio_legacy or level_legacy or type_legacy),
    }
    notices: list[str] = []
    if ratio_legacy:
        notices.append("采样比例无法识别，按 100% 兼容执行")
    if level_legacy:
        notices.append("阻断级别缺失或无法识别，按阻断级别执行")
    if type_legacy:
        notices.append("校验类型未识别，按非空规则兼容执行")
    rule["口径说明"] = notices
    return rule


def _select_rules(values: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    """按显式规则编号或规则范围选择规则；停用规则不参与执行。"""
    rows = store.rows("validator")
    codes = _as_list(values.get("规则编号") or values.get("ruleCodes") or values.get("rule_codes"))
    if codes:
        lookup: dict[str, dict[str, Any]] = {}
        for row in rows:
            lookup[_text(row.get("规则编号"))] = row
            lookup[str(row.get("id"))] = row
        selected = []
        seen: set[int] = set()
        missing: list[str] = []
        for code in codes:
            row = lookup.get(code)
            if row is None or id(row) in seen:
                if row is None:
                    missing.append(code)
                continue
            seen.add(id(row))
            selected.append(row)
        return selected, missing

    scope = _text(values.get("规则范围") or values.get("scope") or "仅已启用规则")
    if scope in {"全部", "全部规则", "all"}:
        return list(rows), []
    return [row for row in rows if _text(row.get("status")) == "已启用"], []


def _first_present(values: dict[str, Any], keys: tuple[str, ...]) -> Any:
    """按顺序取第一个真正提交过的键；空列表也要算提交，表示本次没有采集结果。"""
    for key in keys:
        if key in values and values[key] is not None:
            return values[key]
    return None


def _select_samples(values: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    raw = _first_present(values, ("样本", "采集结果样本", "samples"))
    if isinstance(raw, list):
        samples: list[dict[str, Any]] = []
        for index, item in enumerate(raw, start=1):
            if isinstance(item, dict):
                sample = dict(item)
            else:
                sample = {"值": item}
            sample["样本编号"] = _text(sample.get("样本编号")) or f"SAMP-IN-{index:04d}"
            samples.append(sample)
        return samples, None
    if raw is not None and not isinstance(raw, list):
        return [], "提交的采集结果样本不是列表，无法判定"

    samples = [dict(item) for item in COLLECTION_SAMPLES]
    wanted = set(_as_list(values.get("样本编号") or values.get("sampleCodes") or values.get("sample_codes")))
    if wanted:
        samples = [item for item in samples if _text(item.get("样本编号")) in wanted]
    return samples, None


def _sample_scope(samples: list[dict[str, Any]], ratio: float, rule_code: str) -> list[dict[str, Any]]:
    """按采样比例抽取确定性样本，保证同一规则/比例/样本版本可复现。"""
    if ratio <= 0 or not samples:
        return []
    if ratio >= 1:
        return list(samples)
    seed = _digest([SAMPLE_VERSION, rule_code, round(ratio, 6), len(samples),
                    [item.get("样本编号") for item in samples]])
    import random

    rng = random.Random(seed)
    count = max(1, int(round(len(samples) * ratio)))
    count = min(count, len(samples))
    picked = set(rng.sample(range(len(samples)), count))
    return [item for index, item in enumerate(samples) if index in picked]


# ---------------------------------------------------------------------------
# 字段校验
# ---------------------------------------------------------------------------

def _field_value(sample: dict[str, Any], field: str) -> tuple[bool, Any]:
    if field in sample:
        return True, sample.get(field)
    # 兼容「payload.title」这类嵌套采集结果。
    current: Any = sample
    for part in field.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return False, None
    return True, current


def _check_value(rule: dict[str, Any], value: Any) -> str | None:
    """返回异常原因；None 表示通过。"""
    check_type = rule["校验类型"]
    expected = rule["期望值"]
    text = "" if value is None else str(value).strip()

    if check_type == "非空":
        return None if text else "字段为空"
    if not text:
        return "字段为空，无法执行校验"

    if check_type == "数值":
        try:
            float(text)
        except ValueError:
            return f"取值「{text}」不是有效数值"
        return None

    if check_type == "整数":
        try:
            number = float(text)
        except ValueError:
            return f"取值「{text}」不是整数"
        if not number.is_integer():
            return f"取值「{text}」不是整数"
        parsed = _parse_range(expected)
        if parsed:
            minimum, maximum = parsed
            if expected.startswith(">"):
                return None if number > minimum else f"取值「{text}」不满足期望「{expected}」"
            if expected.startswith(">="):
                return None if number >= minimum else f"取值「{text}」不满足期望「{expected}」"
            if expected.startswith("<="):
                return None if number <= maximum else f"取值「{text}」不满足期望「{expected}」"
            if expected.startswith("<"):
                return None if number < maximum else f"取值「{text}」不满足期望「{expected}」"
            return None
        return None

    if check_type == "数值范围":
        try:
            number = float(text)
        except ValueError:
            return f"取值「{text}」不是有效数值"
        parsed = _parse_range(expected)
        if parsed is None:
            return None
        minimum, maximum = parsed
        tolerance = _tolerance(rule["容错范围"])
        if minimum is not None and number < minimum - tolerance:
            return f"取值「{text}」低于期望下限 {minimum:g}（容错 {tolerance:g}）"
        if maximum is not None and number > maximum + tolerance:
            return f"取值「{text}」高于期望上限 {maximum:g}（容错 {tolerance:g}）"
        if expected.startswith(">") and minimum is not None and number <= minimum:
            return f"取值「{text}」不满足期望「{expected}」"
        if expected.startswith("<") and maximum is not None and number >= maximum:
            return f"取值「{text}」不满足期望「{expected}」"
        return None

    if check_type == "正则":
        try:
            matched = re.search(expected, text) if expected else True
        except re.error:
            return f"期望值「{expected}」不是可执行的正则表达式"
        return None if matched else f"取值「{text}」不匹配正则「{expected}」"

    if check_type == "枚举":
        options = [part.strip() for part in re.split(r"[,，、|/]", expected) if part.strip()]
        return None if text in options else f"取值「{text}」不在候选值「{expected}」中"

    if check_type == "包含":
        return None if expected in text else f"取值「{text}」不包含「{expected}」"

    # 等于
    if text == expected:
        return None
    tolerance = _tolerance(rule["容错范围"])
    if tolerance:
        try:
            if abs(float(text) - float(expected)) <= tolerance:
                return None
        except ValueError:
            pass
    return f"取值「{text}」与期望值「{expected}」不一致"


def _historical_blocking(limit: int = 5) -> list[dict[str, Any]]:
    """历史阻断依据：只用于展示风险，不替代本次判定。"""
    evidence: list[dict[str, Any]] = []
    for run in sorted(RUNS.values(), key=lambda item: int(item["id"]), reverse=True):
        if int(run.get("阻断数量", 0)) <= 0:
            continue
        for entry in run.get("异常样本清单", []):
            if not entry.get("是否阻断"):
                continue
            evidence.append({
                "运行包编号": run["运行包编号"],
                "创建时间": run["创建时间"],
                "样本编号": entry["样本编号"],
                "校验规则": entry["规则编号"],
                "校验字段": entry["字段"],
                "异常原因": entry["异常原因"],
                "阻断级别": entry["阻断级别"],
                "数据版本": run["数据版本"],
            })
            if len(evidence) >= limit:
                return evidence
    return evidence


def _build_abnormal_entry(
    *,
    run_code: str,
    data_version: str,
    rule: dict[str, Any],
    sample: dict[str, Any],
    value: Any,
    reason: str,
    sampled_ids: list[str],
    ratio: float,
) -> dict[str, Any]:
    blocked = rule["阻断级别"] == BLOCK_LEVEL
    scope_text = "、".join(sampled_ids) if sampled_ids else "（本次采样为空）"
    return {
        "样本编号": _text(sample.get("样本编号")),
        "采集批次": _text(sample.get("采集批次")),
        "字段": rule["校验字段"],
        "字段值": "" if value is None else str(value),
        "规则编号": rule["规则编号"],
        "校验类型": rule["校验类型"],
        "期望值": rule["期望值"],
        "异常原因": reason,
        "阻断级别": rule["阻断级别"],
        "是否阻断": blocked,
        "采样比例": f"{round(ratio * 100, 2):g}%",
        "采样范围": scope_text,
        "采样数量": len(sampled_ids),
        "运行包编号": run_code,
        "数据版本": data_version,
        "依据": (
            f"运行包 {run_code} · 采样 {round(ratio * 100, 2):g}%"
            f"（{len(sampled_ids)} 条） · 规则 {rule['规则编号']}（{rule['阻断级别']}）"
        ),
    }


# ---------------------------------------------------------------------------
# 运行包执行
# ---------------------------------------------------------------------------

def create_run(payload: dict[str, Any]) -> tuple[dict[str, Any] | None, str, bool]:
    """执行一次校验并固化运行包。返回 (运行包, 说明, 是否复用已有结果)。"""
    values: dict[str, Any] = dict(payload or {})
    nested = values.get("values")
    if isinstance(nested, dict):
        values = {**nested, **{key: value for key, value in values.items() if key != "values"}}

    # 采样比例：未提交时沿用每条规则自己的比例；显式提交时覆盖本次运行。
    raw_ratio = values.get("采样比例", values.get("samplingRatio", values.get("sampling_ratio")))
    if raw_ratio is None or _text(raw_ratio) == "":
        override_ratio: float | None = None
    elif _text(raw_ratio) in {"按规则配置", "沿用规则", "rule"}:
        override_ratio = None
    else:
        override_ratio, _legacy = normalize_ratio(raw_ratio)
        if override_ratio is None:
            return None, (
                f"采样比例「{_text(raw_ratio)}」无法识别，本次未生成运行包；"
                "请使用 0%~100% 或「按规则配置」后重试"
            ), False

    request_id = _text(values.get("requestId") or values.get("请求编号") or values.get("request_id")
                         or values.get("idempotencyKey"))
    expected_version = values.get("expectedVersion", values.get("数据版本", values.get("dataVersion")))
    operator = _text(values.get("操作人") or values.get("operator")) or "值班人员"

    samples, sample_error = _select_samples(values)
    if sample_error:
        samples = []
    rules, missing_rules = _select_rules(values)

    with LOCK:
        revision = RULE_REVISION
        expected_text = _text(expected_version)
        if expected_text:
            match = re.search(r"\d+", expected_text)
            if match and int(match.group(0)) != revision:
                return (None,
                        f"数据版本冲突：当前规则版本为 R{revision}，本次提交基于 R{int(match.group(0))}，"
                        "未写入运行包；旧口径运行包仍可回查，请刷新后重试",
                        False)

        rule_snapshot = [_rule_snapshot(row) for row in rules]
        executable = [rule for rule in rule_snapshot if rule["规则状态"] != "已停用"]
        rule_digest = _digest(rule_snapshot)
        sample_digest = _digest(samples)
        fingerprint = _digest({
            "request": request_id,
            "rules": rule_digest,
            "samples": sample_digest,
            "ratio": None if override_ratio is None else round(override_ratio, 6),
        })
        dedupe_key = f"req:{request_id}" if request_id else fingerprint

        existing_id = IDEMPOTENCY.get(dedupe_key) or RUN_FINGERPRINTS.get(fingerprint)
        if existing_id is not None and existing_id in RUNS:
            return RUNS[existing_id], f"相同采样与规则已生成运行包 {RUNS[existing_id]['运行包编号']}，本次未重复写入", True

        global RUN_SEQ
        RUN_SEQ += 1
        run_id = RUN_SEQ
        run_code = f"VRUN-{run_id:04d}"
        sample_version_text = f"S{SAMPLE_VERSION}.{sample_digest[:6]}"
        data_version = f"R{revision}-{sample_version_text}"

        rule_results: list[dict[str, Any]] = []
        abnormal: list[dict[str, Any]] = []
        all_sampled_ids: list[str] = []
        boundary: list[str] = []
        legacy_notes: list[str] = []

        if sample_error:
            boundary.append(sample_error)
        if missing_rules:
            boundary.append(f"规则 {('、'.join(missing_rules))} 不存在，未参与本次校验")
        if not rules:
            boundary.append("没有可执行的校验规则：未选择规则或规则均不可用，无法判定")
        if rules and not executable:
            boundary.append("没有可执行的校验规则：所选规则均已停用，无法判定")

        for rule in executable:
            ratio = override_ratio if override_ratio is not None else float(rule["采样比例值"])
            # 运行级采样比例同样不能越过规则自己的采样口径：显式覆盖优先，否则用规则配置。
            ratio = max(0.0, min(1.0, ratio))
            scope = _sample_scope(samples, ratio, rule["规则编号"])
            sampled_ids = [_text(row.get("样本编号")) for row in scope]
            for sample_id in sampled_ids:
                if sample_id not in all_sampled_ids:
                    all_sampled_ids.append(sample_id)

            hits: list[dict[str, Any]] = []
            for sample in scope:
                exists, value = _field_value(sample, rule["校验字段"])
                if not exists:
                    reason = f"字段「{rule['校验字段']}」缺失"
                else:
                    reason = _check_value(rule, value)
                if reason is None:
                    continue
                entry = _build_abnormal_entry(
                    run_code=run_code, data_version=data_version, rule=rule, sample=sample,
                    value=value, reason=reason, sampled_ids=sampled_ids, ratio=ratio,
                )
                hits.append(entry)
                abnormal.append(entry)

            if not scope:
                empty_reason = (
                    "本次采样为空：采样比例为 0%，未抽到样本"
                    if ratio <= 0
                    else "本次采样为空：采集结果样本池没有可用样本"
                )
                boundary.append(f"规则 {rule['规则编号']} {empty_reason}，未参与本次判定")
            if rule["旧口径兼容"]:
                for note in rule["口径说明"]:
                    legacy_notes.append(f"规则 {rule['规则编号']}：{note}")

            rule_results.append({
                "规则编号": rule["规则编号"],
                "校验字段": rule["校验字段"],
                "校验类型": rule["校验类型"],
                "期望值": rule["期望值"],
                "阻断级别": rule["阻断级别"],
                "采样比例": f"{round(ratio * 100, 2):g}%",
                "规则配置采样比例": rule["采样比例"],
                "采样数量": len(scope),
                "采样样本": sampled_ids,
                "命中数量": len(hits),
                "命中样本": [entry["样本编号"] for entry in hits],
                "采样为空": not scope,
                "旧口径兼容": rule["旧口径兼容"],
                "口径说明": rule["口径说明"],
            })

        blocking_count = sum(1 for entry in abnormal if entry["是否阻断"])
        warning_count = len(abnormal) - blocking_count
        total_sampled = len(all_sampled_ids)
        undetermined_reasons: list[str] = []
        if not executable:
            undetermined_reasons.append("没有可执行的校验规则")
        if not samples or total_sampled == 0:
            undetermined_reasons.append("本次采样为空")
        if sample_error:
            undetermined_reasons.append(sample_error)

        undetermined = bool(undetermined_reasons)
        if undetermined:
            conclusion = CONCLUSION_UNKNOWN
            overall_pass = False
            downstream_blocked = True
            if "本次采样为空" in undetermined_reasons:
                boundary.append(
                    "本次采样为空，按无法判定处理并阻止所有下游读取；"
                    "历史阻断依据仅供查看，不替代本次结论；待产生新样本后重试"
                )
            if "没有可执行的校验规则" in undetermined_reasons:
                boundary.append("没有可执行规则，按无法判定处理并阻止下游读取")
        elif blocking_count > 0:
            conclusion = CONCLUSION_BLOCK
            overall_pass = False
            downstream_blocked = True
            rule_codes = sorted({entry["规则编号"] for entry in abnormal if entry["是否阻断"]})
            boundary.append(
                f"阻断规则 {('、'.join(rule_codes))} 命中 {blocking_count} 个样本，"
                "整体结论不显示通过，下游读取被阻断"
            )
        elif abnormal:
            conclusion = CONCLUSION_ALERT
            overall_pass = False
            downstream_blocked = False
            boundary.append(
                f"采样范围内命中 {len(abnormal)} 条告警级异常（不阻断下游），整体结论不可视为通过"
            )
        else:
            conclusion = CONCLUSION_PASS
            overall_pass = True
            downstream_blocked = False
            boundary.append(f"本次采样 {total_sampled} 条，没有命中异常样本；实际抽取的样本见采样范围")

        if legacy_notes:
            boundary.extend(legacy_notes)

        historical = _historical_blocking() if undetermined else []
        if historical:
            current_boundary = "本次空采样" if "本次采样为空" in undetermined_reasons else "本次无法判定"
            boundary.append(
                f"历史风险：运行包 {historical[0]['运行包编号']} 曾命中阻断规则 "
                f"{historical[0]['校验规则']}；{current_boundary}保留本次边界，历史阻断结论仅供查看"
            )

        run = {
            "id": run_id,
            "运行包编号": run_code,
            "创建时间": _now(),
            "创建日期": _today(),
            "创建人": operator,
            "数据版本": data_version,
            "规则版本": f"R{revision}",
            "样本版本": sample_version_text,
            "口径": "旧口径兼容" if legacy_notes else "当前口径",
            "采样比例": "按规则配置" if override_ratio is None else f"{round(override_ratio * 100, 2):g}%",
            "采样比例值": override_ratio,
            "样本池数量": len(samples),
            "采样数量": total_sampled,
            "采样范围": all_sampled_ids,
            "规则范围": [rule["规则编号"] for rule in executable],
            "规则结果": rule_results,
            "异常样本清单": abnormal,
            "异常样本数": len(abnormal),
            "阻断数量": blocking_count,
            "告警数量": warning_count,
            "整体结论": conclusion,
            "整体通过": overall_pass,
            "下游阻断": downstream_blocked,
            "无法判定": undetermined,
            "边界原因": boundary,
            "历史阻断依据": historical,
            "历史风险": bool(historical),
            "规则差异": [],
            "监控回查": None,
            "请求编号": request_id,
            "执行参数": {
                "规则范围": _text(values.get("规则范围") or values.get("scope")) or "仅已启用规则",
                "指定规则": _as_list(values.get("规则编号") or values.get("ruleCodes")),
                "采样比例口径": "按规则配置" if override_ratio is None else "本次运行覆盖",
                "样本来源": "本次提交的采集结果样本" if _first_present(
                    values, ("样本", "采集结果样本", "samples")) is not None else "采集结果样本池",
                "操作人": operator,
            },
            "样本摘要": {
                "样本池数量": len(samples),
                "采样数量": total_sampled,
                "未采样数量": max(len(samples) - total_sampled, 0),
            },
        }
        RUNS[run_id] = run
        IDEMPOTENCY[dedupe_key] = run_id
        RUN_FINGERPRINTS[fingerprint] = run_id
        if request_id:
            IDEMPOTENCY[f"req:{request_id}"] = run_id
        return run, (
            "校验运行包已生成"
            if not undetermined else "本次校验无法判定，已生成运行包并阻断下游读取"
        ), False


def list_runs(page: int = 1, size: int = 20, conclusion: str | None = None) -> tuple[list[dict[str, Any]], int]:
    with LOCK:
        rows = [dict(run) for run in sorted(RUNS.values(), key=lambda item: int(item["id"]), reverse=True)]
    if conclusion:
        rows = [row for row in rows if row.get("整体结论") == conclusion]
    total = len(rows)
    start = (page - 1) * size
    return rows[start:start + size], total


def get_run(run_id: int) -> dict[str, Any] | None:
    with LOCK:
        run = RUNS.get(run_id)
        return dict(run) if run else None


def run_summary(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": run["id"],
        "运行包编号": run["运行包编号"],
        "创建时间": run["创建时间"],
        "创建日期": run["创建日期"],
        "数据版本": run["数据版本"],
        "规则版本": run["规则版本"],
        "口径": run["口径"],
        "采样比例": run["采样比例"],
        "采样数量": run["采样数量"],
        "样本池数量": run["样本池数量"],
        "异常样本数": run["异常样本数"],
        "阻断数量": run["阻断数量"],
        "整体结论": run["整体结论"],
        "整体通过": run["整体通过"],
        "下游阻断": run["下游阻断"],
        "无法判定": run["无法判定"],
        "规则范围": run["规则范围"],
        "执行参数": run.get("执行参数", {}),
        "监控回查": (run.get("监控回查") or {}).get("记录编号"),
    }


def list_abnormal(run_id: int, blocking_only: bool = False) -> tuple[list[dict[str, Any]], str | None]:
    run = get_run(run_id)
    if run is None:
        return [], f"校验运行包 {run_id} 不存在"
    items = run.get("异常样本清单", [])
    if blocking_only:
        items = [item for item in items if item.get("是否阻断")]
    return [dict(item) for item in items], None


# ---------------------------------------------------------------------------
# 采集监控的接力：待办台账、当日告警、规则差异回写
# ---------------------------------------------------------------------------

def _append_todo(run: dict[str, Any], record_code: str, *, entry: dict[str, Any] | None = None,
                 reason: str | None = None) -> bool:
    global TODO_SEQ
    if entry is not None:
        source_sample = entry["样本编号"]
        rule_code = entry["规则编号"]
        field_name = entry["字段"]
        abnormal_reason = entry["异常原因"]
        level = entry["阻断级别"]
        blocked = entry["是否阻断"]
        diff_field = field_name
        dedupe = f"{run['运行包编号']}|{source_sample}|{rule_code}|{field_name}|{abnormal_reason}"
    else:
        source_sample = "—"
        rule_code = "—"
        diff_field = "本次采样"
        abnormal_reason = reason or "无法判定"
        level = "阻断"
        blocked = True
        dedupe = f"{run['运行包编号']}|__NO_SAMPLE__|{abnormal_reason}"
    if dedupe in TODO_KEYS:
        return False
    TODO_SEQ += 1
    TODO_KEYS.add(dedupe)
    TODO_LEDGER.append({
        "id": TODO_SEQ,
        "待办编号": f"VTODO-{TODO_SEQ:04d}",
        "来源运行包": run["运行包编号"],
        "来源运行包ID": run["id"],
        "来源样本": source_sample,
        "校验规则": rule_code,
        "差异字段": diff_field,
        "异常原因": abnormal_reason,
        "阻断级别": level,
        "是否阻断": blocked,
        "处理状态": "待处理",
        "创建时间": _now(),
        "回查记录": record_code,
        "数据版本": run["数据版本"],
        "去重键": dedupe,
    })
    return True


def _writeback_rules(run: dict[str, Any]) -> list[dict[str, Any]]:
    """把运行包差异写回规则；仅当前规则版本的运行包可覆盖。"""
    diffs: list[dict[str, Any]] = []
    latest_revision = f"R{RULE_REVISION}"
    if run["规则版本"] != latest_revision:
        diffs.append({
            "规则编号": "—",
            "状态": "未写回",
            "原因": (
                f"运行包基于规则版本 {run['规则版本']}，当前规则版本 {latest_revision}；"
                "以数据版本为准，旧口径仅保留查看，不覆盖新规则"
            ),
            "数据版本": run["数据版本"],
        })
        return diffs

    lookup = {_text(row.get("规则编号")): row for row in store.rows("validator")}
    for result in run.get("规则结果", []):
        rule = lookup.get(result["规则编号"])
        if rule is None:
            diffs.append({
                "规则编号": result["规则编号"],
                "状态": "未写回",
                "原因": "校验规则不存在或已归档",
                "数据版本": run["数据版本"],
            })
            continue
        current_run_id = int(rule.get("最近校验运行包ID") or 0)
        if current_run_id > int(run["id"]):
            diffs.append({
                "规则编号": result["规则编号"],
                "状态": "未写回",
                "原因": f"规则已被更新的运行包 {rule.get('最近运行包', '—')} 回写，旧运行包不覆盖",
                "数据版本": run["数据版本"],
            })
            continue
        difference = (
            f"采样 {result['采样数量']} 条，异常 {result['命中数量']} 条，"
            f"阻断 {sum(1 for item in run['异常样本清单'] if item['规则编号'] == result['规则编号'] and item['是否阻断'])} 条"
        )
        rule["最近运行包"] = run["运行包编号"]
        rule["最近校验运行包ID"] = int(run["id"])
        rule["最近校验时间"] = run["创建时间"]
        rule["最近采样比例"] = result["采样比例"]
        rule["最近异常样本数"] = result["命中数量"]
        rule["最近阻断数"] = sum(
            1 for item in run["异常样本清单"]
            if item["规则编号"] == result["规则编号"] and item["是否阻断"]
        )
        rule["最近差异"] = difference
        rule["差异数据版本"] = run["数据版本"]
        diffs.append({
            "规则编号": result["规则编号"],
            "状态": "已写回",
            "原因": difference,
            "约定": "以数据版本为准回写，旧口径运行包不覆盖新版本规则",
            "数据版本": run["数据版本"],
        })
    return diffs


def relay_run(run_id: int) -> tuple[dict[str, Any] | None, str]:
    """采集监控服务接力一个运行包：核对运行包 + 异常样本清单，写待办台账并回写规则。"""
    global RECORD_SEQ
    with LOCK:
        run = RUNS.get(run_id)
        if run is None:
            return None, f"校验运行包 {run_id} 不存在，无法核对"
        if run_id in RELAYED_RUNS:
            return RELAYED_RUNS[run_id], f"运行包 {run['运行包编号']} 已由采集监控服务核对，未重复写入"

        RECORD_SEQ += 1
        record_code = f"MREC-{RECORD_SEQ:04d}"
        record = {
            "id": RECORD_SEQ,
            "记录编号": record_code,
            "来源服务": "采集结果校验",
            "来源服务版本": VALIDATOR_SERVICE_VERSION,
            "监控服务版本": MONITOR_SERVICE_VERSION,
            "来源运行包": run["运行包编号"],
            "来源运行包ID": run["id"],
            "核对内容": ["校验运行包", "异常样本清单"],
            "异常样本数": run["异常样本数"],
            "阻断数量": run["阻断数量"],
            "无法判定": run["无法判定"],
            "当日告警计入": run["异常样本数"] + (1 if run["无法判定"] else 0),
            "数据版本": run["数据版本"],
            "整体结论": run["整体结论"],
            "创建时间": _now(),
            "创建日期": _today(),
        }
        MONITOR_RECORDS[record_code] = record

        added = 0
        for entry in run.get("异常样本清单", []):
            if _append_todo(run, record_code, entry=entry):
                added += 1
        if run["无法判定"]:
            reason = "；".join(run.get("边界原因", [])) or "本次采样无法判定"
            if _append_todo(run, record_code, reason=reason):
                added += 1

        diffs = _writeback_rules(run)
        run["规则差异"] = diffs
        run["监控回查"] = record
        result = {
            "运行包编号": run["运行包编号"],
            "监控记录": record,
            "新增待办数": added,
            "待办总数": len(TODO_LEDGER),
            "规则差异": diffs,
            "当日告警计入": record["当日告警计入"],
        }
        RELAYED_RUNS[run_id] = result
        return result, f"采集监控服务已核对运行包 {run['运行包编号']}，新增待办 {added} 条"


def relay_all() -> dict[str, Any]:
    """监控服务同步全部尚未核对的运行包；重复调用不新增待办。"""
    with LOCK:
        pending = [run_id for run_id in sorted(RUNS) if run_id not in RELAYED_RUNS]
        results = []
        for run_id in pending:
            result, _message = relay_run(run_id)
            if result:
                results.append(result)
        return {
            "核对运行包数": len(results),
            "运行包列表": [item["运行包编号"] for item in results],
            "新增待办数": sum(item["新增待办数"] for item in results),
            "待办总数": len(TODO_LEDGER),
        }


def list_todos(status: str | None = None, run_code: str | None = None) -> list[dict[str, Any]]:
    with LOCK:
        rows = [dict(item) for item in TODO_LEDGER]
    if status:
        rows = [row for row in rows if row.get("处理状态") == status]
    if run_code:
        rows = [row for row in rows if row.get("来源运行包") == run_code]
    return rows


def monitor_status() -> dict[str, Any]:
    """采集监控读取运行包的当日口径：异常样本 + 无法判定均计入当日告警。"""
    today = _today()
    with LOCK:
        all_runs = sorted(RUNS.values(), key=lambda item: int(item["id"]))
        today_runs = [run for run in all_runs if run.get("创建日期") == today]
        blocking_count = sum(int(run.get("阻断数量", 0)) for run in today_runs)
        blocking_run_count = sum(1 for run in today_runs if int(run.get("阻断数量", 0)) > 0)
        abnormal_count = sum(int(run.get("异常样本数", 0)) for run in today_runs)
        undetermined_count = sum(1 for run in today_runs if run.get("无法判定"))
        alerts = abnormal_count + undetermined_count
        todos = [dict(item) for item in TODO_LEDGER]
        pending_todos = sum(1 for item in todos if item.get("处理状态") == "待处理")
        latest = run_summary(all_runs[-1]) if all_runs else None
        if not today_runs:
            empty_reason = "今日暂无校验运行包，尚未产生异常样本或无法判定状态，当日告警为 0"
        elif alerts == 0:
            empty_reason = "今日运行包均未命中异常样本，且不存在无法判定状态"
        else:
            empty_reason = ""
        todo_empty_reason = (
            "暂无校验待办：采集监控服务尚未从校验运行包中核对出异常样本或无法判定状态"
            if not todos else ""
        )
        relay_empty_reason = (
            "暂无未接力的校验运行包：所有运行包都已核对，重复核对不会新增待办"
            if all(run_id in RELAYED_RUNS for run_id in RUNS) else ""
        )
        return {
            "日期": today,
            "服务版本": MONITOR_SERVICE_VERSION,
            "校验服务版本": VALIDATOR_SERVICE_VERSION,
            "数据流": DATA_FLOW,
            "运行包数量": len(all_runs),
            "今日运行包数": len(today_runs),
            "阻断数量": blocking_count,
            "阻断运行包数": blocking_run_count,
            "累计阻断数量": sum(int(run.get("阻断数量", 0)) for run in all_runs),
            "异常样本数": abnormal_count,
            "无法判定数": undetermined_count,
            "当日告警": alerts,
            "待办数": len(todos),
            "待处理待办": pending_todos,
            "最近运行包": latest,
            "空态原因": empty_reason,
            "待办空态原因": todo_empty_reason,
            "接力空态原因": relay_empty_reason,
            "核对说明": "采集监控服务读取校验运行包，把异常样本和无法判定状态计入当日告警",
        }


def list_monitor_records() -> list[dict[str, Any]]:
    with LOCK:
        return [dict(item) for item in MONITOR_RECORDS.values()]


def reset_runs() -> None:
    """测试辅助：清空运行包、待办与版本，不影响业务规则表。"""
    global RUN_SEQ, TODO_SEQ, RECORD_SEQ, RULE_REVISION
    with LOCK:
        RUNS.clear()
        TODO_LEDGER.clear()
        TODO_KEYS.clear()
        MONITOR_RECORDS.clear()
        RELAYED_RUNS.clear()
        IDEMPOTENCY.clear()
        RUN_FINGERPRINTS.clear()
        RUN_SEQ = 0
        TODO_SEQ = 0
        RECORD_SEQ = 0
        RULE_REVISION = 1
