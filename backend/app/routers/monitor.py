"""采集监控 分析看板接口：汇总指标、参数运行与结果导出。"""
from __future__ import annotations

import csv
import io
import random
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services import validation_runs
from app.services.monitor import MonitorService

router = APIRouter(prefix="/api/monitor", tags=["采集监控"])
service = MonitorService()
LIST_FIELDS = ["面板编号", "采集源总数", "今日采集量", "成功率", "平均延迟", "失败重试", "异常告警", "更新时间"]
STATUSES = ["正常", "预警", "告警"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("/summary")
def get_summary() -> dict[str, Any]:
    return {"metrics": [
        {"label": "覆盖数据源", "value": random.randint(12, 45), "trend": random.randint(-12, 25)},
        {"label": "最新指标值", "value": round(random.uniform(0.72, 0.99), 3), "trend": random.randint(-8, 15)},
        {"label": "异常记录", "value": random.randint(0, 8), "trend": -random.randint(1, 10)},
    ]}


@router.get("")
def list_results() -> dict[str, Any]:
    return {"items": service.list_results()}


@router.post("/run")
def run_analysis(payload: EntryPayload) -> dict[str, Any]:
    results = service.run_analysis(payload.values)
    return {"items": results}


@router.get("/validation")
def validation_monitor() -> dict[str, Any]:
    """校验监控：读取校验运行包，把异常样本和无法判定状态计入当日告警。"""
    status = validation_runs.monitor_status()
    status["待办"] = validation_runs.list_todos()
    status["核对记录"] = validation_runs.list_monitor_records()
    return status


@router.post("/validation/relay")
def relay_validation_runs() -> dict[str, Any]:
    """采集监控服务接力校验运行包：核对运行包与异常样本清单，写待办台账并回写规则。"""
    return {"ok": True, **validation_runs.relay_all()}


@router.get("/validation/todos")
def list_validation_todos(
    status: str | None = Query(default=None, description="待处理、已处理"),
    run: str | None = Query(default=None, description="按运行包编号筛选"),
) -> dict[str, Any]:
    """校验待办台账：每条待办都能回到来源运行包、异常样本、阻断规则与数据版本。"""
    items = validation_runs.list_todos(status=status, run_code=run)
    return {"items": items, "total": len(items)}


@router.post("/validation/runs/{run_id}/relay")
def relay_validation_run(run_id: int) -> dict[str, Any]:
    """只接力单个运行包；重复接力返回已有核对结果，不重复写待办。"""
    result, message = validation_runs.relay_run(run_id)
    if result is None:
        raise HTTPException(status_code=404, detail=message)
    return {"ok": True, "message": message, **result}


@router.get("/export")
def export_entries() -> StreamingResponse:
    items = service.list_results()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id"]
    if items:
        all_keys = list(items[0].keys())
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=monitor.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"监控面板 {entry_id} 不存在")
    return entry
