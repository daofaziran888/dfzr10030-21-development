"""采集结果校验接口：维护校验规则，覆盖提交测试、启用规则、停用规则等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import validation_runs
from app.services.validator import ValidatorService

router = APIRouter(prefix="/api/validator", tags=["采集结果校验"])

service = ValidatorService()

LIST_FIELDS = ["规则编号", "校验字段", "校验类型", "期望值", "容错范围", "采样比例", "阻断级别", "规则状态"]
STATUSES = ["草稿", "待测试", "已启用", "已停用"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按规则编号检索"),
    status: str | None = Query(default=None, description="草稿、待测试、已启用、已停用"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、规则编号关键字与状态过滤采集结果校验列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="validator.csv"'})


@router.get("/runs", response_model=PageResult[dict])
def list_runs(
    conclusion: str | None = Query(default=None, description="通过、告警、阻断、无法判定"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """校验运行包列表：每条都带采样比例、阻断数量与整体结论，便于值班人员回查。"""
    items, total = validation_runs.list_runs(page=page, size=size, conclusion=conclusion)
    return PageResult(items=[validation_runs.run_summary(item) for item in items],
                      total=total, page=page, size=size)


@router.post("/runs", response_model=ActionResult)
def create_run(payload: EntryPayload) -> ActionResult:
    """按校验字段、采样比例与阻断级别执行一次校验，生成运行包和异常样本清单。

    `values` 支持：规则编号（列表或逗号分隔）、采样比例（覆盖规则配置）、
    样本/采集结果样本（不传时使用采集结果样本池）、requestId（幂等键）、
    expectedVersion（数据版本，版本不一致时不写入）。
    """
    run, message, _reused = validation_runs.create_run(payload.values)
    if run is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=validation_runs.run_summary(run))


@router.get("/runs/export")
def export_runs() -> StreamingResponse:
    """导出校验运行包汇总；异常样本明细在运行包详情里单独导出。"""
    items, _ = validation_runs.list_runs(page=1, size=1000000)
    output = io.StringIO()
    output.write("﻿")
    writer = csv.writer(output, lineterminator="\n")
    fields = ["运行包编号", "创建时间", "数据版本", "口径", "采样比例", "采样数量",
              "异常样本数", "阻断数量", "整体结论", "下游阻断", "规则范围"]
    writer.writerow(["id", *fields])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in fields)]:
            if isinstance(value, (list, tuple)):
                value = "、".join(str(part) for part in value)
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="validator-runs.csv"'})


@router.get("/runs/{run_id}/abnormal", response_model=list[dict])
def list_run_abnormal(run_id: int, blocking: bool = Query(default=False)) -> list[dict]:
    """异常样本清单：每条都可回查采样范围、阻断级别与命中的阻断规则。"""
    items, error = validation_runs.list_abnormal(run_id, blocking_only=blocking)
    if error:
        raise HTTPException(status_code=404, detail=error)
    return items


@router.get("/runs/{run_id}/abnormal/export")
def export_run_abnormal(run_id: int) -> StreamingResponse:
    """导出某个运行包的异常样本清单。"""
    items, error = validation_runs.list_abnormal(run_id)
    if error:
        raise HTTPException(status_code=404, detail=error)
    output = io.StringIO()
    output.write("﻿")
    writer = csv.writer(output, lineterminator="\n")
    fields = ["样本编号", "采集批次", "字段", "字段值", "规则编号", "校验类型", "期望值",
              "异常原因", "阻断级别", "是否阻断", "采样比例", "采样范围", "运行包编号"]
    writer.writerow(["id", *fields])
    for index, item in enumerate(items, start=1):
        row = []
        for value in [index, *(item.get(field, "") for field in fields)]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="validator-run-{run_id}-abnormal.csv"'})


@router.get("/runs/{run_id}", response_model=dict)
def get_run(run_id: int) -> dict:
    """运行包详情：规则结果、异常样本清单、边界原因、历史阻断依据与监控回查记录。"""
    run = validation_runs.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"校验运行包 {run_id} 不存在")
    return run


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条校验规则明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"校验规则 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条校验规则，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="校验规则已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条校验规则执行提交测试、启用规则、停用规则；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
