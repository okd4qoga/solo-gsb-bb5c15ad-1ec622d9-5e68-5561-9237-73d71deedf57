"""FastAPI 入口与路由。"""
import math
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from . import models, schemas, services
from .database import Base, engine, get_db

DbSession = Annotated[Session, Depends(get_db)]


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动即建表，起到“初始化依赖”的作用；已存在则 no-op
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="冷链周转箱事件接收 API",
    description=(
        "接收设备上报的温度事件。\n\n"
        "- 相同箱号 + 相同序号 + 相同内容：重复上报，幂等忽略（200，status=duplicate）\n"
        "- 相同箱号 + 相同序号但内容不同：拒绝（409）\n"
        "- 乱序事件正常入库，最新状态始终按最大序号返回\n"
        "- 事件分页：GET /boxes/{box_code}/events 按 seq 升序分批返回（page_size 1~100，"
        "缺省 50）；首页固定该箱当前最大事件 id 为读取水位，游标保存水位、箱号与本页"
        "末尾 seq，续页只取 id 不超过水位且 seq 更大的记录，翻页期间的乱序补报不改变"
        "本轮遍历结果；末页游标为 null，游标格式错误或跨箱使用返回 422\n"
        "- 交接快照：POST /handoffs 在同一事务中冻结各箱当前状态；"
        "同编号同内容幂等返回原快照，内容不同拒绝（409）；快照只读，不受后续补报影响\n"
        "- 交接对账：GET /handoffs/{from_code}/reconcile/{to_code} 只读比较两份已冻结快照，"
        "给出新增/移出箱、共同箱结论变化与最少移动箱数\n"
        "- 温度漂移校准：POST /calibrations 按箱号 + 版本号保存不可变锚点列表（至少 2 个、"
        "序号严格递增、偏移有限），同箱同版本原样重交幂等、异内容 409；"
        "GET /boxes/{box_code}/calibration-trajectory 按锚点线性插值给出校准温度，"
        "缺报序号不补事件，且不改写原始事件或现有状态与分析结果\n"
        "- 群体遥测静默：GET /telemetry-outages 按去重箱号列表 + 共同序号区间检测"
        "相邻校正间隔严格超过阈值的静默区间（阈值按请求原值精确解析为十进制数，"
        "间隔恰好等于阈值不算静默），端点扫描输出至少指定数量箱同时静默的"
        "极大时间段及段内峰值同时静默数（只读）\n"
        "- 校准版本切换影响审计：GET /boxes/{box_code}/calibration-impact 复用两版"
        "已存锚点的序号线性插值分别计算校准温度，按低于下限/区间内（含边界）/高于上限"
        "判结论，只统计结论改变的事件，将序号连续且变化方向相同的事件合成影响段"
        "（缺号断段，不补点），输出起止序号、事件数、原/新结论及最大校准温差（只读）\n"
        "- 多箱补报调度预览：POST /retransmission-preview 对 1~4 箱（各带权重）在"
        "共同序号闭区间内找缺号，按“指令只请求同箱连续缺号、消耗=握手开销+长度”"
        "在总预算内最大化 Σ(补报序号数×权重)，同分比指令数、再按箱号/起点/终点"
        "字典序定唯一方案，返回指令、预算消耗与各箱仍缺报区间（预览不写库）"
    ),
    version="1.7.0",
    lifespan=lifespan,
)


def _json_safe(value):
    """递归把 NaN/Inf 替换为字符串。

    Starlette 的 JSONResponse 不允许非有限浮点；请求体里带 NaN/Infinity 时，
    校验错误的 input 会带上这些值，若不替换，422 响应自身序列化失败变成 500。
    """
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


@app.exception_handler(RequestValidationError)
async def handle_request_validation(request, exc: RequestValidationError) -> JSONResponse:
    # 与 FastAPI 默认处理一致（422 + detail），仅先把错误详情中的 NaN/Inf 转为字符串
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": _json_safe(exc.errors())}),
    )


@app.get("/health", tags=["system"], summary="健康检查")
def health() -> dict:
    return {"status": "ok"}


@app.post(
    "/events",
    tags=["events"],
    response_model=schemas.EventAck,
    summary="上报一个温度事件（支持重试，幂等）",
)
def post_event(payload: schemas.EventIn, db: DbSession, response: Response) -> schemas.EventAck:
    event, duplicate = services.submit_event(db, payload)
    # 首次入库 201；重复上报 200，表示请求成功但未产生新状态
    response.status_code = (
        status.HTTP_200_OK if duplicate else status.HTTP_201_CREATED
    )
    return schemas.EventAck(
        status="duplicate" if duplicate else "accepted",
        event=schemas.EventOut.model_validate(event),
    )


@app.get(
    "/boxes",
    tags=["boxes"],
    response_model=list[schemas.BoxListItem],
    summary="所有上报过的箱号",
)
def get_boxes(db: DbSession) -> list[schemas.BoxListItem]:
    return [schemas.BoxListItem(box_code=code) for code in services.list_boxes(db)]


@app.get(
    "/boxes/{box_code}/state",
    tags=["boxes"],
    response_model=schemas.BoxState,
    summary="某箱子按最大序号计算的最新状态（乱序安全）",
)
def get_box_state(box_code: str, db: DbSession) -> schemas.BoxState:
    state = services.latest_state(db, box_code.strip())
    if state is None:
        raise HTTPException(status_code=404, detail={"error": "box_not_found", "box_code": box_code})
    return schemas.BoxState(
        box_code=state.box_code,
        seq=state.seq,
        temperature=state.temperature,
        event_time=state.event_time,
    )


@app.get(
    "/states",
    tags=["boxes"],
    response_model=list[schemas.BoxState],
    summary="所有箱子的最新状态",
)
def get_states(db: DbSession) -> list[schemas.BoxState]:
    return [
        schemas.BoxState(
            box_code=e.box_code,
            seq=e.seq,
            temperature=e.temperature,
            event_time=e.event_time,
        )
        for e in services.latest_states(db)
    ]


@app.get(
    "/boxes/{box_code}/events",
    tags=["events"],
    response_model=schemas.EventPage,
    summary="某箱子的事件（按序号升序分批，水位游标翻页，补报不影响本轮遍历）",
)
def get_box_events(
    box_code: str,
    db: DbSession,
    page_size: Annotated[
        int,
        Query(
            ge=1,
            le=services.EVENTS_PAGE_MAX_SIZE,
            description="每页最多返回的事件条数（1~100）",
        ),
    ] = services.EVENTS_PAGE_DEFAULT_SIZE,
    cursor: Annotated[
        str | None,
        Query(description="上一页响应中的 next_cursor，原样回传；不传表示从首页开始"),
    ] = None,
) -> schemas.EventPage:
    page = services.list_events_page(db, box_code.strip(), page_size, cursor)
    if page is None:
        raise HTTPException(status_code=404, detail={"error": "box_not_found", "box_code": box_code})
    return page


def _handoff_out(
    snapshot: models.HandoffSnapshot, items: list[models.HandoffSnapshotItem]
) -> schemas.HandoffOut:
    return schemas.HandoffOut(
        handoff_code=snapshot.handoff_code,
        temp_lower=snapshot.temp_lower,
        temp_upper=snapshot.temp_upper,
        box_count=snapshot.box_count,
        normal_count=snapshot.normal_count,
        out_of_range_count=snapshot.out_of_range_count,
        missing_count=snapshot.missing_count,
        created_at=snapshot.created_at,
        items=[
            schemas.HandoffItemOut(
                box_code=item.box_code,
                status=item.status,
                seq=item.seq,
                temperature=item.temperature,
                event_time=item.event_time,
            )
            for item in items
        ],
    )


@app.post(
    "/handoffs",
    tags=["handoffs"],
    response_model=schemas.HandoffOut,
    summary="创建交接快照（同一事务冻结各箱当前最大序号的状态，幂等）",
)
def post_handoff(
    payload: schemas.HandoffIn, db: DbSession, response: Response
) -> schemas.HandoffOut:
    snapshot, items, created = services.create_handoff(db, payload)
    # 新建 201；同编号同内容的重复请求 200，返回原快照
    response.status_code = (
        status.HTTP_201_CREATED if created else status.HTTP_200_OK
    )
    return _handoff_out(snapshot, items)


@app.get(
    "/handoffs/{handoff_code}",
    tags=["handoffs"],
    response_model=schemas.HandoffOut,
    summary="交接快照的冻结明细与各类数量",
)
def get_handoff(handoff_code: str, db: DbSession) -> schemas.HandoffOut:
    result = services.get_handoff(db, handoff_code.strip())
    if result is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "handoff_not_found", "handoff_code": handoff_code},
        )
    snapshot, items = result
    return _handoff_out(snapshot, items)


@app.get(
    "/handoffs/{from_code}/reconcile/{to_code}",
    tags=["handoffs"],
    response_model=schemas.HandoffReconcileResult,
    summary="只读对账两份交接快照的箱单明细（新增/移出箱、共同箱变化与最少移动箱数）",
)
def get_handoff_reconcile(
    from_code: str, to_code: str, db: DbSession
) -> schemas.HandoffReconcileResult:
    return services.reconcile_handoffs(db, from_code.strip(), to_code.strip())


@app.get(
    "/boxes/{box_code}/clock-audit",
    tags=["boxes"],
    response_model=list[schemas.ClockAuditItem],
    summary="区间事件的设备时钟回拨校正建议（等权保序回归，不改原始记录）",
)
def get_box_clock_audit(
    box_code: str,
    db: DbSession,
    seq_from: Annotated[int, Query(ge=0, description="起始序号（含）")],
    seq_to: Annotated[int, Query(ge=0, description="结束序号（含）")],
) -> list[schemas.ClockAuditItem]:
    if seq_from > seq_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "invalid_range",
                "message": f"seq_from({seq_from}) 不能大于 seq_to({seq_to})",
                "seq_from": seq_from,
                "seq_to": seq_to,
            },
        )
    items = services.clock_audit(db, box_code.strip(), seq_from, seq_to)
    if items is None:
        raise HTTPException(status_code=404, detail={"error": "box_not_found", "box_code": box_code})
    return items


@app.get(
    "/boxes/{box_code}/heat-exposure",
    tags=["boxes"],
    response_model=schemas.HeatExposureResult,
    summary="区间事件的超温积分（℃·秒，线性插值 + 保序校正时间，不改原始记录）",
)
def get_box_heat_exposure(
    box_code: str,
    db: DbSession,
    seq_from: Annotated[int, Query(ge=0, description="起始序号（含）")],
    seq_to: Annotated[int, Query(ge=0, description="结束序号（含）")],
    upper_limit: Annotated[
        float, Query(allow_inf_nan=False, description="温度上限（℃），超过部分计入积分")
    ],
    max_gap_seconds: Annotated[
        float,
        Query(
            gt=0,
            allow_inf_nan=False,
            description="允许计入的最大相邻校正间隔（秒，正数）；间隔为零或超过它的段不计入",
        ),
    ],
) -> schemas.HeatExposureResult:
    if seq_from > seq_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "invalid_range",
                "message": f"seq_from({seq_from}) 不能大于 seq_to({seq_to})",
                "seq_from": seq_from,
                "seq_to": seq_to,
            },
        )
    result = services.heat_exposure(
        db, box_code.strip(), seq_from, seq_to, upper_limit, max_gap_seconds
    )
    if result is None:
        raise HTTPException(status_code=404, detail={"error": "box_not_found", "box_code": box_code})
    return result


@app.get(
    "/boxes/{box_code}/temperature-median",
    tags=["boxes"],
    response_model=schemas.TemperatureMedianResult,
    summary="区间按持续时间加权的温度中位数（线性插值 + 保序校正时间，不改原始记录）",
)
def get_box_temperature_median(
    box_code: str,
    db: DbSession,
    seq_from: Annotated[int, Query(ge=0, description="起始序号（含）")],
    seq_to: Annotated[int, Query(ge=0, description="结束序号（含）")],
    max_gap_seconds: Annotated[
        float,
        Query(
            gt=0,
            allow_inf_nan=False,
            description="允许计入的最大相邻校正间隔（秒，正数）；间隔为零或超过它的段不计入",
        ),
    ],
) -> schemas.TemperatureMedianResult:
    if seq_from > seq_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "invalid_range",
                "message": f"seq_from({seq_from}) 不能大于 seq_to({seq_to})",
                "seq_from": seq_from,
                "seq_to": seq_to,
            },
        )
    result = services.temperature_median(
        db, box_code.strip(), seq_from, seq_to, max_gap_seconds
    )
    if result is None:
        raise HTTPException(status_code=404, detail={"error": "box_not_found", "box_code": box_code})
    return result


@app.get(
    "/boxes/{box_code}/merkle-proof",
    tags=["boxes"],
    response_model=schemas.MerkleProofResult,
    summary="区间事件的 Merkle 包含证明（按 seq 升序建树，只读不写回）",
)
def get_box_merkle_proof(
    box_code: str,
    db: DbSession,
    seq_from: Annotated[int, Query(ge=0, description="起始序号（含）")],
    seq_to: Annotated[int, Query(ge=0, description="结束序号（含）")],
    target_seq: Annotated[
        int, Query(ge=0, description="目标事件序号（须为区间内实际存在的事件）")
    ],
) -> schemas.MerkleProofResult:
    if seq_from > seq_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "invalid_range",
                "message": f"seq_from({seq_from}) 不能大于 seq_to({seq_to})",
                "seq_from": seq_from,
                "seq_to": seq_to,
            },
        )
    result = services.merkle_proof(db, box_code.strip(), seq_from, seq_to, target_seq)
    if result is None:
        raise HTTPException(status_code=404, detail={"error": "box_not_found", "box_code": box_code})
    return result


@app.get(
    "/boxes/{box_code}/trajectory-compression",
    tags=["boxes"],
    response_model=schemas.TrajectoryCompressionResult,
    summary="区间温度轨迹压缩预览（最少保留点，线性插值误差受限，只读不写回）",
)
def get_box_trajectory_compression(
    box_code: str,
    db: DbSession,
    seq_from: Annotated[int, Query(ge=0, description="起始序号（含）")],
    seq_to: Annotated[int, Query(ge=0, description="结束序号（含）")],
    max_error: Annotated[
        float,
        Query(
            ge=0,
            allow_inf_nan=False,
            description="允许的最大重建误差（℃），非负有限值；被省略事件的重建温度与原值之差不得超过它",
        ),
    ],
) -> schemas.TrajectoryCompressionResult:
    if seq_from > seq_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "invalid_range",
                "message": f"seq_from({seq_from}) 不能大于 seq_to({seq_to})",
                "seq_from": seq_from,
                "seq_to": seq_to,
            },
        )
    result = services.trajectory_compression(
        db, box_code.strip(), seq_from, seq_to, max_error
    )
    if result is None:
        raise HTTPException(status_code=404, detail={"error": "box_not_found", "box_code": box_code})
    return result


def _calibration_out(
    calib: models.CalibrationSet, anchors: list[models.CalibrationAnchor]
) -> schemas.CalibrationOut:
    return schemas.CalibrationOut(
        box_code=calib.box_code,
        version=calib.version,
        anchor_count=calib.anchor_count,
        created_at=calib.created_at,
        anchors=[
            schemas.CalibrationAnchorOut(
                seq=anchor.seq, temp_offset=anchor.temp_offset
            )
            for anchor in anchors
        ],
    )


@app.post(
    "/calibrations",
    tags=["calibrations"],
    response_model=schemas.CalibrationOut,
    summary="按箱号 + 版本号保存不可变的温度漂移校准锚点列表（幂等）",
)
def post_calibration(
    payload: schemas.CalibrationIn, db: DbSession, response: Response
) -> schemas.CalibrationOut:
    calib, anchors, created = services.save_calibration(db, payload)
    # 新建 201；同箱同版本同内容的原样重交 200，返回原锚点列表
    response.status_code = (
        status.HTTP_201_CREATED if created else status.HTTP_200_OK
    )
    return _calibration_out(calib, anchors)


@app.get(
    "/boxes/{box_code}/calibrations/{version}",
    tags=["calibrations"],
    response_model=schemas.CalibrationOut,
    summary="查询某箱某版本的不可变校准锚点列表",
)
def get_calibration(box_code: str, version: int, db: DbSession) -> schemas.CalibrationOut:
    result = services.get_calibration(db, box_code.strip(), version)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "calibration_not_found",
                "box_code": box_code,
                "version": version,
            },
        )
    calib, anchors = result
    return _calibration_out(calib, anchors)


@app.get(
    "/boxes/{box_code}/calibration-trajectory",
    tags=["calibrations"],
    response_model=schemas.CalibrationTrajectoryResult,
    summary="指定版本与序号区间的温度漂移校准轨迹（锚点间线性插值，只读不改原始记录）",
)
def get_box_calibration_trajectory(
    box_code: str,
    db: DbSession,
    version: Annotated[int, Query(ge=0, description="校准版本号")],
    seq_from: Annotated[int, Query(ge=0, description="起始序号（含）")],
    seq_to: Annotated[int, Query(ge=0, description="结束序号（含）")],
) -> schemas.CalibrationTrajectoryResult:
    if seq_from > seq_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "invalid_range",
                "message": f"seq_from({seq_from}) 不能大于 seq_to({seq_to})",
                "seq_from": seq_from,
                "seq_to": seq_to,
            },
        )
    result = services.calibration_trajectory(
        db, box_code.strip(), version, seq_from, seq_to
    )
    if result is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "calibration_not_found",
                "box_code": box_code,
                "version": version,
            },
        )
    return result


@app.get(
    "/boxes/{box_code}/calibration-impact",
    tags=["calibrations"],
    response_model=schemas.CalibrationImpactResult,
    summary="校准版本切换的温度结论影响审计（两版锚点各自插值，只读合成影响段）",
)
def get_box_calibration_impact(
    box_code: str,
    db: DbSession,
    from_version: Annotated[int, Query(ge=0, description="原校准版本号")],
    to_version: Annotated[int, Query(ge=0, description="新校准版本号")],
    seq_from: Annotated[int, Query(ge=0, description="起始序号（含）")],
    seq_to: Annotated[int, Query(ge=0, description="结束序号（含）")],
    temp_lower: Annotated[
        float, Query(allow_inf_nan=False, description="温度下限（℃），恰等于算区间内")
    ],
    temp_upper: Annotated[
        float, Query(allow_inf_nan=False, description="温度上限（℃），恰等于算区间内")
    ],
) -> schemas.CalibrationImpactResult:
    if seq_from > seq_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "invalid_range",
                "message": f"seq_from({seq_from}) 不能大于 seq_to({seq_to})",
                "seq_from": seq_from,
                "seq_to": seq_to,
            },
        )
    if temp_lower > temp_upper:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "invalid_range",
                "message": f"temp_lower({temp_lower}) 不能大于 temp_upper({temp_upper})",
                "temp_lower": temp_lower,
                "temp_upper": temp_upper,
            },
        )
    result = services.calibration_impact(
        db,
        box_code.strip(),
        from_version,
        to_version,
        seq_from,
        seq_to,
        temp_lower,
        temp_upper,
    )
    if result is None:
        raise HTTPException(status_code=404, detail={"error": "box_not_found", "box_code": box_code})
    return result


@app.get(
    "/telemetry-outages",
    tags=["telemetry"],
    response_model=schemas.TelemetryOutagesResult,
    summary="多箱群体遥测静默检测（端点扫描，只读不改原始记录）",
)
def get_telemetry_outages(
    db: DbSession,
    box_codes: Annotated[
        list[str],
        Query(
            min_length=1,
            description="参与检测的箱号列表（重复参数逐个给出，如 "
            "box_codes=A&box_codes=B；自动按顺序去重，至少 1 个）",
        ),
    ],
    seq_from: Annotated[int, Query(ge=0, description="起始序号（含）")],
    seq_to: Annotated[int, Query(ge=0, description="结束序号（含）")],
    max_silence_seconds: Annotated[
        Decimal,
        Query(
            gt=0,
            allow_inf_nan=False,
            description="最大允许静默秒数（正数，按请求原值精确解析为十进制数，"
            "不经过二进制浮点）；相邻校正时间间隔严格大于它才计为静默，恰好等于不算",
        ),
    ],
    min_simultaneous_boxes: Annotated[
        int, Query(ge=1, description="最少同时静默箱数（正整数）")
    ],
) -> schemas.TelemetryOutagesResult:
    if seq_from > seq_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "invalid_range",
                "message": f"seq_from({seq_from}) 不能大于 seq_to({seq_to})",
                "seq_from": seq_from,
                "seq_to": seq_to,
            },
        )
    # 阈值已按请求原值精确解析为正且有限的十进制数；但超出 double 有限表示范围
    # 的值（如 1e999）无法在响应中回显（float 溢出为 inf），按非法阈值拒绝
    if not math.isfinite(float(max_silence_seconds)):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "invalid_threshold",
                "message": (
                    f"max_silence_seconds({max_silence_seconds}) 超出可表示的有限数值范围"
                ),
                "max_silence_seconds": str(max_silence_seconds),
            },
        )
    # 箱号去空白后按请求顺序去重；空箱号或超长箱号视为参数无效
    deduped: list[str] = []
    seen: set[str] = set()
    for code in box_codes:
        stripped = code.strip()
        if not stripped or len(stripped) > 64:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error": "invalid_box_code",
                    "message": "箱号去空白后不能为空且长度不能超过 64 字符",
                    "box_code": code,
                },
            )
        if stripped not in seen:
            seen.add(stripped)
            deduped.append(stripped)
    return services.telemetry_outages(
        db, deduped, seq_from, seq_to, max_silence_seconds, min_simultaneous_boxes
    )


@app.post(
    "/retransmission-preview",
    tags=["retransmission"],
    response_model=schemas.RetransmissionPreviewResult,
    summary="多箱设备补报调度预览（预算内最大化加权补报数，只读不写库）",
)
def post_retransmission_preview(
    payload: schemas.RetransmissionPreviewIn, db: DbSession
) -> schemas.RetransmissionPreviewResult:
    return services.retransmission_preview(db, payload)
