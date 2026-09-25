"""业务逻辑：事件入库、幂等去重、乱序后的最新状态查询、事件水位游标分页、时钟回拨校正、超温积分、轨迹压缩预览、Merkle 包含证明、交接快照、交接箱单对账、温度漂移校准、群体遥测静默检测、多箱补报调度预览。"""
import base64
import hashlib
import json
import math
from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from . import models, schemas

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# 单次轨迹压缩预览允许读取的最大区间事件数，超过则拒绝（422）
TRAJECTORY_COMPRESSION_MAX_EVENTS = 120

# 交接快照逐箱结论取值
HANDOFF_STATUS_NORMAL = "normal"
HANDOFF_STATUS_OUT_OF_RANGE = "out_of_range"
HANDOFF_STATUS_MISSING = "missing"

# 校准温度相对给定阈值区间的结论取值（校准版本切换影响审计）
CALIBRATION_STATUS_BELOW = "below"    # 严格低于下限
CALIBRATION_STATUS_NORMAL = "normal"  # 区间内（含边界）
CALIBRATION_STATUS_ABOVE = "above"    # 严格高于上限


def _content_equal(existing: models.BoxEvent, payload: schemas.EventIn) -> bool:
    """内容是否一致（温度 + 事件时间）。

    event_time 在 schema 层已统一换算为 UTC naive，可直接比较；
    设备重试会原样重发 JSON，float 解析结果相同，故直接判等。
    """
    return (
        existing.temperature == payload.temperature
        and existing.event_time == payload.event_time
    )


def submit_event(db: Session, payload: schemas.EventIn) -> tuple[models.BoxEvent, bool]:
    """写入一个事件。

    返回 (事件行, 是否重复)。
    - 同箱号同序号且内容相同：视为设备重试/延迟的重复上报，幂等返回原行，不改状态
    - 同箱号同序号但内容不同：拒绝（409）
    - 新序号：直接入库（即使乱序，不要求 seq 连续）
    """
    existing = db.scalar(
        select(models.BoxEvent).where(
            models.BoxEvent.box_code == payload.box_code,
            models.BoxEvent.seq == payload.seq,
        )
    )

    if existing is not None:
        if not _content_equal(existing, payload):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "seq_conflict",
                    "message": (
                        f"箱号 {payload.box_code} 的序号 {payload.seq} 已存在，"
                        "但温度/事件时间与本次上报不一致，已拒绝"
                    ),
                    "existing": {
                        "temperature": existing.temperature,
                        "event_time": existing.event_time.isoformat() + "Z",
                    },
                    "received": {
                        "temperature": payload.temperature,
                        "event_time": payload.event_time.isoformat() + "Z",
                    },
                },
            )
        return existing, True

    event = models.BoxEvent(
        box_code=payload.box_code,
        seq=payload.seq,
        temperature=payload.temperature,
        event_time=payload.event_time,
    )
    db.add(event)
    try:
        db.flush()  # 触发唯一约束，兜住并发下的竞态
    except IntegrityError:
        db.rollback()
        # 并发请求抢先写入：以库中记录为准重新判定
        existing = db.scalar(
            select(models.BoxEvent).where(
                models.BoxEvent.box_code == payload.box_code,
                models.BoxEvent.seq == payload.seq,
            )
        )
        if existing is None:  # 理论上不会发生
            raise
        if not _content_equal(existing, payload):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "seq_conflict", "message": "同序号内容冲突，已拒绝"},
            )
        return existing, True

    db.commit()
    db.refresh(event)
    return event, False


def list_boxes(db: Session) -> list[str]:
    rows = db.scalars(
        select(models.BoxEvent.box_code)
        .distinct()
        .order_by(models.BoxEvent.box_code)
    ).all()
    return list(rows)


def _latest_subquery():
    """每个箱号当前最大序号（即最新状态），乱序入库也不受影响。"""
    return (
        select(
            models.BoxEvent.box_code.label("box_code"),
            func.max(models.BoxEvent.seq).label("max_seq"),
        )
        .group_by(models.BoxEvent.box_code)
        .subquery()
    )


def latest_states(db: Session) -> list[models.BoxEvent]:
    latest = _latest_subquery()
    rows = db.scalars(
        select(models.BoxEvent)
        .join(
            latest,
            (models.BoxEvent.box_code == latest.c.box_code)
            & (models.BoxEvent.seq == latest.c.max_seq),
        )
        .order_by(models.BoxEvent.box_code)
    ).all()
    return list(rows)


def latest_state(db: Session, box_code: str) -> models.BoxEvent | None:
    latest = _latest_subquery()
    return db.scalar(
        select(models.BoxEvent)
        .join(
            latest,
            (models.BoxEvent.box_code == latest.c.box_code)
            & (models.BoxEvent.seq == latest.c.max_seq),
        ).where(models.BoxEvent.box_code == box_code)
    )


# 事件分页：page_size 允许范围与缺省值（路由层 Query 按 1~100 校验）
EVENTS_PAGE_DEFAULT_SIZE = 50
EVENTS_PAGE_MAX_SIZE = 100

# 事件分页游标格式版本号，不匹配的游标一律视为格式错误
_EVENTS_CURSOR_VERSION = 1


def _encode_events_cursor(box_code: str, watermark: int, last_seq: int) -> str:
    """把翻页状态（读取水位、箱号、本页末尾 seq）编码为不透明游标字符串。

    内容为 JSON（版本号 + 箱号 + 水位 + 末尾 seq），base64url 编码；
    仅供客户端原样回传，服务端续页时严格校验。
    """
    payload = {
        "v": _EVENTS_CURSOR_VERSION,
        "box_code": box_code,
        "watermark": watermark,
        "last_seq": last_seq,
    }
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_events_cursor(cursor: str, box_code: str) -> tuple[int, int]:
    """解码并校验续页游标，返回 (读取水位, 上页末尾 seq)。

    格式错误（非 base64url/JSON、字段缺失或多出、类型或取值非法、版本不符）
    抛 422（invalid_cursor）；游标内箱号与路径箱号不一致（跨箱使用）
    抛 422（cursor_box_mismatch）。

    水位与末尾 seq 均为参与 SQL 绑定的整数，除非负外还必须落在
    SQLite 有符号 64 位范围内（0 ~ 2^63-1），否则续页查询会在绑定参数时
    抛 OverflowError（500），故越界一律按游标格式错误返回 422。
    """
    try:
        raw = base64.b64decode(cursor.encode("ascii"), altchars=b"-_", validate=True)
        payload = json.loads(raw.decode("utf-8"))
    except ValueError:
        # binascii.Error / JSONDecodeError / Unicode*Error 均为 ValueError 子类
        payload = None

    def _is_int(value) -> bool:
        # bool 是 int 子类，明确排除，避免 true/false 被当成序号或水位
        return isinstance(value, int) and not isinstance(value, bool)

    # SQLite 有符号 64 位上界：游标整数参与 SQL 绑定，越界会在查询时
    # 抛 OverflowError，这里在入口统一拒绝（422 invalid_cursor）
    int64_max = schemas.SQLITE_INTEGER_MAX

    if (
        not isinstance(payload, dict)
        or set(payload) != {"v", "box_code", "watermark", "last_seq"}
        or payload["v"] != _EVENTS_CURSOR_VERSION
        or not isinstance(payload["box_code"], str)
        or not _is_int(payload["watermark"])
        or not 0 <= payload["watermark"] <= int64_max
        or not _is_int(payload["last_seq"])
        or not 0 <= payload["last_seq"] <= int64_max
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "invalid_cursor",
                "message": "游标格式错误：请原样回传上一页响应中的 next_cursor",
            },
        )

    if payload["box_code"] != box_code:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "cursor_box_mismatch",
                "message": (
                    f"游标属于箱号 {payload['box_code']}，"
                    f"不能用于查询箱号 {box_code} 的事件"
                ),
                "cursor_box_code": payload["box_code"],
                "box_code": box_code,
            },
        )
    return payload["watermark"], payload["last_seq"]


def list_events_page(
    db: Session, box_code: str, page_size: int, cursor: str | None
) -> schemas.EventPage | None:
    """按 seq 升序分批读取某箱事件（水位游标翻页，只读）。

    - 首页（无游标）：先把该箱**当前最大事件 id** 固定为读取水位，
      只取 id <= 水位的记录；箱号从未上报过返回 None，由路由层转 404
    - 续页（带游标）：游标携带水位、箱号与上页末尾 seq，只取
      id <= 水位 且 seq > 末尾 seq 的记录；翻页期间新入库（含乱序补报）
      的事件 id 必然大于水位，不会进入本轮遍历结果
    - 游标格式错误或跨箱使用抛 422；多取一条判断是否还有下一页，
      末页 next_cursor 为 null
    """
    if cursor is None:
        watermark = db.scalar(
            select(func.max(models.BoxEvent.id)).where(
                models.BoxEvent.box_code == box_code
            )
        )
        if watermark is None:
            return None
        last_seq = -1  # seq 均 >= 0，seq > -1 即从头开始
    else:
        watermark, last_seq = _decode_events_cursor(cursor, box_code)
        box_exists = db.scalar(
            select(func.count())
            .select_from(models.BoxEvent)
            .where(models.BoxEvent.box_code == box_code)
        )
        if not box_exists:
            return None

    # 多取一条：超过 page_size 说明还有下一页，使恰好到末尾的页也直接给出 null 游标
    rows = list(
        db.scalars(
            select(models.BoxEvent)
            .where(
                models.BoxEvent.box_code == box_code,
                models.BoxEvent.id <= watermark,
                models.BoxEvent.seq > last_seq,
            )
            .order_by(models.BoxEvent.seq)
            .limit(page_size + 1)
        ).all()
    )

    has_more = len(rows) > page_size
    page = rows[:page_size]
    next_cursor = (
        _encode_events_cursor(box_code, watermark, page[-1].seq) if has_more else None
    )
    return schemas.EventPage(
        box_code=box_code,
        page_size=page_size,
        event_count=len(page),
        events=[schemas.EventOut.model_validate(e) for e in page],
        next_cursor=next_cursor,
    )


def _to_epoch_micros(dt: datetime) -> int:
    """UTC naive datetime -> 自 Unix 纪元的整数微秒（精确，无浮点误差）。"""
    delta = dt.replace(tzinfo=timezone.utc) - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def _from_epoch_micros(micros: int) -> datetime:
    """整数微秒 -> UTC naive datetime。"""
    secs, rem = divmod(micros, 1_000_000)
    return (_EPOCH + timedelta(seconds=secs, microseconds=rem)).replace(tzinfo=None)


def _isotonic_blocks(values: list[int]) -> list[tuple[int, int, int]]:
    """等权保序回归（PAVA，Pool Adjacent Violators Algorithm）。

    输入按 seq 升序的整数微秒事件时间；在“建议值非递减”约束下最小化
    sum((建议值 - 原值)^2)，等价于把相邻违规块不断合并、块内取算术均值。

    为避免大整数除法的浮点误差，块以 (序号起点, 序号终点, 总和, 个数)
    维护；块间比较与均值输出均用整数运算。均值四舍五入到微秒
    （半数远离零；微秒均值不可能出现半数情形时同样正确）。

    返回 [(index_from, index_to, rounded_mean_micros), ...]，
    下标对应输入 values 的位置，由调用方映射回事件 seq。
    """
    # 每个元素：[seq_from, seq_to, total_micros, count]
    blocks: list[list[int]] = []
    for i, v in enumerate(values):
        block = [i, i, v, 1]
        # 前一块均值 > 当前块均值即为违规，向前合并直到满足非递减
        while blocks:
            prev = blocks[-1]
            # prev.total / prev.count > block.total / block.count
            if prev[2] * block[3] <= block[2] * prev[3]:
                break
            blocks.pop()
            block = [
                prev[0],
                block[1],
                prev[2] + block[2],
                prev[3] + block[3],
            ]
        blocks.append(block)

    def _round_half_away(total: int, count: int) -> int:
        q, r = divmod(abs(total), count)
        # 余数 * 2 >= 分母即小数部分 >= 0.5，向远离零方向进位
        if 2 * r >= count:
            q += 1
        return q if total >= 0 else -q

    return [
        (b[0], b[1], _round_half_away(b[2], b[3])) for b in blocks
    ]


def _events_in_range(
    db: Session,
    box_code: str,
    seq_from: int,
    seq_to: int,
    limit: int | None = None,
) -> list[models.BoxEvent] | None:
    """按 seq 升序取区间内事件（含端点）。

    箱号不存在（从未上报过）返回 None，由调用方转 404；
    箱号存在但区间内无事件返回 []。缺号不补点，只返回库中实际事件。
    limit 给定时最多返回该数量条（用于压缩预览的 120 条上限校验）。
    """
    box_exists = db.scalar(
        select(func.count())
        .select_from(models.BoxEvent)
        .where(models.BoxEvent.box_code == box_code)
    )
    if not box_exists:
        return None

    stmt = (
        select(models.BoxEvent)
        .where(
            models.BoxEvent.box_code == box_code,
            models.BoxEvent.seq >= seq_from,
            models.BoxEvent.seq <= seq_to,
        )
        .order_by(models.BoxEvent.seq)
    )
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(db.scalars(stmt).all())


def _isotonic_suggestions(
    events: list[models.BoxEvent],
) -> tuple[list[int], list[tuple[int, int]]]:
    """对按 seq 升序的事件做等权保序回归校正（clock-audit 同款算法）。

    返回 (逐事件建议时间的整数微秒, 逐事件所属合并块的 (起始序号, 结束序号))，
    下标与 events 一一对应。只读计算，不改动任何原始记录。
    """
    values = [_to_epoch_micros(e.event_time) for e in events]
    blocks = _isotonic_blocks(values)

    # PAVA 输出的块下标对应 events 下标；展开为逐事件的建议
    suggested: list[int] = [0] * len(events)
    block_range: list[tuple[int, int]] = [(0, 0)] * len(events)
    for idx_from, idx_to, mean_micros in blocks:
        seq_from_block = events[idx_from].seq
        seq_to_block = events[idx_to].seq
        for i in range(idx_from, idx_to + 1):
            suggested[i] = mean_micros
            block_range[i] = (seq_from_block, seq_to_block)
    return suggested, block_range


def clock_audit(
    db: Session, box_code: str, seq_from: int, seq_to: int
) -> list[schemas.ClockAuditItem] | None:
    """对指定序号区间（含端点）生成设备时钟回拨校正建议。

    - 箱号不存在（从未上报过）：返回 None，由路由层转 404
    - 箱号存在但区间内无事件：返回 []
    - 校正只在本次查询中即时计算，不改动任何原始记录；乱序补报后再次查询
      会基于最新数据重新计算
    """
    events = _events_in_range(db, box_code, seq_from, seq_to)
    if events is None:
        return None
    if not events:
        return []

    values = [_to_epoch_micros(e.event_time) for e in events]
    suggested, block_range = _isotonic_suggestions(events)

    return [
        schemas.ClockAuditItem(
            seq=event.seq,
            original_time=event.event_time,
            suggested_time=_from_epoch_micros(sugg),
            offset_micros=sugg - values[i],
            block_seq_from=block_range[i][0],
            block_seq_to=block_range[i][1],
        )
        for i, (event, sugg) in enumerate(zip(events, suggested))
    ]


def _merkle_leaf(event: models.BoxEvent) -> tuple[str, bytes]:
    """构造叶子原文并计算叶哈希，返回 (原文, 哈希字节)。

    叶子原文：箱号、序号、UTC 时间字符串（YYYY-MM-DDTHH:MM:SS.ffffffZ）与
    .17g 温度字符串组成的无空格 JSON 数组；中文等非 ASCII 字符原样保留
    （ensure_ascii=False），按 UTF-8 编码参与哈希。
    叶哈希：SHA-256(0x00 || 原文的 UTF-8 字节)。
    """
    time_str = event.event_time.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
    temp_str = format(event.temperature, ".17g")
    leaf_data = json.dumps(
        [event.box_code, event.seq, time_str, temp_str],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(b"\x00" + leaf_data.encode("utf-8")).digest()
    return leaf_data, digest


def _merkle_root_and_proof(
    leaf_hashes: list[bytes], target_index: int
) -> tuple[bytes, list[tuple[bytes, str]]]:
    """按查询顺序（seq 升序）的叶哈希列表建树，返回 (根哈希, 目标叶的证明路径)。

    父节点哈希 = SHA-256(0x01 || 左哈希 || 右哈希)；某层节点数为奇数时
    复制末节点再两两配对。证明路径从叶层到根层逐层给出 (兄弟哈希, 兄弟方位)，
    方位 left/right 表示兄弟在当前节点的左/右；单事件（叶即根）时为空路径。
    """
    level = list(leaf_hashes)
    index = target_index
    proof: list[tuple[bytes, str]] = []
    while len(level) > 1:
        if len(level) % 2 == 1:
            level.append(level[-1])  # 奇数层复制末节点
        sibling = index ^ 1
        side = "right" if index % 2 == 0 else "left"
        proof.append((level[sibling], side))
        level = [
            hashlib.sha256(b"\x01" + level[i] + level[i + 1]).digest()
            for i in range(0, len(level), 2)
        ]
        index //= 2
    return level[0], proof


def merkle_proof(
    db: Session, box_code: str, seq_from: int, seq_to: int, target_seq: int
) -> schemas.MerkleProofResult | None:
    """区间事件的 Merkle 包含证明（只读即时计算，不补缺号、不写回）。

    - 取区间 [seq_from, seq_to]（含端点）内按 seq 升序的实际已存事件作为叶子，
      缺号不补点；目标序号必须对应区间内实际存在的事件
    - 箱号不存在（从未上报过）返回 None，由路由层转 404
    - 目标序号不在区间实际事件中（含区间为空）：抛 404（target_not_in_range）
    """
    events = _events_in_range(db, box_code, seq_from, seq_to)
    if events is None:
        return None

    target_index = next(
        (i for i, e in enumerate(events) if e.seq == target_seq), None
    )
    if target_index is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "target_not_in_range",
                "message": (
                    f"箱号 {box_code} 在区间 [{seq_from}, {seq_to}] 内"
                    f"不存在序号为 {target_seq} 的事件"
                ),
                "box_code": box_code,
                "seq_from": seq_from,
                "seq_to": seq_to,
                "target_seq": target_seq,
            },
        )

    leaf_data_list: list[str] = []
    leaf_hashes: list[bytes] = []
    for event in events:
        leaf_data, digest = _merkle_leaf(event)
        leaf_data_list.append(leaf_data)
        leaf_hashes.append(digest)

    root, proof = _merkle_root_and_proof(leaf_hashes, target_index)

    return schemas.MerkleProofResult(
        box_code=box_code,
        seq_from=seq_from,
        seq_to=seq_to,
        target_seq=target_seq,
        event_count=len(events),
        leaf_data=leaf_data_list[target_index],
        target_leaf_hash=leaf_hashes[target_index].hex(),
        root_hash=root.hex(),
        proof=[
            schemas.MerkleProofStep(sibling_hash=h.hex(), sibling_side=side)
            for h, side in proof
        ],
    )


def _segment_exposure(
    temp_from: float, temp_to: float, upper_limit: float, gap_seconds: float
) -> float:
    """相邻两点间温度线性变化时，超过 upper_limit 部分对时间的精确积分（℃·秒）。

    超出量 a = temp_from - upper_limit、b = temp_to - upper_limit 同样线性变化，
    在时长 gap_seconds（> 0）上对 max(超出量, 0) 积分：
    - 两端都不超：0
    - 两端都超：梯形面积 (a + b) / 2 * d
    - 一端超：线性过限，超出部分是三角形，面积 超端超出量² / (2 * |b - a|) * d
    """
    a = temp_from - upper_limit
    b = temp_to - upper_limit
    if a <= 0.0 and b <= 0.0:
        return 0.0
    if a >= 0.0 and b >= 0.0:
        return (a + b) / 2.0 * gap_seconds
    over = max(a, b)
    return over * over / (2.0 * abs(b - a)) * gap_seconds


def heat_exposure(
    db: Session,
    box_code: str,
    seq_from: int,
    seq_to: int,
    upper_limit: float,
    max_gap_seconds: float,
) -> schemas.HeatExposureResult | None:
    """区间内相邻事件的超温积分（℃·秒），基于 clock-audit 同款校正时间。

    - 相邻事件（按 seq 升序，缺号不补点）间温度视为线性变化，对超过
      upper_limit 的部分做精确积分
    - 事件时间先按等权保序回归校正；校正后间隔为零或超过 max_gap_seconds
      的段不计入积分（也不出现在明细中）
    - 只在查询时即时计算，不改动任何原始记录；补报后重查即重算
    - 箱号不存在返回 None，由路由层转 404；区间无事件返回零值及空明细
    """
    events = _events_in_range(db, box_code, seq_from, seq_to)
    if events is None:
        return None

    corrected, _ = _isotonic_suggestions(events)

    segments: list[schemas.HeatExposureSegment] = []
    exposures: list[float] = []
    skipped = 0
    for i in range(len(events) - 1):
        gap_micros = corrected[i + 1] - corrected[i]
        gap_seconds = gap_micros / 1_000_000
        # 校正时间非递减，间隔只会 >= 0；为 0（同块）或超上限的段不计入
        if gap_micros <= 0 or gap_seconds > max_gap_seconds:
            skipped += 1
            continue
        exposure = _segment_exposure(
            events[i].temperature, events[i + 1].temperature,
            upper_limit, gap_seconds,
        )
        exposures.append(exposure)
        segments.append(
            schemas.HeatExposureSegment(
                start_seq=events[i].seq,
                end_seq=events[i + 1].seq,
                start_time=_from_epoch_micros(corrected[i]),
                end_time=_from_epoch_micros(corrected[i + 1]),
                gap_seconds=gap_seconds,
                start_temperature=events[i].temperature,
                end_temperature=events[i + 1].temperature,
                celsius_seconds=exposure,
            )
        )

    return schemas.HeatExposureResult(
        box_code=box_code,
        seq_from=seq_from,
        seq_to=seq_to,
        upper_limit=upper_limit,
        max_gap_seconds=max_gap_seconds,
        event_count=len(events),
        segment_count=len(segments),
        skipped_segment_count=skipped,
        total_celsius_seconds=math.fsum(exposures),
        segments=segments,
    )


def _time_weighted_temperature_median(
    valid_segments: list[tuple[Fraction, Fraction, int]],
) -> tuple[float | None, float]:
    """计算“按时间持续加权”的温度中位数与有效总时长。

    每段为 (起点温度, 终点温度, 校正后整数微秒时长)，时长均 > 0。
    段内温度在两端之间按时间线性变化，故时长在温度上是均匀分布；
    恒温段（两端温度相同）退化为该温度上的点质量（atom）。

    中位数定义为：累计时长占比首次达到一半（>= 总时长 / 2）的**最低温度**，
    即该混合分布的 CDF 取下侧广义逆。全程用 Fraction 精确计算
    （温度与微秒时长均可无损转为有理数），避免在半总时长边界上因浮点误差选错点。

    做法：收集所有端点温度作为断点；在每个断点处汇总恒温点质量与
    开区间密度（微秒/℃）的净变化（密度 = 段时长 / 温度跨度），然后从低到高扫描：
    先累计当前温度的点质量，再按均匀密度跨过它到下一断点。
    返回 (中位数温度, 总秒数)；valid_segments 为空时返回 (None, 0.0)。
    """
    if not valid_segments:
        return None, 0.0

    # 断点温度 -> [点质量, 密度净变化]，键全部用 Fraction
    point_mass: dict[Fraction, Fraction] = {}
    density_delta: dict[Fraction, Fraction] = {}
    breaks: set[Fraction] = set()
    total = Fraction(0)

    def _add(target: dict[Fraction, Fraction], key: Fraction, value: Fraction) -> None:
        target[key] = target.get(key, Fraction(0)) + value

    for t0_f, t1_f, d in valid_segments:
        total += d
        breaks.add(t0_f)
        breaks.add(t1_f)
        if t0_f == t1_f:
            # 恒温段：整段时长作为该温度的点质量
            _add(point_mass, t0_f, Fraction(d))
        elif t0_f < t1_f:
            # 升温段：时长 d 在 [t0, t1] 上均匀，密度为 d/(t1-t0)（微秒/℃）
            density = Fraction(d) / (t1_f - t0_f)
            _add(density_delta, t0_f, density)
            _add(density_delta, t1_f, -density)
        else:
            # 降温段：时长 d 在 [t1, t0] 上均匀，密度同样为 d/(t0-t1)
            density = Fraction(d) / (t0_f - t1_f)
            _add(density_delta, t1_f, density)
            _add(density_delta, t0_f, -density)

    ordered = sorted(breaks)
    half = total / 2
    cumulative = Fraction(0)
    rho = Fraction(0)  # 当前断点右侧开区间内的温度密度（微秒/℃）
    median: Fraction | None = None
    for idx, x in enumerate(ordered):
        # 1) 该温度处：先并入进入此断点的密度净变化（首个断点为 +密度），
        #    再统计恒温点质量
        rho += density_delta.get(x, Fraction(0))
        mass = point_mass.get(x, Fraction(0))
        if mass > 0 and cumulative + mass >= half:
            median = x
            break
        cumulative += mass
        # 2) 跨过开区间 (x, next_break)，密度恒定为 rho
        if idx + 1 >= len(ordered):
            break  # 末断点右侧无任何分布
        nxt = ordered[idx + 1]
        if rho > 0:
            need = half - cumulative
            span = rho * (nxt - x)
            if span >= need:
                median = x + need / rho
                break
            cumulative += span
        # rho == 0：两断点间没有有效分布，直接越过，不累计时长

    assert median is not None  # 累计到最高断点必达 half
    return float(median), float(total) / 1_000_000.0


def temperature_median(
    db: Session,
    box_code: str,
    seq_from: int,
    seq_to: int,
    max_gap_seconds: float,
) -> schemas.TemperatureMedianResult | None:
    """区间内按持续时间加权的温度中位数，基于 clock-audit 同款校正时间。

    - 相邻事件（按 seq 升序，缺号不补点）间温度视为线性变化，把每个有效段
      的每一瞬间按其持续时间计入温度分布；恒温段的整段时长是该温度的点质量
    - 事件时间先按等权保序回归校正；校正后间隔为零（同一合并块）或超过
      max_gap_seconds 的段不计入分布（以 skipped_segment_count 计数）
    - 中位数取累计时长占比首次达到一半的最低温度；没有有效段时
      median_temperature=None、total_seconds=0.0
    - 只在查询时即时计算，不改动任何原始记录；补报后重查即重算
    - 箱号不存在返回 None，由路由层转 404；区间无事件返回空结果
    """
    events = _events_in_range(db, box_code, seq_from, seq_to)
    if events is None:
        return None

    corrected, _ = _isotonic_suggestions(events)

    valid_segments: list[tuple[Fraction, Fraction, int]] = []
    skipped = 0
    for i in range(len(events) - 1):
        gap_micros = corrected[i + 1] - corrected[i]
        gap_seconds = gap_micros / 1_000_000
        # 校正时间非递减，间隔只会 >= 0；为 0（同块）或超上限的段不计入
        if gap_micros <= 0 or gap_seconds > max_gap_seconds:
            skipped += 1
            continue
        valid_segments.append(
            (Fraction(events[i].temperature), Fraction(events[i + 1].temperature), gap_micros)
        )

    median_temperature, total_seconds = _time_weighted_temperature_median(valid_segments)

    return schemas.TemperatureMedianResult(
        box_code=box_code,
        seq_from=seq_from,
        seq_to=seq_to,
        max_gap_seconds=max_gap_seconds,
        event_count=len(events),
        segment_count=len(valid_segments),
        skipped_segment_count=skipped,
        total_seconds=total_seconds,
        median_temperature=median_temperature,
    )


def _compress_trajectory(
    xs: list[int], ys: list[float], max_error: float
) -> tuple[list[int], Fraction]:
    """最少保留点温度轨迹压缩（DP 精确求解）。

    以序号为横坐标、温度为纵坐标，相邻保留点之间按温度线性插值重建被省略点。
    可行子序列要求：首末点必保留；每个被省略点的重建温度与原值之差 <= max_error。
    在全部可行子序列中选保留数最少的；保留数相同时取保留序号序列字典序最小者。

    方法：在“段 (i,j) 可行”构成的 DAG 上做最短路径 DP，段可行只取决于两端点，
    贪心“尽量跳最远”不能保证同数量下字典序最小，故用 DP 精确求解。
    误差用 Fraction 精确计算（float 可无损转为有理数，序号为整数），
    避免浮点误差在 max_error 边界上误判；区间事件数 <= 120，O(n^3) 足够快。

    返回 (保留点在输入中的下标序列, 被省略点的最大重建误差)。
    """
    n = len(xs)
    yf = [Fraction(y) for y in ys]
    tol = Fraction(max_error)

    def deviation(i: int, k: int, j: int) -> Fraction:
        """点 k 相对弦 (i,j) 线性插值的重建误差绝对值（精确有理数）。"""
        interp = yf[i] + (yf[j] - yf[i]) * Fraction(xs[k] - xs[i], xs[j] - xs[i])
        return abs(yf[k] - interp)

    if n <= 2:  # 0/1/2 条：首末保留即全部，没有被省略点
        return list(range(n)), Fraction(0)

    def feasible(i: int, j: int) -> bool:
        return all(deviation(i, k, j) <= tol for k in range(i + 1, j))

    # best[i] = (从第 i 个事件到末尾的最少保留数, 对应的最优保留下标序列)
    best: list[tuple[int, tuple[int, ...]]] = [(0, ())] * n
    best[n - 1] = (1, (n - 1,))
    for i in range(n - 2, -1, -1):
        chosen: tuple[int, tuple[int, ...]] | None = None
        for j in range(i + 1, n):
            if not feasible(i, j):
                continue
            # 元组比较：先比保留数（最少），数量相同再比下标序列（字典序最小）；
            # 所有候选序列同以 i 开头，等价于挑后缀最优的可行后继 j
            cand = (best[j][0] + 1, (i,) + best[j][1])
            if chosen is None or cand < chosen:
                chosen = cand
        assert chosen is not None  # 相邻两点构成的段没有中间点，必然可行
        best[i] = chosen
    kept = list(best[0][1])

    actual_max = Fraction(0)
    for a, b in zip(kept, kept[1:]):
        for k in range(a + 1, b):
            dev = deviation(a, k, b)
            if dev > actual_max:
                actual_max = dev
    return kept, actual_max


def trajectory_compression(
    db: Session, box_code: str, seq_from: int, seq_to: int, max_error: float
) -> schemas.TrajectoryCompressionResult | None:
    """单箱温度轨迹压缩预览（只读即时计算，不写回任何原始记录）。

    - 取区间 [seq_from, seq_to]（含端点）内按 seq 升序的事件，缺号不补点；
      区间事件超过 TRAJECTORY_COMPRESSION_MAX_EVENTS（120）条 -> 422
    - 以序号为横坐标做最少保留点压缩，保留点之间线性插值重建被省略事件，
      重建误差不得超过 max_error；同数量选保留序号字典序最小者
    - 箱号不存在（从未上报过）返回 None，由路由层转 404
    - 箱号存在但区间内无事件：返回空结果；仅一条事件：直接保留
    """
    events = _events_in_range(
        db,
        box_code,
        seq_from,
        seq_to,
        limit=TRAJECTORY_COMPRESSION_MAX_EVENTS + 1,
    )
    if events is None:
        return None
    if len(events) > TRAJECTORY_COMPRESSION_MAX_EVENTS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "too_many_events",
                "message": (
                    f"序号区间 [{seq_from}, {seq_to}] 内事件超过 "
                    f"{TRAJECTORY_COMPRESSION_MAX_EVENTS} 条，请缩小区间后重试"
                ),
                "seq_from": seq_from,
                "seq_to": seq_to,
                "limit": TRAJECTORY_COMPRESSION_MAX_EVENTS,
            },
        )

    if not events:
        return schemas.TrajectoryCompressionResult(
            box_code=box_code,
            seq_from=seq_from,
            seq_to=seq_to,
            max_error=max_error,
            event_count=0,
            kept_count=0,
            omitted_count=0,
            actual_max_error=None,
            kept_events=[],
        )

    xs = [e.seq for e in events]
    ys = [e.temperature for e in events]
    kept_idx, actual_max = _compress_trajectory(xs, ys, max_error)

    return schemas.TrajectoryCompressionResult(
        box_code=box_code,
        seq_from=seq_from,
        seq_to=seq_to,
        max_error=max_error,
        event_count=len(events),
        kept_count=len(kept_idx),
        omitted_count=len(events) - len(kept_idx),
        actual_max_error=float(actual_max),
        kept_events=[
            schemas.TrajectoryCompressionPoint(
                seq=events[i].seq,
                event_time=events[i].event_time,
                temperature=events[i].temperature,
            )
            for i in kept_idx
        ],
    )


def _load_handoff(
    db: Session, handoff_code: str
) -> tuple[models.HandoffSnapshot, list[models.HandoffSnapshotItem]] | None:
    """按交接编号读取快照头与冻结明细（按请求时的箱号顺序）；不存在返回 None。"""
    snapshot = db.scalar(
        select(models.HandoffSnapshot).where(
            models.HandoffSnapshot.handoff_code == handoff_code
        )
    )
    if snapshot is None:
        return None
    items = list(
        db.scalars(
            select(models.HandoffSnapshotItem)
            .where(models.HandoffSnapshotItem.handoff_id == snapshot.id)
            .order_by(models.HandoffSnapshotItem.position)
        ).all()
    )
    return snapshot, items


def _handoff_content_equal(
    snapshot: models.HandoffSnapshot,
    items: list[models.HandoffSnapshotItem],
    payload: schemas.HandoffIn,
) -> bool:
    """已建快照与本次请求内容是否一致（温度上下限 + 箱号列表及顺序）。

    设备/客户端重试会原样重发 JSON，float 解析结果相同，故上下限直接判等。
    """
    return (
        snapshot.temp_lower == payload.temp_lower
        and snapshot.temp_upper == payload.temp_upper
        and [item.box_code for item in items] == payload.box_codes
    )


def _handoff_conflict(
    payload: schemas.HandoffIn,
    snapshot: models.HandoffSnapshot,
    items: list[models.HandoffSnapshotItem],
) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={
            "error": "handoff_conflict",
            "message": (
                f"交接编号 {payload.handoff_code} 已建快照，"
                "但本次请求的箱号列表或温度上下限与其不一致，已拒绝"
            ),
            "existing": {
                "box_codes": [item.box_code for item in items],
                "temp_lower": snapshot.temp_lower,
                "temp_upper": snapshot.temp_upper,
            },
            "received": {
                "box_codes": payload.box_codes,
                "temp_lower": payload.temp_lower,
                "temp_upper": payload.temp_upper,
            },
        },
    )


def create_handoff(
    db: Session, payload: schemas.HandoffIn
) -> tuple[models.HandoffSnapshot, list[models.HandoffSnapshotItem], bool]:
    """创建交接快照：在同一事务中按各箱当前最大 seq 读取状态并冻结。

    返回 (快照头, 冻结明细, 是否新建)。
    - 交接编号不存在：同一事务内按“每箱当前最大 seq”读取状态，逐箱冻结
      序号、温度、事件时间与结论（缺报/越界/正常；上下限边界值算正常）后一次提交
    - 交接编号已存在且请求内容（箱号列表及顺序、温度上下限）一致：
      幂等返回原快照，不重新读取事件、不改写任何冻结数据
    - 交接编号已存在但内容不同：拒绝（409）
    快照落库后即为只读：后续事件补报（含乱序）只写 box_events，不会改写已建快照。
    """
    existing = _load_handoff(db, payload.handoff_code)
    if existing is not None:
        snapshot, items = existing
        if not _handoff_content_equal(snapshot, items, payload):
            raise _handoff_conflict(payload, snapshot, items)
        return snapshot, items, False

    # 同一事务内按“每箱当前最大 seq”读取最新状态（乱序入库不影响取值）
    latest = _latest_subquery()
    rows = db.scalars(
        select(models.BoxEvent)
        .join(
            latest,
            (models.BoxEvent.box_code == latest.c.box_code)
            & (models.BoxEvent.seq == latest.c.max_seq),
        )
        .where(models.BoxEvent.box_code.in_(payload.box_codes))
    ).all()
    states = {e.box_code: e for e in rows}

    snapshot = models.HandoffSnapshot(
        handoff_code=payload.handoff_code,
        temp_lower=payload.temp_lower,
        temp_upper=payload.temp_upper,
        box_count=len(payload.box_codes),
        normal_count=0,
        out_of_range_count=0,
        missing_count=0,
    )
    db.add(snapshot)
    try:
        db.flush()  # 取得 id，同时触发 handoff_code 唯一约束，兜住并发创建
    except IntegrityError:
        db.rollback()
        # 并发请求已抢先建快照：以库中快照为准重新判定
        existing = _load_handoff(db, payload.handoff_code)
        if existing is None:  # 理论上不会发生
            raise
        snapshot, items = existing
        if not _handoff_content_equal(snapshot, items, payload):
            raise _handoff_conflict(payload, snapshot, items)
        return snapshot, items, False

    items: list[models.HandoffSnapshotItem] = []
    normal = out_of_range = missing = 0
    for position, box_code in enumerate(payload.box_codes):
        event = states.get(box_code)
        if event is None:
            # 缺箱（从未上报过）记缺报，冻结字段为 NULL
            item_status = HANDOFF_STATUS_MISSING
            missing += 1
            seq = temperature = event_time = None
        else:
            # 边界值（恰等于下限或上限）算正常，严格越出区间才算越界
            if payload.temp_lower <= event.temperature <= payload.temp_upper:
                item_status = HANDOFF_STATUS_NORMAL
                normal += 1
            else:
                item_status = HANDOFF_STATUS_OUT_OF_RANGE
                out_of_range += 1
            seq = event.seq
            temperature = event.temperature
            event_time = event.event_time
        items.append(
            models.HandoffSnapshotItem(
                handoff_id=snapshot.id,
                position=position,
                box_code=box_code,
                status=item_status,
                seq=seq,
                temperature=temperature,
                event_time=event_time,
            )
        )

    snapshot.normal_count = normal
    snapshot.out_of_range_count = out_of_range
    snapshot.missing_count = missing
    db.add_all(items)
    db.commit()
    return snapshot, items, True


def get_handoff(
    db: Session, handoff_code: str
) -> tuple[models.HandoffSnapshot, list[models.HandoffSnapshotItem]] | None:
    """按交接编号查询已冻结的快照与明细；不存在返回 None，由路由层转 404。"""
    return _load_handoff(db, handoff_code)


def _lis_length(positions: list[int]) -> int:
    """最长严格递增子序列的长度（耐心排序，O(n log n)）。

    输入为共同箱按来源顺序映射到的目标位置序列；快照内箱号唯一，
    位置互不相同，bisect_left 即得到严格递增的 LIS 长度。
    """
    tails: list[int] = []
    for x in positions:
        i = bisect_left(tails, x)
        if i == len(tails):
            tails.append(x)
        else:
            tails[i] = x
    return len(tails)


def reconcile_handoffs(
    db: Session, from_code: str, to_code: str
) -> schemas.HandoffReconcileResult:
    """对账两份已冻结快照的箱单明细（只读，不重新查询箱子当前事件）。

    - 新增箱：只在目标快照中的箱号，按目标快照顺序列出
    - 移出箱：只在来源快照中的箱号，按来源快照顺序列出
    - 共同箱：两份快照都有的箱号，按来源快照顺序列出，给出原/目标结论与序号
    - 顺序移动：共同箱按来源顺序映射到目标位置后求最长递增子序列（LIS），
      最少需移动箱数 = 共同箱数 - LIS 长度；新增、移出不计移动，
      两份箱单相同（共同箱相对顺序一致）时为 0
    - 来源或目标快照不存在：抛 404（handoff_not_found）
    """
    from_loaded = _load_handoff(db, from_code)
    if from_loaded is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "handoff_not_found",
                "message": f"来源交接快照 {from_code} 不存在",
                "handoff_code": from_code,
            },
        )
    to_loaded = _load_handoff(db, to_code)
    if to_loaded is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "handoff_not_found",
                "message": f"目标交接快照 {to_code} 不存在",
                "handoff_code": to_code,
            },
        )
    _, from_items = from_loaded
    _, to_items = to_loaded

    from_by_code = {item.box_code: item for item in from_items}
    to_by_code = {item.box_code: item for item in to_items}

    added = [item for item in to_items if item.box_code not in from_by_code]
    removed = [item for item in from_items if item.box_code not in to_by_code]
    # 共同箱按来源快照顺序；LIS 的位置映射也基于该顺序
    common = [item for item in from_items if item.box_code in to_by_code]

    to_position = {item.box_code: index for index, item in enumerate(to_items)}
    mapped_positions = [to_position[item.box_code] for item in common]
    lis_length = _lis_length(mapped_positions)
    min_moves = len(common) - lis_length

    common_boxes: list[schemas.HandoffReconcileCommonBox] = []
    status_changed_count = 0
    for from_item in common:
        to_item = to_by_code[from_item.box_code]
        changed = from_item.status != to_item.status
        if changed:
            status_changed_count += 1
        common_boxes.append(
            schemas.HandoffReconcileCommonBox(
                box_code=from_item.box_code,
                from_status=from_item.status,
                to_status=to_item.status,
                from_seq=from_item.seq,
                to_seq=to_item.seq,
                status_changed=changed,
            )
        )

    return schemas.HandoffReconcileResult(
        from_code=from_code,
        to_code=to_code,
        from_box_count=len(from_items),
        to_box_count=len(to_items),
        added_count=len(added),
        removed_count=len(removed),
        common_count=len(common),
        status_changed_count=status_changed_count,
        lis_length=lis_length,
        min_moves=min_moves,
        added_boxes=[
            schemas.HandoffReconcileAddedBox(
                box_code=item.box_code, to_status=item.status, to_seq=item.seq
            )
            for item in added
        ],
        removed_boxes=[
            schemas.HandoffReconcileRemovedBox(
                box_code=item.box_code, from_status=item.status, from_seq=item.seq
            )
            for item in removed
        ],
        common_boxes=common_boxes,
    )


def _load_calibration(
    db: Session, box_code: str, version: int
) -> tuple[models.CalibrationSet, list[models.CalibrationAnchor]] | None:
    """按箱号 + 版本号读取校准头与锚点（按 position 升序）；不存在返回 None。"""
    calib = db.scalar(
        select(models.CalibrationSet).where(
            models.CalibrationSet.box_code == box_code,
            models.CalibrationSet.version == version,
        )
    )
    if calib is None:
        return None
    anchors = list(
        db.scalars(
            select(models.CalibrationAnchor)
            .where(models.CalibrationAnchor.calibration_id == calib.id)
            .order_by(models.CalibrationAnchor.position)
        ).all()
    )
    return calib, anchors


def _calibration_content_equal(
    anchors: list[models.CalibrationAnchor], payload: schemas.CalibrationIn
) -> bool:
    """已存锚点与本次请求内容是否一致（逐点比较序号与温度偏移）。

    客户端重试会原样重发 JSON，float 解析结果相同，故偏移直接判等；
    序号已由 schema 保证严格递增，按提交顺序逐点比较即可。
    """
    if len(anchors) != len(payload.anchors):
        return False
    return all(
        anchor.seq == point.seq and anchor.temp_offset == point.temp_offset
        for anchor, point in zip(anchors, payload.anchors)
    )


def _calibration_conflict(
    payload: schemas.CalibrationIn,
    calib: models.CalibrationSet,
    anchors: list[models.CalibrationAnchor],
) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={
            "error": "calibration_conflict",
            "message": (
                f"箱号 {payload.box_code} 的版本 {payload.version} 已存在校准锚点，"
                "但本次提交的锚点内容与其不一致，已拒绝（锚点不可变）"
            ),
            "existing": {
                "box_code": calib.box_code,
                "version": calib.version,
                "anchors": [
                    {"seq": a.seq, "temp_offset": a.temp_offset} for a in anchors
                ],
            },
            "received": {
                "box_code": payload.box_code,
                "version": payload.version,
                "anchors": [
                    {"seq": p.seq, "temp_offset": p.temp_offset}
                    for p in payload.anchors
                ],
            },
        },
    )


def save_calibration(
    db: Session, payload: schemas.CalibrationIn
) -> tuple[models.CalibrationSet, list[models.CalibrationAnchor], bool]:
    """按箱号 + 版本号保存一份不可变的校准锚点列表。

    返回 (校准头, 锚点行, 是否新建)。
    - (box_code, version) 不存在：同一事务写入头行与全部锚点（至少 2 个，
      序号严格递增、偏移有限，均已由 schema 校验）后一次提交
    - 已存在且锚点内容（逐点序号与偏移）一致：幂等返回原列表，不写入任何行
    - 已存在但内容不同：拒绝（409），锚点不可变
    校准表只增不改，不提供更新入口；保存校准不会触碰 box_events 等任何既有数据。
    """
    existing = _load_calibration(db, payload.box_code, payload.version)
    if existing is not None:
        calib, anchors = existing
        if not _calibration_content_equal(anchors, payload):
            raise _calibration_conflict(payload, calib, anchors)
        return calib, anchors, False

    calib = models.CalibrationSet(
        box_code=payload.box_code,
        version=payload.version,
        anchor_count=len(payload.anchors),
    )
    db.add(calib)
    try:
        db.flush()  # 取得 id，同时触发 (box_code, version) 唯一约束，兜住并发提交
    except IntegrityError:
        db.rollback()
        # 并发请求抢先保存：以库中锚点为准重新判定
        existing = _load_calibration(db, payload.box_code, payload.version)
        if existing is None:  # 理论上不会发生
            raise
        calib, anchors = existing
        if not _calibration_content_equal(anchors, payload):
            raise _calibration_conflict(payload, calib, anchors)
        return calib, anchors, False

    anchors = [
        models.CalibrationAnchor(
            calibration_id=calib.id,
            position=position,
            seq=point.seq,
            temp_offset=point.temp_offset,
        )
        for position, point in enumerate(payload.anchors)
    ]
    db.add_all(anchors)
    db.commit()
    return calib, anchors, True


def get_calibration(
    db: Session, box_code: str, version: int
) -> tuple[models.CalibrationSet, list[models.CalibrationAnchor]] | None:
    """按箱号 + 版本号查询已保存的校准锚点；不存在返回 None，由路由层转 404。"""
    return _load_calibration(db, box_code, version)


def _anchor_offset_fraction(
    anchor_seqs: list[int], anchor_offsets: list[Fraction], seq: int
) -> Fraction:
    """序号 seq 处的温度偏移（Fraction 精确值）。

    - seq 恰好落在锚点上：该锚点偏移
    - 位于两个相邻锚点之间：按序号线性插值（与事件时间无关）
    - 早于首个锚点 / 晚于末个锚点：两端沿用最近锚点偏移（钳平，不外推）
    anchor_seqs 严格递增，调用方保证至少两个锚点。
    """
    idx = bisect_right(anchor_seqs, seq)
    if idx == 0:
        return anchor_offsets[0]
    if idx >= len(anchor_seqs):
        return anchor_offsets[-1]
    seq_lo, seq_hi = anchor_seqs[idx - 1], anchor_seqs[idx]
    if seq == seq_lo:
        return anchor_offsets[idx - 1]
    ratio = Fraction(seq - seq_lo, seq_hi - seq_lo)
    return anchor_offsets[idx - 1] + (
        anchor_offsets[idx] - anchor_offsets[idx - 1]
    ) * ratio


def calibration_trajectory(
    db: Session,
    box_code: str,
    version: int,
    seq_from: int,
    seq_to: int,
) -> schemas.CalibrationTrajectoryResult | None:
    """用指定版本的校准锚点计算区间事件的温度漂移校准轨迹（只读即时计算）。

    - 按 seq 升序取闭区间 [seq_from, seq_to] 内的实际事件，缺报序号不补事件
      （区间无事件返回 event_count=0、points=[]，不会因缺号造点）
    - 每条事件的偏移：相邻锚点之间按序号线性插值；区间两端（含锚点序号范围之外）
      沿用最近锚点偏移，不做外推
    - 校准温度 = 原始温度 + 偏移，用 Fraction 精确插值后转 float
    - 校准结果只用于本接口返回：不写库、不改写原始事件，也不影响最新状态、
      交接快照与 clock-audit / heat-exposure 等既有分析接口
    - 指定版本不存在返回 None，由路由层转 404
    """
    loaded = _load_calibration(db, box_code, version)
    if loaded is None:
        return None
    _, anchors = loaded

    anchor_seqs = [a.seq for a in anchors]
    anchor_offsets = [Fraction(a.temp_offset) for a in anchors]

    events = list(
        db.scalars(
            select(models.BoxEvent)
            .where(
                models.BoxEvent.box_code == box_code,
                models.BoxEvent.seq >= seq_from,
                models.BoxEvent.seq <= seq_to,
            )
            .order_by(models.BoxEvent.seq)
        ).all()
    )

    points: list[schemas.CalibrationPoint] = []
    for event in events:
        offset = _anchor_offset_fraction(anchor_seqs, anchor_offsets, event.seq)
        calibrated = Fraction(event.temperature) + offset
        points.append(
            schemas.CalibrationPoint(
                seq=event.seq,
                event_time=event.event_time,
                temperature=event.temperature,
                temp_offset=float(offset),
                calibrated_temperature=float(calibrated),
            )
        )

    return schemas.CalibrationTrajectoryResult(
        box_code=box_code,
        version=version,
        seq_from=seq_from,
        seq_to=seq_to,
        event_count=len(events),
        points=points,
    )


def _calibration_status(value: Fraction, lower: Fraction, upper: Fraction) -> str:
    """校准温度相对 [lower, upper] 的结论（Fraction 精确比较，边界算区间内）。"""
    if value < lower:
        return CALIBRATION_STATUS_BELOW
    if value > upper:
        return CALIBRATION_STATUS_ABOVE
    return CALIBRATION_STATUS_NORMAL


def calibration_impact(
    db: Session,
    box_code: str,
    from_version: int,
    to_version: int,
    seq_from: int,
    seq_to: int,
    temp_lower: float,
    temp_upper: float,
) -> schemas.CalibrationImpactResult:
    """对比两版校准在序号区间内对温度结论的影响（只读即时计算）。

    - 两版校准锚点均须已存（(box_code, version) 精确匹配），任一缺失抛 404
      （calibration_not_found，detail 给出缺失版本列表）
    - 箱号从未上报过事件返回 None，由路由层转 404（box_not_found）；
      箱号存在但区间内无事件返回空段结果
    - 区间内只取实际事件（按 seq 升序，缺号不补点），各自用两版锚点按序号
      线性插值（区间两端沿用最近锚点偏移，不外推）得到校准温度，
      再按“低于下限 / 区间内（含边界）/ 高于上限”判结论；
      插值与阈值比较全程用 Fraction 精确数值，避免边界误判
    - 仅保留两版结论不同的事件；序号连续（后一条 seq = 前一条 seq + 1）
      且变化方向（from_status, to_status）相同的事件合成一段，
      缺号或遇到结论未变的事件即断段，不为缺号造点
    - 全程只读：不写库、不改校准数据、不改写原始事件与任何既有分析结果
    """
    # 1) 两版校准必须都已存在
    missing_versions: list[int] = []
    loaded_sets: dict[int, tuple[list[int], list[Fraction]]] = {}
    for version in (from_version, to_version):
        loaded = _load_calibration(db, box_code, version)
        if loaded is None:
            missing_versions.append(version)
        else:
            _, anchors = loaded
            loaded_sets[version] = (
                [a.seq for a in anchors],
                [Fraction(a.temp_offset) for a in anchors],
            )
    if missing_versions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "calibration_not_found",
                "message": (
                    f"箱号 {box_code} 不存在校准版本：{', '.join(map(str, missing_versions))}"
                ),
                "box_code": box_code,
                "missing_versions": missing_versions,
            },
        )

    # 2) 箱号不存在 -> None（路由层 404）；区间无事件 -> 空段结果
    events = _events_in_range(db, box_code, seq_from, seq_to)
    if events is None:
        return None

    lower = Fraction(temp_lower)
    upper = Fraction(temp_upper)
    from_seqs, from_offsets = loaded_sets[from_version]
    to_seqs, to_offsets = loaded_sets[to_version]

    # 3) 逐条事件精确计算两版校准温度与结论，只保留结论改变的事件
    changes: list[tuple[int, str, str, Fraction]] = []
    # 每项：(seq, from_status, to_status, |新版校准温度 - 原版校准温度|)
    for event in events:
        from_calibrated = Fraction(event.temperature) + _anchor_offset_fraction(
            from_seqs, from_offsets, event.seq
        )
        to_calibrated = Fraction(event.temperature) + _anchor_offset_fraction(
            to_seqs, to_offsets, event.seq
        )
        from_status = _calibration_status(from_calibrated, lower, upper)
        to_status = _calibration_status(to_calibrated, lower, upper)
        if from_status != to_status:
            changes.append(
                (
                    event.seq,
                    from_status,
                    to_status,
                    abs(to_calibrated - from_calibrated),
                )
            )

    # 4) 合成影响段：序号连续（seq 差恰为 1）且变化方向相同才延续，否则断段
    raw_segments: list[list] = []
    for seq, from_status, to_status, delta in changes:
        if raw_segments:
            seg = raw_segments[-1]
            if (
                seq == seg["end_seq"] + 1
                and from_status == seg["from_status"]
                and to_status == seg["to_status"]
            ):
                seg["end_seq"] = seq
                seg["event_count"] += 1
                if delta > seg["max_delta"]:
                    seg["max_delta"] = delta
                continue
        raw_segments.append(
            {
                "start_seq": seq,
                "end_seq": seq,
                "event_count": 1,
                "from_status": from_status,
                "to_status": to_status,
                "max_delta": delta,
            }
        )

    segments = [
        schemas.CalibrationImpactSegment(
            start_seq=seg["start_seq"],
            end_seq=seg["end_seq"],
            event_count=seg["event_count"],
            from_status=seg["from_status"],
            to_status=seg["to_status"],
            max_calibration_delta=float(seg["max_delta"]),
        )
        for seg in raw_segments
    ]

    return schemas.CalibrationImpactResult(
        box_code=box_code,
        from_version=from_version,
        to_version=to_version,
        seq_from=seq_from,
        seq_to=seq_to,
        temp_lower=temp_lower,
        temp_upper=temp_upper,
        event_count=len(events),
        changed_event_count=len(changes),
        segment_count=len(segments),
        segments=segments,
    )


# 补报调度预览：共同序号区间每箱最多覆盖的序号数（与 schema 校验一致）
RETRANSMISSION_MAX_SPAN = 60


def telemetry_outages(
    db: Session,
    box_codes: list[str],
    seq_from: int,
    seq_to: int,
    max_silence_seconds: Decimal,
    min_simultaneous_boxes: int,
) -> schemas.TelemetryOutagesResult:
    """多箱群体遥测静默检测（只读即时计算，不写库、不改任何原始记录）。

    - box_codes 为已按请求顺序去重的箱号列表（路由层负责去重与空值校验）；
      任一指定箱号不存在（从未上报过）抛 404，并在 detail 中列出全部缺失箱号
    - 每箱只取闭区间 [seq_from, seq_to] 内按 seq 升序的实际事件，缺号不补点；
      事件时间做 clock-audit 同款等权保序回归校正
    - 相邻校正时间间隔**严格大于** max_silence_seconds 时，以两端校正时间构成
      该箱的半开静默区间 [起点, 终点)；间隔恰好等于阈值不算静默
    - 不足两条事件的箱不产生静默区间
    - 对各箱静默区间做端点扫描（半开区间：同一时刻先处理区间结束、再处理区间开始），
      输出同时静默箱数 >= min_simultaneous_boxes 的极大时间段及段内峰值同时静默数；
      端点相接的合格段合并为一段
    - max_silence_seconds 在路由层按请求原值（十进制文本）精确解析为 Decimal，
      不经过二进制浮点；这里换算为精确微秒数后与保序校正后的整数微秒间隔比较，
      避免浮点误差在“恰好等于阈值”的边界上误判（例如阈值 0.3 与恰好 0.3 秒
      即 300000 微秒的间隔比较时，不会错判为静默）
    """
    # 1) 指定箱号必须全部存在（至少上报过一条事件），否则 404
    existing_rows = db.scalars(
        select(models.BoxEvent.box_code)
        .where(models.BoxEvent.box_code.in_(box_codes))
        .distinct()
    ).all()
    existing = set(existing_rows)
    missing = [code for code in box_codes if code not in existing]
    if missing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "box_not_found",
                "message": f"以下箱号不存在（从未上报过事件）：{', '.join(missing)}",
                "missing_box_codes": missing,
            },
        )

    # 2) 一次查出所有指定箱在区间内的实际事件，按箱分组（缺号不补点）
    rows = db.scalars(
        select(models.BoxEvent)
        .where(
            models.BoxEvent.box_code.in_(box_codes),
            models.BoxEvent.seq >= seq_from,
            models.BoxEvent.seq <= seq_to,
        )
        .order_by(models.BoxEvent.box_code, models.BoxEvent.seq)
    ).all()
    events_by_box: dict[str, list[models.BoxEvent]] = {code: [] for code in box_codes}
    for row in rows:
        events_by_box[row.box_code].append(row)

    # 3) 逐箱做等权保序校正，间隔严格大于阈值 -> 半开静默区间（整数微秒）
    # 阈值为按请求原值精确解析的 Decimal，Fraction 转换无损（如 0.3 -> 300000 整）
    threshold_micros = Fraction(max_silence_seconds) * 1_000_000
    silence_intervals: list[tuple[int, int]] = []  # (起点微秒, 终点微秒)，半开
    for code in box_codes:
        events = events_by_box[code]
        if len(events) < 2:
            continue  # 不足两条事件的箱不产生区间
        corrected, _ = _isotonic_suggestions(events)
        for i in range(len(events) - 1):
            start_micros, end_micros = corrected[i], corrected[i + 1]
            if Fraction(end_micros - start_micros) > threshold_micros:
                silence_intervals.append((start_micros, end_micros))

    # 4) 端点扫描：半开区间在同一时刻先结束(-1)后开始(+1)，
    #    相邻事件点之间同时静默箱数恒定，构成基本段
    sweep: dict[int, int] = {}
    for start_micros, end_micros in silence_intervals:
        sweep[start_micros] = sweep.get(start_micros, 0) + 1
        sweep[end_micros] = sweep.get(end_micros, 0) - 1
    points = sorted(sweep)

    outages: list[schemas.TelemetryOutageSegment] = []
    active = 0
    open_start: int | None = None  # 当前未闭合合格段的起点（微秒）
    open_peak = 0                  # 当前未闭合合格段的峰值同时静默数
    for idx, t in enumerate(points):
        active += sweep[t]  # 该时刻之后的基本段 [t, 下一时刻) 的同时静默数
        if idx + 1 >= len(points):
            break  # 最后一个事件点右侧不再有区间
        nxt = points[idx + 1]
        if active >= min_simultaneous_boxes:
            if open_start is None:
                open_start, open_peak = t, active
            elif active > open_peak:
                open_peak = active
        elif open_start is not None:
            # 跌破要求数量：闭合当前段；端点相接的合格段自然并成一段
            outages.append(
                schemas.TelemetryOutageSegment(
                    start_time=_from_epoch_micros(open_start),
                    end_time=_from_epoch_micros(t),
                    duration_seconds=(t - open_start) / 1_000_000,
                    peak_simultaneous_boxes=open_peak,
                )
            )
            open_start, open_peak = None, 0
    if open_start is not None:
        # 扫描结束时仍合格：闭合到最后一个事件点
        end_micros = points[-1]
        outages.append(
            schemas.TelemetryOutageSegment(
                start_time=_from_epoch_micros(open_start),
                end_time=_from_epoch_micros(end_micros),
                duration_seconds=(end_micros - open_start) / 1_000_000,
                peak_simultaneous_boxes=open_peak,
            )
        )

    return schemas.TelemetryOutagesResult(
        box_codes=box_codes,
        box_count=len(box_codes),
        seq_from=seq_from,
        seq_to=seq_to,
        # 响应字段为 float；路由层已保证阈值可表示为有限 double，回显安全
        max_silence_seconds=float(max_silence_seconds),
        min_simultaneous_boxes=min_simultaneous_boxes,
        outage_count=len(outages),
        outages=outages,
    )


def _missing_segments(
    received: set[int], seq_from: int, seq_to: int
) -> list[tuple[int, int]]:
    """把闭区间 [seq_from, seq_to] 内未收到的序号合并为极大连续缺号段。

    返回 [(段起点, 段终点), ...]，含端点、按起点升序；received 为空集
    （箱号从未上报过）时自然得到整个区间一段。
    """
    segments: list[tuple[int, int]] = []
    start: int | None = None
    for s in range(seq_from, seq_to + 1):
        if s in received:
            if start is not None:
                segments.append((start, s - 1))
                start = None
        elif start is None:
            start = s
    if start is not None:
        segments.append((start, seq_to))
    return segments


def _canonical_commands(
    box_code: str, seg_start: int, k: int, max_len: int
) -> list[tuple[str, int, int]]:
    """在缺号段内从段起点覆盖 k 个连续序号的字典序最小指令切分。

    覆盖块固定在段起点（起点越小，(箱号, 起点, 终点) 字典序越小）；
    q = ceil(k / max_len) 条指令均分覆盖块时，靠前的指令取“剩余序号
    还能被后续指令装下”的最短长度（终点越小字典序越小），即后载切分：
    第 i 条长度 = max(1, remaining - (q - i - 1) * max_len)。
    返回 [(box_code, start_seq, end_seq), ...]，按起点升序。
    """
    q = (k + max_len - 1) // max_len
    commands: list[tuple[str, int, int]] = []
    start = seg_start
    remaining = k
    for i in range(q):
        rest_after = q - i - 1
        length = max(1, remaining - rest_after * max_len)
        commands.append((box_code, start, start + length - 1))
        start += length
        remaining -= length
    return commands


def _plan_key(plan: tuple[int, int, tuple]) -> tuple:
    """方案比较键（取最小者）：收益大者优先，其次指令数少者，再指令列表字典序小者。"""
    value, command_count, commands = plan
    return (-value, command_count, commands)


def retransmission_preview(
    db: Session, payload: schemas.RetransmissionPreviewIn
) -> schemas.RetransmissionPreviewResult:
    """多箱设备补报调度预览（只读即时计算，不写库、不改任何原始记录）。

    - 依据事件表找出各箱在共同闭区间 [seq_from, seq_to] 内的缺号并合并为
      极大连续缺号段；从未上报过的箱按整个指定区间缺报处理（不 404）
    - 一条指令只能请求同箱同段内的连续缺号（不跨已收到的序号），长度
      1..max_command_length，消耗 = 握手开销 + 请求长度，
      收益 = 请求长度 × 该箱权重
    - 同一段内覆盖 k 个序号的最省方式是从段起点连续覆盖、用 ceil(k/m) 条
      指令（更多指令只增加握手开销，收益不变），因此每段归约为
      “选 k = 0..段长”的多选背包，按预算做 DP 精确求解
    - 目标：预算内最大化 Σ(补报序号数 × 箱权重)；同收益选指令总数更少；
      再按 (箱号, 起点, 终点) 字典序取唯一方案。段按 (箱号, 段起点) 升序
      处理，指令元组按处理顺序连接即全局字典序，故 DP 中逐状态保字典序
      最小的指令列表即可得到全局字典序最小方案
    - 乱序补报后重查即基于最新事件表重算；预览不写库
    """
    codes = [b.box_code for b in payload.boxes]
    weights = {b.box_code: b.weight for b in payload.boxes}
    seq_from, seq_to = payload.seq_from, payload.seq_to
    max_len = payload.max_command_length
    handshake = payload.handshake_cost
    budget = payload.budget

    # 1) 一次查出所有指定箱在共同区间内的已收到序号（缺号不补点）
    rows = db.execute(
        select(models.BoxEvent.box_code, models.BoxEvent.seq).where(
            models.BoxEvent.box_code.in_(codes),
            models.BoxEvent.seq >= seq_from,
            models.BoxEvent.seq <= seq_to,
        )
    ).all()
    received: dict[str, set[int]] = {code: set() for code in codes}
    for box_code, seq in rows:
        received[box_code].add(seq)

    # 2) 每箱的极大连续缺号段（从未上报的箱 -> 整个区间一段）
    segments_by_box = {
        code: _missing_segments(received[code], seq_from, seq_to) for code in codes
    }

    # 3) 多选背包 DP：段按 (箱号, 段起点) 升序处理；dp[b] 为恰好消耗 b 的
    #    最优方案 (收益, 指令数, 指令元组)，按 _plan_key 取最小
    ordered_segments = [
        (code, seg_start, seg_end)
        for code in sorted(codes)
        for seg_start, seg_end in segments_by_box[code]
    ]
    dp: list[tuple[int, int, tuple] | None] = [None] * (budget + 1)
    dp[0] = (0, 0, ())
    for code, seg_start, seg_end in ordered_segments:
        seg_len = seg_end - seg_start + 1
        weight = weights[code]
        # 该段选项：k = 0..seg_len 个连续补报序号（k=0 表示不补该段）
        options: list[tuple[int, int, int, tuple]] = [(0, 0, 0, ())]
        for k in range(1, min(seg_len, budget) + 1):
            n_cmd = (k + max_len - 1) // max_len
            cost = n_cmd * handshake + k
            if cost > budget:
                break  # 消耗随 k 严格递增，后面都超预算
            options.append(
                (
                    cost,
                    k * weight,
                    n_cmd,
                    tuple(_canonical_commands(code, seg_start, k, max_len)),
                )
            )
        next_dp: list[tuple[int, int, tuple] | None] = [None] * (budget + 1)
        for used in range(budget + 1):
            current = dp[used]
            if current is None:
                continue
            for cost, value, n_cmd, cmds in options:
                new_used = used + cost
                if new_used > budget:
                    continue
                candidate = (
                    current[0] + value,
                    current[1] + n_cmd,
                    current[2] + cmds,
                )
                existing = next_dp[new_used]
                if existing is None or _plan_key(candidate) < _plan_key(existing):
                    next_dp[new_used] = candidate
        dp = next_dp

    best_used, best = min(
        ((used, plan) for used, plan in enumerate(dp) if plan is not None),
        key=lambda item: _plan_key(item[1]),
    )
    total_value, command_count, chosen = best

    # 4) 组装指令明细（chosen 按段处理顺序已按 (箱号, 起点, 终点) 有序，显式排序兜底）
    commands: list[schemas.RetransmissionCommand] = []
    covered_by_box: dict[str, set[int]] = {code: set() for code in codes}
    covered_count = 0
    for box_code, start, end in sorted(chosen):
        length = end - start + 1
        covered_count += length
        covered_by_box[box_code].update(range(start, end + 1))
        commands.append(
            schemas.RetransmissionCommand(
                box_code=box_code,
                start_seq=start,
                end_seq=end,
                length=length,
                cost=handshake + length,
                value=length * weights[box_code],
            )
        )

    # 5) 各箱仍缺报区间 = 调度前缺号段扣除本方案覆盖的序号（按请求顺序列出）
    box_results: list[schemas.RetransmissionBoxResult] = []
    for box_in in payload.boxes:
        code = box_in.box_code
        missing_count = sum(
            seg_end - seg_start + 1 for seg_start, seg_end in segments_by_box[code]
        )
        remaining_segments = _missing_segments(
            received[code] | covered_by_box[code], seq_from, seq_to
        )
        covered = len(covered_by_box[code])
        box_results.append(
            schemas.RetransmissionBoxResult(
                box_code=code,
                weight=box_in.weight,
                missing_count=missing_count,
                covered_count=covered,
                covered_value=covered * box_in.weight,
                remaining_missing_count=missing_count - covered,
                remaining_missing_ranges=[
                    schemas.RetransmissionMissingRange(
                        start_seq=seg_start,
                        end_seq=seg_end,
                        length=seg_end - seg_start + 1,
                    )
                    for seg_start, seg_end in remaining_segments
                ],
            )
        )

    return schemas.RetransmissionPreviewResult(
        seq_from=seq_from,
        seq_to=seq_to,
        max_command_length=max_len,
        handshake_cost=handshake,
        budget=budget,
        budget_used=best_used,
        budget_remaining=budget - best_used,
        total_value=total_value,
        covered_count=covered_count,
        command_count=command_count,
        commands=commands,
        boxes=box_results,
    )
