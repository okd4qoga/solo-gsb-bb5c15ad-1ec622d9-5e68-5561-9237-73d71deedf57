"""Pydantic 请求/响应模型。"""
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class EventIn(BaseModel):
    box_code: str = Field(..., min_length=1, max_length=64, description="周转箱箱号")
    seq: int = Field(..., ge=0, description="该箱递增的事件序号，从 0 开始")
    temperature: float = Field(..., ge=-100, le=100, description="摄氏温度")
    event_time: datetime = Field(..., description="ISO 8601，建议带时区，如 2026-09-23T10:00:00+08:00")

    @field_validator("box_code")
    @classmethod
    def _strip_box_code(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("box_code 不能为空")
        return v

    @field_validator("event_time")
    @classmethod
    def _to_utc(cls, v: datetime) -> datetime:
        # 无时区信息按 UTC 处理；有时区则换算为 UTC 后去掉 tzinfo 落库
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc).replace(tzinfo=None)
        return v.astimezone(timezone.utc).replace(tzinfo=None)


class EventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    box_code: str
    seq: int
    temperature: float
    event_time: datetime
    created_at: datetime


class EventAck(BaseModel):
    status: str = Field(..., description="accepted=首次入库，duplicate=重复上报（幂等忽略）")
    event: EventOut


class EventPage(BaseModel):
    box_code: str = Field(..., description="周转箱箱号")
    page_size: int = Field(..., description="本次请求的每页大小上限（1~100）")
    event_count: int = Field(..., description="本页实际返回的事件条数")
    events: list[EventOut] = Field(..., description="本页事件（按 seq 升序）")
    next_cursor: str | None = Field(
        ...,
        description=(
            "下一页游标（不透明字符串，原样回传即可）；已到末页时为 null"
        ),
    )


class BoxState(BaseModel):
    box_code: str
    seq: int
    temperature: float
    event_time: datetime


class BoxListItem(BaseModel):
    box_code: str


class HandoffIn(BaseModel):
    handoff_code: str = Field(
        ..., min_length=1, max_length=64, description="交接编号，全局唯一，作为幂等键"
    )
    box_codes: list[str] = Field(
        ..., min_length=1, description="参与交接的箱号列表，至少 1 个且不可重复"
    )
    temp_lower: float = Field(
        ..., allow_inf_nan=False, description="温度下限（℃），边界值算正常"
    )
    temp_upper: float = Field(
        ..., allow_inf_nan=False, description="温度上限（℃），边界值算正常"
    )

    @field_validator("handoff_code")
    @classmethod
    def _strip_handoff_code(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("handoff_code 不能为空")
        return v

    @field_validator("box_codes")
    @classmethod
    def _strip_and_check_box_codes(cls, v: list[str]) -> list[str]:
        codes = [c.strip() for c in v]
        if any(not c for c in codes):
            raise ValueError("箱号不能为空")
        if any(len(c) > 64 for c in codes):
            raise ValueError("箱号长度不能超过 64 字符")
        seen: set[str] = set()
        duplicates: list[str] = []
        for c in codes:
            if c in seen and c not in duplicates:
                duplicates.append(c)
            seen.add(c)
        if duplicates:
            raise ValueError(f"箱号列表存在重复：{', '.join(duplicates)}")
        return codes

    @model_validator(mode="after")
    def _check_limits(self) -> "HandoffIn":
        if self.temp_lower > self.temp_upper:
            raise ValueError(
                f"温度下限({self.temp_lower})不能大于温度上限({self.temp_upper})"
            )
        return self


class HandoffItemOut(BaseModel):
    box_code: str
    status: str = Field(
        ..., description="冻结结论：normal=正常，out_of_range=越界，missing=缺报"
    )
    seq: int | None = Field(..., description="冻结时刻该箱最大事件序号；缺报为 null")
    temperature: float | None = Field(..., description="冻结时刻的温度（℃）；缺报为 null")
    event_time: datetime | None = Field(
        ..., description="冻结时刻的 UTC 事件时间；缺报为 null"
    )


class HandoffOut(BaseModel):
    handoff_code: str
    temp_lower: float
    temp_upper: float
    box_count: int = Field(..., description="快照内箱号总数")
    normal_count: int = Field(..., description="结论为正常的箱数")
    out_of_range_count: int = Field(..., description="结论为越界的箱数")
    missing_count: int = Field(..., description="结论为缺报的箱数")
    created_at: datetime = Field(..., description="快照创建时间（UTC）")
    items: list[HandoffItemOut] = Field(
        ..., description="逐箱冻结明细，顺序与创建请求的箱号列表一致"
    )


class HandoffReconcileAddedBox(BaseModel):
    box_code: str = Field(..., description="新增箱号（只在目标快照中）")
    to_status: str = Field(..., description="目标快照中的冻结结论")
    to_seq: int | None = Field(..., description="目标快照中的冻结序号；缺报为 null")


class HandoffReconcileRemovedBox(BaseModel):
    box_code: str = Field(..., description="移出箱号（只在来源快照中）")
    from_status: str = Field(..., description="来源快照中的冻结结论")
    from_seq: int | None = Field(..., description="来源快照中的冻结序号；缺报为 null")


class HandoffReconcileCommonBox(BaseModel):
    box_code: str
    from_status: str = Field(..., description="来源快照中的冻结结论")
    to_status: str = Field(..., description="目标快照中的冻结结论")
    from_seq: int | None = Field(..., description="来源快照中的冻结序号；缺报为 null")
    to_seq: int | None = Field(..., description="目标快照中的冻结序号；缺报为 null")
    status_changed: bool = Field(
        ..., description="冻结结论是否变化（from_status != to_status）"
    )


class HandoffReconcileResult(BaseModel):
    from_code: str = Field(..., description="来源交接编号")
    to_code: str = Field(..., description="目标交接编号")
    from_box_count: int = Field(..., description="来源快照箱数")
    to_box_count: int = Field(..., description="目标快照箱数")
    added_count: int = Field(..., description="新增箱数（只在目标快照中）")
    removed_count: int = Field(..., description="移出箱数（只在来源快照中）")
    common_count: int = Field(..., description="共同箱数（两份快照都有）")
    status_changed_count: int = Field(..., description="共同箱中冻结结论变化的箱数")
    lis_length: int = Field(
        ..., description="共同箱按来源顺序映射到目标位置后的最长递增子序列长度"
    )
    min_moves: int = Field(
        ...,
        description="最少需移动箱数 = common_count - lis_length；新增、移出不计移动",
    )
    added_boxes: list[HandoffReconcileAddedBox] = Field(
        ..., description="新增箱，按目标快照顺序列出"
    )
    removed_boxes: list[HandoffReconcileRemovedBox] = Field(
        ..., description="移出箱，按来源快照顺序列出"
    )
    common_boxes: list[HandoffReconcileCommonBox] = Field(
        ..., description="共同箱，按来源快照顺序列出"
    )


class ClockAuditItem(BaseModel):
    seq: int = Field(..., description="事件序号")
    original_time: datetime = Field(..., description="原始 UTC 事件时间（不改动原始记录）")
    suggested_time: datetime = Field(..., description="保序回归后的建议 UTC 时间")
    offset_micros: int = Field(
        ..., description="建议时间 - 原时间（微秒，正为向前修正，负为回拨修正）"
    )
    block_seq_from: int = Field(..., description="该事件所属合并块的起始序号")
    block_seq_to: int = Field(..., description="该事件所属合并块的结束序号")


class TrajectoryCompressionPoint(BaseModel):
    seq: int = Field(..., description="保留事件的原始序号")
    event_time: datetime = Field(..., description="保留事件的原始 UTC 事件时间")
    temperature: float = Field(..., description="保留事件的原始温度（℃）")


class TrajectoryCompressionResult(BaseModel):
    box_code: str = Field(..., description="周转箱箱号")
    seq_from: int = Field(..., description="查询起始序号（含）")
    seq_to: int = Field(..., description="查询结束序号（含）")
    max_error: float = Field(..., description="允许的最大重建误差（℃），非负有限")
    event_count: int = Field(..., description="区间内事件条数")
    kept_count: int = Field(
        ..., description="保留事件数（满足误差约束的最少值；同数量时保留序号序列字典序最小）"
    )
    omitted_count: int = Field(..., description="被省略事件数（event_count - kept_count）")
    actual_max_error: float | None = Field(
        ...,
        description=(
            "被省略事件的重建温度与原值之差的最大绝对值（℃）；"
            "无被省略事件时为 0.0，区间内无事件时为 null"
        ),
    )
    kept_events: list[TrajectoryCompressionPoint] = Field(
        ..., description="保留事件（按序号升序），原始记录不被改动"
    )


class HeatExposureSegment(BaseModel):
    start_seq: int = Field(..., description="段起点事件序号")
    end_seq: int = Field(..., description="段终点事件序号")
    start_time: datetime = Field(..., description="段起点校正后 UTC 时间（等权保序回归）")
    end_time: datetime = Field(..., description="段终点校正后 UTC 时间（等权保序回归）")
    gap_seconds: float = Field(..., description="校正后段时长（秒）")
    start_temperature: float = Field(..., description="段起点温度（℃）")
    end_temperature: float = Field(..., description="段终点温度（℃）")
    celsius_seconds: float = Field(..., description="该段超过上限部分的精确积分（℃·秒）")


class HeatExposureResult(BaseModel):
    box_code: str = Field(..., description="周转箱箱号")
    seq_from: int = Field(..., description="查询起始序号（含）")
    seq_to: int = Field(..., description="查询结束序号（含）")
    upper_limit: float = Field(..., description="温度上限（℃）")
    max_gap_seconds: float = Field(..., description="允许计入的最大段时长（秒）")
    event_count: int = Field(..., description="区间内事件条数")
    segment_count: int = Field(..., description="计入积分的段数")
    skipped_segment_count: int = Field(
        ..., description="因校正后间隔为零或超过 max_gap_seconds 而未计入的段数"
    )
    total_celsius_seconds: float = Field(..., description="各段积分合计（℃·秒）")
    segments: list[HeatExposureSegment] = Field(..., description="计入积分的逐段明细")


class MerkleProofStep(BaseModel):
    sibling_hash: str = Field(
        ..., description="该层兄弟节点哈希（SHA-256，64 位小写十六进制）"
    )
    sibling_side: str = Field(
        ...,
        description=(
            "兄弟节点相对当前节点的位置：left=兄弟在左"
            "（父哈希 = SHA-256(0x01 || 兄弟 || 当前)）；"
            "right=兄弟在右（父哈希 = SHA-256(0x01 || 当前 || 兄弟)）"
        ),
    )


class MerkleProofResult(BaseModel):
    box_code: str = Field(..., description="周转箱箱号")
    seq_from: int = Field(..., description="查询起始序号（含）")
    seq_to: int = Field(..., description="查询结束序号（含）")
    target_seq: int = Field(..., description="目标事件序号")
    event_count: int = Field(
        ..., description="区间内实际事件条数（即 Merkle 树叶子数，缺号不补）"
    )
    leaf_data: str = Field(
        ...,
        description=(
            "目标叶子原文：由箱号、序号、UTC 时间字符串（YYYY-MM-DDTHH:MM:SS.ffffffZ）"
            "与 .17g 温度字符串组成的无空格 JSON 数组（中文等非 ASCII 字符原样 UTF-8 编码）"
        ),
    )
    target_leaf_hash: str = Field(
        ..., description="目标叶哈希：SHA-256(0x00 || leaf_data 的 UTF-8 字节)，十六进制"
    )
    root_hash: str = Field(..., description="Merkle 根哈希（十六进制）")
    proof: list[MerkleProofStep] = Field(
        ..., description="从叶层到根层逐层的兄弟哈希与方位；单事件时为空数组"
    )


class TemperatureMedianResult(BaseModel):
    box_code: str = Field(..., description="周转箱箱号")
    seq_from: int = Field(..., description="查询起始序号（含）")
    seq_to: int = Field(..., description="查询结束序号（含）")
    max_gap_seconds: float = Field(..., description="允许计入的最大段时长（秒）")
    event_count: int = Field(..., description="区间内事件条数")
    segment_count: int = Field(
        ..., description="计入分布的有效段数（校正后时长大于 0 且不超过 max_gap_seconds）"
    )
    skipped_segment_count: int = Field(
        ..., description="因校正后间隔为零或超过 max_gap_seconds 而未计入的段数"
    )
    total_seconds: float = Field(..., description="各有效段校正后时长合计（秒）")
    median_temperature: float | None = Field(
        ...,
        description=(
            "时间加权温度中位数（℃）：累计时长占比首次达到一半的最低温度；"
            "无有效段时为 null"
        ),
    )


class CalibrationAnchorIn(BaseModel):
    seq: int = Field(..., ge=0, description="锚点序号（非负整数），列表内须严格递增")
    temp_offset: float = Field(
        ...,
        allow_inf_nan=False,
        description="该序号处的温度偏移（℃，有限值），校准温度 = 原温度 + 偏移",
    )


class CalibrationIn(BaseModel):
    box_code: str = Field(..., min_length=1, max_length=64, description="周转箱箱号")
    version: int = Field(
        ..., ge=0, description="校准版本号（非负整数，建议从 1 开始；同箱不同版本并存）"
    )
    anchors: list[CalibrationAnchorIn] = Field(
        ..., min_length=2, description="校准锚点列表，至少 2 个，序号严格递增"
    )

    @field_validator("box_code")
    @classmethod
    def _strip_box_code(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("box_code 不能为空")
        return v

    @field_validator("anchors")
    @classmethod
    def _check_anchors_strictly_increasing(
        cls, v: list[CalibrationAnchorIn]
    ) -> list[CalibrationAnchorIn]:
        # min_length=2 已保证至少两个锚点；这里只校验序号严格递增（不接受相等或乱序）
        for prev, cur in zip(v, v[1:]):
            if cur.seq <= prev.seq:
                raise ValueError(
                    f"锚点序号必须严格递增，但序号 {prev.seq} 之后出现了 {cur.seq}"
                )
        return v


class CalibrationAnchorOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    seq: int = Field(..., description="锚点序号")
    temp_offset: float = Field(..., description="该序号处的温度偏移（℃，有限值）")


class CalibrationOut(BaseModel):
    box_code: str
    version: int = Field(..., description="校准版本号")
    anchor_count: int = Field(..., description="锚点数（至少 2）")
    created_at: datetime = Field(..., description="校准列表创建时间（UTC），落库后不变")
    anchors: list[CalibrationAnchorOut] = Field(
        ..., description="不可变校准锚点列表（按序号升序）"
    )


class CalibrationPoint(BaseModel):
    seq: int = Field(..., description="事件序号")
    event_time: datetime = Field(..., description="原始 UTC 事件时间（不改动原始记录）")
    temperature: float = Field(..., description="原始温度（℃，库中记录，未被修改）")
    temp_offset: float = Field(
        ...,
        description=(
            "应用的温度偏移（℃）：相邻锚点间按序号线性插值得到；"
            "区间两端沿用最近锚点偏移（不外推）"
        ),
    )
    calibrated_temperature: float = Field(
        ..., description="校准温度（℃）= 原始温度 + 温度偏移"
    )


class CalibrationTrajectoryResult(BaseModel):
    box_code: str = Field(..., description="周转箱箱号")
    version: int = Field(..., description="使用的校准版本号")
    seq_from: int = Field(..., description="查询起始序号（含）")
    seq_to: int = Field(..., description="查询结束序号（含）")
    event_count: int = Field(
        ..., description="区间内实际事件条数（缺报序号不补事件）"
    )
    points: list[CalibrationPoint] = Field(
        ...,
        description=(
            "逐条校准结果（按序号升序）：原始温度、温度偏移与校准温度；"
            "仅在本接口即时计算，原始事件、现有状态与分析结果均不被改写"
        ),
    )


class CalibrationImpactSegment(BaseModel):
    start_seq: int = Field(..., description="影响段起始事件序号（含）")
    end_seq: int = Field(..., description="影响段结束事件序号（含）；缺号断段，不补点")
    event_count: int = Field(..., description="段内结论改变且序号连续的事件数")
    from_status: str = Field(
        ...,
        description=(
            "原版本（from_version）校准温度的结论："
            "below=低于下限，normal=区间内（含边界），above=高于上限"
        ),
    )
    to_status: str = Field(
        ...,
        description=(
            "新版本（to_version）校准温度的结论，取值同 from_status；"
            "段内所有事件均为 from_status -> to_status 的同一变化方向"
        ),
    )
    max_calibration_delta: float = Field(
        ...,
        description=(
            "段内各事件新版本与原版本校准温度之差的最大绝对值（℃），"
            "即 max(|新版本校准温度 - 原版本校准温度|)"
        ),
    )


class CalibrationImpactResult(BaseModel):
    box_code: str = Field(..., description="周转箱箱号")
    from_version: int = Field(..., description="原校准版本号")
    to_version: int = Field(..., description="新校准版本号")
    seq_from: int = Field(..., description="查询起始序号（含）")
    seq_to: int = Field(..., description="查询结束序号（含）")
    temp_lower: float = Field(..., description="温度下限（℃），恰等于算区间内")
    temp_upper: float = Field(..., description="温度上限（℃），恰等于算区间内")
    event_count: int = Field(
        ..., description="区间内实际事件总数（缺报序号不补事件）"
    )
    changed_event_count: int = Field(
        ..., description="两版校准温度结论不同的事件数（各段 event_count 之和）"
    )
    segment_count: int = Field(..., description="影响段数（即 segments 长度）")
    segments: list[CalibrationImpactSegment] = Field(
        ...,
        description=(
            "结论改变事件合成的影响段（按起始序号升序）：仅纳入结论改变的事件，"
            "序号连续且变化方向相同才合段，缺号或结论未变即断段；"
            "仅在本接口即时计算，原始事件、校准数据与既有分析结果均不被改写"
        ),
    )


class RetransmissionBoxIn(BaseModel):
    box_code: str = Field(..., min_length=1, max_length=64, description="周转箱箱号")
    weight: int = Field(..., ge=1, le=10, description="该箱补报权重（1~10 的整数）")

    @field_validator("box_code")
    @classmethod
    def _strip_box_code(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("box_code 不能为空")
        return v


class RetransmissionPreviewIn(BaseModel):
    boxes: list[RetransmissionBoxIn] = Field(
        ..., min_length=1, max_length=4, description="参与调度的箱号及权重，1~4 个且箱号互异"
    )
    seq_from: int = Field(..., ge=0, description="共同序号区间起点（含，非负整数）")
    seq_to: int = Field(..., ge=0, description="共同序号区间终点（含，非负整数）")
    max_command_length: int = Field(
        ..., ge=1, le=20, description="单条补报指令最多请求的连续序号个数（1~20）"
    )
    handshake_cost: int = Field(
        ..., ge=0, le=20, description="每条指令的固定握手开销（0~20），指令消耗 = 握手开销 + 请求长度"
    )
    budget: int = Field(..., ge=0, le=200, description="总传输预算（0~200），各指令消耗之和不得超过它")

    @model_validator(mode="after")
    def _check_boxes_and_range(self) -> "RetransmissionPreviewIn":
        codes = [b.box_code for b in self.boxes]
        seen: set[str] = set()
        duplicates: list[str] = []
        for c in codes:
            if c in seen and c not in duplicates:
                duplicates.append(c)
            seen.add(c)
        if duplicates:
            raise ValueError(f"箱号列表存在重复：{', '.join(duplicates)}")
        if self.seq_from > self.seq_to:
            raise ValueError(
                f"序号区间起点({self.seq_from})不能大于终点({self.seq_to})"
            )
        if self.seq_to - self.seq_from + 1 > 60:
            raise ValueError(
                f"共同序号区间 [{self.seq_from}, {self.seq_to}] 含 "
                f"{self.seq_to - self.seq_from + 1} 个序号，超过每箱最多 60 个的限制"
            )
        return self


class RetransmissionCommand(BaseModel):
    box_code: str = Field(..., description="目标箱号")
    start_seq: int = Field(..., description="请求的起始序号（含）")
    end_seq: int = Field(..., description="请求的结束序号（含）")
    length: int = Field(..., description="请求长度（end_seq - start_seq + 1）")
    cost: int = Field(..., description="该指令消耗（握手开销 + 请求长度）")
    value: int = Field(..., description="该指令收益（请求长度 × 该箱权重）")


class RetransmissionMissingRange(BaseModel):
    start_seq: int = Field(..., description="仍缺报区间起点（含）")
    end_seq: int = Field(..., description="仍缺报区间终点（含）")
    length: int = Field(..., description="该区间缺报序号数")


class RetransmissionBoxResult(BaseModel):
    box_code: str = Field(..., description="箱号（按请求顺序列出）")
    weight: int = Field(..., description="该箱补报权重")
    missing_count: int = Field(..., description="调度前区间内缺报序号数（从未上报的箱为整个区间）")
    covered_count: int = Field(..., description="本方案计划补报的序号数")
    covered_value: int = Field(..., description="该箱收益（covered_count × weight）")
    remaining_missing_count: int = Field(..., description="执行本方案后仍缺报的序号数")
    remaining_missing_ranges: list[RetransmissionMissingRange] = Field(
        ..., description="执行本方案后该箱仍缺报的连续区间（按起点升序）"
    )


class RetransmissionPreviewResult(BaseModel):
    seq_from: int = Field(..., description="共同序号区间起点（含）")
    seq_to: int = Field(..., description="共同序号区间终点（含）")
    max_command_length: int = Field(..., description="单指令最大连续长度")
    handshake_cost: int = Field(..., description="每条指令的固定握手开销")
    budget: int = Field(..., description="总传输预算")
    budget_used: int = Field(..., description="本方案实际消耗（各指令消耗之和，不超过预算）")
    budget_remaining: int = Field(..., description="剩余预算（budget - budget_used）")
    total_value: int = Field(..., description="总收益：各指令（请求长度 × 该箱权重）之和")
    covered_count: int = Field(..., description="计划补报的序号总数（各指令长度之和）")
    command_count: int = Field(..., description="指令条数（同收益时取最少）")
    commands: list[RetransmissionCommand] = Field(
        ...,
        description=(
            "补报指令列表（按箱号、起点、终点字典序排列）：每条只请求同箱连续缺号，"
            "不跨已收到的序号；预览不写库"
        ),
    )
    boxes: list[RetransmissionBoxResult] = Field(
        ..., description="各箱缺报与补报结果（按请求顺序列出）"
    )


class TelemetryOutageSegment(BaseModel):
    start_time: datetime = Field(
        ..., description="段起点 UTC 时间（含）：同时静默箱数首次达到要求数量的时刻"
    )
    end_time: datetime = Field(
        ..., description="段终点 UTC 时间（不含）：同时静默箱数跌破要求数量的时刻"
    )
    duration_seconds: float = Field(..., description="段时长（秒）")
    peak_simultaneous_boxes: int = Field(
        ..., description="段内峰值同时静默箱数（不小于 min_simultaneous_boxes）"
    )


class TelemetryOutagesResult(BaseModel):
    box_codes: list[str] = Field(
        ..., description="参与查询的箱号（按请求顺序去重后）"
    )
    box_count: int = Field(..., description="去重后的箱号数量")
    seq_from: int = Field(..., description="查询起始序号（含）")
    seq_to: int = Field(..., description="查询结束序号（含）")
    max_silence_seconds: float = Field(
        ..., description="最大允许静默秒数：相邻校正时间间隔严格大于它才计为静默"
    )
    min_simultaneous_boxes: int = Field(
        ..., description="最少同时静默箱数：达到该数量的时间段才输出"
    )
    outage_count: int = Field(..., description="群体静默时间段数（即 outages 长度）")
    outages: list[TelemetryOutageSegment] = Field(
        ...,
        description=(
            "至少 min_simultaneous_boxes 箱同时静默的极大时间段（按起点升序，"
            "半开区间 [start_time, end_time)，端点相接的合格段已合并）"
        ),
    )
