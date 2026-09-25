# 冷链周转箱事件接收 API

基于 **Python 3.11 + FastAPI + SQLAlchemy 2.0 + SQLite** 的设备温度事件接收服务。
设备会**重试**或**延迟/乱序**上报，服务按以下规则处理：

- **相同箱号 + 相同序号 + 相同内容**（温度、事件时间）→ 判定为设备重试的重复上报，
  幂等返回已有记录（HTTP `200`，`status=duplicate`），**不改变任何状态**。
- **相同箱号 + 相同序号但内容不同** → 拒绝（HTTP `409 Conflict`）。
- **乱序/延迟事件**（序号先到 3 后到 1、中间跳号等）→ 正常入库；
  查询“最新状态”时始终取每个箱子**序号最大**的一条，与到达顺序无关。

事件列表接口 `GET /boxes/{box_code}/events` 支持**水位游标分页**：按 `seq` 升序
分批返回（`page_size` 1～100），首次查询固定该箱**当前最大事件 id** 为读取水位，
游标保存水位、箱号与本页末尾 `seq`，续页只取水位之内的记录，
**翻页期间的乱序补报不会改变本轮遍历结果**；末页游标为 `null`。

此外支持**交接快照**：`POST /handoffs` 在同一事务中按各箱当前最大序号冻结状态与
缺报/越界/正常结论；快照落库后只读，后续乱序补报不会改写已建快照。
两份快照之间可用 `GET /handoffs/{from_code}/reconcile/{to_code}` **只读对账**：
比较冻结明细得出新增/移出箱、共同箱结论变化与最少移动箱数，不重新查询箱子当前事件。

此外支持**温度漂移校准**：`POST /calibrations` 按**箱号 + 版本号**保存一份
**不可变**的校准锚点列表（至少 2 个锚点、序号严格递增、每点给出有限温度偏移），
同箱同版本原样重交幂等返回、内容不同返回 `409`；
`GET /boxes/{box_code}/calibration-trajectory` 用指定版本的锚点给出
序号区间内的**校准轨迹**（相邻锚点间按序号线性插值、两端沿用最近锚点偏移、
缺报序号不补事件），逐条返回原温度、偏移和校准温度。校准结果**只用于该新接口**，
不改写原始事件，也不影响现有状态与分析结果。

此外支持**群体遥测静默检测**：`GET /telemetry-outages` 按去重后的箱号列表、
共同序号区间、最大允许静默秒数与最少同时静默箱数，对每箱区间内实际事件做
等权保序校正时间，把相邻校正间隔**严格大于**阈值的部分作为半开静默区间
（阈值按请求原值精确解析为十进制数，间隔恰好等于阈值不算静默），
再对各箱区间做**端点扫描**，输出至少指定数量箱同时静默的**极大时间段**及
段内峰值同时静默数（端点相接的合格段合并）。全程只读，不写库、不改任何记录。

此外支持**校准版本切换影响审计**：`GET /boxes/{box_code}/calibration-impact`
同时复用该箱**两个已存校准版本**的锚点（相邻锚点间按序号线性插值、两端沿用
最近锚点偏移），对序号区间内的实际事件分别计算两版校准温度，按
**低于下限 / 区间内（含边界）/ 高于上限**判定结论；只统计两版结论不同的事件，
把**序号连续且变化方向相同**的事件合成影响段（缺号断段、不补点），逐段输出
起止序号、事件数、原/新结论与段内最大校准温差。插值与阈值判断全程使用精确数值，
边界不会误判；缺版本或箱号返回 `404`，空区间返回空段。全程只读，保留现有
校准及全部分析结果。

此外支持**多箱设备补报调度预览**：`POST /retransmission-preview` 对
**1～4 个互异箱号**（各带 1～10 的整数权重）在**共同序号闭区间**（每箱最多
60 个序号）内依据事件表找出缺号；一条补报指令只能请求**同箱连续缺号**
（不跨已收到的序号），长度受单指令最大连续长度（1～20）限制，
消耗 = 固定握手开销（0～20）+ 请求长度。在总传输预算（0～200）内
**最大化 Σ(补报序号数 × 该箱权重)**；同分选指令数更少，再按
**箱号、起点、终点字典序**确定唯一方案。返回请求指令、预算消耗与各箱
仍缺报区间；**从未上报的箱按整个指定区间缺报处理**，乱序补报后重查即重算，
预览不写库。

## 目录结构

```
.
├── app/
│   ├── __init__.py
│   ├── config.py          # 环境变量配置
│   ├── database.py        # 引擎/会话，启动建表
│   ├── models.py          # BoxEvent、交接快照、温度漂移校准模型，(box_code, seq)、handoff_code、(box_code, version) 唯一约束
│   ├── schemas.py         # 请求/响应模型与时间归一化
│   ├── services.py        # 幂等去重、冲突判定、最新状态查询、事件水位游标分页、保序回归时钟校正、超温积分、时间加权温度中位数、轨迹压缩预览、Merkle 包含证明、交接快照冻结、温度漂移校准、校准版本切换影响审计、群体遥测静默检测、多箱补报调度预览
│   └── main.py            # FastAPI 路由
├── Dockerfile
├── docker-compose.yml
├── docker-entrypoint.sh   # 容器启动：初始化建表 -> 启动服务
└── requirements.txt
```

## 一键启动（Docker Compose）

```bash
docker compose up -d --build
```

启动过程会自动完成：

1. 基于 `requirements.txt` 在镜像内安装全部依赖；
2. 容器入口脚本 `docker-entrypoint.sh` 创建数据目录并执行建表初始化（幂等）；
3. 启动 uvicorn。

数据持久化在命名卷 `coldchain-data`（容器内 `/app/data`），重启/重建容器不丢数据。

常用命令：

```bash
docker compose logs -f        # 查看日志
docker compose down           # 停止
docker compose down -v        # 停止并删除数据卷（清空数据）
```

## 本地直接运行（不用 Docker）

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

首次请求前/启动时会自动创建 SQLite 表（`./data/coldchain.db`）。

## 配置

通过环境变量配置（均有默认值）：

| 环境变量       | 默认值                          | 说明                                       |
| -------------- | ------------------------------- | ------------------------------------------ |
| `DATABASE_URL` | `sqlite:///./data/coldchain.db` | 数据库连接串（SQLAlchemy 格式）            |
| `APP_HOST`     | `0.0.0.0`                       | 监听地址                                   |
| `APP_PORT`     | `8000`                          | 监听端口（Compose 左侧映射端口同样取它）   |

Compose 下可用 `.env` 或临时变量覆盖，例如改宿主端口：

```bash
APP_PORT=9000 docker compose up -d --build
# 此时宿主访问端口为 9000
```

## API 地址与端口

- 服务地址：`http://localhost:8000`（按上面的端口配置调整）
- Swagger 交互文档：<http://localhost:8000/docs>
- ReDoc：<http://localhost:8000/redoc>

## 接口一览

| 方法 | 路径                          | 说明                                   |
| ---- | ----------------------------- | -------------------------------------- |
| GET  | `/health`                     | 健康检查                               |
| POST | `/events`                     | 上报一个事件（幂等，支持设备重试）     |
| GET  | `/states`                     | 所有箱子的最新状态（按最大序号）       |
| GET  | `/boxes`                      | 所有上报过的箱号                       |
| GET  | `/boxes/{box_code}/state`     | 单个箱子的最新状态                     |
| GET  | `/boxes/{box_code}/events`    | 单个箱子的事件（按序号升序分批，水位游标翻页） |
| GET  | `/boxes/{box_code}/clock-audit` | 区间事件的设备时钟回拨校正建议（保序回归，不改原始记录） |
| GET  | `/boxes/{box_code}/heat-exposure` | 区间事件的超温积分（℃·秒，线性插值 + 保序校正时间，不改原始记录） |
| GET  | `/boxes/{box_code}/temperature-median` | 区间按持续时间加权的温度中位数（线性插值 + 保序校正时间，不改原始记录） |
| GET  | `/boxes/{box_code}/trajectory-compression` | 区间温度轨迹压缩预览（最少保留点 + 线性插值误差受限，只读不写回） |
| GET  | `/boxes/{box_code}/merkle-proof` | 区间事件的 Merkle 包含证明（按 seq 升序建树，只读不写回） |
| POST | `/handoffs`                     | 创建交接快照（同一事务冻结各箱当前最大序号的状态，幂等） |
| GET  | `/handoffs/{handoff_code}`      | 交接快照的冻结明细与各类数量               |
| GET  | `/handoffs/{from_code}/reconcile/{to_code}` | 只读对账两份交接快照：新增/移出箱、共同箱结论变化与最少移动箱数 |
| POST | `/calibrations`                 | 按箱号 + 版本号保存不可变温度漂移校准锚点列表（至少 2 点、序号严格递增，幂等） |
| GET  | `/boxes/{box_code}/calibrations/{version}` | 查询某箱某版本的不可变校准锚点列表 |
| GET  | `/boxes/{box_code}/calibration-trajectory` | 指定版本与序号区间的校准轨迹（锚点间按序号线性插值，不改原始记录） |
| GET  | `/boxes/{box_code}/calibration-impact` | 校准版本切换影响审计：两版锚点分别插值判结论，合成结论改变的影响段（精确数值，只读） |
| GET  | `/telemetry-outages` | 多箱群体遥测静默检测（端点扫描，输出同时静默箱数达标的极大时间段与峰值，只读） |
| POST | `/retransmission-preview` | 多箱设备补报调度预览（预算内最大化加权补报数，返回指令与各箱仍缺报区间，只读不写库） |

### 上报事件

`POST /events`

请求体：

```json
{
  "box_code": "BOX-001",
  "seq": 0,
  "temperature": -4.5,
  "event_time": "2026-09-23T10:00:00+08:00"
}
```

字段说明：

- `box_code`：箱号，非空字符串（≤64 字符）。
- `seq`：该箱递增的事件序号，非负整数；不要求连续，乱序/补报均可。
  取值范围为 **0 ～ 9223372036854775807**（即 `2^63 - 1`，SQLite 有符号 64 位
  INTEGER 上界），超出（含负数）返回 `422`，不会在写库时触发 500。
- `temperature`：摄氏温度，范围 `-100 ~ 100`。
- `event_time`：ISO 8601 时间；**建议带时区**。服务统一换算为 UTC 存储与比较，
  不带时区的时间按 UTC 处理。

### 整数取值范围（SQLite 有符号 64 位）

所有需要**入库或参与 SQL 查询**的整数——事件序号（`POST /events` 的 `seq`、
各区间分析接口的 `seq_from` / `seq_to` / `target_seq`、校准锚点的 `seq`、
补报预览的区间端点）、**校准版本号**（`POST /calibrations` 的 `version`、
`GET /boxes/{box_code}/calibrations/{version}` 的路径参数、
`calibration-trajectory` / `calibration-impact` 的 `version` / `from_version` /
`to_version`）以及**事件分页游标内的整数**（读取水位 `watermark`、上页末尾
`last_seq`）——统一约束为 SQLite 有符号 64 位 INTEGER 范围：

```
0 ≤ 整数 ≤ 9223372036854775807   （2^63 - 1）
```

- 请求体 / 查询参数 / 路径参数中的序号或版本号越界（如
  `seq=9223372036854775808`、负数）→ `422 Unprocessable Entity`；
- 游标内水位或末尾序号超出该范围（或为负数、类型错误等）→
  `422`（`invalid_cursor`），续页查询不再因 SQLite 参数绑定溢出而返回 `500`；
- 合法最大值 `9223372036854775807` 可正常提交、入库、查询与翻页；游标由服务端
  基于自增事件 id 生成，正常翻页与跨箱游标（`cursor_box_mismatch`）行为不变。

响应：

- 首次入库：`201 Created`，`status = "accepted"`
- 重复上报（同序号且温度、事件时间完全一致）：`200 OK`，`status = "duplicate"`
- 同序号内容不一致：`409 Conflict`，响应体带库中已有值与本次值，便于排查
- 参数非法：`422 Unprocessable Entity`
- 查询不存在的箱子：`404 Not Found`

## 使用示例

```bash
# 上报 seq=1
curl -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-001","seq":1,"temperature":-4.5,
       "event_time":"2026-09-23T10:01:00+08:00"}'

# 设备重试：原样重发 -> 200 duplicate，状态不变
curl -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-001","seq":1,"temperature":-4.5,
       "event_time":"2026-09-23T10:01:00+08:00"}'

# 同序号改温度 -> 409 拒绝
curl -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-001","seq":1,"temperature":9.9,
       "event_time":"2026-09-23T10:01:00+08:00"}'

# 乱序补报 seq=0 / seq=2 ... 最新状态始终取最大 seq
curl http://localhost:8000/boxes/BOX-001/state
curl http://localhost:8000/states
```

## 事件分页（/boxes/{box_code}/events）

`GET /boxes/{box_code}/events`（默认访问端口 **8000**，如
`http://localhost:8000/boxes/BOX-001/events`）按 **`seq` 升序分批**返回某箱事件，
通过**游标**翻页：

- **首次查询**（不带 `cursor`）把该箱**当前最大事件 id** 固定为**读取水位**，
  只返回 `id <= 水位` 的记录；
- 响应中的 `next_cursor` 保存**水位、箱号与本页末尾 `seq`**，续页**原样回传**即可，
  服务端只取 `id <= 水位 且 seq > 末尾 seq` 的记录；
- 事件 id 自增、只增不改，因此**翻页期间的乱序补报**（无论补的是中间缺号还是
  更大序号）id 必然大于水位，**不会改变本轮遍历结果**；
  重新发起首页查询则基于最新数据重新开始；
- **末页 `next_cursor` 为 `null`**，翻页结束。

查询参数：

| 参数        | 说明                                                                   |
| ----------- | ---------------------------------------------------------------------- |
| `page_size` | 每页最多返回的事件条数，**1～100 的整数**，缺省 `50`；越界或非法返回 `422` |
| `cursor`    | 上一页响应的 `next_cursor`，**原样回传**；不传表示从首页开始             |

返回字段：

| 字段          | 说明                                                               |
| ------------- | ------------------------------------------------------------------ |
| `box_code`    | 箱号                                                               |
| `page_size`   | 本次请求的页大小上限                                               |
| `event_count` | 本页实际返回的事件条数                                             |
| `events[]`    | 本页事件（按 `seq` 升序），字段与上报回执中的事件一致              |
| `next_cursor` | 下一页游标（不透明字符串，内含读取水位、箱号与本页末尾 `seq`）；**末页为 `null`** |

错误情况：游标**格式错误**（无法解码、字段缺失/非法、版本不符，或游标内
水位 / 末尾序号超出 `0 ~ 9223372036854775807` 的有符号 64 位范围等）返回
`422`（`invalid_cursor`）；**跨箱使用**（游标内箱号与路径箱号不一致）返回
`422`（`cursor_box_mismatch`）；箱号**从未上报过**返回 `404`（带不带游标都一样）。

```bash
# 首页：固定读取水位，按 seq 升序返回前 2 条
curl 'http://localhost:8000/boxes/BOX-001/events?page_size=2'
```

响应（还有下一页，`next_cursor` 非空）：

```json
{
  "box_code": "BOX-001",
  "page_size": 2,
  "event_count": 2,
  "events": [
    {"box_code": "BOX-001", "seq": 0, "temperature": -4.0,
     "event_time": "2026-09-25T08:00:00", "created_at": "2026-09-25T08:00:01.123456"},
    {"box_code": "BOX-001", "seq": 1, "temperature": -4.1,
     "event_time": "2026-09-25T08:01:00", "created_at": "2026-09-25T08:01:01.123456"}
  ],
  "next_cursor": "eyJ2IjoxLCJib3hfY29kZSI6IkJPWC0wMDEiLCJ3YXRlcm1hcmsiOjUsImxhc3Rfc2VxIjoxfQ=="
}
```

```bash
# 续页：原样回传上一页的 next_cursor（URL 中含 = 结尾，建议加引号）
curl 'http://localhost:8000/boxes/BOX-001/events?page_size=2&cursor=eyJ2IjoxLCJib3hfY29kZSI6IkJPWC0wMDEiLCJ3YXRlcm1hcmsiOjUsImxhc3Rfc2VxIjoxfQ=='
# -> events 为 seq 2、3，并给出新的 next_cursor；再翻一页得到 seq 4 且
#    next_cursor 为 null（末页）。翻页期间新上报的事件不会插入本轮遍历。

# page_size 越界（0、101 等）-> 422
curl -i 'http://localhost:8000/boxes/BOX-001/events?page_size=0'
curl -i 'http://localhost:8000/boxes/BOX-001/events?page_size=101'

# 游标格式错误 -> 422（invalid_cursor）
curl -i 'http://localhost:8000/boxes/BOX-001/events?cursor=not-a-cursor'

# 跨箱使用游标（把 BOX-001 的游标用于 BOX-002）-> 422（cursor_box_mismatch）
curl -i 'http://localhost:8000/boxes/BOX-002/events?cursor=eyJ2IjoxLCJib3hfY29kZSI6IkJPWC0wMDEiLCJ3YXRlcm1hcmsiOjUsImxhc3Rfc2VxIjoxfQ=='

# 箱号从未上报过 -> 404
curl -i 'http://localhost:8000/boxes/NO-SUCH-BOX/events'
```

## 时钟回拨校正（clock-audit）

设备 RTC 发生回拨时，后发生的事件其 `event_time` 反而更早，物理时间序列会出现“倒退”。
`GET /boxes/{box_code}/clock-audit?seq_from=&seq_to=` 对**指定序号区间（含端点）**
即时生成校正建议，**不会改动任何原始记录**；乱序补报后再次查询会基于最新数据重新计算。

算法：按 `seq` 升序取出区间内事件，将 UTC 事件时间换算为**整数微秒**，做
**等权保序回归**（isotonic regression，PAVA）——在建议时间非递减的约束下，
最小化各事件时间偏移的平方和；实现方式即把相邻的违规块不断合并、块内取算术均值，
均值四舍五入到微秒。例如连续两个时间分别为第 10 秒、第 0 秒时，两条都会建议为
第 5 秒（前面的往后拨、后面的往前调），而不是只把后者改成 10 秒。

查询参数：

- `seq_from` / `seq_to`：序号区间，**均为整数且含端点**，必须 `>= 0`、`seq_from <= seq_to`，且均不超过 `9223372036854775807`（`2^63 - 1`，SQLite 有符号 64 位上界，越界 `422`）。

返回：数组，每个元素对应一条区间内事件（按 seq 升序）；区间内无事件返回 `[]`。

| 字段             | 说明                                                       |
| ---------------- | ---------------------------------------------------------- |
| `seq`            | 事件序号                                                   |
| `original_time`  | 原始 UTC 事件时间（库中记录，未被修改）                    |
| `suggested_time` | 保序回归后的建议 UTC 时间                                  |
| `offset_micros`  | 建议时间 − 原时间，单位微秒（正=向前修正，负=回拨修正）    |
| `block_seq_from` / `block_seq_to` | 该事件所属合并块的序号范围（同一块内建议时间相同） |

错误情况：箱号不存在返回 `404`；缺少参数、参数非整数、负数或 `seq_from > seq_to`
返回 `422`。

```bash
# 先造一组带时钟回拨的事件（UTC）：seq=0 在第 10 秒，seq=1 倒退到第 0 秒
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-AUDIT","seq":0,"temperature":-4.5,
       "event_time":"2026-09-23T02:00:10+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-AUDIT","seq":1,"temperature":-4.6,
       "event_time":"2026-09-23T02:00:00+00:00"}'
# 再补一条正常推进的 seq=2
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-AUDIT","seq":2,"temperature":-4.7,
       "event_time":"2026-09-23T02:00:20+00:00"}'

# 查询 0..2 的校正建议
curl 'http://localhost:8000/boxes/BOX-AUDIT/clock-audit?seq_from=0&seq_to=2'
```

响应（seq=0、seq=1 合并为一个块，都建议为 02:00:05；seq=2 独立成块）：

```json
[
  {
    "seq": 0,
    "original_time": "2026-09-23T02:00:10",
    "suggested_time": "2026-09-23T02:00:05",
    "offset_micros": -5000000,
    "block_seq_from": 0,
    "block_seq_to": 1
  },
  {
    "seq": 1,
    "original_time": "2026-09-23T02:00:00",
    "suggested_time": "2026-09-23T02:00:05",
    "offset_micros": 5000000,
    "block_seq_from": 0,
    "block_seq_to": 1
  },
  {
    "seq": 2,
    "original_time": "2026-09-23T02:00:20",
    "suggested_time": "2026-09-23T02:00:20",
    "offset_micros": 0,
    "block_seq_from": 2,
    "block_seq_to": 2
  }
]
```

```bash
# 箱号存在但区间内无事件 -> 200，空数组
curl 'http://localhost:8000/boxes/BOX-AUDIT/clock-audit?seq_from=100&seq_to=200'
# []

# 箱号不存在 -> 404
curl -i 'http://localhost:8000/boxes/NO-SUCH-BOX/clock-audit?seq_from=0&seq_to=1'

# 范围非法（seq_from > seq_to）-> 422
curl -i 'http://localhost:8000/boxes/BOX-AUDIT/clock-audit?seq_from=5&seq_to=2'
```

## 超温积分（heat-exposure）

`GET /boxes/{box_code}/heat-exposure`（默认访问端口 **8000**）计算指定序号区间内
温度超过上限的**累计暴露量**，单位为 **℃·秒**：

- 按 `seq` 升序取区间内事件，事件时间沿用 clock-audit 的**等权保序回归校正**
  （只读即时计算，不改原始记录；乱序补报后重查即自动重算）。
- 相邻两条事件之间温度视为**线性变化**，对超过 `upper_limit` 的部分做**精确积分**
  （两端都超为梯形、一端超出为三角形、都不超为 0）。
- **缺号不自动补点**：相邻指区间内实际存在的相邻两条事件，序号可以跳号。
- 校正后间隔**为零**（同一合并块）或**超过 `max_gap_seconds`** 的段**不计入积分**，
  也不出现在明细中（以 `skipped_segment_count` 计数）。
- 箱号不存在返回 `404`；区间内无事件返回零值与空明细；参数非法返回 `422`。

查询参数：

- `seq_from` / `seq_to`：序号区间，**均为整数且含端点**，必须 `>= 0`、`seq_from <= seq_to`，且均不超过 `9223372036854775807`（`2^63 - 1`，SQLite 有符号 64 位上界，越界 `422`）。
- `upper_limit`：温度上限（℃），超过部分计入积分，须为有限数值。
- `max_gap_seconds`：允许计入的最大相邻校正间隔（秒），**必须为正数**（有限数值）。

返回字段：

| 字段                     | 说明                                                       |
| ------------------------ | ---------------------------------------------------------- |
| `event_count`            | 区间内事件条数                                             |
| `segment_count`          | 计入积分的段数（即 `segments` 长度）                       |
| `skipped_segment_count`  | 因校正后间隔为零或超过 `max_gap_seconds` 而未计入的段数    |
| `total_celsius_seconds`  | 各段积分合计（℃·秒）                                       |
| `segments[]`             | 计入积分的逐段明细                                         |

`segments[]` 中每段包含：`start_seq` / `end_seq`（端点事件序号）、
`start_time` / `end_time`（**校正后**的 UTC 时间）、`gap_seconds`（校正后段时长）、
`start_temperature` / `end_temperature`、`celsius_seconds`（该段积分）。

```bash
# 先上报一组事件（UTC）：-5℃ -> 1℃ -> 3℃ -> -1℃，每分钟一条
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-HEAT","seq":0,"temperature":-5.0,
       "event_time":"2026-09-23T02:00:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-HEAT","seq":1,"temperature":1.0,
       "event_time":"2026-09-23T02:01:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-HEAT","seq":2,"temperature":3.0,
       "event_time":"2026-09-23T02:02:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-HEAT","seq":3,"temperature":-1.0,
       "event_time":"2026-09-23T02:03:00+00:00"}'

# 查询 0..3 区间、上限 -2℃、最大间隔 120 秒的超温积分
curl 'http://localhost:8000/boxes/BOX-HEAT/heat-exposure?seq_from=0&seq_to=3&upper_limit=-2&max_gap_seconds=120'
```

响应（段 0→1 线性穿越上限，超出部分为三角形 45；段 1→2、2→3 全程超限，梯形 240、180）：

```json
{
  "box_code": "BOX-HEAT",
  "seq_from": 0,
  "seq_to": 3,
  "upper_limit": -2.0,
  "max_gap_seconds": 120.0,
  "event_count": 4,
  "segment_count": 3,
  "skipped_segment_count": 0,
  "total_celsius_seconds": 465.0,
  "segments": [
    {
      "start_seq": 0,
      "end_seq": 1,
      "start_time": "2026-09-23T02:00:00",
      "end_time": "2026-09-23T02:01:00",
      "gap_seconds": 60.0,
      "start_temperature": -5.0,
      "end_temperature": 1.0,
      "celsius_seconds": 45.0
    },
    {
      "start_seq": 1,
      "end_seq": 2,
      "start_time": "2026-09-23T02:01:00",
      "end_time": "2026-09-23T02:02:00",
      "gap_seconds": 60.0,
      "start_temperature": 1.0,
      "end_temperature": 3.0,
      "celsius_seconds": 240.0
    },
    {
      "start_seq": 2,
      "end_seq": 3,
      "start_time": "2026-09-23T02:02:00",
      "end_time": "2026-09-23T02:03:00",
      "gap_seconds": 60.0,
      "start_temperature": 3.0,
      "end_temperature": -1.0,
      "celsius_seconds": 180.0
    }
  ]
}
```

```bash
# 箱号存在但区间内无事件 -> 200，零值 + 空明细
curl 'http://localhost:8000/boxes/BOX-HEAT/heat-exposure?seq_from=100&seq_to=200&upper_limit=-2&max_gap_seconds=120'

# 箱号不存在 -> 404
curl -i 'http://localhost:8000/boxes/NO-SUCH-BOX/heat-exposure?seq_from=0&seq_to=1&upper_limit=-2&max_gap_seconds=120'

# 参数非法（seq_from > seq_to、max_gap_seconds <= 0 等）-> 422
curl -i 'http://localhost:8000/boxes/BOX-HEAT/heat-exposure?seq_from=5&seq_to=2&upper_limit=-2&max_gap_seconds=120'
curl -i 'http://localhost:8000/boxes/BOX-HEAT/heat-exposure?seq_from=0&seq_to=3&upper_limit=-2&max_gap_seconds=0'
```

## 时间加权温度中位数（temperature-median）

`GET /boxes/{box_code}/temperature-median`（默认访问端口 **8000**，
如 `http://localhost:8000/boxes/BOX-MED/temperature-median`）返回指定序号区间内
**按持续时间加权的温度中位数**与有效总秒数：

- 按 `seq` 升序取闭区间 `[seq_from, seq_to]` 内事件，事件时间沿用 clock-audit 的
  **等权保序回归校正**（只读即时计算，不改原始记录；乱序补报后重查即自动重算）。
- 相邻两条事件（**缺号不补点**，序号可以跳号）之间温度视为**随时间线性变化**，
  把每个有效段的**每一瞬间按其持续时间**计入温度分布：
  - 两端温度不同的段，时长在该温度区间上**均匀分布**；
  - **恒温段**的整段时长作为该温度上的**点质量**（离散原子）。
- 仅纳入校正后时长**大于 0 且不超过 `max_gap_seconds`** 的段；间隔为零
  （同一合并块）或超过上限的段不计入（以 `skipped_segment_count` 计数）。
- `median_temperature` 取**累计时长占比首次达到一半（≥ 1/2）的最低温度**
  （即分布函数的下侧广义逆；并列时取低温侧）。全程用精确有理数计算，
  半总时长边界上的并列不会因浮点误差选错点。
- **没有任何有效段**（区间无事件、只有一条事件、或全部段都被过滤）时
  `median_temperature = null`、`total_seconds = 0.0`。
- 箱号不存在返回 `404`；参数非法返回 `422`。

查询参数：

- `seq_from` / `seq_to`：序号区间，**均为整数且含端点**，必须 `>= 0`、`seq_from <= seq_to`，且均不超过 `9223372036854775807`（`2^63 - 1`，SQLite 有符号 64 位上界，越界 `422`）。
- `max_gap_seconds`：允许计入的最大相邻校正间隔（秒），**必须为正数**（有限数值）。

返回字段：

| 字段                      | 说明                                                       |
| ------------------------- | ---------------------------------------------------------- |
| `event_count`             | 区间内事件条数                                             |
| `segment_count`           | 计入分布的有效段数（校正后时长 > 0 且 ≤ `max_gap_seconds`） |
| `skipped_segment_count`   | 因校正后间隔为零或超过 `max_gap_seconds` 而未计入的段数    |
| `total_seconds`           | 各有效段校正后时长合计（秒）；无有效段为 `0.0`             |
| `median_temperature`      | 累计占比首次达到一半的最低温度（℃）；无有效段为 `null`     |

```bash
# 先上报：seq=0/1 设备时钟回拨（00:10、00:00，校正后同为 00:05 -> 该段零间隔，不计入）；
# seq=2 在 00:01:40 为 0℃；seq=3 在 00:03:40 为 10℃（缺号不补点，直接与 seq=5 相邻）；
# seq=5 在 00:05:40 恒温 10℃
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-MED","seq":0,"temperature":0.0,"event_time":"2026-09-24T00:00:10+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-MED","seq":1,"temperature":4.0,"event_time":"2026-09-24T00:00:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-MED","seq":2,"temperature":0.0,"event_time":"2026-09-24T00:01:40+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-MED","seq":3,"temperature":10.0,"event_time":"2026-09-24T00:03:40+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-MED","seq":5,"temperature":10.0,"event_time":"2026-09-24T00:05:40+00:00"}'

# 查询 0..5、最大间隔 120 秒
curl 'http://localhost:8000/boxes/BOX-MED/temperature-median?seq_from=0&seq_to=5&max_gap_seconds=120'
```

响应（seq=0、1 校正后同刻，段 0→1 零间隔跳过；有效段为 95 秒 4→0、
120 秒 0→10、120 秒恒温 10℃，合计 335 秒；累计一半落在升温段内的 6.0417℃）：

```json
{
  "box_code": "BOX-MED",
  "seq_from": 0,
  "seq_to": 5,
  "max_gap_seconds": 120.0,
  "event_count": 5,
  "segment_count": 3,
  "skipped_segment_count": 1,
  "total_seconds": 335.0,
  "median_temperature": 6.041666666666667
}
```

恒温段作为点质量的简化示例（100 秒恒温 0℃ + 200 秒线性 0→10℃，共 300 秒，
一半即 150 秒：0℃ 点质量占 100 秒，剩余 50 秒在升温段内，对应 2.5℃）：

```bash
# 无有效段（区间无事件 / 只有一条事件 / 段全被过滤）-> median 为 null、总秒数为 0
curl 'http://localhost:8000/boxes/BOX-MED/temperature-median?seq_from=100&seq_to=200&max_gap_seconds=120'
# {"event_count":0,"segment_count":0,"skipped_segment_count":0,
#  "total_seconds":0.0,"median_temperature":null, ...}

# 间隔恰好等于 max_gap_seconds 计入；调小到 94 秒后所有段被过滤 -> null / 0
curl 'http://localhost:8000/boxes/BOX-MED/temperature-median?seq_from=0&seq_to=5&max_gap_seconds=94'

# 箱号不存在 -> 404
curl -i 'http://localhost:8000/boxes/NO-SUCH-BOX/temperature-median?seq_from=0&seq_to=1&max_gap_seconds=120'

# 参数非法（seq_from > seq_to、max_gap_seconds <= 0、NaN/Infinity、缺参数等）-> 422
curl -i 'http://localhost:8000/boxes/BOX-MED/temperature-median?seq_from=5&seq_to=2&max_gap_seconds=120'
curl -i 'http://localhost:8000/boxes/BOX-MED/temperature-median?seq_from=0&seq_to=3&max_gap_seconds=0'
```

## 温度轨迹压缩预览（trajectory-compression）

`GET /boxes/{box_code}/trajectory-compression`（默认访问端口 **8000**，
如 `http://localhost:8000/boxes/BOX-CMP/trajectory-compression`）对指定序号区间内的
温度轨迹做**有损压缩预览**：以**序号为横坐标**，在保留点之间按温度**线性插值**
重建被省略的事件，**预览结果只在查询时即时计算，不写回任何原始记录**。

- 按 `seq` 升序取闭区间 `[seq_from, seq_to]` 内事件（缺号不补点，只读库中实际事件），
  **单次至多 120 条**，区间内超过 120 条返回 `422`。
- 给定非负且有限的允许最大重建误差 `max_error`（℃），寻找**保留事件数最少**的子序列：
  **首末事件必须保留**；每个被省略事件的重建温度与其原值之差不得超过 `max_error`。
- 保留数量相同时，选择**保留序号序列字典序最小**的方案
  （即更早的事件优先被保留；用 DAG 最短路 DP 精确求解，而非简单贪心跳最远）。
- 区间内**只有一条事件时直接保留**；箱号存在但**区间内无事件返回空结果**
  （`kept_events=[]`、`actual_max_error=null`）；箱号不存在返回 `404`。

查询参数：

- `seq_from` / `seq_to`：序号区间，**均为整数且含端点**，必须 `>= 0`、`seq_from <= seq_to`，且均不超过 `9223372036854775807`（`2^63 - 1`，SQLite 有符号 64 位上界，越界 `422`）。
- `max_error`：允许的最大重建误差（℃），**非负、有限**数值（传负数、`NaN`、`Infinity` 均为 `422`）。

返回字段：

| 字段                | 说明                                                          |
| ------------------- | ------------------------------------------------------------- |
| `event_count`       | 区间内事件条数                                                |
| `kept_count`        | 保留事件数（满足误差约束的最少值，同数量取字典序最小方案）    |
| `omitted_count`     | 被省略事件数（`event_count - kept_count`）                    |
| `actual_max_error`  | 被省略事件重建温度与原值之差的最大绝对值（℃）；无省略点为 `0.0`，区间无事件为 `null` |
| `kept_events[]`     | 保留事件的原始 `seq` / `event_time` / `temperature`（按序号升序） |

```bash
# 先上报 8 条事件（UTC，每分钟一条）：前 5 条在 -5℃ 附近小幅波动，随后升温
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-CMP","seq":0,"temperature":-5.0,"event_time":"2026-09-23T02:00:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-CMP","seq":1,"temperature":-4.8,"event_time":"2026-09-23T02:01:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-CMP","seq":2,"temperature":-5.2,"event_time":"2026-09-23T02:02:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-CMP","seq":3,"temperature":-4.9,"event_time":"2026-09-23T02:03:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-CMP","seq":4,"temperature":-5.0,"event_time":"2026-09-23T02:04:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-CMP","seq":5,"temperature":-3.0,"event_time":"2026-09-23T02:05:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-CMP","seq":6,"temperature":-2.6,"event_time":"2026-09-23T02:06:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-CMP","seq":7,"temperature":-2.0,"event_time":"2026-09-23T02:07:00+00:00"}'

# 以 0.3℃ 的最大重建误差预览 0..7 的压缩结果
curl 'http://localhost:8000/boxes/BOX-CMP/trajectory-compression?seq_from=0&seq_to=7&max_error=0.3'
```

响应（seq 1/2/3 由弦 (0,4) 重建，最大偏差 0.2℃；seq 6 由弦 (5,7) 重建，偏差 0.1℃；
seq 5 温度跳变必须保留，共省略 4 条）：

```json
{
  "box_code": "BOX-CMP",
  "seq_from": 0,
  "seq_to": 7,
  "max_error": 0.3,
  "event_count": 8,
  "kept_count": 4,
  "omitted_count": 4,
  "actual_max_error": 0.20000000000000018,
  "kept_events": [
    {"seq": 0, "event_time": "2026-09-23T02:00:00", "temperature": -5.0},
    {"seq": 4, "event_time": "2026-09-23T02:04:00", "temperature": -5.0},
    {"seq": 5, "event_time": "2026-09-23T02:05:00", "temperature": -3.0},
    {"seq": 7, "event_time": "2026-09-23T02:07:00", "temperature": -2.0}
  ]
}
```

同数量方案的字典序 tie-break 示例（温度 `-5,-4,-4,-5`，`max_error=0.5`）：
保留 `{0,1,3}`（省略 seq=2，弦 (1,3) 在 seq=2 重建为 -4.5，误差恰为 0.5）
与保留 `{0,2,3}`（省略 seq=1，误差同样为 0.5）都是 3 个保留点的可行最优方案，
按序号序列字典序最小选择前者：

```bash
# 仅一条事件 -> 直接保留，actual_max_error=0.0
curl 'http://localhost:8000/boxes/BOX-CMP/trajectory-compression?seq_from=2&seq_to=2&max_error=0.3'

# 箱号存在但区间内无事件 -> 200，空结果（actual_max_error=null）
curl 'http://localhost:8000/boxes/BOX-CMP/trajectory-compression?seq_from=100&seq_to=200&max_error=0.3'

# 箱号不存在 -> 404
curl -i 'http://localhost:8000/boxes/NO-SUCH-BOX/trajectory-compression?seq_from=0&seq_to=1&max_error=0.3'

# 区间内超过 120 条 -> 422
curl -i 'http://localhost:8000/boxes/BOX-CMP/trajectory-compression?seq_from=0&seq_to=1000&max_error=0.3'

# 参数非法（seq_from > seq_to、负误差、NaN/Infinity 等）-> 422
curl -i 'http://localhost:8000/boxes/BOX-CMP/trajectory-compression?seq_from=5&seq_to=2&max_error=0.3'
curl -i 'http://localhost:8000/boxes/BOX-CMP/trajectory-compression?seq_from=0&seq_to=3&max_error=-0.1'
```

## 事件区间包含证明（merkle-proof）

`GET /boxes/{box_code}/merkle-proof`（默认访问端口 **8000**，
如 `http://localhost:8000/boxes/BOX-MK/merkle-proof`）对指定序号区间内的实际已存事件
构造 **Merkle 树**，并返回目标事件的**包含证明**（inclusion proof），供调用方离线重算根哈希、
核对事件未被篡改。**只在查询时即时计算，不补缺号、不写回任何记录**。

建树规则（叶子顺序即查询顺序：按 `seq` 升序的实际事件，缺号不补）：

- **叶子原文**：由箱号、序号、UTC 时间字符串（`YYYY-MM-DDTHH:MM:SS.ffffffZ`）和
  `.17g` 格式的温度字符串组成的**无空格 JSON 数组**，中文等非 ASCII 字符**原样保留**
  （不按 `\uXXXX` 转义），按 UTF-8 编码。例如
  `["BOX-MK",2,"2026-09-23T02:02:30.123456Z","-4.125"]`。
- **叶哈希**：`SHA-256(0x00 || 叶子原文的 UTF-8 字节)`。
- **父节点哈希**：`SHA-256(0x01 || 左哈希 || 右哈希)`；某层节点数为奇数时
  **复制末节点**再两两配对。
- **单事件**时叶即根，证明路径为空数组。

查询参数：

- `seq_from` / `seq_to`：序号区间，**均为整数且含端点**，必须 `>= 0`、`seq_from <= seq_to`，且均不超过 `9223372036854775807`（`2^63 - 1`，SQLite 有符号 64 位上界，越界 `422`）。
- `target_seq`：目标事件序号，**须为区间内实际存在的事件**（缺号不算）；非负且
  不超过 `9223372036854775807`（`2^63 - 1`），越界 `422`。

返回字段：

| 字段                | 说明                                                          |
| ------------------- | ------------------------------------------------------------- |
| `event_count`       | 区间内实际事件条数（即叶子数，缺号不补）                      |
| `leaf_data`         | 目标叶子原文（无空格 JSON 数组，可直接哈希核对）              |
| `target_leaf_hash`  | 目标叶哈希（十六进制）                                        |
| `root_hash`         | Merkle 根哈希（十六进制）                                     |
| `proof[]`           | 从叶层到根层逐层的 `sibling_hash` 与 `sibling_side`（`left`/`right` 表示兄弟在当前节点的左/右）；单事件时为空 |

错误情况：箱号不存在、目标序号不在区间实际事件中（含区间为空）返回 `404`；
缺少参数、参数非整数、负数或 `seq_from > seq_to` 返回 `422`。

```bash
# 先上报 5 条事件（UTC；注意跳过 seq=3，缺号不补）
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-MK","seq":0,"temperature":-5.0,"event_time":"2026-09-23T02:00:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-MK","seq":1,"temperature":-4.5,"event_time":"2026-09-23T02:01:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-MK","seq":2,"temperature":-4.125,"event_time":"2026-09-23T02:02:30.123456+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-MK","seq":4,"temperature":0.1,"event_time":"2026-09-23T02:04:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-MK","seq":5,"temperature":-3.0,"event_time":"2026-09-23T02:05:00+00:00"}'

# 取 seq=2 在区间 [0,5] 上的包含证明（5 个叶子，奇数层复制末节点）
curl 'http://localhost:8000/boxes/BOX-MK/merkle-proof?seq_from=0&seq_to=5&target_seq=2'
```

响应：

```json
{
  "box_code": "BOX-MK",
  "seq_from": 0,
  "seq_to": 5,
  "target_seq": 2,
  "event_count": 5,
  "leaf_data": "[\"BOX-MK\",2,\"2026-09-23T02:02:30.123456Z\",\"-4.125\"]",
  "target_leaf_hash": "fd39e4b65b44f6b44dd7d5fa1f5bacf7201db3eb19a637d7d62571ce41f33aaf",
  "root_hash": "9129c6bc27889ec02bec4ce2e390850dbcf21d2dc80b315538dbc3d0e6aa7c8a",
  "proof": [
    {"sibling_hash": "52b0998366034c294b23a9b92b03b57ae42a83ff949927d888de653b07178a25", "sibling_side": "right"},
    {"sibling_hash": "3f375bfaa74beb9431f4b8369abe3ed72b3e80800874b4254cd753b66acfdafd", "sibling_side": "left"},
    {"sibling_hash": "e58746fe5459714a8852f6e302a845c4252d40d3be8d17e8c715d98728be18da", "sibling_side": "right"}
  ]
}
```

### 验证示例（调用方重算根）

把上面响应保存为 `proof.json`，用以下脚本（仅标准库）重算叶哈希与根哈希并比对：

```python
import hashlib, json

resp = json.load(open("proof.json"))

# 1) 由 leaf_data 重算目标叶哈希：SHA-256(0x00 || UTF-8 字节)
leaf = hashlib.sha256(b"\x00" + resp["leaf_data"].encode("utf-8")).hexdigest()
assert leaf == resp["target_leaf_hash"], "叶哈希不匹配"

# 2) 沿证明路径逐层重算：兄弟在左则 H(0x01||兄弟||当前)，在右则 H(0x01||当前||兄弟)
cur = bytes.fromhex(resp["target_leaf_hash"])
for step in resp["proof"]:
    sib = bytes.fromhex(step["sibling_hash"])
    pair = (sib + cur) if step["sibling_side"] == "left" else (cur + sib)
    cur = hashlib.sha256(b"\x01" + pair).digest()
assert cur.hex() == resp["root_hash"], "根哈希不匹配"
print("验证通过：根哈希 =", resp["root_hash"])
```

```bash
curl -s 'http://localhost:8000/boxes/BOX-MK/merkle-proof?seq_from=0&seq_to=5&target_seq=2' > proof.json
python3 verify_proof.py
# 验证通过：根哈希 = 9129c6bc27889ec02bec4ce2e390850dbcf21d2dc80b315538dbc3d0e6aa7c8a
```

```bash
# 单事件 -> 证明路径为空，根哈希即叶哈希
curl 'http://localhost:8000/boxes/BOX-MK/merkle-proof?seq_from=2&seq_to=2&target_seq=2'

# 目标序号不在区间实际事件中（缺号 seq=3 / 超出区间 / 区间无事件）-> 404
curl -i 'http://localhost:8000/boxes/BOX-MK/merkle-proof?seq_from=0&seq_to=5&target_seq=3'

# 箱号不存在 -> 404
curl -i 'http://localhost:8000/boxes/NO-SUCH-BOX/merkle-proof?seq_from=0&seq_to=1&target_seq=0'

# 范围非法（seq_from > seq_to、负数、缺参数等）-> 422
curl -i 'http://localhost:8000/boxes/BOX-MK/merkle-proof?seq_from=5&seq_to=2&target_seq=3'
```

## 交接快照（handoffs）

`POST /handoffs` 在**同一事务**中按各箱**当前最大 seq** 读取状态，把每箱的序号、温度、
事件时间与结论**冻结**为一张交接快照；`GET /handoffs/{handoff_code}` 返回冻结明细与各类数量。
快照落库后即为只读，**后续乱序补报不会改写已建快照**。
服务经 `docker compose up -d --build` 启动后（见上文“一键启动”与“配置”），
以下示例均访问 `http://localhost:8000`。

请求体：

```json
{
  "handoff_code": "HO-20260924-01",
  "box_codes": ["BOX-001", "BOX-002", "BOX-003"],
  "temp_lower": -8.0,
  "temp_upper": 2.0
}
```

字段与校验（不满足返回 `422`）：

- `handoff_code`：交接编号，去空白后非空（≤64 字符），全局唯一，作为幂等键。
- `box_codes`：箱号列表，**至少 1 个**；箱号去空白后非空（≤64 字符）且**不可重复**
  （重复即 `422`，响应中指出重复项）。
- `temp_lower` / `temp_upper`：温度下/上限（℃），须为**有限数值**（`NaN`/`Infinity` 拒绝）
  且 `temp_lower <= temp_upper`。

冻结结论（按冻结时刻各箱最大 seq 的事件温度判定）：

- 箱号从未上报过 → `missing`（缺报），`seq` / `temperature` / `event_time` 为 `null`；
- 温度**严格**越出 `[temp_lower, temp_upper]` → `out_of_range`（越界）；
- 其余（含**恰等于上限或下限的边界值**）→ `normal`（正常）。

幂等与冲突：

- 交接编号相同且请求内容（箱号列表及顺序、温度上下限）一致 → 返回**原快照**
  （`200`），不重新读取事件；
- 交接编号相同但内容不同 → `409 Conflict`，响应体带已建快照与本次请求的内容，便于排查；
- 新建成功 → `201 Created`。

```bash
# 先上报两条事件：BOX-001 正常（-4.5℃），BOX-002 越界（5.0℃）；BOX-003 不上报（缺报）
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-001","seq":0,"temperature":-4.5,"event_time":"2026-09-24T08:00:00+08:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-002","seq":0,"temperature":5.0,"event_time":"2026-09-24T08:00:00+08:00"}'

# 创建交接快照（上下限 [-8, 2]）
curl -X POST http://localhost:8000/handoffs -H 'Content-Type: application/json' \
  -d '{"handoff_code":"HO-20260924-01","box_codes":["BOX-001","BOX-002","BOX-003"],
       "temp_lower":-8.0,"temp_upper":2.0}'
```

响应（`201`；BOX-001 正常、BOX-002 越界、BOX-003 缺报）：

```json
{
  "handoff_code": "HO-20260924-01",
  "temp_lower": -8.0,
  "temp_upper": 2.0,
  "box_count": 3,
  "normal_count": 1,
  "out_of_range_count": 1,
  "missing_count": 1,
  "created_at": "2026-09-24T00:12:34.567890",
  "items": [
    {"box_code": "BOX-001", "status": "normal", "seq": 0, "temperature": -4.5, "event_time": "2026-09-24T00:00:00"},
    {"box_code": "BOX-002", "status": "out_of_range", "seq": 0, "temperature": 5.0, "event_time": "2026-09-24T00:00:00"},
    {"box_code": "BOX-003", "status": "missing", "seq": null, "temperature": null, "event_time": null}
  ]
}
```

```bash
# 同编号 + 同内容重发 -> 200，返回原快照
curl -X POST http://localhost:8000/handoffs -H 'Content-Type: application/json' \
  -d '{"handoff_code":"HO-20260924-01","box_codes":["BOX-001","BOX-002","BOX-003"],
       "temp_lower":-8.0,"temp_upper":2.0}'

# 乱序补报更高序号也不会改写已建快照
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-001","seq":1,"temperature":20.0,"event_time":"2026-09-24T09:00:00+08:00"}'
curl http://localhost:8000/handoffs/HO-20260924-01   # 仍是冻结时的 -4.5℃ / normal

# 同编号但内容不同（改了上限）-> 409
curl -i -X POST http://localhost:8000/handoffs -H 'Content-Type: application/json' \
  -d '{"handoff_code":"HO-20260924-01","box_codes":["BOX-001","BOX-002","BOX-003"],
       "temp_lower":-8.0,"temp_upper":3.0}'

# 查询不存在的交接编号 -> 404
curl -i http://localhost:8000/handoffs/NO-SUCH-HANDOFF

# 箱号重复 / 下限大于上限 -> 422
curl -i -X POST http://localhost:8000/handoffs -H 'Content-Type: application/json' \
  -d '{"handoff_code":"HO-X","box_codes":["BOX-001","BOX-001"],"temp_lower":-8,"temp_upper":2}'
curl -i -X POST http://localhost:8000/handoffs -H 'Content-Type: application/json' \
  -d '{"handoff_code":"HO-Y","box_codes":["BOX-001"],"temp_lower":2,"temp_upper":-8}'
```

## 交接箱单对账（reconcile）

`GET /handoffs/{from_code}/reconcile/{to_code}`（默认访问端口 **8000**）对两份
**已冻结**的交接快照做只读对账：只比较两份快照的冻结明细，**不重新查询箱子当前事件**，
因此结果只取决于两张快照本身，后续补报事件后重查结果不变。
来源或目标快照任一不存在返回 `404`。

返回内容：

- **新增箱** `added_boxes`：只在目标快照中的箱号，**按目标快照顺序**列出，
  附目标快照中的冻结结论与序号；
- **移出箱** `removed_boxes`：只在来源快照中的箱号，**按来源快照顺序**列出，
  附来源快照中的冻结结论与序号；
- **共同箱** `common_boxes`：两份快照都有的箱号，**按来源快照顺序**列出，
  逐项给出原/目标结论（`from_status` / `to_status`）与原/目标序号
  （`from_seq` / `to_seq`），`status_changed` 表示结论是否变化；
- **最少移动箱数** `min_moves`：把共同箱按来源顺序逐箱映射到它在目标快照中的
  位置（0 起），得到一个位置序列，求其**最长递增子序列**（LIS）长度
  `lis_length`，则 `min_moves = common_count - lis_length`。
  新增、移出箱**不计**移动；两份箱单相同（共同箱相对顺序一致）时 `min_moves` 为 0。

返回字段：

| 字段                    | 说明                                                       |
| ----------------------- | ---------------------------------------------------------- |
| `from_code` / `to_code` | 来源 / 目标交接编号                                        |
| `from_box_count` / `to_box_count` | 来源 / 目标快照箱数                            |
| `added_count` / `removed_count` / `common_count` | 新增 / 移出 / 共同箱数        |
| `status_changed_count`  | 共同箱中冻结结论变化的箱数                                 |
| `lis_length`            | 共同箱来源顺序映射到目标位置后的最长递增子序列长度         |
| `min_moves`             | 最少需移动箱数（`common_count - lis_length`）              |
| `added_boxes[]`         | 新增箱（目标快照顺序）：`box_code` / `to_status` / `to_seq` |
| `removed_boxes[]`       | 移出箱（来源快照顺序）：`box_code` / `from_status` / `from_seq` |
| `common_boxes[]`        | 共同箱（来源快照顺序）：`box_code`、原/目标结论与序号、`status_changed` |

```bash
# 接上文示例：HO-20260924-01 已冻结 [BOX-001 正常, BOX-002 越界, BOX-003 缺报]，
# 且 BOX-001 之后补报了 seq=1（20.0℃）。再补两条事件并建第二份快照：
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-003","seq":0,"temperature":-3.0,"event_time":"2026-09-25T08:00:00+08:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-004","seq":0,"temperature":-5.0,"event_time":"2026-09-25T08:00:00+08:00"}'

# 第二份快照：箱单不含 BOX-002，新增 BOX-004，顺序也不同
curl -X POST http://localhost:8000/handoffs -H 'Content-Type: application/json' \
  -d '{"handoff_code":"HO-20260925-01","box_codes":["BOX-003","BOX-001","BOX-004"],
       "temp_lower":-8.0,"temp_upper":2.0}'

# 对账：从 HO-20260924-01 到 HO-20260925-01
curl http://localhost:8000/handoffs/HO-20260924-01/reconcile/HO-20260925-01
```

响应（BOX-004 新增、BOX-002 移出；共同箱 BOX-001 由正常变越界、BOX-003 由缺报变正常；
共同箱来源顺序 [BOX-001, BOX-003] 映射到目标位置 [1, 0]，LIS 长度 1，最少移动 1 箱）：

```json
{
  "from_code": "HO-20260924-01",
  "to_code": "HO-20260925-01",
  "from_box_count": 3,
  "to_box_count": 3,
  "added_count": 1,
  "removed_count": 1,
  "common_count": 2,
  "status_changed_count": 2,
  "lis_length": 1,
  "min_moves": 1,
  "added_boxes": [
    {"box_code": "BOX-004", "to_status": "normal", "to_seq": 0}
  ],
  "removed_boxes": [
    {"box_code": "BOX-002", "from_status": "out_of_range", "from_seq": 0}
  ],
  "common_boxes": [
    {"box_code": "BOX-001", "from_status": "normal", "to_status": "out_of_range",
     "from_seq": 0, "to_seq": 1, "status_changed": true},
    {"box_code": "BOX-003", "from_status": "missing", "to_status": "normal",
     "from_seq": null, "to_seq": 0, "status_changed": true}
  ]
}
```

```bash
# 自己对自己对账（箱单完全相同）-> min_moves = 0
curl http://localhost:8000/handoffs/HO-20260924-01/reconcile/HO-20260924-01

# 任一快照不存在 -> 404
curl -i http://localhost:8000/handoffs/HO-20260924-01/reconcile/NO-SUCH-HANDOFF
curl -i http://localhost:8000/handoffs/NO-SUCH-HANDOFF/reconcile/HO-20260924-01
```

## 温度漂移校准（calibrations / calibration-trajectory）

周转箱温度传感器存在随序号缓慢漂移的系统误差，可用若干**校准锚点**描述
“在某序号处应给原温度加多少偏移”。服务提供两类接口（默认访问端口 **8000**）：

- `POST /calibrations`：按**箱号 + 版本号**保存一份**不可变**的校准锚点列表；
- `GET /boxes/{box_code}/calibration-trajectory?version=&seq_from=&seq_to=`：
  用指定版本的锚点计算序号区间内事件的校准轨迹。

**保存规则（POST /calibrations）**

- 请求体包含 `box_code`、`version`（非负整数，建议从 1 开始；同箱不同版本并存、
  互不影响；取值范围 0 ～ `9223372036854775807`，越界 `422`）与 `anchors` 锚点列表。
- 锚点**至少 2 个**；每个锚点为 `{ "seq": 非负整数, "temp_offset": 有限数值 }`，
  锚点 `seq` 同样不得超过 `9223372036854775807`（越界 `422`）。
- 锚点序号必须**严格递增**（相等或乱序返回 `422`）；偏移拒绝 `NaN`/`Infinity`（`422`）。
- **不可变 + 幂等**：同一 `(box_code, version)`
  - 首次提交 → `201 Created`；
  - **原样重交**（锚点逐点的序号与偏移完全一致）→ `200 OK`，返回原列表，
    `created_at` 不变，不写任何新行；
  - **内容不同**（锚点数、任一点序号或偏移不同）→ `409 Conflict`，
    响应体同时带已存锚点与本次提交内容，便于排查。服务不提供更新/删除入口。

```bash
# 启动（保持 Docker Compose 一键启动）
docker compose up -d --build

# 保存 v1 锚点：seq=0 偏移 0℃，seq=10 偏移 1℃
curl -X POST http://localhost:8000/calibrations \
  -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-CAL","version":1,"anchors":[
        {"seq":0,"temp_offset":0.0},
        {"seq":10,"temp_offset":1.0}]}'

# 原样重交 -> 200，返回原列表（幂等）
curl -X POST http://localhost:8000/calibrations \
  -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-CAL","version":1,"anchors":[
        {"seq":0,"temp_offset":0.0},
        {"seq":10,"temp_offset":1.0}]}'

# 改任一偏移 -> 409（锚点不可变）
curl -i -X POST http://localhost:8000/calibrations \
  -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-CAL","version":1,"anchors":[
        {"seq":0,"temp_offset":0.0},
        {"seq":10,"temp_offset":2.0}]}'

# 只有一个锚点 / 序号未严格递增 / NaN 偏移 -> 422
curl -i -X POST http://localhost:8000/calibrations \
  -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-CAL","version":3,"anchors":[{"seq":0,"temp_offset":0.0}]}'

# 查询某箱某版本的锚点；不存在 -> 404
curl http://localhost:8000/boxes/BOX-CAL/calibrations/1
curl -i http://localhost:8000/boxes/BOX-CAL/calibrations/99
```

保存成功响应（`201`；原样重交为 `200`，`created_at` 与下方一致）：

```json
{
  "box_code": "BOX-CAL",
  "version": 1,
  "anchor_count": 2,
  "created_at": "2026-09-25T01:23:45.678901",
  "anchors": [
    {"seq": 0, "temp_offset": 0.0},
    {"seq": 10, "temp_offset": 1.0}
  ]
}
```

**校准轨迹（GET /boxes/{box_code}/calibration-trajectory）**

查询参数：

- `version`：使用的校准版本号（非负整数，取值 0 ～ `9223372036854775807`，越界
  `422`）；该箱该版本不存在 → `404`。
- `seq_from` / `seq_to`：序号区间，**均为整数且含端点**，必须 `>= 0`、
  `seq_from <= seq_to` 且不超过 `9223372036854775807`（`2^63 - 1`），否则 `422`。

计算规则（均为**查询时即时计算，不写库**）：

- 只取区间内**实际存在**的事件（按 `seq` 升序），**缺报序号不补事件**
  （区间无事件返回 `event_count=0`、`points=[]`）；
- 事件序号**恰好落在锚点上** → 该锚点偏移；
- 位于**两个相邻锚点之间** → 按序号**线性插值**偏移（与事件时间无关）；
- **早于首个锚点 / 晚于末个锚点** → **两端沿用最近锚点偏移**（钳平，不外推）；
- 逐条返回原温度 `temperature`、温度偏移 `temp_offset` 与
  **校准温度** `calibrated_temperature = 原温度 + 偏移`。

校准结果**只用于本接口**：`/events`、`/states`、交接快照以及 clock-audit /
heat-exposure / temperature-median / trajectory-compression / merkle-proof 等
现有状态与分析结果一律仍基于**原始温度**，不受任何影响。

```bash
# 上报 4 条原始事件（原始温度都是 -5.0℃；seq 15 晚于末锚点 10）
for s in 0 5 10 15; do
  curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
    -d "{\"box_code\":\"BOX-CAL\",\"seq\":$s,\"temperature\":-5.0,
         \"event_time\":\"2026-09-25T02:00:$(printf %02d $s)+00:00\"}"
done

# 用 v1 锚点计算 [0,20] 的校准轨迹
curl 'http://localhost:8000/boxes/BOX-CAL/calibration-trajectory?version=1&seq_from=0&seq_to=20'
```

响应（seq=5 在锚点 0→10 之间插值为 0.5；seq=15 晚于末锚点，沿用 1.0；
缺报的 1..4、6..9、11..14 不产生事件）：

```json
{
  "box_code": "BOX-CAL",
  "version": 1,
  "seq_from": 0,
  "seq_to": 20,
  "event_count": 4,
  "points": [
    {"seq": 0,  "event_time": "2026-09-25T02:00:00", "temperature": -5.0,
     "temp_offset": 0.0, "calibrated_temperature": -5.0},
    {"seq": 5,  "event_time": "2026-09-25T02:00:05", "temperature": -5.0,
     "temp_offset": 0.5, "calibrated_temperature": -4.5},
    {"seq": 10, "event_time": "2026-09-25T02:00:10", "temperature": -5.0,
     "temp_offset": 1.0, "calibrated_temperature": -4.0},
    {"seq": 15, "event_time": "2026-09-25T02:00:15", "temperature": -5.0,
     "temp_offset": 1.0, "calibrated_temperature": -4.0}
  ]
}
```

```bash
# 区间内无事件（缺报不补点）-> 200，event_count=0、points=[]
curl 'http://localhost:8000/boxes/BOX-CAL/calibration-trajectory?version=1&seq_from=100&seq_to=200'

# 指定版本不存在 -> 404
curl -i 'http://localhost:8000/boxes/BOX-CAL/calibration-trajectory?version=99&seq_from=0&seq_to=1'

# seq_from > seq_to -> 422
curl -i 'http://localhost:8000/boxes/BOX-CAL/calibration-trajectory?version=1&seq_from=9&seq_to=1'
```

## 校准版本切换影响审计（calibration-impact）

`GET /boxes/{box_code}/calibration-impact`（默认访问端口 **8000**，
如 `http://localhost:8000/boxes/BOX-IMP/calibration-impact`）审计**从一个校准版本
切换到另一个版本**后，区间内各事件的温度结论是否改变。全程**只读**：不写库、
不改锚点、不改写原始事件，也不影响最新状态、交接快照与其他既有分析结果；
补报事件或新增校准版本后重查即基于最新数据重算。

计算规则（与 `calibration-trajectory` 复用同一套已存锚点插值逻辑）：

- 同时加载该箱的 **`from_version`（原版本）与 `to_version`（新版本）**两份锚点；
  任一版本不存在返回 `404`（`detail.missing_versions` 列出全部缺失版本）。
- 只取闭区间 `[seq_from, seq_to]` 内**实际存在**的事件（按 `seq` 升序），
  **缺报序号不补事件**（空区间返回空段）。
- 每条事件分别用两版锚点计算校准温度：序号落在锚点上取该点偏移，相邻锚点之间按
  **序号**线性插值，早于首锚点 / 晚于末锚点时两端沿用最近锚点偏移（不外推）。
- 按给定阈值把校准温度判为三档结论：**低于下限** `below`、**区间内（含边界）**
  `normal`、**高于上限** `above`。插值与阈值比较全程使用精确有理数
  （`Fraction`），校准温度**恰等于**下/上限时稳定判为 `normal`，不会因浮点
  误差在边界上误判。
- **只保留两版结论不同的事件**；把其中**序号连续**（后一条 `seq` 恰为前一条 `seq+1`）
  且**变化方向相同**（`from_status -> to_status` 一致）的事件**合成一个影响段**；
  一旦遇到**缺号**或结论未变的事件即**断段，不为缺号造点**，变化方向不同也分开成段。
- 每段给出起止序号、段内事件数、原结论 `from_status`、新结论 `to_status` 与
  **最大校准温差** `max_calibration_delta = max(|新版本校准温度 − 原版本校准温度|)`。

查询参数：

| 参数            | 说明                                                                |
| --------------- | ------------------------------------------------------------------- |
| `from_version`  | 原校准版本号（非负整数，不超过 `9223372036854775807`，越界 `422`），该箱该版本须已存在，否则 `404`            |
| `to_version`    | 新校准版本号（取值范围同上，越界 `422`），同上；允许与原版本相同（此时无结论改变）  |
| `seq_from` / `seq_to` | 序号区间，**均为整数且含端点**，必须 `>= 0`、`seq_from <= seq_to` 且不超过 `9223372036854775807`（`2^63 - 1`） |
| `temp_lower`    | 温度下限（℃），**有限数值**，恰等于算区间内                         |
| `temp_upper`    | 温度上限（℃），**有限数值**，须 `>= temp_lower`，恰等于算区间内     |

返回字段：

| 字段                          | 说明                                                          |
| ----------------------------- | ------------------------------------------------------------- |
| `event_count`                 | 区间内实际事件总数（缺报不补）                                |
| `changed_event_count`         | 两版结论不同的事件数（各段 `event_count` 之和）               |
| `segment_count`               | 影响段数（即 `segments` 长度；无改变时为 0）                  |
| `segments[].start_seq`/`end_seq` | 影响段起止事件序号（含端点；缺号断段，不补点）             |
| `segments[].event_count`      | 段内序号连续且变化方向相同的事件数                            |
| `segments[].from_status`/`to_status` | 原/新结论：`below`/`normal`/`above`，段内各事件方向一致 |
| `segments[].max_calibration_delta` | 段内两版校准温差的最大绝对值（℃）                         |

启动与端口：先按上文方式一键启动 `docker compose up -d --build`
（本地不用 Docker 时为 `uvicorn app.main:app --host 0.0.0.0 --port 8000`），
服务监听 **8000** 端口，以下示例访问 `http://localhost:8000`。

```bash
# 保存两版校准（BOX-IMP）：
# v1 锚点 seq=0/8 偏移均为 0（校准温度=原温度）
# v2 锚点 seq=0 偏移 0、seq=8 偏移 1（每号 +0.125 线性漂移，8 之后沿用 1.0）
curl -X POST http://localhost:8000/calibrations -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-IMP","version":1,"anchors":[
        {"seq":0,"temp_offset":0.0},
        {"seq":8,"temp_offset":0.0}]}'
curl -X POST http://localhost:8000/calibrations -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-IMP","version":2,"anchors":[
        {"seq":0,"temp_offset":0.0},
        {"seq":8,"temp_offset":1.0}]}'

# 上报事件（原始温度；故意缺 seq=8）：
# seq=0 温度 4.0（恰上限，两版都 normal）；seq=1..4 温度 3.5（v2 仍在区间）；
# seq=5/6/7 温度 3.5（v2 = 4.125/4.25/4.375，全部 normal->above）；
# seq=9 温度 -0.5（v1 below；v2 晚于末锚点沿用偏移 1.0 -> 0.5，below->normal）
for s in 0 1 2 3 4 5 6 7 9; do
  t=$([ "$s" = "9" ] && echo '-0.5' || echo '3.5'); [ "$s" = "0" ] && t=4.0
  curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
    -d "{\"box_code\":\"BOX-IMP\",\"seq\":$s,\"temperature\":$t,
         \"event_time\":\"2026-09-25T03:00:$(printf %02d $s)+00:00\"}"
done

# 审计 v1 -> v2，阈值 [0, 4]，序号区间 [0,10]
curl 'http://localhost:8000/boxes/BOX-IMP/calibration-impact?from_version=1&to_version=2&seq_from=0&seq_to=10&temp_lower=0&temp_upper=4'
```

响应（seq=5/6/7 序号连续、方向相同，合成一段，最大温差 0.875；
seq=9 因缺 seq=8 断段且方向不同，单独成段，温差恰为 1.0）：

```json
{
  "box_code": "BOX-IMP",
  "from_version": 1,
  "to_version": 2,
  "seq_from": 0,
  "seq_to": 10,
  "temp_lower": 0.0,
  "temp_upper": 4.0,
  "event_count": 9,
  "changed_event_count": 4,
  "segment_count": 2,
  "segments": [
    {
      "start_seq": 5,
      "end_seq": 7,
      "event_count": 3,
      "from_status": "normal",
      "to_status": "above",
      "max_calibration_delta": 0.875
    },
    {
      "start_seq": 9,
      "end_seq": 9,
      "event_count": 1,
      "from_status": "below",
      "to_status": "normal",
      "max_calibration_delta": 1.0
    }
  ]
}
```

```bash
# 区间内无事件（缺报不补点）-> 200，event_count=0、changed_event_count=0、segments=[]
curl 'http://localhost:8000/boxes/BOX-IMP/calibration-impact?from_version=1&to_version=2&seq_from=100&seq_to=200&temp_lower=0&temp_upper=4'

# 任一校准版本不存在 -> 404，detail.missing_versions 列出全部缺失版本
curl -i 'http://localhost:8000/boxes/BOX-IMP/calibration-impact?from_version=1&to_version=99&seq_from=0&seq_to=1&temp_lower=0&temp_upper=4'

# 箱号从未上报过事件 -> 404（box_not_found）
curl -i 'http://localhost:8000/boxes/NO-SUCH-BOX/calibration-impact?from_version=1&to_version=2&seq_from=0&seq_to=1&temp_lower=0&temp_upper=4'

# 参数非法（seq_from > seq_to、下限大于上限、NaN/Infinity、缺参数等）-> 422
curl -i 'http://localhost:8000/boxes/BOX-IMP/calibration-impact?from_version=1&to_version=2&seq_from=9&seq_to=1&temp_lower=0&temp_upper=4'
curl -i 'http://localhost:8000/boxes/BOX-IMP/calibration-impact?from_version=1&to_version=2&seq_from=0&seq_to=1&temp_lower=4&temp_upper=0'
```

## 群体遥测静默检测（telemetry-outages）

`GET /telemetry-outages`（默认访问端口 **8000**，如
`http://localhost:8000/telemetry-outages`）检测**多台周转箱在同一时间段同时丢失遥测**
的群体性中断，全程**只读**：不写库、不改写原始事件与任何既有结果，补报后重查即重算。

计算规则：

- `box_codes` 列表**自动按请求顺序去重**；每箱只取共同序号闭区间
  `[seq_from, seq_to]` 内**实际存在**的事件（按 `seq` 升序），**缺号不补点**。
- 每箱事件时间先做与 clock-audit 相同的**等权保序回归校正**（整数微秒 + PAVA）。
- 相邻两条事件的**校正时间间隔严格大于** `max_silence_seconds` 时，以这两个校正时间
  作为该箱的**半开静默区间** `[start_time, end_time)`；间隔**恰好等于阈值不算静默**，
  校正后间隔为零（同一合并块）也不算。**不足两条事件的箱不产生任何区间**。
- **阈值边界规则（精确判定）**：`max_silence_seconds` 按请求原值（十进制文本）
  **精确解析**，不经过二进制浮点，再换算为整数微秒与保序校正后的整数微秒间隔
  比较。因此像 `0.3` 这样无法被 double 精确表示的阈值也不会误判：校正后恰好
  相隔 0.3 秒（300000 微秒）且阈值为 `0.3` 时**不**生成静默区间；只有间隔
  **严格超过**阈值（如 0.300001 秒）才生成半开静默区间。
- 对各箱静默区间做**端点扫描**（半开语义：同一时刻先处理区间结束、再处理区间开始，
  同箱的相邻静默区间不会重复计数），输出同时静默箱数 **`>= min_simultaneous_boxes`**
  的**极大时间段**，并给出段内**峰值同时静默箱数**；
  **端点相接的合格段自动合并**为一段（中间被非合格时间隔开的仍分开输出）。
- `min_simultaneous_boxes` 大于箱数、或没有任何同时静默时间段时，`outages` 返回空数组。

查询参数（全部通过 query string 给出，箱号用重复参数）：

| 参数                    | 说明                                                                   |
| ----------------------- | ---------------------------------------------------------------------- |
| `box_codes`             | 参与检测的箱号，**重复参数逐个给出**（如 `box_codes=A&box_codes=B`）；至少 1 个，自动去重（保序）；去空白后为空或超过 64 字符返回 `422` |
| `seq_from` / `seq_to`   | 共同序号区间，**均为整数且含端点**，必须 `>= 0`、`seq_from <= seq_to` 且不超过 `9223372036854775807`（`2^63 - 1`） |
| `max_silence_seconds`   | 最大允许静默秒数，**必须为正的有限数值**（负数、0、`NaN`/`Infinity`、非数值、超出 double 有限表示范围的值均 `422`）；**按请求原值精确解析为十进制数**（不经过二进制浮点），间隔**严格大于**它才算静默，恰好等于不算 |
| `min_simultaneous_boxes`| 最少同时静默箱数，**正整数**（`>= 1`）；达到该数量的时间段才输出        |

返回字段：

| 字段                           | 说明                                                       |
| ------------------------------ | ---------------------------------------------------------- |
| `box_codes` / `box_count`      | 去重后的箱号列表（保序）及数量                             |
| `seq_from` / `seq_to`          | 本次查询的共同序号区间                                     |
| `max_silence_seconds`          | 本次使用的最大允许静默秒数                                 |
| `min_simultaneous_boxes`       | 本次使用的最少同时静默箱数                                 |
| `outage_count`                 | 群体静默时间段数（即 `outages` 长度）                      |
| `outages[].start_time`         | 段起点 UTC 时间（**含**）：同时静默数首次达标的时刻        |
| `outages[].end_time`           | 段终点 UTC 时间（**不含**）：同时静默数跌破阈值的时刻      |
| `outages[].duration_seconds`   | 段时长（秒）                                               |
| `outages[].peak_simultaneous_boxes` | 段内峰值同时静默箱数（不小于 `min_simultaneous_boxes`） |

错误情况：**任一**指定箱号不存在（从未上报过事件）返回 `404`，`detail.missing_box_codes`
列出全部缺失箱号（按请求顺序）；缺少参数、参数非法（含 `seq_from > seq_to`、
阈值非正数 / `NaN` / `Infinity` / 非数值 / 超出 double 有限表示范围、箱号为空等）返回 `422`。

```bash
# 三台箱（UTC；秒）：
# BOX-O1 0/300/600     -> 静默 [0,300)、[300,600)（端点相接）
# BOX-O2 100/500       -> 静默 [100,500)
# BOX-O3 0/50/60/700   -> 只有 [60,700)（50 秒、10 秒的间隔不超阈值）
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-O1","seq":0,"temperature":-5.0,"event_time":"2026-09-25T00:00:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-O1","seq":1,"temperature":-5.0,"event_time":"2026-09-25T00:05:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-O1","seq":2,"temperature":-5.0,"event_time":"2026-09-25T00:10:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-O2","seq":0,"temperature":-5.0,"event_time":"2026-09-25T00:01:40+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-O2","seq":1,"temperature":-5.0,"event_time":"2026-09-25T00:08:20+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-O3","seq":0,"temperature":-5.0,"event_time":"2026-09-25T00:00:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-O3","seq":1,"temperature":-5.0,"event_time":"2026-09-25T00:00:50+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-O3","seq":2,"temperature":-5.0,"event_time":"2026-09-25T00:01:00+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-O3","seq":3,"temperature":-5.0,"event_time":"2026-09-25T00:11:40+00:00"}'
```

```bash
# 查询：阈值 100 秒、至少 2 箱同时静默
curl -G 'http://localhost:8000/telemetry-outages' \
  --data-urlencode 'box_codes=BOX-O1' \
  --data-urlencode 'box_codes=BOX-O2' \
  --data-urlencode 'box_codes=BOX-O3' \
  --data-urlencode 'seq_from=0' --data-urlencode 'seq_to=10' \
  --data-urlencode 'max_silence_seconds=100' \
  --data-urlencode 'min_simultaneous_boxes=2'
```

响应（扫描结果：`[0,60)` 仅 BOX-O1 静默；`[60,100)` 有 BOX-O1+BOX-O3；
`[100,500)` 三箱齐静默；`[500,600)` 剩 BOX-O1+BOX-O3；`[600,700)` 仅 BOX-O3。
三个合格基本段端点相接，合并为极大段 `[00:01:00, 00:10:00)`，时长 540 秒，峰值 3）：

```json
{
  "box_codes": ["BOX-O1", "BOX-O2", "BOX-O3"],
  "box_count": 3,
  "seq_from": 0,
  "seq_to": 10,
  "max_silence_seconds": 100.0,
  "min_simultaneous_boxes": 2,
  "outage_count": 1,
  "outages": [
    {
      "start_time": "2026-09-25T00:01:00",
      "end_time": "2026-09-25T00:10:00",
      "duration_seconds": 540.0,
      "peak_simultaneous_boxes": 3
    }
  ]
}
```

```bash
# 箱号重复给出 -> 自动去重（保序），box_count 以去重后为准
curl -G 'http://localhost:8000/telemetry-outages' \
  --data-urlencode 'box_codes=BOX-O1' --data-urlencode 'box_codes=BOX-O1' \
  --data-urlencode 'seq_from=0' --data-urlencode 'seq_to=10' \
  --data-urlencode 'max_silence_seconds=100' --data-urlencode 'min_simultaneous_boxes=1'

# 间隔恰好等于阈值不算静默（阈值改成 300 后，BOX-O1 的 300 秒间隔被排除）
curl -G 'http://localhost:8000/telemetry-outages' \
  --data-urlencode 'box_codes=BOX-O1' --data-urlencode 'box_codes=BOX-O2' \
  --data-urlencode 'box_codes=BOX-O3' \
  --data-urlencode 'seq_from=0' --data-urlencode 'seq_to=10' \
  --data-urlencode 'max_silence_seconds=300' --data-urlencode 'min_simultaneous_boxes=2'

# 无任何同时静默时间段（阈值箱数高于实际值）-> 200，outages=[]
curl -G 'http://localhost:8000/telemetry-outages' \
  --data-urlencode 'box_codes=BOX-O1' --data-urlencode 'box_codes=BOX-O2' \
  --data-urlencode 'seq_from=0' --data-urlencode 'seq_to=10' \
  --data-urlencode 'max_silence_seconds=100' --data-urlencode 'min_simultaneous_boxes=3'

# 指定箱不存在 -> 404，detail.missing_box_codes 列出全部缺失箱号
curl -i -G 'http://localhost:8000/telemetry-outages' \
  --data-urlencode 'box_codes=BOX-O1' --data-urlencode 'box_codes=NO-SUCH-BOX' \
  --data-urlencode 'seq_from=0' --data-urlencode 'seq_to=10' \
  --data-urlencode 'max_silence_seconds=100' --data-urlencode 'min_simultaneous_boxes=1'

# 参数非法（seq_from > seq_to、阈值非正数、NaN/Infinity、空箱号、缺参数等）-> 422
curl -i -G 'http://localhost:8000/telemetry-outages' \
  --data-urlencode 'box_codes=BOX-O1' \
  --data-urlencode 'seq_from=5' --data-urlencode 'seq_to=2' \
  --data-urlencode 'max_silence_seconds=100' --data-urlencode 'min_simultaneous_boxes=1'
curl -i -G 'http://localhost:8000/telemetry-outages' \
  --data-urlencode 'box_codes=BOX-O1' \
  --data-urlencode 'seq_from=0' --data-urlencode 'seq_to=10' \
  --data-urlencode 'max_silence_seconds=0' --data-urlencode 'min_simultaneous_boxes=1'
```

阈值边界示例（精确判定规则）：下面两箱的原始事件时间分别为
`00:00:00.000 / 00:00:00.400 / 00:00:00.200`（seq=2 发生时钟回拨），保序校正后
为 `0 / 0.3s / 0.3s`，即相邻校正间隔**恰好 0.3 秒**。阈值按请求原值精确解析，
`0.3` 不会被二进制浮点误差缩小，因此恰隔 0.3 秒**不**算静默：

```bash
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-P1","seq":0,"temperature":-5.0,"event_time":"2026-09-25T00:00:00.000000+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-P1","seq":1,"temperature":-5.0,"event_time":"2026-09-25T00:00:00.400000+00:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-P1","seq":2,"temperature":-5.0,"event_time":"2026-09-25T00:00:00.200000+00:00"}'
# BOX-P2 同样上报上述三条事件……

# 阈值 0.3、校正后恰隔 0.3 秒 -> 不产生静默区间（outages=[]），不会误报多箱同时静默
curl -G 'http://localhost:8000/telemetry-outages' \
  --data-urlencode 'box_codes=BOX-P1' --data-urlencode 'box_codes=BOX-P2' \
  --data-urlencode 'seq_from=0' --data-urlencode 'seq_to=10' \
  --data-urlencode 'max_silence_seconds=0.3' --data-urlencode 'min_simultaneous_boxes=2'
# {"box_codes":["BOX-P1","BOX-P2"],"box_count":2,...,"outage_count":0,"outages":[]}

# 阈值 0.2（严格小于 0.3 秒间隔）-> 两箱在 [00:00:00, 00:00:00.300000) 同时静默，峰值 2
curl -G 'http://localhost:8000/telemetry-outages' \
  --data-urlencode 'box_codes=BOX-P1' --data-urlencode 'box_codes=BOX-P2' \
  --data-urlencode 'seq_from=0' --data-urlencode 'seq_to=10' \
  --data-urlencode 'max_silence_seconds=0.2' --data-urlencode 'min_simultaneous_boxes=2'
```

## 多箱设备补报调度预览（retransmission-preview）

`POST /retransmission-preview`（默认访问端口 **8000**，如
`http://localhost:8000/retransmission-preview`）为**多台周转箱的缺号补报**
制定一份传输调度**预览**：在总传输预算内决定向哪些箱、请求哪些连续序号段，
使“补报序号数 × 该箱权重”之和最大。全程**只读**：不写库、不改写原始事件与
任何既有结果；乱序补报后重查即基于最新事件表重算。

计算规则：

- 请求给出 **1～4 个互异箱号**（各带 **1～10 的整数权重**）、**非负共同序号
  闭区间** `[seq_from, seq_to]`（每箱最多 **60** 个序号）、**单指令最大连续
  长度** `max_command_length`（1～20）、**固定握手开销** `handshake_cost`
  （0～20）与**总传输预算** `budget`（0～200）。
- 依据事件表找出各箱在共同区间内的**缺号**并合并为极大连续缺号段；
  **从未上报过的箱按整个指定区间缺报处理**（不返回 404）。
- 一条指令只能请求**同箱同段内的连续缺号**（**不跨已收到的序号**），长度
  1～`max_command_length`；**指令消耗 = 握手开销 + 请求长度**，
  **指令收益 = 请求长度 × 该箱权重**；各指令消耗之和不得超过预算。
- **目标**：预算内最大化 **Σ(补报序号数 × 该箱权重)**；同分（收益相同）选
  **指令总数更少**的方案；再相同则按**箱号、起点、终点字典序**取唯一方案
  （指令列表按该键排序后逐条比较，覆盖块取段内最早起点、同块切分取字典序
  最小——靠前的指令尽量短）。
- 求解方式：同一段内覆盖 k 个序号的最省方式是从段起点连续覆盖并用
  ⌈k/单指令最大长度⌉ 条指令（更多指令只增加握手开销），因此每段归约为
  “选 k = 0..段长”的**多选背包**，按预算做 DP 精确求解（预算 ≤ 200，
  规模很小），字典序 tie-break 在 DP 中逐状态保留最小指令列表实现。

请求体：

```json
{
  "boxes": [
    {"box_code": "BOX-R1", "weight": 5},
    {"box_code": "BOX-R2", "weight": 3}
  ],
  "seq_from": 0,
  "seq_to": 9,
  "max_command_length": 4,
  "handshake_cost": 1,
  "budget": 12
}
```

字段与校验（不满足返回 `422`）：

- `boxes`：1～4 项；`box_code` 去空白后非空（≤64 字符）且**互异**（重复即 422）；
  `weight` 为 1～10 的整数。
- `seq_from` / `seq_to`：非负整数（不超过 `9223372036854775807`，越界 `422`）且
  `seq_from <= seq_to`，区间序号数（`seq_to - seq_from + 1`）不超过 60。
- `max_command_length`：1～20 的整数；`handshake_cost`：0～20 的整数；
  `budget`：0～200 的整数。

返回字段：

| 字段                          | 说明                                                       |
| ----------------------------- | ---------------------------------------------------------- |
| `budget_used` / `budget_remaining` | 本方案实际消耗 / 剩余预算（消耗 = Σ(握手开销 + 指令长度)，不超过预算） |
| `total_value`                 | 总收益：Σ(补报序号数 × 该箱权重)                           |
| `covered_count`               | 计划补报的序号总数（各指令长度之和）                       |
| `command_count`               | 指令条数（同收益时取最少）                                 |
| `commands[]`                  | 补报指令（按箱号、起点、终点字典序）：`box_code` / `start_seq` / `end_seq` / `length` / `cost` / `value` |
| `boxes[]`                     | 各箱结果（**按请求顺序**）：`missing_count`（调度前缺报数）、`covered_count` / `covered_value`、`remaining_missing_count` 与 `remaining_missing_ranges[]`（执行本方案后仍缺报的连续区间） |

```bash
# BOX-R1 已收到 seq=2、5（缺 0-1、3-4、6-9 三段）；BOX-R2 从未上报（整段缺报）
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-R1","seq":2,"temperature":-4.5,"event_time":"2026-09-25T08:00:00+08:00"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-R1","seq":5,"temperature":-4.5,"event_time":"2026-09-25T08:05:00+08:00"}'

# 调度预览：区间 [0,9]，单指令最长 4，握手开销 1，预算 12
curl -X POST http://localhost:8000/retransmission-preview \
  -H 'Content-Type: application/json' \
  -d '{"boxes":[{"box_code":"BOX-R1","weight":5},{"box_code":"BOX-R2","weight":3}],
       "seq_from":0,"seq_to":9,"max_command_length":4,"handshake_cost":1,"budget":12}'
```

响应（BOX-R1 权重高，预算优先补满它的 8 个缺号：3 条指令共耗 11，收益 40；
剩余 1 预算不足以发任何指令，BOX-R2 整段仍缺报）：

```json
{
  "seq_from": 0,
  "seq_to": 9,
  "max_command_length": 4,
  "handshake_cost": 1,
  "budget": 12,
  "budget_used": 11,
  "budget_remaining": 1,
  "total_value": 40,
  "covered_count": 8,
  "command_count": 3,
  "commands": [
    {"box_code": "BOX-R1", "start_seq": 0, "end_seq": 1, "length": 2, "cost": 3, "value": 10},
    {"box_code": "BOX-R1", "start_seq": 3, "end_seq": 4, "length": 2, "cost": 3, "value": 10},
    {"box_code": "BOX-R1", "start_seq": 6, "end_seq": 9, "length": 4, "cost": 5, "value": 20}
  ],
  "boxes": [
    {"box_code": "BOX-R1", "weight": 5, "missing_count": 8, "covered_count": 8,
     "covered_value": 40, "remaining_missing_count": 0, "remaining_missing_ranges": []},
    {"box_code": "BOX-R2", "weight": 3, "missing_count": 10, "covered_count": 0,
     "covered_value": 0, "remaining_missing_count": 10,
     "remaining_missing_ranges": [{"start_seq": 0, "end_seq": 9, "length": 10}]}
  ]
}
```

```bash
# 预览不写库：上面查询后 BOX-R1 仍只有 2 条事件
curl http://localhost:8000/boxes/BOX-R1/events

# 乱序补报 BOX-R1 的缺号后重查 -> 自动重算（预算转而投给 BOX-R2）
for s in 0 1 3 4 6 7 8 9; do
  curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
    -d "{\"box_code\":\"BOX-R1\",\"seq\":$s,\"temperature\":-4.6,
         \"event_time\":\"2026-09-25T09:00:00+08:00\"}"
done
curl -X POST http://localhost:8000/retransmission-preview \
  -H 'Content-Type: application/json' \
  -d '{"boxes":[{"box_code":"BOX-R1","weight":5},{"box_code":"BOX-R2","weight":3}],
       "seq_from":0,"seq_to":9,"max_command_length":4,"handshake_cost":1,"budget":12}'
# -> BOX-R1 不再缺报；BOX-R2 段 [0,9] 补 9 个（3 条指令 [0,0]、[1,4]、[5,8]，耗 12，收益 27）

# 参数非法（箱号重复、区间超过 60 个序号、权重/长度/开销/预算越界等）-> 422
curl -i -X POST http://localhost:8000/retransmission-preview \
  -H 'Content-Type: application/json' \
  -d '{"boxes":[{"box_code":"BOX-R1","weight":5},{"box_code":"BOX-R1","weight":3}],
       "seq_from":0,"seq_to":9,"max_command_length":4,"handshake_cost":1,"budget":12}'
curl -i -X POST http://localhost:8000/retransmission-preview \
  -H 'Content-Type: application/json' \
  -d '{"boxes":[{"box_code":"BOX-R1","weight":5}],
       "seq_from":0,"seq_to":60,"max_command_length":4,"handshake_cost":1,"budget":12}'
```

## 设计说明

- **去重键**：数据库对 `(box_code, seq)` 建唯一约束（`uq_box_seq`）。
  应用层先查后插，且捕获并发下的 `IntegrityError` 重新判定，
  因此设备并发重试也不会产生重复行或错误覆盖。
- **内容相等**：比较 `temperature` 与已归一化为 UTC 的 `event_time`。
  重试通常原样重发 JSON，浮点解析结果一致，可直接判等。
- **乱序安全**：最新状态通过“每箱 `MAX(seq)` 关联回原行”的子查询得到，
  不依赖插入先后，也不要求序号连续。
- **事件分页水位**：`GET /boxes/{box_code}/events` 首页把该箱当前最大事件 id
  固定为读取水位，随游标（base64url 编码的 JSON：版本号、箱号、水位、本页末尾
  `seq`）下发；续页只取 `id <= 水位 且 seq > 上页末尾 seq` 的记录，按 `seq`
  升序返回。事件 id 自增、只增不改，翻页期间新入库的乱序补报 id 必然大于水位，
  不会插入本轮遍历，已翻过的页也不会因补报而重复或遗漏；每页多取一条判断是否
  还有下一页，末页游标为 `null`。游标解码做严格格式校验（字段、类型、版本，
  且水位与末尾序号均为 `0 ~ 9223372036854775807` 内的整数——超出 SQLite 有符号
  64 位范围会在 SQL 参数绑定时抛 OverflowError，统一按格式错误拒绝），
  格式错误或箱号不匹配（跨箱使用）返回 `422`；箱号从未上报返回 `404`。
- **整数取值范围**：凡入库或参与 SQL 绑定的整数（事件序号、校准版本号、游标内的
  水位与末尾序号等）统一约束为 SQLite 有符号 64 位 INTEGER 范围
  `0 ~ 9223372036854775807`：请求体字段由 Pydantic 的 `ge/le` 校验，查询/路径参数
  在路由层校验，游标整数在解码时校验，任何越界（含负数与超过上界）都在进入
  SQL 之前返回 `422`，不会再以 `OverflowError` 形式变成 `500`；合法最大值
  `9223372036854775807` 的提交、查询与翻页行为与普通取值完全一致。
- **时钟校正只读**：`clock-audit` 在查询时即时计算，不写库、不改动原始事件；
  乱序补报后重查即自动重算。时间先精确换算为整数微秒（整数运算，无浮点误差），
  再用 PAVA 做等权保序回归：相邻块出现“前块均值 > 后块均值”时向前合并、
  块内取算术均值并四舍五入到微秒，天然得到非递减的建议序列，
  且最小化各事件偏移的平方和。
- **超温积分只读**：`heat-exposure` 复用同一套保序回归校正时间，把相邻事件间的
  温度视为线性变化，对超过 `upper_limit` 的部分解析求积（梯形/三角形公式），
  逐段积分用 `math.fsum` 精确求和；校正后间隔为零或超过 `max_gap_seconds` 的段
  直接跳过，缺号不补点。同样不写库，补报后重查即重算。
- **时间加权中位数只读**：`temperature-median` 复用区间事件读取与同一套保序校正
  时间，相邻事件间温度线性变化、每一瞬间按持续时间计入温度分布。变温段时长在其
  温度区间上均匀（密度 = 段时长 / 温度跨度），恒温段时长是该温度的点质量；
  实现上汇总各温度断点的点质量与密度净变化后从低到高扫描，累计时长首次达到
  总时长一半时的最低温度即中位数。温度与微秒时长全程用 `Fraction` 精确计算，
  占半并列时稳定落在低温侧；校正后间隔为零或超过 `max_gap_seconds` 的段不计入，
  缺号不补点。查询不写库，乱序补报后重查即自动重算。
- **轨迹压缩只读**：`trajectory-compression` 复用区间事件查询（按 `seq` 升序、
  缺号不补点），以序号为横坐标做最少保留点压缩。段 `(i,j)` 可行仅取决于两端点，
  因此问题等价于在“可行段”构成的 DAG 上求 0→n-1 的最短路径：用 DP 精确求解
  保留数最少、同数量时保留序号字典序最小的方案（贪心跳最远无法保证字典序最小）。
  误差比较用 `Fraction` 精确计算（float 温度与整数序号均可无损转为有理数），
  避免浮点误差在 `max_error` 边界上误判；单次限 120 条，O(n³) 开销可控。
  预览不写库，乱序补报后重查即自动重算。
- **包含证明只读**：`merkle-proof` 复用区间事件查询（按 `seq` 升序、缺号不补点），
  以实际事件为叶子即时建树：叶子是箱号、序号、`YYYY-MM-DDTHH:MM:SS.ffffffZ` UTC
  时间字符串与 `.17g` 温度字符串组成的无空格 JSON 数组（中文原样 UTF-8 编码），
  叶哈希 `SHA-256(0x00||数据)`、父哈希 `SHA-256(0x01||左||右)`、奇数层复制末节点；
  证明路径逐层给出兄弟哈希与左右方位，调用方可离线重算根。不写库，补报后重查即重算。
- **交接快照冻结**：`POST /handoffs` 在同一事务中按“每箱 `MAX(seq)` 关联回原行”读取
  各箱当前状态，把序号、温度、事件时间与结论（缺报/越界/正常，上下限边界值算正常）
  复制到 `handoff_snapshots` / `handoff_snapshot_items` 两张表后一次提交；
  `handoff_code` 有唯一约束，并发下同编号创建由 `IntegrityError` 兜底重判。
  快照行只增不改，后续事件补报（含乱序）只写 `box_events`，不会触碰已建快照；
  同编号同内容（箱号列表及顺序、温度上下限）的重复请求直接返回原快照，内容不同返回 409。
- **交接对账只读**：`GET /handoffs/{from_code}/reconcile/{to_code}` 只读取两份已冻结的
  快照明细做比较，不查询 `box_events`，结果只取决于两张快照本身，补报事件后重查不变。
  新增箱按目标快照顺序、移出箱按来源快照顺序列出；共同箱按来源顺序列出并给出
  原/目标结论与序号。顺序差异用 LIS 度量：共同箱按来源顺序映射到目标位置后，
  用耐心排序（bisect，O(n log n)）求最长递增子序列长度，
  最少需移动箱数 = 共同箱数 − LIS 长度；新增、移出不计移动，
  两份箱单相同（共同箱相对顺序一致）时为 0；任一快照不存在返回 404。
- **温度漂移校准**：校准数据独立存于 `calibration_sets` / `calibration_anchors`
  两张表，`(box_code, version)` 唯一约束作为幂等键，锚点行在创建时同一事务
  一次性写入，之后只增不改（服务层无更新/删除入口）。同箱同版本原样重交按
  逐点（序号、偏移）比对后返回原列表（200），异内容返回 409；并发下的同时
  提交由唯一约束的 `IntegrityError` 兜底重判。校准轨迹只读取 `box_events`
  与锚点：相邻锚点间按**序号**线性插值（用 `Fraction` 精确计算后转 float），
  区间两端沿用最近锚点偏移而不外推，缺报序号不补事件。校准结果仅在
  `/boxes/{box_code}/calibration-trajectory` 返回，**不写库、不改写原始事件，
  也不影响最新状态、交接快照与既有分析接口**；补报事件后重查即基于最新数据重算。
- **校准版本切换影响审计只读**：`/boxes/{box_code}/calibration-impact` 先加载
  from/to 两版已存锚点（任一缺失即 404，`missing_versions` 一并返回），再复用
  `calibration-trajectory` 同款 `_anchor_offset_fraction` 序号线性插值
  （两端钳平、不外推）与区间实际事件读取（缺号不补点），箱号从未上报则 404。
  每条事件的两版校准温度与下/上限的比较全程用 `Fraction` 精确进行：三档结论为
  below（严格低于下限）、normal（区间内含边界）、above（严格高于上限），
  恰等于边界时稳定落入 normal，杜绝浮点边界误判。仅收集两版结论不同的事件，
  按 `seq` 升序单趟扫描合成影响段——后一条序号恰为前一条 +1 且
  `(from_status, to_status)` 相同才延续，否则（缺号、结论未变或方向变化）断段，
  段内维护两版校准温差绝对值的最大值。审计只读取 `box_events` 与校准锚点，
  不写库、不改锚点，现有校准数据与全部分析结果原样保留。
- **群体静默检测只读**：`/telemetry-outages` 先在路由层对 `box_codes` 去空白、
  按请求顺序去重并拒绝空/超长箱号（422）；服务层一次性校验全部箱号存在
  （任一缺失即 404，`missing_box_codes` 一并返回），再单次查询所有箱区间内
  实际事件（按箱 + seq 排序），缺号不补点。每箱复用 clock-audit 同款整数微秒
  PAVA 等权保序校正；`max_silence_seconds` 在路由层按请求原值（十进制文本）
  精确解析为 `Decimal`（不经过二进制浮点，正数且有限才合法，否则 422），
  服务层将其无损换算为整数微秒后与校正后的整数微秒间隔比较：
  静默区间为**半开** `[t_i, t_{i+1})`，仅当间隔**严格大于**阈值时产生，
  恰好等于阈值不算（如阈值 `0.3` 与恰好 0.3 秒即 300000 微秒的间隔，
  不会因 double 无法精确表示 0.3 而误判为静默）；不足两条事件的箱不产生区间。
  随后对各箱区间做端点扫描
  （同一时刻先结算区间结束、再结算区间开始，半开语义下同箱相邻区间不会重复
  计数），相邻事件点之间同时静默箱数恒定为一个基本段；把端点相接的合格基本段
  直接并入同一个未闭合段，即得到同时静默数达标的极大时间段，段内峰值同时静默
  箱数在扫描中维护。全程只读 `box_events`，不写库，补报后重查即重算。
- **补报调度预览只读**：`/retransmission-preview` 在路由层用 schema 校验
  1～4 个互异箱号、权重 1～10、共同区间每箱最多 60 个序号、单指令长度 1～20、
  握手开销 0～20、预算 0～200（均不满足即 422）；服务层一次查出各箱区间内
  已收到序号，合并出极大连续缺号段（从未上报的箱自然得到整个区间一段，不 404）。
  同一缺号段内覆盖 k 个序号的最省方案是从段起点连续覆盖、用 ⌈k/单指令最大长度⌉
  条指令（消耗 ⌈k/m⌉×握手开销 + k，更多指令只增加握手开销、不增收益），
  因此每段归约为“选 k = 0..段长”的多选背包，按预算（≤200）做 DP 精确求解：
  段按 (箱号, 段起点) 升序处理，指令元组按处理顺序连接即全局 (箱号, 起点, 终点)
  字典序，DP 逐状态保留 (收益, 指令数, 指令列表) 中收益最大、指令数最少、
  字典序最小的方案；段内同 k 的覆盖块取段起点、切分取后载（靠前指令尽量短）
  即字典序最小。结果为唯一最优方案，含指令明细、预算消耗与各箱仍缺报区间；
  全程只读 `box_events`，不写库，乱序补报后重查即重算。
- **校验错误兜底**：请求体携带 `NaN`/`Infinity` 时，校验错误的详情里会带上这些
  非有限浮点，而 Starlette 的 JSON 响应不允许序列化它们；`main.py` 自定义了
  `RequestValidationError` 处理器，先把错误详情中的 NaN/Inf 替换为字符串再返回 422，
  避免 422 响应自身序列化失败变成 500。
- **持久化**：默认 SQLite + WAL，卷挂载于 `/app/data`；
  如需 PostgreSQL/MySQL，只需将 `DATABASE_URL` 换成对应 SQLAlchemy 连接串
  （并安装相应驱动），建表逻辑通用。
