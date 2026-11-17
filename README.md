# 水质数仓化验表 API

固定站点、UTC 采样时刻，外加至多 8 个可空浮点指标的化验表服务。
技术栈：**FastAPI + PyArrow + SQLite + Parquet 分片**，可通过 Docker Compose 一键启动。

## 1. 核心模型（为什么改名/删名不会串列）

| 概念 | 说明 |
| --- | --- |
| 指标身份 `indicator_id` | 指标的稳定身份。**改名只改名字，不换身份**；删除只是 `active=0`，身份永不复用。 |
| 字段版 `schema_version` | 每次新增/改名/删除都产生一个不可变新版本，版内固化“字段名 → 指标身份”的完整映射。 |
| 分片 shard | 每次 Parquet 上传产生一个分片，物理列名为身份列 `i{indicator_id}`，与当前叫什么名字完全解耦。 |
| 快照 snapshot | 首次查询时固化“当时的字段名集合 + 当时的全部分片”，此后分页、字段变更、上传、重启都不改变它。 |

典型故事（与需求一致）：

1. 建指标 **浓度**（身份 1，字段版 2），上传历史分片 → 物理列 `i1`。
2. **浓度 改名为 硝酸盐**（身份仍是 1，字段版 3）。
3. 再建指标 **浓度**（同名复用 → 身份 2，字段版 4，语义为磷酸盐，不继承任何旧值）。
4. 用**旧字段版 2** 上传：旧版“浓度”映射到身份 1，数据进 `i1`（硝酸盐），不串列。
5. 用**字段版 4** 上传：当前“浓度”映射到身份 2，数据进 `i2`（磷酸盐）。
6. 查询按当前名合读：旧分片没有 `i2` → 磷酸盐列为 `NULL`；新分片没有 `i1` → 硝酸盐列为 `NULL`。

## 2. 启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 服务：http://localhost:8000
# 交互式文档（Swagger）：http://localhost:8000/docs
# OpenAPI：http://localhost:8000/openapi.json
# 健康检查：http://localhost:8000/health
```

数据持久化在命名卷 `water_quality_data`（容器内 `/data`，含 `warehouse.db` 与 `shards/*.parquet`），
重启容器后字段、分片、快照全部保留。

### 本地直接运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 3. API 一览

| 方法/路径 | 作用 |
| --- | --- |
| `GET /schema` | 当前字段版号、固定字段、存续指标（含身份 ID） |
| `GET /schema/versions/{version_id}` | 查看任意字段版的不可变映射（含已删指标） |
| `POST /schema/indicators` | 新增指标（body `{"name": "..."}`）；同名复用得到**新身份** |
| `POST /schema/indicators/rename` | 改名（body `{"old_name": "...", "new_name": "..."}`），身份不变 |
| `DELETE /schema/indicators/{name}` | 删除指标（历史分片保留） |
| `POST /ingest/parquet` | 表单上传：`schema_version`（必填）+ `file`（Parquet） |
| `GET /rows` | 分页查询；首次不带 `snapshot_id` 即建快照，之后带快照号翻页 |
| `GET /snapshots/{snapshot_id}` | 快照元数据（固化的字段名与分片） |
| `POST /coverage/verifications` | 日覆盖核验，生成不可变报告 |
| `GET /coverage/reports/{report_id}` | 按报告号读取原始结论（过期后仍保留） |
| `GET /coverage/supply` | 当前供数裁决（只认最新且未过期且齐全的报告） |
| `GET /docs` | Swagger UI |

### Parquet 上传规则

* 必含固定列：`station`（非空字符串）、`sampled_at`（UTC 时刻；支持带时区 timestamp、
  ISO-8601 字符串含 `Z`/偏移量、date；无时区值按 UTC 处理）。
* 指标列必须可转 `float64`（整数允许、布尔拒绝、全 NULL 允许），至多 8 个。
* 列名按**指定字段版**裁决：
  * 版内不存在的列（未知列）→ **整次拒绝（409）**；
  * 列映射到已删除指标 → **整次拒绝（409）**；
  * 版内未包含的指标可以缺列（指标本身可空）。
* 空文件、0 行分片、无法解析的 Parquet → 409；字段版不存在 → 404。
* 失败只回滚事务并清理临时文件，**不改动任何已有字段、数据与快照**。

### 查询与快照语义

* `GET /rows?page=1&page_size=100`：首次调用返回 `snapshot_id` 与 `snapshot_created=true`。
* 之后 `GET /rows?snapshot_id=...&page=2&page_size=100` 始终读同一批分片、同一组字段名；
  新字段在旧分片为 `NULL`，同名复用的新身份在旧分片同样为 `NULL`。
* 不带快照号的新查询会生成新快照，反映最新字段与分片。
* 分片内按 `(station, sampled_at)` 稳定排序后分页；`page_size` 上限 10000。

### 并发与一致性

所有字段变更、上传、快照创建与覆盖核验都在同一把进程写锁 + SQLite `BEGIN IMMEDIATE` 事务内裁决：
字段变更与上传/核验交错时，每个请求看到的都是一个完整的已提交字段状态，不存在半份结果。
供数查询在只读事务内同时裁决“指标是否存续”与“最新报告状态”，不会读到新旧混合的结论。

## 3.1 日覆盖核验

水质数据员需要向建模组确认：**某站某指标某一天的例行采样是否齐全**。

### 应采时刻与到齐判定

* 输入：站点、**存续指标的身份 ID**、UTC 日期（`YYYY-MM-DD`）、采样间隔（分钟）。
* 采样间隔必须是正整数且**整除 1440**；应采时刻从当日 00:00 起每间隔一个（不含次日 00:00），
  共 `1440 / interval` 个。
* 某应采时刻“到齐”的充要条件：存在一行，其 `station` 等于目标站点、`sampled_at` 等于该应采时刻、
  且**该指标身份列** `i{indicator_id}` 的值**非 NULL**。
  * 只认该身份：同名新建的其他指标、其他身份的列都不算。
  * 其他站点、非应采时刻（例如 00:07）的值都不算。
  * 同一时刻多行（同分片或跨分片）只算**一次**；报告的 `arrived_instants[].shard_ids`
    列出该时刻的来源分片号（去重升序）。
* 报告同时给出全部应采时刻、缺测时刻、已到时刻与来源分片。

### 报告不可变 / 过期 / 供数

| 行为 | 语义 |
| --- | --- |
| `POST /coverage/verifications` | 生成一个**不可变**报告，返回 `report_id` 与当时结论（201） |
| `GET /coverage/reports/{report_id}` | 按号读取原结论；**过期后 `complete`、缺测明细一字不改**，只多出 `expired/expired_at/expired_reason` |
| `GET /coverage/supply` | 只认该键（站/指标/日/间隔）**最新且未过期且齐全**的报告 |

供数不可用的原因码：

| `reason_code` | 含义 |
| --- | --- |
| `no_report` | 尚无该键的核验报告 |
| `report_expired` | 最新报告已过期（当日有新分片或指标已删除） |
| `report_incomplete` | 最新报告不齐全（`missing_instants` 给出缺测时刻） |
| `indicator_unavailable` | 指标不存在或已删除 |

过期触发（**只追加过期标记，不改写原结论**）：

* **同站、同指标、同日出现新分片**：上传回执 `expired_report_ids` 列出本次被撤回供数的报告号；
  其他站点、其他日期、其他指标的到货不影响该报告。
* **指标被删除**：其全部未过期报告立即过期，删除接口回执同样列出 `expired_report_ids`。
* **改名**沿用原身份：报告继续按原 `indicator_id` 有效；**同名新建**是新身份，不能沿用旧报告。

### 一致性与失败语义

* 核验与上传、字段变更在同一把写锁 + 立即事务内串行裁决，失败整体回滚：
  不会产生半份报告，也不会误撤供数（例如含未知列的坏上传被整次拒绝，旧报告仍可用）。
* 交叉执行后每份报告都满足 `arrived_count + missing_count == expected_count`，且
  `complete == (missing_count == 0)`；供数裁决始终与最终已提交状态一致。

### 示例

```bash
# 1) 核验（站点 S1、指标身份 1、2026-01-01、每小时一次）
curl -X POST 'http://localhost:8000/coverage/verifications?station=S1&indicator_id=1&date=2026-01-01&interval_minutes=60'

# 2) 供数裁决
curl 'http://localhost:8000/coverage/supply?station=S1&indicator_id=1&date=2026-01-01&interval_minutes=60'

# 3) 读取历史报告（过期后原结论保留）
curl 'http://localhost:8000/coverage/reports/1'
```

## 4. 目录结构

```
app/
  config.py    # 常量与数据目录
  db.py        # SQLite 建表、连接、立即事务
  models.py    # Pydantic 模型与错误类型
  catalog.py   # 指标身份与字段版（新增/改名/删除）
  ingest.py    # Parquet 校验、按字段版映射身份、原子落盘
  query.py     # 快照创建与分页合读
  coverage.py  # 日覆盖核验、报告不可变结论、过期与供数裁决
  main.py      # FastAPI 路由与并发控制
tests/         # pytest 自测 + 独立 HTTP 冒烟脚本
Dockerfile
docker-compose.yml
```

## 5. 自测

```bash
pip install -r requirements.txt
pip install pytest httpx
python -m pytest tests/ -v
# 对已运行的服务做独立 HTTP 冒烟：
python tests/manual_smoke.py
```
