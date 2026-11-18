# 部署完成通知 · 幂等送达与人工复核演示系统

发布值班员通过网页向审计端发送「部署完成通知」。网络超时时无法判断对方是否已记账，
盲目重发又会产生重复记录。本系统用 **FastAPI + SQLite** 实现两端（各自独立数据库），
以「**幂等键 + 回执核对**」解决送达问题，并在接收端支持审计员的**人工复核**闭环：

- 相同「环境 + 发布号」的同内容重试，两端都只记一次；
- 同键不同内容 → **409 冲突**；
- 丢回执 / 接收端不可达 / 发送端重启 → 状态保持 **待核**，**绝不虚报成功**，
  恢复后凭接收端回执核对定案（已入账→已确认；未入账→回退待发送，可安全重发）；
- 审计员在确认通知已送达（回执已入账）后，在接收端网页按「环境 + 发布号」提交
  **复核人、结论、备注**：同回执同内容重试只保留一条，不同内容报 409，不存在的回执不能复核；
- 发送端核对时**同时显示送达与复核状态**；复核与发送、核对交错执行时，
  复核状态只单向推进（无复核→已复核），不会出现孤立或倒退；任何本地写入失败都保持原状态。

## 目录结构

```
├── docker-compose.yml      # 一键启动两端
├── sender/                 # 发送端（值班网页 + 发送/核对逻辑），端口 8000
│   ├── Dockerfile
│   └── app/{main.py, static/index.html}
├── receiver/               # 审计接收端（回执台账 + 人工复核，含故障注入），端口 8001
│   ├── Dockerfile
│   └── app/{main.py, static/index.html}
└── tests/
    ├── selftest.py         # 77 项关键情形自测（仅标准库）
    └── run_local.sh        # 无 Docker 时的本地进程运行器
```

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

- 发送端网页：http://localhost:8000 （填写环境、发布号、结果，提交并发送，同时查看送达与复核状态）
- 接收端台账：http://localhost:8001 （实时查看已入账回执，并在每条回执上提交人工复核）

两端各持有一个独立 SQLite 库（compose 命名卷 `sender_data` / `receiver_data`）。

无 Docker 的本地环境（需 Python 3.11+ 及 fastapi/uvicorn/httpx）：

```bash
tests/run_local.sh start-all     # 两个独立进程 + 两个独立 db 文件
tests/run_local.sh stop-all
```

## 状态机（发送端）

| 状态 | 含义 | 进入条件 |
|---|---|---|
| 待发送 PENDING | 本地已登记，未发送或已确认对方未记账 | 登记成功；核对时接收端明确无此记录 |
| 待核 UNCERTAIN | 已发出但回执缺失/对方不可达，送达与否未知 | 发送前落库；超时、连接失败、对方 5xx |
| 已确认 ACKED | 收到接收端内容一致的回执 | 发送响应 2xx 且内容哈希一致；核对命中 |
| 冲突 CONFLICT | 同环境同发布号在接收端已存在不同内容 | 发送/核对时发现内容不一致（需人工处理） |

关键约束：**只有接收端回执能把记录变为「已确认」**。发送动作会先把「待核」落库再发 HTTP，
即使进程在发送途中崩溃，重启后也知道必须核对，而不是误报成功或误当未发。

送达状态只允许前进（`PENDING/UNCERTAIN → ACKED/CONFLICT`，`UNCERTAIN → PENDING` 仅在接收端
明确无记录时回退）；`ACKED`、`CONFLICT` 为终态，任何交错操作都不会把它们降级。

## 复核状态机（发送端镜像接收端复核）

| 状态 | 含义 | 进入条件 |
|---|---|---|
| 未复核 NONE | 接收端回执尚无人工复核 | 初始登记；接收端 `review=null` |
| 已复核 REVIEWED | 接收端已留下复核人/结论/备注 | 发送或核对拿到的回执内嵌 `review` 对象 |

复核状态**单向推进**：已复核后任何发送/核对都不会清空或回退复核（SQL 用 `COALESCE`
与 `CASE WHEN review_status='NONE'` 守卫），因此复核与发送、核对交错时不会出现孤立或倒退。

## 演示场景

1. **正常送达**：网页填写 `prod / R2026-001 / 成功` → 提交 → 状态「已确认」，接收端台账出现一条。
2. **重复提交**：再次提交相同内容 → 发送端幂等去重、接收端仍只有一条。
3. **内容冲突**：同环境同发布号改选「失败」→ 提交即报 409 冲突，原记录不被篡改。
4. **入库后丢回执**：勾选「模拟接收端故障」再提交 → 状态变「待核」（接收端其实已入账）；
   点「重试」→ 接收端幂等去重，凭回执转「已确认」，仍只有一条。
5. **接收端不可用**：`docker compose stop receiver` → 发送/核对均保持「待核」（不虚报成功）；
   `docker compose start receiver` 后点「核对」→ 若对方无记录则回退「待发送」，可安全重发。
6. **发送端重启**：`docker compose restart sender` → 状态持久不丢；启动后自动核对所有待核记录，
   凭接收端回执结案（接收端不可达时保持待核）。
7. **人工复核闭环**：接收端网页在已入账回执上填写「复核人/结论/备注」提交 →
   同内容再提交只保留一条（200 `deduplicated`）；改内容提交报 409，原复核不变；
   对未送达的回执提交报 404。回到发送端点「核对」→ 同一行同时显示「已确认 + 已复核」及复核明细。
8. **写入失败保持原状态**：发送端 `?simulate=write_fail` 或核对 `?write_fail=true` 注入本地写失败 →
   返回 500 且送达/复核状态原样保持，恢复后可继续核对；接收端复核 `review_write_fail=true`
   注入 → 500 且无复核落库，重试成功。

## API 摘要

发送端（:8000）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/notifications` | 登记（同键同内容幂等，同键不同内容 409） |
| GET | `/api/notifications` | 列表（同时含送达与复核字段） |
| POST | `/api/notifications/{id}/send[?simulate=lose_response\|slow\|write_fail]` | 发送/重发；`write_fail` 仅模拟本端写失败 |
| POST | `/api/notifications/{id}/reconcile[?write_fail=true]` | 凭接收端回执核对送达与复核 |
| POST | `/api/reconcile-all` | 核对全部待核/待发送 |

接收端（:8001）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/receipts` | 幂等入账；body 可带 `"simulate":"lose_response"`（入库后丢响应）或 `"slow"`（慢回执） |
| POST | `/api/receipts/review` | 人工复核：回执不存在 404；同内容重试 200 幂等只留一条；不同内容 409；`review_write_fail=true` 模拟写失败 |
| GET | `/api/receipts?env=..&release_no=..` | 查询单条回执（404 表示无记录），响应内嵌 `review` 对象 |
| GET | `/api/receipts` | 台账列表（每条均含 `review`，未复核为 `null`） |

## 幂等与并发设计

- 两端均以 `UNIQUE(env, release_no)` 为幂等键，通知内容身份为 `{env, release_no, result}` 的 SHA-256，
  复核内容身份为 `{env, release_no, reviewer, conclusion, remark}` 的 SHA-256；
- 写入路径 `BEGIN IMMEDIATE` 事务内「先查后插」，配合 WAL 与 busy_timeout：
  并发下同键只落一行，后到者走幂等返回或 409（通知与复核各一张表，规则一致）；
- 重发安全：接收端对同内容重试返回既有回执（200, `deduplicated`），不新增记录；
- 复核只挂在已存在的回执上（`reviews.receipt_id` 外键 + 入账校验），不存在的回执无法复核；
- 发送端每条状态变更用一条原子 UPDATE 完成，送达迁移受白名单守卫、复核字段只 `COALESCE` 补入，
  杜绝交错操作下的状态倒退或复核丢失；
- 任一端写入失败都不会留下错误的成功状态：接收端写失败→5xx→发送端保持待核；
  发送端本地写失败（含 `write_fail` 注入）→500 且原状态不变；拿到回执但本地更新失败→仍为待核，可靠核对补回。
- 旧版发送端数据库启动时自动 `ALTER TABLE` 补齐复核列，历史记录默认 `review_status='NONE'`。

## 自测

```bash
# Docker 模式（先 docker compose up --build）
python3 tests/selftest.py --lifecycle        # 含停接收端/重启发送端场景（钩子默认 docker compose）

# 无 Docker 本地模式
tests/run_local.sh test
```

覆盖：正常送达、同内容重试去重、同键冲突、丢回执后重发/核对、慢回执超时、
12 线程并发同内容、10 线程并发不同内容、接收端停机不虚报、发送端重启后待核持久与自动补核；
以及复核专项 R1–R10：无回执 404、复核幂等去重、复核 409、发送/核对/复核交错不倒退不孤立、
两端写入失败保持原状态、并发复核与并发交错核对、复核状态重启持久（共 77 项断言）。

> 本地运行器默认关闭发送端启动自动核对（`STARTUP_RECONCILE=0`），避免后台核对线程与
> 慢回执用例产生时序竞争；S11「重启自动核对」通过 `restart-sender-autoreconcile` 钩子
> 单独以 `STARTUP_RECONCILE=1` 重启验证。Docker 模式下发送端默认开启启动自动核对。
