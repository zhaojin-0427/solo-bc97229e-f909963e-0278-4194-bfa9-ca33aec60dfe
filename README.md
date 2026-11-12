# 门户阅读会话重建服务（FastAPI + SQLite）

从门户访问日志还原读者的阅读会话，定位读者离开前最后访问的页面。服务批量接收
访问/退出日志，支持按 `log_id` 补传与更正，在后台重建受影响访客的会话，并通过
只读接口返回每个访客最新两版会话（旧版 + 新版）。

## 1. 快速启动（Docker Compose）

```bash
docker compose up --build
```

启动两个容器、共享一个持久化数据卷 `portal-data`：

| 服务 | 职责 |
| --- | --- |
| `api` | FastAPI 进程，接收日志、提供查询；端口 `8000` |
| `worker` | 后台重建进程，轮询持久化待处理队列，在独立 work 库中计算，短事务发布 |

- 交互式 API 文档（Swagger UI）：<http://localhost:8000/docs>
- ReDoc：<http://localhost:8000/redoc>
- 健康检查：`GET /health`

在 Swagger UI 中可直接演示「提交 → 更正 → 查询」全流程；也可以运行脚本化演示：

```bash
python3 scripts/demo.py          # 仅依赖标准库，默认访问 http://localhost:8000
```

停止与清理：

```bash
docker compose down             # 保留数据卷，重启后接续处理
docker compose down -v          # 同时删除数据卷
```

## 2. 会话规则

1. 同一访客的事件按 `(ts, log_id)` 升序排列（UTC 整秒时刻，同刻以日志 ID 兜底）。
2. 相邻两条事件间隔 **严格大于 30 分钟（1800 秒）** 即断开为两个会话；
   恰好 30 分钟不断开。
3. 遇到 `exit` 事件立即断开；退出事件不计入页面序列，也没有页面路径。
4. 多标签页交错访问按同一排序规则自然合并；重复访问的路径全部保留。
5. 不含任何访问的空会话不输出；若某访客的事件被全部更正走，会发布一个
   零会话的空代际。
6. 每个会话返回 `start_ts / end_ts`（Unix 整秒）与 ISO-8601（UTC，`Z` 结尾）
   时间，以及有序的 `page_paths`；`end_ts` 即读者离开该会话前最后一个访问页。

## 3. API 一览

### `POST /logs` — 批量提交 / 补传 / 更正

```json
{
  "logs": [
    {"log_id": "evt-01", "visitor_id": "visitor-7", "ts": 1731400000,
     "action": "visit", "page_path": "/home"},
    {"log_id": "evt-04", "visitor_id": "visitor-7", "ts": 1731400060,
     "action": "exit"}
  ]
}
```

- `visit` 必须带非空 `page_path`；`exit` 不允许带 `page_path`；`ts >= 0`。
- 同一 `log_id` 重复提交即为更正：内容完全相同记为 `unchanged`（不触发重建）；
  内容变化记为 `corrected`。
- 更正若更换 `visitor_id`，**新旧两个访客都会递增版本并排队重建**。
- 单批内同一 `log_id` 出现多次时，以最后一条为准。
- 响应逐条给出 `inserted / corrected / unchanged`、受影响访客列表，以及
  `pending_rebuild` 标记。所有变更与待处理状态在一个事务内落库后才返回。

### `GET /visitors/{visitor_id}/sessions` — 查询

单个只读事务内读取该访客**完整的新旧两版**会话：

```json
{
  "visitor_id": "visitor-7",
  "pending_rebuild": false,
  "current_version": 2,
  "input_version": 2,
  "generations": [
    {"version": 2, "sessions": [
      {"session_index": 0, "start_ts": 1731400000, "end_ts": 1731400080,
       "start_time": "2024-11-12T08:26:40Z", "end_time": "2024-11-12T08:28:00Z",
       "page_paths": ["/home", "/pricing", "/products/42", "/products/42", "/blog"]}
    ]},
    {"version": 1, "sessions": [ /* 上一版完整会话 */ ]}
  ]
}
```

- `pending_rebuild=true` 表示原始日志已有新变更、对应重建尚未发布；此时
  返回的仍是上一版完整可用结果，读取不被阻塞。
- `input_version` 是原始输入的当前版本，`current_version` 是已发布版本，
  二者相等表示已追平。

### `POST /admin/visitors/{visitor_id}/rebuild`

将指定访客重新排队（运维/演示辅助）。

## 4. 并发与一致性设计（对应关键约束）

```
api 容器 ──写──▶ portal.db (WAL)
                   ▲            │ 只读短事务取原始日志
                   │ 短事务发布  ▼
worker 容器 ──计算── work/rebuild-*.db（独立临时库，用完即删）
```

- **重建不占用正式库写事务**：重计算只发生在 `WORK_DIR` 下的独立 SQLite
  临时库；对正式库只有一次只读事务（取该访客全部原始日志）和一次
  `BEGIN IMMEDIATE` 短发布事务。重建期间 API 持续接收日志、持续返回旧会话。
- **版本号 + 比较并交换（CAS）发布**：每次被接受的更正使相关访客版本 +1，
  待处理行记录目标版本。发布时若发现输入版本已前进（说明重建期间又有补传/
  更正），本次结果作废并按最新版本重新排队——**迟到计算永远不会覆盖新结果**。
- **整访客整体替换**：发布在一个事务内删除待处理行、写入整代会话、登记
  代际并裁剪到最新两版，读者不会看到半个结果。已确认的补传在发布前必然
  已计入目标版本，因此发布天然包含全部已确认变更。
- **中间记录变化可拆分或合并会话**：重建是全量重算，任何事件的增改都会在
  新版本中重新决定 30 分钟断点与退出断点。
- **持久化与重启接续**：原始日志、访客版本、待处理队列全部落在
  `portal.db`（WAL 模式）。worker 启动时把残留的 `processing` 行收回为
  `pending`，并清理临时 work 文件，随后继续处理。
- **WAL 与 `busy_timeout`**：读写并发不互斥；两个容器共享数据卷，写冲突
  由 SQLite 锁配合 5 秒忙等待串行化（队列行本身也保证同一访客同一时刻
  只有一个重建任务）。

### 数据表

| 表 | 作用 |
| --- | --- |
| `raw_logs` | 原始日志，`log_id` 主键，按 `(visitor_id, ts, log_id)` 索引 |
| `visitor_versions` | 每访客单调递增的输入版本号 |
| `pending_rebuilds` | 持久化重建队列（`pending` / `processing`） |
| `sessions` | 已发布会话（JSON 页面序列），整代替换 |
| `published_generations` | 代际登记，含零会话的空代际，驱动两版保留 |

## 5. 本地开发与测试

```bash
pip install -r requirements-dev.txt
pytest                     # 单元 + 端到端（含 CAS 防覆盖、重启接续、双方修正）
```

无网络环境下可用标准库冒烟脚本验证核心规则与发布事务（不依赖第三方包）：

```bash
python3 -I scripts/offline_smoke.py
```

单进程本地运行（API 内嵌 worker，便于调试；生产 Compose 仍为双容器）：

```bash
RUN_WORKER_IN_PROCESS=true DATA_DIR=./data uvicorn app.main:app --reload
```

## 6. 目录结构

```
app/
  config.py        环境变量与常量（30 分钟阈值、数据目录等）
  db.py            连接与建表（WAL、显式事务）
  sessions.py      纯函数：事件排序与会话切分
  schemas.py       Pydantic 请求/响应模型与校验
  worker.py        后台重建：领取 → 临时库 → 计算 → CAS 发布
  worker_main.py   worker 容器入口
  main.py          FastAPI：/logs、查询、管理接口
scripts/
  demo.py          标准库演示脚本（提交、更正、换人、查询）
  offline_smoke.py 离线冒烟测试
tests/             pytest 单元与端到端测试
docker-compose.yml  api + worker 双容器 + 共享数据卷
Dockerfile
```
