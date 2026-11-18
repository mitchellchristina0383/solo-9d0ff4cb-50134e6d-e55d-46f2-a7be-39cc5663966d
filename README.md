# HTTP 入口网关（支持优雅排空 / 健康探测 / 轮询转发 / 故障区间记录）

滚动更新场景下的入口网关：运维人员先排空待停机实例，使新请求不再进入，
同时保证在途长请求完整取回原响应，全部结束后才报告排空完成。

网关按节点记录**故障区间**（SQLite 持久化）：转发遇 5xx / 超时 / 连接失败、
健康探测失败时开启区间；只有故障发生之后启动的成功探测才能结束区间。
值班交接时可通过 `/admin/nodes/{name}/intervals` 查清某实例何时停止接流、
何时经探测确认恢复，网关重启后未确认结束的旧区间会标记为「监测中断」。

技术栈：**FastAPI + httpx + SQLite**，由 **Docker Compose** 启动 1 个网关 +
2 个可模拟慢请求 / 故障的上游。

## 目录结构

```
.
├── docker-compose.yml      # 编排：gateway + upstream1 + upstream2（含区间库数据卷）
├── gateway/
│   ├── app.py              # 网关：转发、轮询、探测、排空/恢复/状态、故障区间记录
│   ├── requirements.txt
│   └── Dockerfile
├── upstream/
│   ├── app.py              # 模拟上游：slow / flaky / 故障模式控制
│   ├── requirements.txt
│   └── Dockerfile
└── tests/
    ├── self_test.py            # 本地端到端自测（21 组场景，103 项断言）
    └── test_intervals_unit.py  # 故障区间状态机单元自测（23 项断言）
```

## 一键启动

```bash
docker compose up --build
# 网关:        http://localhost:18080
# 上游 node1:  http://localhost:18081
# 上游 node2:  http://localhost:18082
# 交互文档:    http://localhost:18080/docs
```

网关环境变量（见 `docker-compose.yml`）：

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `UPSTREAMS` | `node1=http://upstream1:8000,node2=http://upstream2:8000` | 上游列表 `name=url` |
| `PROBE_INTERVAL` | `2.0` | 周期探测间隔（秒） |
| `PROBE_TIMEOUT` | `1.5` | 探测超时（秒） |
| `REQUEST_TIMEOUT` | `15.0` | 转发请求超时（秒） |
| `DRAIN_DEFAULT_TIMEOUT` | `30.0` | 排空 API 默认等待上限（秒） |
| `INTERVAL_DB_PATH` | `gateway/intervals.db`（compose 中为 `/data/intervals.db`） | 故障区间 SQLite 路径 |
| `INTERVAL_STORE_FAIL_WRITES_FILE` | 空（关闭） | 故障注入：该文件存在时区间写操作全部失败（演练用） |

compose 中区间库放在命名卷 `gateway-data`，网关容器重启后历史区间仍可查询。

## API 一览（均可在 /docs 直接操作）

转发（仅 GET）：

```
GET /proxy/{path}      # 轮询转发到健康且未排空的节点，透传查询串与响应
                       # 无可用节点 -> 503；上游超时 -> 504；连不上 -> 502
                       # 非 GET 方法 -> 405
```

管理：

```
GET  /admin/status                     # 全部节点健康/排空/在途/计数/最近探测/当前故障区间
POST /admin/nodes/{name}/drain?timeout=# 排空：立即停接新流，阻塞到在途清零
                                       #   在途归零 -> 200 drained=true
                                       #   超时仍有在途 -> 202 drained=false
POST /admin/nodes/{name}/resume        # 恢复：清排空，立即探测，由探测决定能否接流
POST /admin/probe                      # 手动触发一轮探测
```

故障区间查询：

```
GET /admin/nodes/{name}/intervals?state=&limit=   # 按节点查询故障区间（含当前是否处于故障中）
GET /admin/intervals?node=&state=&limit=          # 全局查询，可按节点/状态过滤
# state: open=进行中, recovered=已恢复, interrupted=监测中断
# 每条区间含 opened_at / recovered_at / interrupted_at（epoch + ISO8601）、
# 开启原因 cause（request_5xx / request_timeout / request_connect_error /
# probe_failure）、恢复探测启动时刻 recovery_probe_started_at 等。
```

上游模拟控制（演练用，直连上游端口，不经过网关）：

```
GET  /control/state            # 查看模式与在途数
POST /control/mode?mode=       # healthy | slow | error | hang
GET  /slow?seconds=N           # 慢请求（验证在途不被截断）
GET  /flaky                    # 按模式返回 200 / 500 / 挂起
GET  /                         # 健康端点（error->503, hang->挂起）
```

## 典型演练

```bash
# 1. 发一个慢请求（10 秒），在另一终端立刻排空 node2
curl "http://localhost:18080/proxy/slow?seconds=10" &
curl -X POST "http://localhost:18080/admin/nodes/node2/drain?timeout=30"
# 现象：drain 请求阻塞，约 10 秒后才返回 drained=true；慢请求拿到完整 200

# 2. 让 node1 进入故障模式，观察探测自动摘除并开启故障区间
curl -X POST "http://localhost:18081/control/mode?mode=error"
sleep 3
curl http://localhost:18080/admin/status                 # node1 healthy=false
curl "http://localhost:18080/admin/nodes/node1/intervals"  # 新增 open 区间

# 3. 故障未消除时恢复 node1：不会绕过探测，节点仍不可接流
curl -X POST http://localhost:18080/admin/nodes/node1/resume
# eligible=false；上游修好后经周期探测自动恢复接流，区间被关闭为 recovered

# 4. 上游恢复后查询完整故障区间（值班交接）
curl -X POST "http://localhost:18081/control/mode?mode=healthy"
sleep 3
curl "http://localhost:18080/admin/nodes/node1/intervals?state=recovered"

# 5. 重启网关：未确认结束的旧区间被标记为「监测中断」，不倒填恢复时刻
docker compose restart gateway
curl "http://localhost:18080/admin/intervals?state=interrupted"
```

## 关键设计与并发正确性

- **两个独立标志位**：`healthy` 只由探测（含请求失败降级）写入；`draining` 只由
  人工 drain/resume 写入。因此**探测成功永不解除排空**，两者交错互不串改。
- **恢复不绕过故障探测**：`resume` 先清排空并保守置 `healthy=false`，再做一次
  真实探测；探测通过前该节点不会被轮询选中。
- **轮询 + 原子登记**：在同一把 `asyncio.Condition` 锁内完成选节点与
  `inflight += 1`，保证"选中即可接流、计数即生效"。
- **在途计数可靠**：转发用 `try/finally` 归还计数，成功、5xx、超时、连接错误、
  客户端取消都会准确递减；排空等待在条件变量上，请求一结束即被唤醒。
- **排空语义**：置位后新请求立即绕过该节点；已在途请求不被取消，继续等待并
  透传原响应；只有 `inflight` 归零才返回 `drained=true`。
- **不虚报成功**：5xx 如实透传并即时降级；超时返回 504、连接错误返回 502，
  均标记失败并刷新健康状态，绝不返回虚假 200。
- **无节点可接流**：所有节点都不健康或排空中时，`GET /proxy/*` 返回 503。

### 故障区间

- **开启**：转发遇 5xx / 超时 / 连接失败、健康探测失败时开启区间；同一故障
  期间内的重复失败不新增区间。
- **结束**：只有「故障发生之后启动」的成功探测才能结束区间（记录
  `recovery_probe_started_at >= opened_at` 供核对）；在故障前已启动、
  故障后才返回的探测不算数。人工排空/恢复永不关闭区间。
- **重启**：网关启动时把上次运行遗留的 `open` 区间标记为 `interrupted`
  （监测中断），恢复时刻未知；启动探测只影响新一轮状态，绝不对旧区间
  倒填恢复时刻。
- **一致性**：区间的内存状态变更与落库在同一把条件变量锁内串行完成，
  并发请求与并发探测不会产生重复区间或倒序恢复。落库失败时：
  故障节点保持不健康（不放行），恢复确认不生效（不宣称已恢复），
  待写库恢复后由下一次成功探测补录/关闭区间。
- **故障注入演练**：设置 `INTERVAL_STORE_FAIL_WRITES_FILE=/path/to/flag`
  后，只要该文件存在，所有区间写操作即失败，可用于验证上述一致性语义。

## 自测

```bash
# 无需 docker；需本地具备 fastapi/uvicorn/httpx（可 pip install 到独立目录）
python3 tests/self_test.py
# 故障区间状态机单元自测（确定性交错：故障前启动的探测不得结束区间等）
python3 tests/test_intervals_unit.py
```

端到端覆盖 21 组场景、103 项断言，含两个关键交错（**探测成功不解除排空**、
**恢复不绕过故障探测**）、在途慢请求排空、多并发在途排空、503/504/502、
超时不虚报、在途计数准确、405、/docs，以及故障区间全链路：**开启/去重/
恢复、排空不算恢复、重启监测中断不倒填、落库失败不放行不宣称恢复、并发
不重复不倒序、连接失败开启区间**。单元自测 23 项断言，确定性覆盖
「故障前启动的探测不得结束区间」「关闭不存在区间报错」等交错分支。
