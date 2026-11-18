"""HTTP 入口网关（FastAPI + httpx）。

职责：
  * 仅转发 GET 请求到上游，按轮询（round-robin）选择「健康且未排空」的节点；
  * 周期性主动探测上游，探测结论只影响健康标志，绝不改动人工排空标志；
  * 提供排空 / 恢复 / 状态管理 API；所有节点都不可接流时返回 503；
  * 排空期间新请求不再进入该节点，已在途的请求继续等待原响应返回；
  * 排空 API 阻塞到在途请求全部结束后才报告 drained=true；
  * 超时 / 连接错误 / 5xx 不虚报成功，并同步刷新健康状态，在途计数靠 try/finally 保证准确；
  * 按节点记录故障区间（SQLite 持久化），供值班交接查询：
      - 转发遇 5xx / 超时 / 连接失败、健康探测失败时开启区间，重复失败不新增；
      - 仅「故障发生之后启动」的成功探测可结束区间；人工排空不算恢复；
      - 网关重启后，未确认结束的旧区间标记为「监测中断」，启动探测不倒填恢复时刻；
      - 状态变化与记录一致：区间落库失败时不放行故障节点、不宣称其已恢复。
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, Response

# ----------------------------- 配置 -----------------------------

UPSTREAMS_RAW = os.environ.get(
    "UPSTREAMS",
    "node1=http://upstream1:8000,node2=http://upstream2:8000",
)
PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "2.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "1.5"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "15.0"))
DRAIN_DEFAULT_TIMEOUT = float(os.environ.get("DRAIN_DEFAULT_TIMEOUT", "30.0"))
# 故障区间 SQLite 留存路径
INTERVAL_DB_PATH = os.environ.get(
    "INTERVAL_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "intervals.db"),
)
# 故障注入（演练/自测用）：该环境变量指向的文件存在时，所有区间写操作失败，
# 用于验证「写入失败时不放行故障节点、不宣称恢复」的一致性语义。
INTERVAL_STORE_FAIL_WRITES_FILE = os.environ.get("INTERVAL_STORE_FAIL_WRITES_FILE", "")

# 透传响应时需要剥离的逐跳（hop-by-hop）头
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-encoding",
}

_STATE_NOTES = {
    "open": "故障进行中，尚未被成功探测确认恢复",
    "recovered": "已被故障发生之后启动的成功探测确认恢复",
    "interrupted": "监测中断：网关重启前该区间未确认结束，恢复时刻未知（启动探测不倒填）",
}


def _parse_upstreams(raw: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, url = item.partition("=")
        if not _:
            raise ValueError(f"非法 UPSTREAMS 条目: {item!r}，应为 name=url")
        out.append((name.strip(), url.strip().rstrip("/")))
    if not out:
        raise ValueError("至少需要配置一个上游节点")
    return out


def _iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


# ----------------------------- 故障区间存储 -----------------------------

class IntervalStoreError(Exception):
    """区间记录读写失败。"""


class IntervalStore:
    """故障区间的 SQLite 持久化。

    线程安全（内部分 threading.Lock 保护单连接）；调用方用 asyncio.to_thread
    避免阻塞事件循环。所有写操作失败时抛 IntervalStoreError，由调用方决定
    状态侧如何保守处理（不放行、不宣称恢复）。
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.Lock()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS failure_intervals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node TEXT NOT NULL,
                opened_at REAL NOT NULL,
                cause TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT 'open',
                recovered_at REAL,
                recovery_probe_started_at REAL,
                recovery_detail TEXT,
                interrupted_at REAL,
                recorded_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_failure_intervals_node "
            "ON failure_intervals(node, id)"
        )
        conn.commit()
        return conn

    def _ensure(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = self._connect()
        return self._conn

    @staticmethod
    def _check_fault_injection() -> None:
        if INTERVAL_STORE_FAIL_WRITES_FILE and os.path.exists(
            INTERVAL_STORE_FAIL_WRITES_FILE
        ):
            raise IntervalStoreError(
                f"模拟写入失败（故障注入文件存在: {INTERVAL_STORE_FAIL_WRITES_FILE}）"
            )

    def open_store(self) -> None:
        """启动时打开库；失败仅记录日志，后续每次写入会重试连接。"""
        with self._lock:
            try:
                self._ensure()
            except Exception as exc:
                print(f"[intervals] 打开区间库失败（写入时将重试）: {exc!r}")

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def mark_interrupted_on_startup(self, now: float) -> int:
        """把上次运行遗留的未结束区间标记为监测中断。返回标记条数。"""
        with self._lock:
            conn = self._ensure()
            cur = conn.execute(
                "UPDATE failure_intervals SET state='interrupted', interrupted_at=? "
                "WHERE state='open'",
                (now,),
            )
            conn.commit()
            return cur.rowcount

    def open_interval(self, node: str, opened_at: float, cause: str, detail: str) -> int:
        """开启一个故障区间，返回区间 id。"""
        with self._lock:
            self._check_fault_injection()
            conn = self._ensure()
            cur = conn.execute(
                "INSERT INTO failure_intervals "
                "(node, opened_at, cause, detail, state, recorded_at) "
                "VALUES (?, ?, ?, ?, 'open', ?)",
                (node, opened_at, cause, detail, time.time()),
            )
            conn.commit()
            return int(cur.lastrowid)

    def close_interval(
        self, interval_id: int, recovered_at: float, probe_started_at: float, detail: str
    ) -> None:
        """确认恢复：关闭指定区间。区间不存在或已结束时抛错（不静默成功）。"""
        with self._lock:
            self._check_fault_injection()
            conn = self._ensure()
            cur = conn.execute(
                "UPDATE failure_intervals SET state='recovered', recovered_at=?, "
                "recovery_probe_started_at=?, recovery_detail=? "
                "WHERE id=? AND state='open'",
                (recovered_at, probe_started_at, detail, interval_id),
            )
            conn.commit()
            if cur.rowcount != 1:
                raise IntervalStoreError(f"区间 {interval_id} 不存在或已结束")

    def insert_recovered_interval(
        self,
        node: str,
        opened_at: float,
        recovered_at: float,
        probe_started_at: float,
        cause: str,
        detail: str,
    ) -> int:
        """补录完整区间：故障发生时落库失败，恢复确认时一次性写入。

        opened_at 为真实故障发生时刻（内存中保留），recovered_at 为本次探测
        确认时刻；recorded_at 记录实际落库时刻以便审计。
        """
        with self._lock:
            self._check_fault_injection()
            conn = self._ensure()
            cur = conn.execute(
                "INSERT INTO failure_intervals "
                "(node, opened_at, cause, detail, state, recovered_at, "
                " recovery_probe_started_at, recovery_detail, recorded_at) "
                "VALUES (?, ?, ?, ?, 'recovered', ?, ?, ?, ?)",
                (
                    node,
                    opened_at,
                    cause,
                    detail,
                    recovered_at,
                    probe_started_at,
                    "补录：故障发生时落库失败；" + detail,
                    time.time(),
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    def list_intervals(
        self,
        node: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 200,
    ) -> list[dict]:
        with self._lock:
            conn = self._ensure()
            sql = "SELECT * FROM failure_intervals"
            conds: list[str] = []
            args: list[object] = []
            if node is not None:
                conds.append("node = ?")
                args.append(node)
            if state is not None:
                conds.append("state = ?")
                args.append(state)
            if conds:
                sql += " WHERE " + " AND ".join(conds)
            sql += " ORDER BY id DESC LIMIT ?"
            args.append(limit)
            rows = conn.execute(sql, args).fetchall()
            return [dict(r) for r in rows]


# ----------------------------- 节点状态 -----------------------------

@dataclass
class NodeState:
    name: str
    base_url: str
    # 健康标志：只由探测任务（以及请求失败时的降级）写入
    healthy: bool = False
    # 排空标志：只由人工 drain/resume 写入，探测永不触碰
    draining: bool = False
    inflight: int = 0
    rr_hits: int = 0
    success_count: int = 0
    failure_count: int = 0
    last_probe_at: Optional[float] = None
    last_probe_ok: Optional[bool] = None
    last_probe_detail: str = "尚未探测"
    last_change_at: float = field(default_factory=time.time)
    # ---- 故障区间的内存索引（权威记录见 SQLite）----
    # 当前是否处于未恢复的故障区间中
    failure_episode: bool = False
    # 本次故障发生时刻（区间开启时刻）
    failed_since: Optional[float] = None
    # DB 中未结束区间的 id；None 表示开启落库失败（区间未记录）
    open_interval_id: Optional[int] = None
    # 开启落库是否失败（失败则恢复时需补录完整区间）
    interval_unrecorded: bool = False
    # 开启区间时的原因与细节（补录时沿用）
    failure_cause: str = ""
    failure_detail: str = ""

    def can_serve(self) -> bool:
        return self.healthy and not self.draining

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "base_url": self.base_url,
            "healthy": self.healthy,
            "draining": self.draining,
            "eligible": self.can_serve(),
            "inflight": self.inflight,
            "rr_hits": self.rr_hits,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "last_probe_at": self.last_probe_at,
            "last_probe_ok": self.last_probe_ok,
            "last_probe_detail": self.last_probe_detail,
            "failure_episode": self.failure_episode,
            "failed_since": self.failed_since,
            "open_interval_id": self.open_interval_id,
        }


class Gateway:
    def __init__(self, nodes: list[NodeState], intervals: IntervalStore) -> None:
        self.nodes = nodes
        self._by_name = {n.name: n for n in nodes}
        self.intervals = intervals
        # 一把条件变量保护所有节点状态；请求结束 / 排空等待都在其上通知。
        # 故障区间的内存状态变更与落库也都在该锁内串行完成，
        # 因此并发请求与探测不会产生重复区间或倒序恢复。
        self.cond = asyncio.Condition()
        self._rr = 0
        self.client: Optional[httpx.AsyncClient] = None
        self._probe_task: Optional[asyncio.Task] = None

    def get(self, name: str) -> Optional[NodeState]:
        return self._by_name.get(name)

    async def pick_node(self) -> Optional[NodeState]:
        """轮询选取健康且未排空的节点，并立即登记在途计数。"""
        async with self.cond:
            n = len(self.nodes)
            for offset in range(n):
                idx = (self._rr + offset) % n
                node = self.nodes[idx]
                if node.can_serve():
                    self._rr = (idx + 1) % n
                    node.inflight += 1
                    node.rr_hits += 1
                    return node
            return None

    async def release_node(self, node: NodeState, ok: bool) -> None:
        async with self.cond:
            node.inflight -= 1
            if ok:
                node.success_count += 1
            else:
                node.failure_count += 1
            # 在途归零可能让 drain 等待者满足条件
            self.cond.notify_all()

    # ---- 故障区间：开启与恢复（均须在 self.cond 锁内调用）----

    async def _record_failure_locked(
        self, node: NodeState, cause: str, detail: str, at: float
    ) -> None:
        """开启故障区间；重复失败不新增。

        落库失败时区间在内存中照常开启（节点保持不健康，绝不放行故障节点），
        并标记 interval_unrecorded，待恢复确认时补录完整区间。
        """
        if node.failure_episode:
            return  # 重复失败不新增区间
        node.failure_episode = True
        node.failed_since = at
        node.open_interval_id = None
        node.interval_unrecorded = False
        node.failure_cause = cause
        node.failure_detail = detail
        try:
            node.open_interval_id = await asyncio.to_thread(
                self.intervals.open_interval, node.name, at, cause, detail
            )
        except Exception as exc:
            node.interval_unrecorded = True
            print(f"[intervals] 故障区间开启落库失败 node={node.name}: {exc!r}")

    async def _record_recovery_locked(
        self, node: NodeState, probe_started_at: float, at: float, detail: str
    ) -> bool:
        """成功探测尝试结束故障区间，返回是否允许置为健康。

        规则：
          * 仅「故障发生之后启动」的探测可结束区间（probe_started_at >= failed_since）；
          * 落库（关闭或补录）失败时不宣称恢复，节点保持不健康，留待下次探测重试。
        """
        if not node.failure_episode:
            return True  # 无故障区间，成功探测直接置健康
        if node.failed_since is not None and probe_started_at < node.failed_since:
            # 该探测启动早于故障发生，其成功不能证明故障后已恢复
            return False
        try:
            if node.open_interval_id is not None:
                await asyncio.to_thread(
                    self.intervals.close_interval,
                    node.open_interval_id,
                    at,
                    probe_started_at,
                    detail,
                )
            elif node.interval_unrecorded:
                await asyncio.to_thread(
                    self.intervals.insert_recovered_interval,
                    node.name,
                    node.failed_since,
                    at,
                    probe_started_at,
                    node.failure_cause,
                    node.failure_detail,
                )
        except Exception as exc:
            print(f"[intervals] 故障区间结束落库失败 node={node.name}: {exc!r}")
            return False  # 写入失败，不能宣称已恢复
        node.failure_episode = False
        node.failed_since = None
        node.open_interval_id = None
        node.interval_unrecorded = False
        node.failure_cause = ""
        node.failure_detail = ""
        return True

    async def mark_unhealthy(self, node: NodeState, cause: str, detail: str) -> None:
        """请求失败时的即时降级；只改健康标志，不动 draining，并开启故障区间。"""
        async with self.cond:
            now = time.time()
            node.healthy = False
            node.last_probe_ok = False
            node.last_probe_detail = f"请求失败降级: {detail}"
            node.last_probe_at = now
            node.last_change_at = now
            await self._record_failure_locked(node, cause, detail, now)

    async def probe_one(self, node: NodeState) -> None:
        """对单个节点做主动探测。成功只置 healthy=True，绝不解除 draining。"""
        assert self.client is not None
        started_at = time.time()  # 探测启动时刻：只有故障后启动的探测才能结束区间
        try:
            resp = await self.client.get(
                f"{node.base_url}/", timeout=PROBE_TIMEOUT
            )
            ok = resp.status_code < 500
            detail = f"HTTP {resp.status_code}"
        except httpx.TimeoutException as exc:
            ok, detail = False, f"探测超时: {type(exc).__name__}"
        except httpx.RequestError as exc:
            ok, detail = False, f"探测连接错误: {type(exc).__name__}: {str(exc)[:100]}"

        now = time.time()
        async with self.cond:
            if ok:
                # 仅故障后启动的成功探测可结束区间；落库失败则不宣称恢复
                node.healthy = await self._record_recovery_locked(
                    node, started_at, now, detail
                )
            else:
                node.healthy = False
                await self._record_failure_locked(node, "probe_failure", detail, now)
            # 注意：这里刻意不修改 node.draining
            node.last_probe_at = now
            node.last_probe_ok = ok
            node.last_probe_detail = detail
            node.last_change_at = now

    async def probe_all(self) -> None:
        await asyncio.gather(*(self.probe_one(n) for n in self.nodes))

    async def _probe_loop(self) -> None:
        while True:
            try:
                await self.probe_all()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 探测循环本身不能崩
                print(f"[probe-loop] unexpected error: {exc!r}")
            await asyncio.sleep(PROBE_INTERVAL)

    async def drain(
        self, node: NodeState, timeout: Optional[float]
    ) -> tuple[bool, int]:
        """置排空标志并等待在途清零。返回 (是否排空完成, 剩余在途数)。"""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + max(timeout, 0.0)
        async with self.cond:
            node.draining = True
            node.last_change_at = time.time()
            while node.inflight > 0:
                if deadline is None:
                    await self.cond.wait()
                    continue
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return False, node.inflight
                # asyncio.Condition.wait 不支持 timeout，用 wait_for 限时等待
                try:
                    await asyncio.wait_for(self.cond.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass  # 回到循环重新计算剩余时间 / 在途数
            return True, 0

    async def resume(self, node: NodeState) -> None:
        """恢复接流：先清排空并保守置为不健康，再立即探测，由探测决定能否接流。"""
        async with self.cond:
            node.draining = False
            # 恢复不绕过故障探测：探测结论出来前一律不接流，杜绝状态窗口
            node.healthy = False
            node.last_probe_ok = None
            node.last_probe_detail = "恢复接流，等待首次探测确认"
            node.last_change_at = time.time()
        await self.probe_one(node)


# ----------------------------- 应用生命周期 -----------------------------

gateway = Gateway(
    [NodeState(name, url) for name, url in _parse_upstreams(UPSTREAMS_RAW)],
    IntervalStore(INTERVAL_DB_PATH),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    gateway.intervals.open_store()
    try:
        # 重启语义：上次运行未确认结束的区间标记为监测中断；
        # 之后的启动探测只影响新一轮状态，绝不对旧区间倒填恢复时刻。
        interrupted = await asyncio.to_thread(
            gateway.intervals.mark_interrupted_on_startup, time.time()
        )
        if interrupted:
            print(f"[intervals] {interrupted} 个未确认结束的旧故障区间已标记为监测中断")
    except Exception as exc:
        print(f"[intervals] 启动时标记监测中断失败: {exc!r}")
    async with httpx.AsyncClient() as client:
        gateway.client = client
        # 启动先做一轮探测，避免把流量打到未知状态的节点
        await gateway.probe_all()
        gateway._probe_task = asyncio.create_task(gateway._probe_loop())
        try:
            yield
        finally:
            if gateway._probe_task:
                gateway._probe_task.cancel()
                try:
                    await gateway._probe_task
                except asyncio.CancelledError:
                    pass
    gateway.intervals.close()


app = FastAPI(
    title="http-entry-gateway",
    version="1.1.0",
    description=(
        "仅转发 GET 的入口网关：轮询健康且未排空的上游，支持排空/恢复/状态管理，"
        "无可接流节点时返回 503。按节点记录故障区间（SQLite 持久化）："
        "转发 5xx/超时/连接失败与探测失败开启区间，故障后启动的成功探测结束区间，"
        "重启后未确认结束的旧区间标记为监测中断。"
    ),
    lifespan=lifespan,
)


# ----------------------------- 转发接口 -----------------------------

@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": "http-entry-gateway",
        "proxy": "GET /proxy/{path} 仅支持 GET",
        "admin": [
            "GET  /admin/status",
            "POST /admin/nodes/{name}/drain?timeout=",
            "POST /admin/nodes/{name}/resume",
            "POST /admin/probe",
            "GET  /admin/intervals?node=&state=",
            "GET  /admin/nodes/{name}/intervals",
        ],
    }


@app.get("/proxy/{path:path}", tags=["proxy"])
async def proxy(path: str, request: Request) -> Response:
    node = await gateway.pick_node()
    if node is None:
        return JSONResponse(
            status_code=503,
            content={
                "error": "no_eligible_upstream",
                "detail": "当前没有健康且未排空的上游节点可接流",
            },
        )

    target_url = f"{node.base_url}/{path}"
    ok = False
    try:
        try:
            upstream_resp = await gateway.client.request(
                "GET",
                target_url,
                params=request.query_params,
                timeout=REQUEST_TIMEOUT,
            )
        except httpx.TimeoutException as exc:
            await gateway.mark_unhealthy(
                node, "request_timeout", f"请求超时 {type(exc).__name__}"
            )
            return JSONResponse(
                status_code=504,
                content={"error": "upstream_timeout", "node": node.name},
            )
        except httpx.RequestError as exc:
            await gateway.mark_unhealthy(
                node, "request_connect_error", f"{type(exc).__name__}: {str(exc)[:100]}"
            )
            return JSONResponse(
                status_code=502,
                content={"error": "upstream_unreachable", "node": node.name},
            )

        # 5xx 视为上游故障：如实透传状态码并降级，绝不虚报成功
        if upstream_resp.status_code >= 500:
            await gateway.mark_unhealthy(
                node, "request_5xx", f"上游返回 HTTP {upstream_resp.status_code}"
            )

        ok = upstream_resp.status_code < 500
        headers = {
            k: v
            for k, v in upstream_resp.headers.items()
            if k.lower() not in _HOP_BY_HOP
        }
        headers["X-Gateway"] = "http-entry-gateway"
        headers["X-Upstream-Node"] = node.name
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=headers,
            media_type=upstream_resp.headers.get("content-type"),
        )
    finally:
        # 无论成功、失败还是取消，在途计数都必须准确归还
        await gateway.release_node(node, ok)


# 非 GET 方法访问转发路径：FastAPI 自动返回 405 Method Not Allowed


# ----------------------------- 管理接口 -----------------------------

@app.get("/admin/status", tags=["admin"])
async def admin_status() -> dict:
    async with gateway.cond:
        return {
            "probe_interval": PROBE_INTERVAL,
            "request_timeout": REQUEST_TIMEOUT,
            "nodes": [n.snapshot() for n in gateway.nodes],
            "eligible_nodes": [n.name for n in gateway.nodes if n.can_serve()],
        }


@app.post("/admin/nodes/{name}/drain", tags=["admin"])
async def admin_drain(name: str, timeout: Optional[float] = DRAIN_DEFAULT_TIMEOUT):
    """排空节点：立即停止分配新请求，并阻塞等待在途请求全部结束。

    只有在途数归零才返回 drained=true；超时仍有在途请求时返回 202，
    drained=false 且节点继续保持排空状态。排空是人工运维动作，
    不会被记为故障恢复，也不影响故障区间。
    """
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    drained, inflight = await gateway.drain(node, timeout)
    body = {
        "node": name,
        "draining": True,
        "drained": drained,
        "inflight": inflight,
        "detail": "在途请求已全部结束" if drained else "等待在途请求结束超时，节点仍处于排空中",
    }
    return JSONResponse(status_code=200 if drained else 202, content=body)


@app.post("/admin/nodes/{name}/resume", tags=["admin"])
async def admin_resume(name: str):
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    await gateway.resume(node)
    snap = node.snapshot()
    return {
        "node": name,
        "draining": False,
        "healthy": snap["healthy"],
        "eligible": snap["eligible"],
        "last_probe_detail": snap["last_probe_detail"],
        "detail": "已解除排空；是否可接流以健康探测结论为准",
    }


@app.post("/admin/probe", tags=["admin"])
async def admin_probe() -> dict:
    """手动触发一轮探测（探测不会改变任何节点的排空标志）。"""
    await gateway.probe_all()
    async with gateway.cond:
        return {"nodes": [n.snapshot() for n in gateway.nodes]}


# ----------------------------- 故障区间查询 -----------------------------

def _interval_view(row: dict) -> dict:
    state = row["state"]
    end = row["recovered_at"] if state == "recovered" else row["interrupted_at"]
    return {
        "id": row["id"],
        "node": row["node"],
        "state": state,
        "state_note": _STATE_NOTES.get(state, ""),
        "cause": row["cause"],
        "detail": row["detail"],
        "opened_at": row["opened_at"],
        "opened_at_iso": _iso(row["opened_at"]),
        "recovered_at": row["recovered_at"],
        "recovered_at_iso": _iso(row["recovered_at"]),
        "recovery_probe_started_at": row["recovery_probe_started_at"],
        "recovery_detail": row["recovery_detail"],
        "interrupted_at": row["interrupted_at"],
        "interrupted_at_iso": _iso(row["interrupted_at"]),
        "recorded_at": row["recorded_at"],
        "recorded_at_iso": _iso(row["recorded_at"]),
        "duration_seconds": round(end - row["opened_at"], 3) if end is not None else None,
    }


async def _query_intervals(
    node: Optional[str], state: Optional[str], limit: int
) -> JSONResponse | dict:
    try:
        rows = await asyncio.to_thread(
            gateway.intervals.list_intervals, node, state, limit
        )
    except Exception as exc:
        return JSONResponse(
            status_code=503,
            content={"error": "interval_store_unavailable", "detail": str(exc)},
        )
    return {"count": len(rows), "intervals": [_interval_view(r) for r in rows]}


@app.get("/admin/intervals", tags=["intervals"])
async def admin_intervals(
    node: Optional[str] = Query(default=None, description="按节点名过滤，缺省返回全部节点"),
    state: Optional[str] = Query(
        default=None, pattern="^(open|recovered|interrupted)$",
        description="按区间状态过滤: open=进行中, recovered=已恢复, interrupted=监测中断",
    ),
    limit: int = Query(default=200, ge=1, le=1000),
):
    """查询故障区间（可按节点/状态过滤），供值班交接核对故障与恢复时刻。"""
    return await _query_intervals(node, state, limit)


@app.get("/admin/nodes/{name}/intervals", tags=["intervals"])
async def admin_node_intervals(
    name: str,
    state: Optional[str] = Query(
        default=None, pattern="^(open|recovered|interrupted)$",
        description="按区间状态过滤: open=进行中, recovered=已恢复, interrupted=监测中断",
    ),
    limit: int = Query(default=200, ge=1, le=1000),
):
    """按节点查询故障区间，并给出该节点当前是否处于故障区间中。"""
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    result = await _query_intervals(name, state, limit)
    if isinstance(result, JSONResponse):
        return result
    async with gateway.cond:
        result["node"] = name
        result["current_failure_episode"] = node.failure_episode
        result["failed_since"] = node.failed_since
        result["failed_since_iso"] = _iso(node.failed_since)
    return result
