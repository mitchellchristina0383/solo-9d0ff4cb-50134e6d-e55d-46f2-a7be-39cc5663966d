#!/usr/bin/env python3
"""故障区间核心规则的单元级自测（不起 HTTP 服务，直接驱动 Gateway 内部状态机）。

覆盖 HTTP 端到端难以稳定复现的确定性交错：
  1. 故障发生「之前」启动的探测成功 -> 不得结束区间、不得置健康
  2. 故障发生「之后」启动的探测成功 -> 结束区间并置健康
  3. 重复失败不新增区间
  4. 重启后旧 open 区间标记监测中断，且不倒填 recovered_at
  5. 落库失败一致性：开启写失败不放行；恢复写失败不宣称恢复；写恢复后补录

运行：PYTHONPATH=/tmp/pylibs python3 tests/test_intervals_unit.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile

# 必须在 import 网关应用前设置环境（模块导入时读取）
_DB_DIR = tempfile.mkdtemp(prefix="gw-intervals-unit-")
os.environ["UPSTREAMS"] = "node1=http://127.0.0.1:1,node2=http://127.0.0.1:2"
os.environ["INTERVAL_DB_PATH"] = os.path.join(_DB_DIR, "unit.db")

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "gateway"))

import app as gw_app  # noqa: E402

passed = 0
failed = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global passed, failed
    if cond:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} {detail}")


def new_gateway(db_name: str) -> gw_app.Gateway:
    store = gw_app.IntervalStore(os.path.join(_DB_DIR, db_name))
    store.open_store()
    return gw_app.Gateway([gw_app.NodeState("n1", "http://127.0.0.1:1")], store)


def test_stale_probe_cannot_close() -> None:
    print("== 1. 故障前启动的探测不得结束区间 ==")
    g = new_gateway("t1.db")
    node = g.nodes[0]

    async def run():
        async with g.cond:
            await g._record_failure_locked(node, "request_5xx", "HTTP 500", 100.0)
        check("故障后区间开启", node.failure_episode and node.failed_since == 100.0)
        async with g.cond:
            ok = await g._record_recovery_locked(node, 99.0, 105.0, "HTTP 200")
        check("故障前启动的探测不允许置健康", ok is False)
        check("区间保持开启", node.failure_episode is True)
        async with g.cond:
            ok = await g._record_recovery_locked(node, 101.0, 106.0, "HTTP 200")
        check("故障后启动的探测允许置健康", ok is True)
        check("区间已结束", node.failure_episode is False)

    asyncio.run(run())
    rows = g.intervals.list_intervals("n1")
    check("库中仅 1 个区间且 recovered", len(rows) == 1 and rows[0]["state"] == "recovered",
          str(rows))
    check("恢复探测启动时刻为 101（非倒填 99）",
          rows and rows[0]["recovery_probe_started_at"] == 101.0, str(rows))
    g.intervals.close()


def test_repeated_failure_no_duplicate() -> None:
    print("== 2. 重复失败不新增区间 ==")
    g = new_gateway("t2.db")
    node = g.nodes[0]

    async def run():
        async with g.cond:
            await g._record_failure_locked(node, "probe_failure", "HTTP 503", 100.0)
            await g._record_failure_locked(node, "request_timeout", "timeout", 101.0)
            await g._record_failure_locked(node, "request_5xx", "HTTP 500", 102.0)

    asyncio.run(run())
    rows = g.intervals.list_intervals("n1")
    check("三次失败只开启 1 个区间", len(rows) == 1, str(rows))
    check("区间开启时刻为首次失败", rows and rows[0]["opened_at"] == 100.0, str(rows))
    check("区间原因记录首次失败", rows and rows[0]["cause"] == "probe_failure", str(rows))
    g.intervals.close()


def test_restart_marks_interrupted() -> None:
    print("== 3. 重启标记监测中断、不倒填 ==")
    path = os.path.join(_DB_DIR, "t3.db")
    g1 = new_gateway("t3.db")
    node = g1.nodes[0]

    async def run():
        async with g1.cond:
            await g1._record_failure_locked(node, "request_connect_error", "refused", 100.0)

    asyncio.run(run())
    g1.intervals.close()

    # 模拟重启：新 Gateway 实例打开同一库，启动时标记监测中断
    store2 = gw_app.IntervalStore(path)
    store2.open_store()
    marked = store2.mark_interrupted_on_startup(200.0)
    check("1 个旧区间被标记监测中断", marked == 1, str(marked))
    rows = store2.list_intervals("n1")
    check("旧区间 state=interrupted", rows[0]["state"] == "interrupted", str(rows))
    check("旧区间无 recovered_at（不倒填）", rows[0]["recovered_at"] is None, str(rows))
    check("旧区间记录 interrupted_at", rows[0]["interrupted_at"] == 200.0, str(rows))
    store2.close()


def test_write_failure_consistency() -> None:
    print("== 4. 落库失败一致性 ==")
    g = new_gateway("t4.db")
    node = g.nodes[0]

    def boom(*_a):
        raise gw_app.IntervalStoreError("模拟写失败")

    async def run():
        real_open = g.intervals.open_interval
        real_insert = g.intervals.insert_recovered_interval
        # 开启落库失败：节点仍进入故障态（不放行），区间未记录
        g.intervals.open_interval = boom
        async with g.cond:
            await g._record_failure_locked(node, "request_5xx", "HTTP 500", 100.0)
        check("开启写失败但故障态生效", node.failure_episode is True)
        check("开启写失败标记未记录", node.interval_unrecorded is True)
        check("库中无区间", len(g.intervals.list_intervals("n1")) == 0)
        # 恢复落库也失败：不宣称恢复
        g.intervals.insert_recovered_interval = boom
        async with g.cond:
            ok = await g._record_recovery_locked(node, 101.0, 105.0, "HTTP 200")
        check("恢复写失败不宣称恢复", ok is False and node.failure_episode is True)
        # 写库恢复：补录完整区间并允许置健康
        g.intervals.insert_recovered_interval = real_insert
        g.intervals.open_interval = real_open
        async with g.cond:
            ok = await g._record_recovery_locked(node, 106.0, 107.0, "HTTP 200")
        check("写库恢复后允许置健康", ok is True and node.failure_episode is False)

    asyncio.run(run())
    rows = g.intervals.list_intervals("n1")
    check("补录 1 个完整区间", len(rows) == 1, str(rows))
    check("补录区间 opened_at 为真实故障时刻 100",
          rows and rows[0]["opened_at"] == 100.0, str(rows))
    check("补录区间 recovered_at=107 且 state=recovered",
          rows and rows[0]["recovered_at"] == 107.0 and rows[0]["state"] == "recovered",
          str(rows))
    g.intervals.close()


def test_close_nonexistent_interval_raises() -> None:
    print("== 5. 关闭不存在的区间报错（不静默成功） ==")
    g = new_gateway("t5.db")
    try:
        g.intervals.close_interval(999, 1.0, 1.0, "x")
        check("关闭不存在区间应抛错", False)
    except gw_app.IntervalStoreError:
        check("关闭不存在区间应抛错", True)
    g.intervals.close()


def main() -> int:
    test_stale_probe_cannot_close()
    test_repeated_failure_no_duplicate()
    test_restart_marks_interrupted()
    test_write_failure_consistency()
    test_close_nonexistent_interval_raises()
    print(f"\n==== 单元自测: {passed} passed, {failed} failed ====")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
