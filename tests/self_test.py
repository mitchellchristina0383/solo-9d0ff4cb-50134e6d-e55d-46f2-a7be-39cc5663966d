#!/usr/bin/env python3
"""网关本地自测（不依赖 docker，使用 stdlib + 三个 uvicorn 子进程）。

覆盖关键场景：
  1.  轮询分发
  2.  状态 API
  3.  空闲节点排空 -> 立即 drained=true
  4.  排空后新请求不进入该节点
  5.  有在途慢请求时排空 -> 等在途结束才完成，且原响应可取回
  6.  全部不可接流 -> 503
  7.  上游 5xx -> 不虚报成功、节点降级、在途归零
  8.  探测成功不解除排空（交错核心）
  9.  恢复不绕过故障探测（交错核心）
  10. 仅 GET：POST 转发路径返回 405
  11. /docs 可打开
  12. 排空等待超时 -> 202，在途数准确
  13. 上游挂起 -> 网关请求超时 504，不虚报成功、在途归零
  14. 多并发在途请求排空
  15. 未知节点管理接口 404
  16. 故障区间：请求 5xx 开启区间，重复失败不新增，探测恢复关闭区间
  17. 故障区间：人工排空/恢复不算恢复，仅故障后启动的成功探测关闭区间
  18. 故障区间：重启后未确认结束的旧区间标记监测中断，启动探测不倒填
  19. 故障区间：落库失败时不放行故障节点、不宣称恢复；恢复后补录
  20. 故障区间：并发请求与探测不产生重复区间、不倒序恢复
  21. 故障区间：连接失败（connection refused）开启区间
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

PYLIBS = os.environ.get("PYLIBS", "/tmp/pylibs")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable

GW_PORT = 19080
U1_PORT = 19081
U2_PORT = 19082

# 故障区间库目录（每次自测独立，验证重启语义时跨进程复用同一文件）
DB_DIR = tempfile.mkdtemp(prefix="gw-intervals-")
FAIL_WRITES_FILE = os.path.join(DB_DIR, "fail_writes")

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


def http(method: str, url: str, timeout: float = 20.0, data: bytes | None = None):
    """返回 (status, headers, body_text)。4xx/5xx 不抛异常。"""
    req = urllib.request.Request(url, method=method, data=data)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read().decode()


def jget(url: str, timeout: float = 20.0):
    status, _, body = http("GET", url, timeout)
    return status, json.loads(body)


def wait_ready(url: str, timeout: float = 25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _, _ = http("GET", url, timeout=2)
            if status == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def spawn(module_dir: str, port: int, extra_env: dict | None = None):
    # -I 隔离模式会忽略 PYTHONPATH 且不含 cwd，故用 runpy 包装器显式注入依赖与应用目录
    launcher = (
        "import sys,runpy,os;"
        f"sys.path[:0]=[{PYLIBS!r}, os.getcwd()];"
        "sys.argv=['uvicorn']+sys.argv[1:];"
        "runpy.run_module('uvicorn', run_name='__main__')"
    )
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.update(extra_env or {})
    proc = subprocess.Popen(
        [PYTHON, "-I", "-c", launcher, "app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=module_dir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return proc


def kill(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        try:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=8)
        except Exception:
            proc.kill()


def node_status(node: str) -> dict:
    status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
    assert status == 200
    return next(n for n in body["nodes"] if n["name"] == node)


def set_mode(port: int, mode: str) -> None:
    status, _, body = http("POST", f"http://127.0.0.1:{port}/control/mode?mode={mode}")
    assert status == 200, body


def main() -> int:
    procs = []
    try:
        print("== 启动两个上游和网关 ==")
        u1 = spawn(os.path.join(ROOT, "upstream"), U1_PORT, {"NODE_ID": "node1"})
        u2 = spawn(os.path.join(ROOT, "upstream"), U2_PORT, {"NODE_ID": "node2"})
        procs = [u1, u2]
        assert wait_ready(f"http://127.0.0.1:{U1_PORT}/")
        assert wait_ready(f"http://127.0.0.1:{U2_PORT}/")

        gw_env = {
            "UPSTREAMS": f"node1=http://127.0.0.1:{U1_PORT},node2=http://127.0.0.1:{U2_PORT}",
            "PROBE_INTERVAL": "1",
            "PROBE_TIMEOUT": "1",
            "REQUEST_TIMEOUT": "15",
            "DRAIN_DEFAULT_TIMEOUT": "10",
            "INTERVAL_DB_PATH": os.path.join(DB_DIR, "gw0.db"),
        }
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs"), "网关启动失败"
        time.sleep(1.2)  # 等首轮探测结果稳定

        # 1. 轮询分发
        print("== 1. 轮询分发 ==")
        seen = []
        for _ in range(4):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            assert status == 200, body
            seen.append(body["node"])
        check("4 个请求在两节点间交替", seen == ["node1", "node2", "node1", "node2"], str(seen))
        st = node_status("node1")
        check("rr_hits 各 2 次", st["rr_hits"] == 2, str(st))
        check("在途计数已归零", st["inflight"] == 0, str(st))

        # 2. 状态 API 内容
        print("== 2. 状态 API ==")
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
        check("两节点均健康", all(n["healthy"] for n in body["nodes"]))
        check("两节点均未排空", all(not n["draining"] for n in body["nodes"]))
        check("eligible 含两节点", sorted(body["eligible_nodes"]) == ["node1", "node2"])

        # 3. 空闲节点排空立即完成
        print("== 3. 空闲节点排空 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=5")
        b = json.loads(body)
        check("排空返回 200", status == 200, body)
        check("drained=true", b.get("drained") is True, body)
        check("inflight=0", b.get("inflight") == 0, body)
        st = node_status("node1")
        check("状态显示 draining", st["draining"] is True and st["eligible"] is False)

        # 4. 排空后新请求不进入 node1
        print("== 4. 排空后新请求只去 node2 ==")
        nodes = set()
        for _ in range(3):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            nodes.add(body["node"])
        check("新请求全部落在 node2", nodes == {"node2"}, str(nodes))

        # 5. 在途慢请求 + 排空交错：排空等在途结束，原响应可取回
        print("== 5. 在途慢请求期间排空 node2 ==")
        result = {}

        def slow_call():
            result["status"], _, result["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3", timeout=20
            )

        t = threading.Thread(target=slow_call)
        t.start()
        # 等到请求确实进入 node2
        inflight_seen = 0
        deadline = time.time() + 5
        while time.time() < deadline:
            st2 = node_status("node2")
            inflight_seen = max(inflight_seen, st2["inflight"])
            if st2["inflight"] == 1:
                break
            time.sleep(0.1)
        check("慢请求已进入 node2 且 inflight=1", inflight_seen == 1, str(st2))

        drain_started = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=10"
        )
        drain_elapsed = time.time() - drain_started
        b = json.loads(body)
        t.join(timeout=20)
        check("排空请求被在途请求阻塞约 3s", drain_elapsed >= 2.5, f"{drain_elapsed:.2f}s")
        check("排空最终成功 drained=true", status == 200 and b.get("drained") is True, body)
        check("慢请求原响应成功取回 200", result.get("status") == 200, str(result))
        check("响应来自 node2 且内容完整",
              json.loads(result["body"]).get("node") == "node2"
              and json.loads(result["body"]).get("slept", 0) >= 2.5,
              result.get("body"))
        check("结束后两节点在途均为 0",
              all(node_status(n)["inflight"] == 0 for n in ("node1", "node2")))

        # 6. 两节点都排空 -> 503
        print("== 6. 无可用节点返回 503 ==")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("返回 503", status == 503, body)
        check("503 原因可解释", json.loads(body).get("error") == "no_eligible_upstream", body)

        # 7. 上游 5xx：不虚报成功 + 节点降级 + 在途归零
        print("== 7. 上游故障 (5xx) ==")
        # 先恢复 node1 以便发请求（node2 仍排空）
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        set_mode(U1_PORT, "error")
        time.sleep(1.3)  # 等周期性探测先发现故障
        st = node_status("node1")
        check("探测已将 node1 标记为不健康", st["healthy"] is False, str(st))
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("无健康节点时返回 503 而非虚假 200", status == 503, body)
        # 临时恢复健康标志位才能真正打到 5xx（模拟请求路径上的失败降级）
        set_mode(U1_PORT, "healthy")
        time.sleep(1.3)
        st = node_status("node1")
        check("探测恢复后 node1 健康", st["healthy"] is True, str(st))
        set_mode(U1_PORT, "error")  # 不等待探测，让请求先撞上 5xx
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("5xx 状态如实返回", status == 500, f"status={status} body={body}")
        time.sleep(0.3)
        st = node_status("node1")
        check("请求失败后节点即时降级", st["healthy"] is False, str(st))
        check("失败后在途计数仍归零", st["inflight"] == 0, str(st))

        # 8. 交错核心：探测成功不得解除排空
        print("== 8. 探测成功不解除排空 ==")
        set_mode(U1_PORT, "healthy")
        # node1 当前未排空，先排空，再让周期性探测持续成功
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        assert status == 200
        time.sleep(2.5)  # 跨越至少 2 轮探测，上游为 healthy，探测必然成功
        st = node_status("node1")
        check("多轮探测成功后排空标志仍在", st["draining"] is True, str(st))
        check("探测成功只更新健康位", st["healthy"] is True and st["eligible"] is False, str(st))

        # 9. 交错核心：恢复不绕过故障探测
        print("== 9. 恢复不绕过故障探测 ==")
        set_mode(U1_PORT, "error")
        time.sleep(1.3)  # 探测确认故障
        st_before = node_status("node1")
        check("恢复前 node1 不健康", st_before["healthy"] is False, str(st_before))
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        b = json.loads(body)
        check("resume 清除排空但节点仍不可接流",
              status == 200 and b["draining"] is False and b["eligible"] is False, body)
        st = node_status("node1")
        check("draining=False 且 healthy=False",
              st["draining"] is False and st["healthy"] is False, str(st))
        # 故障节点上请求不会被分配：node2 仍排空 -> 503
        status, _, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("故障节点不会被轮询选中 (503)", status == 503)
        # 上游恢复后经探测自动回到可接流
        set_mode(U1_PORT, "healthy")
        time.sleep(1.5)
        st = node_status("node1")
        check("探测恢复后节点重新 eligible", st["eligible"] is True, str(st))
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("流量恢复进入 node1", status == 200 and body["node"] == "node1")

        # 10. 仅 GET 转发
        print("== 10. 非 GET 方法被拒 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/proxy/echo", data=b"x")
        check("POST 转发路径返回 405", status == 405, f"status={status}")

        # 11. /docs 可打开
        print("== 11. /docs 与 openapi.json ==")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/docs")
        check("/docs 返回 200", status == 200 and "swagger" in body.lower())
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/openapi.json")
        paths = body["paths"]
        check("openapi 含管理与转发接口",
              "/admin/nodes/{name}/drain" in paths and "/proxy/{path}" in paths)
        check("openapi 含故障区间查询接口",
              "/admin/intervals" in paths and "/admin/nodes/{name}/intervals" in paths)

        # 12. 排空等待超时：202 + 在途准确
        print("== 12. 排空超时返回 202 ==")
        result12 = {}

        def slow_call12():
            result12["status"], _, result12["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=4", timeout=15
            )

        t12 = threading.Thread(target=slow_call12)
        t12.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] != 1:
            time.sleep(0.1)
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=1"
        )
        b = json.loads(body)
        check("短超时排空返回 202", status == 202, body)
        check("drained=false 且 inflight=1", b.get("drained") is False and b.get("inflight") == 1, body)
        t12.join(timeout=15)
        check("在途慢请求仍拿到完整 200", result12.get("status") == 200, str(result12))
        time.sleep(0.3)
        # 在途归零后再次排空（节点已在排空中）应立即完成
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        b = json.loads(body)
        check("在途归零后排空立即完成", status == 200 and b["drained"] is True, body)

        # 13. 请求超时：重启网关为短超时，上游 hang（探测与业务请求都会挂住）
        print("== 13. 上游挂起 -> 网关 504 超时 ==")
        kill(gw)
        procs.remove(gw)
        short_env = dict(gw_env)
        short_env["REQUEST_TIMEOUT"] = "1"
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, short_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        set_mode(U1_PORT, "healthy")
        # 等首轮探测把 node1 标记为健康可接流（网关重启后状态全清）
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        check("重启后 node1 经探测恢复可接流", node_status("node1")["eligible"] is True)
        # 切 hang 后立即发请求：赶在下一轮周期探测摘掉它之前验证网关自身的请求超时
        set_mode(U1_PORT, "hang")
        t0 = time.time()
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
        elapsed = time.time() - t0
        if status == 503:
            # 探测抢先把节点摘掉，则手动恢复后重试一次，确保覆盖请求超时路径
            set_mode(U1_PORT, "healthy")
            deadline = time.time() + 6
            while time.time() < deadline and not node_status("node1")["eligible"]:
                time.sleep(0.2)
            set_mode(U1_PORT, "hang")
            t0 = time.time()
            status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
            elapsed = time.time() - t0
        check("挂起请求在约 1s 内返回 504", status == 504 and elapsed < 4,
              f"status={status} elapsed={elapsed:.1f}s body={body}")
        check("未虚报成功", status != 200)
        time.sleep(0.3)
        st = node_status("node1")
        check("超时后在途计数准确归零", st["inflight"] == 0, str(st))
        check("超时后节点被标记不健康", st["healthy"] is False, str(st))
        # 请求超时（或并发的探测超时）应已开启故障区间
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/intervals?state=open")
        open_iv = body["intervals"]
        check("超时故障已开启区间",
              status == 200 and len(open_iv) >= 1
              and open_iv[0]["cause"] in ("request_timeout", "probe_failure"),
              str(open_iv[:1]))
        set_mode(U1_PORT, "healthy")

        # 14. 多个并发在途请求：排空须等全部结束，在途计数全程准确
        print("== 14. 多并发在途请求排空 ==")
        # 恢复正常请求超时并重启网关（清空场景 13 的短超时与节点状态）
        kill(gw)
        procs.remove(gw)
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        check("重启后 node1 可接流", node_status("node1")["eligible"] is True)
        # 先排空 node2，把流量集中到 node1
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        assert status == 200
        results14 = []

        def slow_call14(idx: int):
            s, _, b = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3", timeout=20)
            results14.append((idx, s, b))

        threads = [threading.Thread(target=slow_call14, args=(i,)) for i in range(4)]
        for th in threads:
            th.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] < 4:
            time.sleep(0.1)
        st = node_status("node1")
        check("4 个在途请求计数准确", st["inflight"] == 4, str(st))
        t0 = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=15"
        )
        elapsed = time.time() - t0
        b = json.loads(body)
        for th in threads:
            th.join(timeout=20)
        check("排空阻塞到全部 4 个在途请求结束", elapsed >= 2.5 and b.get("drained") is True,
              f"elapsed={elapsed:.1f}s body={body}")
        check("4 个慢请求全部取回完整 200",
              len(results14) == 4 and all(s == 200 for _, s, _ in results14),
              str(results14))
        check("全部结束后在途归零", node_status("node1")["inflight"] == 0)

        # 15. 未知节点管理接口返回 404
        print("== 15. 未知节点 404 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/drain")
        check("排空未知节点返回 404", status == 404, body)
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/resume")
        check("恢复未知节点返回 404", status == 404, body)
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/intervals")
        check("查询未知节点区间返回 404", status == 404, body)

        # ---------------------------------------------------------------
        # 故障区间场景（16-21）：换用「手动探测」网关，PROBE_INTERVAL 拉到 300s，
        # 探测只通过 POST /admin/probe 触发，消除周期探测带来的时序竞争。
        # ---------------------------------------------------------------
        print("== 重启网关：故障区间场景（手动探测模式） ==")
        kill(gw)
        procs.remove(gw)
        iv_env = dict(gw_env)
        iv_env["PROBE_INTERVAL"] = "300"
        iv_env["INTERVAL_DB_PATH"] = os.path.join(DB_DIR, "gw1.db")
        iv_env["INTERVAL_STORE_FAIL_WRITES_FILE"] = FAIL_WRITES_FILE
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, iv_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs"), "网关启动失败"
        set_mode(U1_PORT, "healthy")
        set_mode(U2_PORT, "healthy")
        # 启动探测已在 lifespan 完成，两节点应健康可接流
        st = node_status("node1")
        check("区间场景网关启动后 node1 健康", st["healthy"] is True, str(st))

        def intervals(node: str, state: str | None = None):
            url = f"http://127.0.0.1:{GW_PORT}/admin/nodes/{node}/intervals"
            if state:
                url += f"?state={state}"
            s, b = jget(url)
            assert s == 200, b
            return b

        def trigger_probe():
            s, _, b = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/probe", timeout=10)
            assert s == 200, b
            return json.loads(b)

        # 16. 请求 5xx 开启区间；重复失败不新增；故障后启动的成功探测关闭区间
        print("== 16. 故障区间：开启/去重/恢复 ==")
        base = intervals("node1")["count"]
        # 排空 node2，让请求只落在 node1
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        assert status == 200, body
        t_fail = time.time()
        set_mode(U1_PORT, "error")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("node1 返回 500 如实透传", status == 500, f"status={status} body={body}")
        iv = intervals("node1")
        check("5xx 后开启 1 个区间", iv["count"] == base + 1, str(iv))
        cur = iv["intervals"][0]
        check("区间状态 open 且原因为 request_5xx",
              cur["state"] == "open" and cur["cause"] == "request_5xx", str(cur))
        check("区间开启时刻不早于故障注入", cur["opened_at"] >= t_fail - 1, str(cur))
        check("当前处于故障区间中", iv["current_failure_episode"] is True, str(iv))
        # 重复失败（手动探测失败 + resume 触发的探测失败）不新增区间
        trigger_probe()
        trigger_probe()
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        iv = intervals("node1")
        check("重复失败不新增区间", iv["count"] == base + 1, str(iv["count"]))
        check("区间仍 open", iv["intervals"][0]["state"] == "open", str(iv["intervals"][0]))
        # 故障后启动的成功探测关闭区间
        set_mode(U1_PORT, "healthy")
        trigger_probe()
        st = node_status("node1")
        check("探测成功后节点恢复健康", st["healthy"] is True, str(st))
        iv = intervals("node1")
        rec = iv["intervals"][0]
        check("区间已 recovered", rec["state"] == "recovered", str(rec))
        check("恢复时刻 >= 开启时刻", rec["recovered_at"] >= rec["opened_at"], str(rec))
        check("恢复探测启动时刻 >= 开启时刻（故障后启动）",
              rec["recovery_probe_started_at"] >= rec["opened_at"], str(rec))
        check("不再处于故障区间", iv["current_failure_episode"] is False, str(iv))

        # 17. 人工排空/恢复不算故障恢复；仅故障后启动的成功探测关闭区间
        print("== 17. 故障区间：排空不算恢复 ==")
        set_mode(U1_PORT, "error")
        trigger_probe()  # 探测失败 -> 开启新区间
        iv = intervals("node1")
        check("探测失败开启第二个区间", iv["count"] == base + 2, str(iv["count"]))
        check("新区间原因为 probe_failure",
              iv["intervals"][0]["cause"] == "probe_failure", str(iv["intervals"][0]))
        # 排空 + 恢复（上游仍故障）：区间不得被这些人工动作关闭
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=5")
        check("故障中排空 node1 成功", status == 200, body)
        iv = intervals("node1")
        check("排空后区间仍 open（排空不算恢复）",
              iv["intervals"][0]["state"] == "open", str(iv["intervals"][0]))
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        iv = intervals("node1")
        check("resume（探测失败）后区间仍 open",
              iv["intervals"][0]["state"] == "open", str(iv["intervals"][0]))
        # 上游恢复，故障后启动的成功探测才关闭区间
        set_mode(U1_PORT, "healthy")
        trigger_probe()
        iv = intervals("node1")
        rec = iv["intervals"][0]
        check("探测成功后第二区间 recovered", rec["state"] == "recovered", str(rec))
        check("恢复探测启动时刻 >= 开启时刻",
              rec["recovery_probe_started_at"] >= rec["opened_at"], str(rec))

        # 18. 重启：未确认结束的旧区间标记监测中断，启动探测不倒填恢复时刻
        print("== 18. 故障区间：重启监测中断 ==")
        set_mode(U1_PORT, "error")
        trigger_probe()  # 开启第三个区间
        iv = intervals("node1")
        check("重启前开启第三个区间", iv["count"] == base + 3, str(iv["count"]))
        third_id = iv["intervals"][0]["id"]
        # 网关停机期间上游已恢复：若启动探测倒填，会把旧区间标成 recovered
        set_mode(U1_PORT, "healthy")
        kill(gw)
        procs.remove(gw)
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, iv_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs"), "网关重启失败"
        iv = intervals("node1")
        third = next(i for i in iv["intervals"] if i["id"] == third_id)
        check("旧区间标记为监测中断", third["state"] == "interrupted", str(third))
        check("监测中断区间无恢复时刻", third["recovered_at"] is None, str(third))
        check("监测中断区间有中断标记时刻", third["interrupted_at"] is not None, str(third))
        check("启动探测未新增区间（上游已恢复）", iv["count"] == base + 3, str(iv["count"]))
        st = node_status("node1")
        check("重启后启动探测确认 node1 健康", st["healthy"] is True, str(st))

        # 19. 落库失败一致性：不放行故障节点、不宣称恢复；恢复写可用后补录
        print("== 19. 故障区间：落库失败一致性 ==")
        # node2 已在场景 16 排空，重启后标志清零，重新排空以聚焦 node1
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        assert status == 200, body
        open(FAIL_WRITES_FILE, "w").close()  # 故障注入：区间写操作全部失败
        base19 = intervals("node1")["count"]
        t_fail19 = time.time()
        set_mode(U1_PORT, "error")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("写库失败时 5xx 仍如实返回", status == 500, f"status={status}")
        st = node_status("node1")
        check("区间开启落库失败但节点不放行（healthy=False）",
              st["healthy"] is False and st["failure_episode"] is True, str(st))
        check("落库失败未产生区间记录", intervals("node1")["count"] == base19)
        status, _, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("故障节点不被放行（503 而非再打故障节点）", status == 503, f"status={status}")
        # 上游恢复，但恢复落库仍失败：不得宣称已恢复
        set_mode(U1_PORT, "healthy")
        trigger_probe()
        st = node_status("node1")
        check("恢复落库失败时不宣称恢复（healthy 仍为 False）",
              st["healthy"] is False, str(st))
        status, _, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("未宣称恢复的节点仍不放行", status == 503, f"status={status}")
        # 写库恢复：下次成功探测补录完整区间并置健康
        os.remove(FAIL_WRITES_FILE)
        trigger_probe()
        st = node_status("node1")
        check("写库恢复后探测确认节点健康", st["healthy"] is True, str(st))
        iv = intervals("node1")
        check("补录生成完整区间", iv["count"] == base19 + 1, str(iv["count"]))
        rec = iv["intervals"][0]
        check("补录区间直接为 recovered", rec["state"] == "recovered", str(rec))
        check("补录区间开启时刻为真实故障时刻",
              abs(rec["opened_at"] - t_fail19) < 5, str(rec))
        check("补录区间恢复时刻 >= 开启时刻",
              rec["recovered_at"] >= rec["opened_at"], str(rec))

        # 20. 并发：并发失败请求 + 并发探测不产生重复区间、不倒序恢复
        print("== 20. 故障区间：并发不重复、不倒序 ==")
        base20 = intervals("node1")["count"]
        set_mode(U1_PORT, "error")
        results20 = []

        def flaky_call():
            s, _, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=10)
            results20.append(s)

        threads = [threading.Thread(target=flaky_call) for _ in range(8)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=15)
        check("并发请求均如实失败（500/503）",
              len(results20) == 8 and all(s in (500, 503) for s in results20), str(results20))
        # 并发手动探测（失败）与并发恢复探测
        def probe_call():
            trigger_probe()

        threads = [threading.Thread(target=probe_call) for _ in range(3)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=15)
        iv = intervals("node1")
        check("并发失败只开启 1 个区间", iv["count"] == base20 + 1, str(iv["count"]))
        set_mode(U1_PORT, "healthy")
        threads = [threading.Thread(target=probe_call) for _ in range(3)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=15)
        iv = intervals("node1")
        check("并发恢复探测不新增区间", iv["count"] == base20 + 1, str(iv["count"]))
        check("并发恢复后区间唯一 recovered",
              iv["intervals"][0]["state"] == "recovered", str(iv["intervals"][0]))
        # 全量区间时序校验：无倒序恢复、无重叠区间
        all_iv = list(reversed(iv["intervals"]))  # 按 id 升序
        ok_order = True
        prev_end = 0.0
        for row in all_iv:
            if row["recovered_at"] is not None and row["recovered_at"] < row["opened_at"]:
                ok_order = False
            if row["recovery_probe_started_at"] is not None \
                    and row["recovery_probe_started_at"] < row["opened_at"]:
                ok_order = False
            if row["opened_at"] < prev_end - 0.01:
                ok_order = False
            end = row["recovered_at"] or row["interrupted_at"] or row["opened_at"]
            prev_end = max(prev_end, end)
        check("全部区间无倒序恢复、无重叠", ok_order, str(all_iv))

        # 21. 连接失败开启区间
        print("== 21. 故障区间：连接失败 ==")
        base21 = intervals("node1")["count"]
        kill(u1)
        procs.remove(u1)
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("连接失败返回 502", status == 502, f"status={status} body={body}")
        iv = intervals("node1")
        check("连接失败开启区间", iv["count"] == base21 + 1, str(iv["count"]))
        check("区间原因为 request_connect_error",
              iv["intervals"][0]["cause"] == "request_connect_error", str(iv["intervals"][0]))
        # 上游回来，探测恢复
        u1 = spawn(os.path.join(ROOT, "upstream"), U1_PORT, {"NODE_ID": "node1"})
        procs.append(u1)
        assert wait_ready(f"http://127.0.0.1:{U1_PORT}/")
        trigger_probe()
        iv = intervals("node1")
        check("连接失败区间已恢复", iv["intervals"][0]["state"] == "recovered",
              str(iv["intervals"][0]))
        # 全局查询接口（不过滤节点）
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/intervals")
        check("全局区间查询可用", status == 200 and body["count"] >= 6, str(body.get("count")))
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/intervals?state=interrupted")
        check("按状态过滤监测中断区间", status == 200 and body["count"] == 1, str(body.get("count")))

        print(f"\n==== 结果: {passed} passed, {failed} failed ====")
        return 1 if failed else 0

    finally:
        for p in procs:
            kill(p)


if __name__ == "__main__":
    sys.exit(main())
