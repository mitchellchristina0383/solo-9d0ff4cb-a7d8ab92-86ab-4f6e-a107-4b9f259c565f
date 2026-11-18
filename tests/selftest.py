#!/usr/bin/env python3
"""关键情形端到端自测（仅依赖 Python 标准库）。

覆盖：
  S1  正常发送 → 已确认
  S2  同内容重试只记一次（两端均不重复）
  S3  同键不同内容 → 409 冲突（发送端登记与接收端直连均验证）
  S4  入库后丢回执 → 待核 → 重发去重 → 已确认
  S5  入库后丢回执 → 待核 → 核对补账 → 已确认
  S6  慢回执（发送端超时）→ 待核 → 核对 → 已确认
  S7  并发同内容提交 → 两端各仅一条
  S8  并发不同内容 → 仅一条，败者 409
  S9  接收端不可用 → 待核且不虚报成功（--lifecycle）
  S10 发送端重启（接收端停机中）→ 待核持久 → 恢复后凭回执补核（--lifecycle）
  S11 发送端重启（接收端正常）→ 启动自动核对凭回执结案（--lifecycle）
  R1  不存在的回执不能复核（404，且无复核记录）
  R2  复核正常提交：发送端核对后同时显示送达与复核状态
  R3  同回执同内容重试：幂等 200，复核只保留一条
  R4  同回执不同内容：409 冲突，原复核不被篡改
  R5  复核后再发送/再核对交错：复核状态不倒退、不孤立
  R6  发送端本地写入失败（write_fail）：500 且原状态不变，恢复后可继续
  R7  接收端复核写入失败（review_write_fail）：无复核落库，重试成功
  R8  并发复核提交（同内容/不同内容）：仅一条，败者 409
  R9  并发交错：发送与核对竞争同一记录，无孤立/倒退终态
  R10 复核状态随发送端重启持久（--lifecycle）

用法：
  python3 tests/selftest.py [--lifecycle]
环境变量：
  SENDER_URL / RECEIVER_URL   服务地址（默认 http://127.0.0.1:8000 / :8001）
  RN_LIFECYCLE=1              等价于 --lifecycle
  RN_HOOK_STOP_RECEIVER 等    生命周期钩子命令（默认 docker compose ...）
"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

SENDER = os.environ.get("SENDER_URL", "http://127.0.0.1:8000").rstrip("/")
RECEIVER = os.environ.get("RECEIVER_URL", "http://127.0.0.1:8001").rstrip("/")
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN = str(int(time.time()))
ENV = "itest"

PASSED, FAILED = [], []


def check(name, cond, extra=""):
    (PASSED if cond else FAILED).append(name)
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + ("" if cond else f"  | {extra}"), flush=True)


def req(method, url, body=None, timeout=20):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw.decode(errors="replace")}
        return e.code, payload
    except urllib.error.URLError as e:
        return 0, {"error": str(e)}


def sender_create(env, rel, result):
    return req("POST", SENDER + "/api/notifications",
               {"env": env, "release_no": rel, "result": result})


def sender_send(nid, simulate=None):
    url = f"{SENDER}/api/notifications/{nid}/send"
    if simulate:
        url += f"?simulate={simulate}"
    return req("POST", url)


def sender_reconcile(nid):
    return req("POST", f"{SENDER}/api/notifications/{nid}/reconcile")


def sender_rows(rel):
    _, d = req("GET", SENDER + "/api/notifications")
    return [x for x in d.get("items", []) if x["env"] == ENV and x["release_no"] == rel]


def receiver_rows(rel):
    _, d = req("GET", RECEIVER + "/api/receipts")
    return [x for x in d.get("items", []) if x["env"] == ENV and x["release_no"] == rel]


def review_rows(rel):
    """直接查接收端台账内嵌的复核对象。"""
    rows = receiver_rows(rel)
    return [r["review"] for r in rows if r.get("review")]


def submit_review(rel, reviewer="auditor", conclusion="approved", remark="ok",
                  write_fail=False):
    body = {"env": ENV, "release_no": rel, "reviewer": reviewer,
            "conclusion": conclusion, "remark": remark}
    if write_fail:
        body["review_write_fail"] = True
    return req("POST", RECEIVER + "/api/receipts/review", body)


def sender_get(nid):
    _, d = req("GET", SENDER + "/api/notifications")
    for x in d.get("items", []):
        if x["id"] == nid:
            return x
    return None


def hook(env_var, default_cmd):
    cmd = os.environ.get(env_var, default_cmd)
    print(f"  >> 钩子: {cmd}", flush=True)
    r = subprocess.run(cmd, shell=True, cwd=PROJECT_ROOT,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if r.returncode != 0:
        print(f"  !! 钩子返回码 {r.returncode}", flush=True)


def wait_up(base, tries=60):
    for _ in range(tries):
        code, _ = req("GET", base + "/api/health", timeout=3)
        if code == 200:
            return True
        time.sleep(0.5)
    return False


def key(tag):
    return f"R{RUN}-{tag}"


# ---------------- 场景 ----------------

def s1_basic():
    k = key("k1")
    code, n = sender_create(ENV, k, "success")
    check("S1 登记通知成功且初始为待发送", code in (200, 201) and n.get("status") == "PENDING", f"code={code} {n}")
    code, n2 = sender_send(n["id"])
    check("S1 发送后已确认且有回执号", n2.get("status") == "ACKED" and bool(n2.get("receipt_id")), f"{n2}")
    check("S1 接收端仅一条记录", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")


def s2_retry_dedup():
    k = key("k1")  # 与 S1 同键：同内容重试
    code, n = sender_create(ENV, k, "success")
    check("S2 同内容重复登记被幂等去重", code == 200 and n.get("deduplicated") is True, f"code={code} {n}")
    code, n2 = sender_send(n["id"])
    check("S2 重发后仍为已确认", n2.get("status") == "ACKED", f"{n2}")
    check("S2 发送端同键仅一条", len(sender_rows(k)) == 1)
    check("S2 接收端同键仍仅一条", len(receiver_rows(k)) == 1)


def s3_conflict():
    k = key("k1")
    code, n = sender_create(ENV, k, "failed")
    check("S3 同键不同内容登记报 409 冲突", code == 409, f"code={code} {n}")
    rows = sender_rows(k)
    check("S3 发送端原记录未被篡改", len(rows) == 1 and rows[0]["result"] == "success", f"{rows}")
    code, _ = req("POST", RECEIVER + "/api/receipts",
                  {"env": ENV, "release_no": k, "result": "failed"})
    check("S3 接收端直连同键不同内容也报 409", code == 409, f"code={code}")
    check("S3 接收端同键仍仅一条", len(receiver_rows(k)) == 1)


def s4_lost_response_resend():
    k = key("k4")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    code, n2 = sender_send(nid, simulate="lose_response")
    check("S4 丢回执后为待核而非成功", n2.get("status") == "UNCERTAIN", f"{n2}")
    check("S4 接收端其实已记账（1 条）", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")
    code, n3 = sender_send(nid)
    check("S4 重发后凭回执确认", n3.get("status") == "ACKED", f"{n3}")
    check("S4 重发未产生重复记录", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")


def s5_lost_response_reconcile():
    k = key("k5")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid, simulate="lose_response")
    check("S5 丢回执后状态为待核", sender_rows(k)[0]["status"] == "UNCERTAIN")
    code, n3 = sender_reconcile(nid)
    check("S5 核对后凭接收端回执确认", n3.get("status") == "ACKED" and bool(n3.get("receipt_id")), f"{n3}")
    check("S5 接收端仍仅一条", len(receiver_rows(k)) == 1)


def s6_slow_response():
    k = key("k6")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    code, n2 = sender_send(nid, simulate="slow")
    # 必须以发送响应为准断言：慢回执会在 6s 后落库，随后的核对即可确认，
    # 不能在核对之后再回读状态（那时已被核对推进为已确认）。
    check("S6 慢回执超时后发送响应为待核", n2.get("status") == "UNCERTAIN", f"{n2}")
    code, n3 = sender_reconcile(nid)
    check("S6 核对后确认", n3.get("status") == "ACKED", f"{n3}")
    check("S6 接收端仅一条", len(receiver_rows(k)) == 1)


def s7_concurrent_same():
    k = key("k7")
    results = []

    def worker():
        try:
            c1, n1 = sender_create(ENV, k, "success")
            c2, n2 = sender_send(n1["id"])
            results.append((c1, c2, n2.get("status")))
        except Exception as e:  # noqa: BLE001
            results.append(("EXC", str(e), None))

    threads = [threading.Thread(target=worker) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("S7 并发同内容：无线程异常", all(r[0] != "EXC" for r in results), f"{results}")
    check("S7 并发同内容：发送端仅一条", len(sender_rows(k)) == 1, f"{sender_rows(k)}")
    check("S7 并发同内容：接收端仅一条", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")
    rows = sender_rows(k)
    check("S7 并发同内容：最终已确认", bool(rows) and rows[0]["status"] == "ACKED", f"{rows}")


def s8_concurrent_diff():
    k = key("k8")
    codes = []

    def worker(i):
        content = "success" if i % 2 == 0 else "failed"
        c, _ = sender_create(ENV, k, content)
        codes.append(c)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ok = sum(1 for c in codes if c in (200, 201))
    conflict = sum(1 for c in codes if c == 409)
    check("S8 并发不同内容：一组入库一组 409", ok == 5 and conflict == 5, f"codes={codes}")
    rows = sender_rows(k)
    check("S8 并发不同内容：发送端仅一条", len(rows) == 1, f"{rows}")
    if rows:
        code, n2 = sender_send(rows[0]["id"])
        check("S8 胜出发送后已确认", n2.get("status") == "ACKED", f"{n2}")
        rrows = receiver_rows(k)
        check("S8 接收端仅一条且内容与发送端一致",
              len(rrows) == 1 and rrows[0]["result"] == rows[0]["result"], f"{rrows} vs {rows}")


def s9_receiver_down():
    k = key("k9")
    hook("RN_HOOK_STOP_RECEIVER", "docker compose stop receiver")
    time.sleep(1)
    code, n = sender_create(ENV, k, "success")
    check("S9 接收端停机时本地可登记", code in (200, 201), f"code={code} {n}")
    nid = n["id"]
    code, n2 = sender_send(nid)
    check("S9 接收端不可达 → 待核而非成功", n2.get("status") == "UNCERTAIN", f"{n2}")
    code, n3 = sender_reconcile(nid)
    check("S9 不可达时核对仍保持待核（不虚报）", n3.get("status") == "UNCERTAIN", f"{n3}")
    return nid


def s10_sender_restart_while_down(nid_k9):
    k = key("k10")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid)  # 接收端仍停机 → 待核
    check("S10 停机期间发送为待核", sender_rows(k)[0]["status"] == "UNCERTAIN")
    hook("RN_HOOK_RESTART_SENDER", "docker compose restart sender")
    if not wait_up(SENDER):
        check("S10 发送端重启后恢复服务", False)
        return
    time.sleep(4)  # 等启动核对线程跑完（接收端仍停机，应保持待核）
    rows = sender_rows(k)
    check("S10 重启后待核状态持久（未丢未虚报）", bool(rows) and rows[0]["status"] == "UNCERTAIN", f"{rows}")
    hook("RN_HOOK_START_RECEIVER", "docker compose start receiver")
    if not wait_up(RECEIVER):
        check("S10 接收端恢复", False)
        return
    code, n4 = sender_reconcile(nid)
    check("S10 恢复后核对判定未送达（回退待发送）", n4.get("status") == "PENDING", f"{n4}")
    code, n4b = sender_send(nid)
    check("S10 安全重发后确认且接收端仅一条",
          n4b.get("status") == "ACKED" and len(receiver_rows(k)) == 1, f"{n4b}")
    code, n5 = sender_reconcile(nid_k9)
    check("S10 此前停机期间的通知核对同为未送达", n5.get("status") == "PENDING", f"{n5}")
    code, n5b = sender_send(nid_k9)
    check("S10 补发后确认", n5b.get("status") == "ACKED", f"{n5b}")
    check("S10 接收端两键各仅一条",
          len(receiver_rows(key("k9"))) == 1 and len(receiver_rows(k)) == 1)


def s11_auto_reconcile_after_restart():
    k = key("k11")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid, simulate="lose_response")
    check("S11 重启前为待核", sender_rows(k)[0]["status"] == "UNCERTAIN")
    # 仅本场景需要启动自动核对：本地钩子以 STARTUP_RECONCILE=1 重启；
    # Docker 模式下发送端默认即开启启动核对，普通 restart 即可。
    hook("RN_HOOK_RESTART_SENDER_AUTO", "docker compose restart sender")
    if not wait_up(SENDER):
        check("S11 发送端重启后恢复服务", False)
        return
    final = None
    for _ in range(20):  # 启动核对线程约 2s 后触发
        rows = sender_rows(k)
        if rows and rows[0]["status"] == "ACKED":
            final = rows[0]
            break
        time.sleep(0.5)
    check("S11 重启后自动凭接收端回执结案", final is not None, f"{sender_rows(k)}")
    check("S11 接收端仍仅一条", len(receiver_rows(k)) == 1)


# ---------------- 人工复核场景 ----------------

def r1_review_unknown_receipt():
    k = key("r1")
    code, d = submit_review(k)
    check("R1 不存在的回执不能复核（404）", code == 404, f"code={code} {d}")
    check("R1 失败尝试未留下任何复核", review_rows(k) == [], f"{review_rows(k)}")


def r2_review_happy_path():
    k = key("r2")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    code, n2 = sender_send(nid)
    check("R2 送达后初始未复核", n2.get("status") == "ACKED" and n2.get("review_status") == "NONE", f"{n2}")
    code, rv = submit_review(k, reviewer="alice", conclusion="approved", remark="looks good")
    check("R2 复核提交成功（201）", code == 201 and bool(rv.get("id")), f"code={code} {rv}")
    code, n3 = sender_reconcile(nid)
    check("R2 核对后同时显示已确认与已复核",
          n3.get("status") == "ACKED" and n3.get("review_status") == "REVIEWED", f"{n3}")
    check("R2 复核明细已同步（复核人/结论/备注）",
          n3.get("reviewer") == "alice" and n3.get("review_conclusion") == "approved"
          and n3.get("review_remark") == "looks good" and bool(n3.get("review_id")), f"{n3}")


def r3_review_idempotent_retry():
    k = key("r3")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid)
    payload = {"env": ENV, "release_no": k, "reviewer": "bob",
               "conclusion": "needs_followup", "remark": "retry me"}
    c1, d1 = req("POST", RECEIVER + "/api/receipts/review", payload)
    c2, d2 = req("POST", RECEIVER + "/api/receipts/review", payload)
    c3, d3 = req("POST", RECEIVER + "/api/receipts/review", payload)
    check("R3 同内容复核重试：首次 201，后续 200 幂等",
          c1 == 201 and c2 == 200 and d2.get("deduplicated") is True
          and c3 == 200 and d3.get("deduplicated") is True, f"{c1} {c2} {c3}")
    check("R3 同回执复核只保留一条", len(review_rows(k)) == 1 and review_rows(k)[0]["id"] == d1["id"],
          f"{review_rows(k)}")
    code, n3 = sender_reconcile(nid)
    check("R3 发送端复核状态仍只有一份", n3.get("review_id") == d1["id"], f"{n3}")


def r4_review_conflict():
    k = key("r4")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid)
    c1, _ = submit_review(k, reviewer="alice", conclusion="approved", remark="v1")
    c2, d2 = submit_review(k, reviewer="bob", conclusion="rejected", remark="v2")
    check("R4 同回执不同内容复核报 409", c1 == 201 and c2 == 409, f"{c1} {c2} {d2}")
    rows = review_rows(k)
    check("R4 原复核未被篡改且仍只一条",
          len(rows) == 1 and rows[0]["reviewer"] == "alice"
          and rows[0]["conclusion"] == "approved", f"{rows}")
    code, n3 = sender_reconcile(nid)
    check("R4 发送端同步到的是原始复核", n3.get("reviewer") == "alice"
          and n3.get("review_conclusion") == "approved", f"{n3}")


def r5_review_interleave_no_regression():
    k = key("r5")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    # 先制造待核，再由审计员复核，交错执行发送与核对
    sender_send(nid, simulate="lose_response")
    submit_review(k, reviewer="mia", conclusion="approved", remark="interleave")
    c1, n1 = sender_send(nid)  # 重发拿到带复核的回执
    check("R5 交错重发后已确认且已复核",
          n1.get("status") == "ACKED" and n1.get("review_status") == "REVIEWED", f"{n1}")
    c2, n2 = sender_reconcile(nid)  # 再次核对
    check("R5 再次核对不降级、不丢复核",
          n2.get("status") == "ACKED" and n2.get("review_status") == "REVIEWED"
          and n2.get("reviewer") == "mia", f"{n2}")
    c3, n3 = req("POST", f"{SENDER}/api/notifications/{nid}/send")  # 已确认重发
    check("R5 已确认重发为幂等返回且复核保持",
          c3 == 200 and n3.get("status") == "ACKED" and n3.get("review_status") == "REVIEWED", f"{n3}")
    check("R5 接收端回执与复核各仅一条",
          len(receiver_rows(k)) == 1 and len(review_rows(k)) == 1)


def r6_sender_write_fail_keeps_state():
    k = key("r6")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    # 6a 发送阶段本地写入失败：回执其实已到接收端，本地保持待核
    c1, d1 = req("POST", f"{SENDER}/api/notifications/{nid}/send?simulate=write_fail")
    n1 = sender_get(nid)
    check("R6a 发送写入失败返回 500", c1 == 500, f"code={c1} {d1}")
    check("R6a 本地保持待核、未虚报成功",
          n1.get("status") == "UNCERTAIN" and n1.get("review_status") == "NONE", f"{n1}")
    check("R6a 接收端已入账一条", len(receiver_rows(k)) == 1)
    # 6b 核对阶段本地写入失败：原状态同样不变
    c2, d2 = req("POST", f"{SENDER}/api/notifications/{nid}/reconcile?write_fail=true")
    n2 = sender_get(nid)
    check("R6b 核对写入失败返回 500 且状态不变",
          c2 == 500 and n2.get("status") == "UNCERTAIN", f"code={c2} {d2} {n2}")
    # 6c 恢复后正常核对结案
    c3, n3 = sender_reconcile(nid)
    check("R6 恢复后凭回执核对为已确认", c3 == 200 and n3.get("status") == "ACKED", f"{n3}")
    check("R6 全程接收端仅一条", len(receiver_rows(k)) == 1)


def r7_receiver_review_write_fail():
    k = key("r7")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid)
    c1, d1 = submit_review(k, reviewer="dan", conclusion="approved", write_fail=True)
    check("R7 复核写入失败返回 500", c1 == 500, f"code={c1} {d1}")
    check("R7 失败后无复核落库", review_rows(k) == [], f"{review_rows(k)}")
    c2, rv = submit_review(k, reviewer="dan", conclusion="approved")
    check("R7 重试提交成功", c2 == 201 and bool(rv.get("id")), f"code={c2} {rv}")
    check("R7 最终复核仅一条", len(review_rows(k)) == 1)
    code, n3 = sender_reconcile(nid)
    check("R7 发送端随后可同步复核", n3.get("review_status") == "REVIEWED", f"{n3}")


def r8_concurrent_reviews():
    k = key("r8")
    code, n = sender_create(ENV, k, "success")
    sender_send(n["id"])
    results = []

    def worker(i):
        if i % 2 == 0:
            c, d = submit_review(k, reviewer="team", conclusion="approved", remark="same")
        else:
            c, d = submit_review(k, reviewer=f"user{i}", conclusion="rejected", remark=f"d{i}")
        results.append((i, c, d.get("deduplicated", False)))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    rows = review_rows(k)
    check("R8 并发复核后接收端仅一条", len(rows) == 1, f"{rows}")
    created = [r for r in results if r[1] == 201]
    conflicts = [r for r in results if r[1] == 409]
    check("R8 仅一个线程写入成功，其余为 409（或幂等 200）",
          len(created) == 1 and len(conflicts) + sum(1 for r in results if r[1] == 200) == 9,
          f"{results}")


def r9_concurrent_send_reconcile():
    k = key("r9")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    outcomes = []

    def send_worker():
        try:
            c, d = req("POST", f"{SENDER}/api/notifications/{nid}/send?simulate=lose_response")
            outcomes.append(("send", c, d.get("status"), d.get("review_status")))
        except Exception as e:  # noqa: BLE001
            outcomes.append(("send", "EXC", str(e), None))

    def reconcile_worker():
        try:
            c, d = req("POST", f"{SENDER}/api/notifications/{nid}/reconcile")
            outcomes.append(("rec", c, d.get("status"), d.get("review_status")))
        except Exception as e:  # noqa: BLE001
            outcomes.append(("rec", "EXC", str(e), None))

    # 交错：发送（丢回执）与核对同时进行
    t1 = threading.Thread(target=send_worker)
    t2 = threading.Thread(target=reconcile_worker)
    t1.start(); t2.start(); t1.join(); t2.join()
    check("R9 交错操作无异常", all(o[1] != "EXC" for o in outcomes), f"{outcomes}")
    mid = sender_get(nid)
    check("R9 交错后无孤立状态（待核/已确认之一）",
          mid.get("status") in ("UNCERTAIN", "ACKED"), f"{mid}")
    # 审计员复核后再次并发核对，验证复核不丢失、不倒退
    submit_review(k, reviewer="neo", conclusion="approved", remark="final")
    threads = [threading.Thread(target=reconcile_worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    final = sender_get(nid)
    check("R9 并发核对后终态为已确认+已复核且不倒退",
          final.get("status") == "ACKED" and final.get("review_status") == "REVIEWED"
          and final.get("reviewer") == "neo", f"{final}")
    check("R9 接收端回执与复核各仅一条",
          len(receiver_rows(k)) == 1 and len(review_rows(k)) == 1)


def r10_review_persists_restart():
    k = key("r10")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid)
    submit_review(k, reviewer="eva", conclusion="approved", remark="persist")
    code, n2 = sender_reconcile(nid)
    check("R10 重启前已确认且已复核",
          n2.get("status") == "ACKED" and n2.get("review_status") == "REVIEWED", f"{n2}")
    hook("RN_HOOK_RESTART_SENDER", "docker compose restart sender")
    if not wait_up(SENDER):
        check("R10 发送端重启后恢复服务", False)
        return
    time.sleep(4)  # 等启动核对线程跑完
    final = sender_get(nid)
    check("R10 重启后送达与复核状态均可继续查询",
          final is not None and final.get("status") == "ACKED"
          and final.get("review_status") == "REVIEWED"
          and final.get("reviewer") == "eva", f"{final}")


def main():
    lifecycle = "--lifecycle" in sys.argv or os.environ.get("RN_LIFECYCLE") == "1"
    print(f"发送端 {SENDER}  接收端 {RECEIVER}  lifecycle={lifecycle}", flush=True)
    if not wait_up(SENDER) or not wait_up(RECEIVER):
        print("服务未就绪，请先启动两端。")
        sys.exit(2)

    s1_basic()
    s2_retry_dedup()
    s3_conflict()
    s4_lost_response_resend()
    s5_lost_response_reconcile()
    s6_slow_response()
    s7_concurrent_same()
    s8_concurrent_diff()
    r1_review_unknown_receipt()
    r2_review_happy_path()
    r3_review_idempotent_retry()
    r4_review_conflict()
    r5_review_interleave_no_regression()
    r6_sender_write_fail_keeps_state()
    r7_receiver_review_write_fail()
    r8_concurrent_reviews()
    r9_concurrent_send_reconcile()
    if lifecycle:
        nid_k9 = s9_receiver_down()
        s10_sender_restart_while_down(nid_k9)
        s11_auto_reconcile_after_restart()
        r10_review_persists_restart()

    print(f"\n通过 {len(PASSED)} 项，失败 {len(FAILED)} 项")
    if FAILED:
        print("失败项：")
        for name in FAILED:
            print(f"  - {name}")
        sys.exit(1)


if __name__ == "__main__":
    main()
