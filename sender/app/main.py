"""发布值班发送端。

职责：登记“部署完成通知”，调用审计接收端接口，并仅凭接收端回执确认送达与复核状态。
原则：
- 发送前先把“已发出、结果未知（待核）”落库，进程崩溃也不会丢失该事实；
- 只有拿到接收端内容一致的回执才置为“已确认”，绝不虚报成功；
- 丢回执 / 接收端不可达 / 发送端重启后，均可凭接收端回执核对补账；
- 复核状态只随接收端回执单向推进（无复核 -> 已复核），任何交错操作都不会
  清空或倒退复核；送达状态同样只允许前进，已确认/冲突等终态不会被降级；
- 本地写入失败（含演示用 write_fail 故障注入）一律返回 5xx 且原状态保持不变。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

DB_PATH = os.environ.get("DB_PATH", "/data/sender.db")
RECEIVER_URL = os.environ.get("RECEIVER_URL", "http://localhost:8001").rstrip("/")
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "3"))
STARTUP_RECONCILE = os.environ.get("STARTUP_RECONCILE", "1") == "1"
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

STATUS_PENDING = "PENDING"        # 待发送：已登记，尚未发送或已确认对方未记账
STATUS_UNCERTAIN = "UNCERTAIN"    # 待核：已发出但回执缺失/对方不可达，是否送达未知
STATUS_ACKED = "ACKED"            # 已确认：收到接收端内容一致的回执
STATUS_CONFLICT = "CONFLICT"      # 冲突：同环境同发布号在接收端已存在不同内容

REVIEW_NONE = "NONE"              # 接收端尚无人工复核
REVIEW_REVIEWED = "REVIEWED"      # 接收端已留下人工复核

# 接收端认识的故障注入；write_fail 是本端写入故障，绝不转发给接收端
RECEIVER_SIMULATE = {"lose_response", "slow"}
VALID_SIMULATE = RECEIVER_SIMULATE | {"write_fail"}

# 送达状态允许的迁移表：终态（已确认/冲突）不可降级，杜绝交错操作下的状态倒退
ALLOWED_TRANSITIONS = {
    STATUS_PENDING: {STATUS_UNCERTAIN, STATUS_ACKED, STATUS_CONFLICT},
    STATUS_UNCERTAIN: {STATUS_PENDING, STATUS_ACKED, STATUS_CONFLICT},
    STATUS_ACKED: {STATUS_ACKED},
    STATUS_CONFLICT: {STATUS_CONFLICT},
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def content_hash(env: str, release_no: str, result: str) -> str:
    canonical = json.dumps(
        {"env": env, "release_no": release_no, "result": result},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, isolation_level=None, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                env TEXT NOT NULL,
                release_no TEXT NOT NULL,
                result TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                receipt_id INTEGER,
                receipt_at TEXT,
                last_error TEXT,
                review_status TEXT NOT NULL DEFAULT 'NONE',
                review_id INTEGER,
                reviewer TEXT,
                review_conclusion TEXT,
                review_remark TEXT,
                reviewed_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(env, release_no)
            )
            """
        )
        # 兼容旧库：补齐复核与相关列（ALTER TABLE 仅在缺列时执行）
        existing = {r["name"] for r in conn.execute("PRAGMA table_info(notifications)")}
        migrations = {
            "review_status": "ALTER TABLE notifications ADD COLUMN review_status TEXT NOT NULL DEFAULT 'NONE'",
            "review_id": "ALTER TABLE notifications ADD COLUMN review_id INTEGER",
            "reviewer": "ALTER TABLE notifications ADD COLUMN reviewer TEXT",
            "review_conclusion": "ALTER TABLE notifications ADD COLUMN review_conclusion TEXT",
            "review_remark": "ALTER TABLE notifications ADD COLUMN review_remark TEXT",
            "reviewed_at": "ALTER TABLE notifications ADD COLUMN reviewed_at TEXT",
        }
        for col, ddl in migrations.items():
            if col not in existing:
                conn.execute(ddl)
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def fetch_dict(conn: sqlite3.Connection, nid: int) -> dict:
    row = conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
    return row_to_dict(row)


def extract_review(receipt: dict) -> dict | None:
    """从接收端回执中取出人工复核信息；无复核时返回 None。"""
    rv = receipt.get("review")
    if isinstance(rv, dict) and rv.get("id"):
        return {
            "review_id": rv.get("id"),
            "reviewer": rv.get("reviewer"),
            "review_conclusion": rv.get("conclusion"),
            "review_remark": rv.get("remark", ""),
            "reviewed_at": rv.get("reviewed_at"),
        }
    return None


def apply_outcome(
    conn: sqlite3.Connection,
    nid: int,
    current_status: str,
    target_status: str,
    last_error: str | None,
    receipt: dict | None = None,
    fail_write: bool = False,
) -> bool:
    """单条原子 UPDATE 同时落送达状态与复核状态。

    - 送达状态按 ALLOWED_TRANSITIONS 守卫，非法迁移保持原状态；
    - 复核状态单向推进（NONE -> REVIEWED），已有的复核信息只 COALESCE 不覆盖；
    - fail_write=True 时在执行 SQL 前抛错，模拟本地写入失败（原状态不变）。
    返回是否发生了状态/字段变更。
    """
    if fail_write:
        raise sqlite3.OperationalError("模拟故障：本地状态写入失败")

    sets: list[str] = []
    vals: list = []
    changed = False

    if target_status != current_status and target_status in ALLOWED_TRANSITIONS[current_status]:
        sets.append("status=?")
        vals.append(target_status)
        changed = True
    sets.append("last_error=?")
    vals.append(last_error)

    if receipt is not None:
        if receipt.get("id") is not None:
            sets.append("receipt_id=COALESCE(?, receipt_id)")
            vals.append(receipt.get("id"))
        if receipt.get("received_at") is not None:
            sets.append("receipt_at=COALESCE(?, receipt_at)")
            vals.append(receipt.get("received_at"))
        rv = extract_review(receipt)
        if rv is not None:
            # 复核单向推进：只有 NONE 才置 REVIEWED，其余情况保持原值
            sets.append(
                "review_status=CASE WHEN review_status=? THEN ? ELSE review_status END"
            )
            vals.extend([REVIEW_NONE, REVIEW_REVIEWED])
            for col in ("review_id", "reviewer", "review_conclusion", "review_remark", "reviewed_at"):
                sets.append(f"{col}=COALESCE(?, {col})")
                vals.append(rv[col])
            changed = True

    vals.extend([now_iso(), nid])
    conn.execute(
        f"UPDATE notifications SET {', '.join(sets)}, updated_at=? WHERE id=?", vals
    )
    return changed


# ---------------- 核心动作 ----------------

def do_send(nid: int, simulate: str | None = None) -> tuple[int, dict]:
    """发送（或重发）一条通知。只有拿到接收端内容一致的回执才置为已确认。"""
    conn = connect()
    try:
        row = conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
        if row is None:
            return 404, {"detail": "通知不存在"}
        if row["status"] == STATUS_ACKED:
            out = row_to_dict(row)
            out["note"] = "已确认，无需重复发送"
            return 200, out
        if row["status"] == STATUS_CONFLICT:
            # 冲突是终态：不发请求、不改状态，交由人工处理，避免交错下的状态倒退
            out = row_to_dict(row)
            out["note"] = "冲突记录需人工处理，不能直接重发"
            return 409, out

        fail_write = simulate == "write_fail"
        # 1) 先把“已发出、结果未知”落库：即使此刻进程崩溃，
        #    重启后也知道必须核对，而不是误以为未发或误报成功。
        conn.execute(
            "UPDATE notifications SET status=?, attempt_count=attempt_count+1, updated_at=? WHERE id=?",
            (STATUS_UNCERTAIN, now_iso(), nid),
        )

        # 2) 调用接收端（write_fail 是本端故障，不转发）
        payload = {"env": row["env"], "release_no": row["release_no"], "result": row["result"]}
        if simulate in RECEIVER_SIMULATE:
            payload["simulate"] = simulate
        try:
            resp = httpx.post(f"{RECEIVER_URL}/api/receipts", json=payload, timeout=HTTP_TIMEOUT)
        except httpx.HTTPError as exc:
            apply_outcome(conn, nid, STATUS_UNCERTAIN, STATUS_UNCERTAIN,
                          f"接收端不可达或回执丢失（{exc.__class__.__name__}）",
                          fail_write=fail_write)
            return 200, fetch_dict(conn, nid)

        # 3) 只有接收端 2xx 回执且内容一致才算送达；同一事务语句顺带落入复核状态
        if resp.status_code in (200, 201):
            receipt = resp.json()
            if receipt.get("content_hash") == row["content_hash"]:
                apply_outcome(conn, nid, STATUS_UNCERTAIN, STATUS_ACKED, None,
                              receipt=receipt, fail_write=fail_write)
            else:
                apply_outcome(conn, nid, STATUS_UNCERTAIN, STATUS_CONFLICT,
                              "回执内容与本地登记不一致", fail_write=fail_write)
        elif resp.status_code == 409:
            detail = resp.json().get("detail", "接收端已存在不同内容")
            apply_outcome(conn, nid, STATUS_UNCERTAIN, STATUS_CONFLICT, detail,
                          fail_write=fail_write)
        else:
            apply_outcome(conn, nid, STATUS_UNCERTAIN, STATUS_UNCERTAIN,
                          f"接收端返回 {resp.status_code}，回执缺失", fail_write=fail_write)
        return 200, fetch_dict(conn, nid)
    except sqlite3.Error as exc:
        # 任何本地写入失败：不改动既有状态，原样返回供值班员稍后重试/核对
        return 500, {"detail": f"本地写入失败，状态保持不变：{exc}"}
    finally:
        conn.close()


def do_reconcile(nid: int, fail_write: bool = False) -> tuple[int, dict]:
    """凭接收端回执核对一条通知的送达与复核状态。"""
    conn = connect()
    try:
        row = conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
        if row is None:
            return 404, {"detail": "通知不存在"}
        try:
            resp = httpx.get(
                f"{RECEIVER_URL}/api/receipts",
                params={"env": row["env"], "release_no": row["release_no"]},
                timeout=HTTP_TIMEOUT,
            )
        except httpx.HTTPError:
            # 接收端暂不可查：保持现状（待核仍为待核），绝不虚报成功
            apply_outcome(conn, nid, row["status"], row["status"], "核对待定：接收端暂不可达")
            out = fetch_dict(conn, nid)
            out["note"] = "接收端暂不可达，状态保持不变"
            return 200, out

        if resp.status_code == 404:
            # 接收端明确无此记录：未送达，可安全重发（仅待核允许回退待发送）
            apply_outcome(conn, nid, row["status"], STATUS_PENDING, None,
                          fail_write=fail_write)
            out = fetch_dict(conn, nid)
            if out["status"] == STATUS_PENDING:
                out["note"] = "接收端无此记录，可安全重发"
            return 200, out
        if resp.status_code == 200:
            receipt = resp.json()
            if receipt.get("content_hash") == row["content_hash"]:
                # 已确认是终态：不降级，但回执里若带来了新复核仍单向补入
                apply_outcome(conn, nid, row["status"], STATUS_ACKED, None,
                              receipt=receipt, fail_write=fail_write)
            else:
                apply_outcome(conn, nid, row["status"], STATUS_CONFLICT,
                              "接收端已存在同环境同发布号的不同内容",
                              fail_write=fail_write)
            return 200, fetch_dict(conn, nid)

        apply_outcome(conn, nid, row["status"], row["status"],
                      f"核对失败：接收端返回 {resp.status_code}")
        return 200, fetch_dict(conn, nid)
    except sqlite3.Error as exc:
        return 500, {"detail": f"本地写入失败，状态保持不变：{exc}"}
    finally:
        conn.close()


def reconcile_all_open() -> int:
    """核对所有未结案（待核/待发送）记录，返回处理条数。"""
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT id FROM notifications WHERE status IN (?, ?)",
            (STATUS_UNCERTAIN, STATUS_PENDING),
        ).fetchall()
    finally:
        conn.close()
    for r in rows:
        do_reconcile(r["id"])
    return len(rows)


def _startup_reconcile() -> None:
    """发送端重启后主动核对所有待核记录：只凭接收端回执定案。"""
    time.sleep(2)  # 等待接收端就绪
    try:
        n = reconcile_all_open()
        print(f"[startup] 重启核对完成，共处理 {n} 条待核/待发送记录", flush=True)
    except Exception as exc:  # noqa: BLE001 - 启动核对失败不致命，可稍后手动核对
        print(f"[startup] 重启核对失败（可稍后手动核对）: {exc}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if STARTUP_RECONCILE:
        threading.Thread(target=_startup_reconcile, daemon=True).start()
    yield


app = FastAPI(title="发布值班发送端", lifespan=lifespan)


class NotificationIn(BaseModel):
    env: str
    release_no: str
    result: str


@app.get("/api/health")
def health():
    return {"ok": True}


@app.post("/api/notifications")
def create_notification(body: NotificationIn):
    """登记通知。同 (env, release_no) 同内容幂等去重，内容不同报 409 冲突。"""
    env, release_no, result = body.env.strip(), body.release_no.strip(), body.result.strip()
    if not env or not release_no or not result:
        return JSONResponse(status_code=400, content={"detail": "env / release_no / result 均不能为空"})
    if len(env) > 64 or len(release_no) > 64 or len(result) > 64:
        return JSONResponse(status_code=400, content={"detail": "字段长度不能超过 64 字符"})

    ch = content_hash(env, release_no, result)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM notifications WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is not None:
                conn.execute("COMMIT")
                if row["content_hash"] == ch:
                    out = row_to_dict(row)
                    out["deduplicated"] = True
                    return JSONResponse(status_code=200, content=out)
                return JSONResponse(status_code=409, content={
                    "detail": "相同环境与发布号已存在不同内容",
                    "existing": row_to_dict(row),
                })
            now = now_iso()
            cur = conn.execute(
                """INSERT INTO notifications
                   (env, release_no, result, content_hash, status, attempt_count,
                    review_status, created_at, updated_at)
                   VALUES (?,?,?,?,?,0,?, ?,?)""",
                (env, release_no, result, ch, STATUS_PENDING, REVIEW_NONE, now, now),
            )
            nid = cur.lastrowid
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        return JSONResponse(status_code=201, content=fetch_dict(conn, nid))
    except sqlite3.Error as exc:
        return JSONResponse(status_code=500, content={"detail": f"本地写入失败：{exc}"})
    finally:
        conn.close()


@app.get("/api/notifications")
def list_notifications():
    conn = connect()
    try:
        rows = conn.execute("SELECT * FROM notifications ORDER BY id DESC").fetchall()
        return {"items": [row_to_dict(r) for r in rows]}
    finally:
        conn.close()


@app.post("/api/notifications/{nid}/send")
def send_notification(nid: int, simulate: str | None = Query(default=None)):
    if simulate is not None and simulate not in VALID_SIMULATE:
        return JSONResponse(status_code=400, content={"detail": f"simulate 仅支持：{sorted(VALID_SIMULATE)}"})
    code, payload = do_send(nid, simulate)
    return JSONResponse(status_code=code, content=payload)


@app.post("/api/notifications/{nid}/reconcile")
def reconcile_notification(nid: int, write_fail: bool = Query(default=False)):
    code, payload = do_reconcile(nid, fail_write=write_fail)
    return JSONResponse(status_code=code, content=payload)


@app.post("/api/reconcile-all")
def reconcile_all_endpoint():
    n = reconcile_all_open()
    return {"reconciled": n}


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
