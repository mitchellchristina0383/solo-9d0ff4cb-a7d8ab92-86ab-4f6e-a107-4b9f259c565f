"""审计接收端（本地演示用）。

接收“部署完成通知”，按 (env, release_no) 幂等入库，并提供回执查询。
- 相同 (env, release_no) 且内容一致的重试：返回既有回执，不新增记录；
- 相同 (env, release_no) 但内容不同：409 冲突；
- 审计员在确认通知已送达（回执已入账）后，可对回执提交一次人工复核：
  * 复核按 (env, release_no) 定位回执，回执不存在 -> 404，不能复核；
  * 同回执同内容（复核人/结论/备注均相同）重试 -> 幂等返回，只保留一条复核；
  * 同回执不同内容 -> 409 冲突，原复核不被篡改；
- 故障注入（仅演示）：
  * simulate=lose_response（入账后丢响应）/ slow（入账后慢回执）；
  * review_write_fail=1（复核校验通过后写入前抛错，验证写入失败保持原状态）。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

DB_PATH = os.environ.get("DB_PATH", "/data/receiver.db")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
VALID_SIMULATE = {"lose_response", "slow"}
VALID_CONCLUSIONS = {"approved", "rejected", "needs_followup"}
MAX_REMARK_LEN = 256


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def content_hash(env: str, release_no: str, result: str) -> str:
    canonical = json.dumps(
        {"env": env, "release_no": release_no, "result": result},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def review_hash(env: str, release_no: str, reviewer: str, conclusion: str, remark: str) -> str:
    canonical = json.dumps(
        {
            "env": env,
            "release_no": release_no,
            "reviewer": reviewer,
            "conclusion": conclusion,
            "remark": remark,
        },
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
            CREATE TABLE IF NOT EXISTS receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                env TEXT NOT NULL,
                release_no TEXT NOT NULL,
                result TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(env, release_no)
            )
            """
        )
        # 人工复核：每条回执至多一条复核（UNIQUE(env, release_no)），
        # 外键挂回执；回执不存在时无法插入，保证“不存在的回执不能复核”。
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                receipt_id INTEGER NOT NULL,
                env TEXT NOT NULL,
                release_no TEXT NOT NULL,
                reviewer TEXT NOT NULL,
                conclusion TEXT NOT NULL,
                remark TEXT NOT NULL DEFAULT '',
                review_hash TEXT NOT NULL,
                reviewed_at TEXT NOT NULL,
                UNIQUE(env, release_no),
                FOREIGN KEY(receipt_id) REFERENCES receipts(id)
            )
            """
        )
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def fetch_dict(conn: sqlite3.Connection, rid: int) -> dict:
    row = conn.execute("SELECT * FROM receipts WHERE id=?", (rid,)).fetchone()
    return row_to_dict(row)


def attach_review(conn: sqlite3.Connection, receipt: dict) -> dict:
    """把复核状态合并进回执字典；未复核时 review 为 None。"""
    rrow = conn.execute(
        "SELECT * FROM reviews WHERE env=? AND release_no=?",
        (receipt["env"], receipt["release_no"]),
    ).fetchone()
    receipt["review"] = row_to_dict(rrow) if rrow is not None else None
    return receipt


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="审计接收端", lifespan=lifespan)


class ReceiptIn(BaseModel):
    env: str
    release_no: str
    result: str
    simulate: str | None = None  # lose_response | slow


class ReviewIn(BaseModel):
    env: str
    release_no: str
    reviewer: str
    conclusion: str
    remark: str | None = ""
    review_write_fail: bool = False  # 仅演示：写入前故障注入


@app.get("/api/health")
def health():
    return {"ok": True}


@app.post("/api/receipts")
def create_receipt(body: ReceiptIn):
    env, release_no, result = body.env.strip(), body.release_no.strip(), body.result.strip()
    if not env or not release_no or not result:
        return JSONResponse(status_code=400, content={"detail": "env / release_no / result 均不能为空"})
    if len(env) > 64 or len(release_no) > 64 or len(result) > 64:
        return JSONResponse(status_code=400, content={"detail": "字段长度不能超过 64 字符"})
    if body.simulate is not None and body.simulate not in VALID_SIMULATE:
        return JSONResponse(status_code=400, content={"detail": f"simulate 仅支持：{sorted(VALID_SIMULATE)}"})

    ch = content_hash(env, release_no, result)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM receipts WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is not None:
                conn.execute("COMMIT")
                if row["content_hash"] == ch:
                    # 同内容重试：幂等返回既有回执，绝不重复记账
                    out = row_to_dict(row)
                    out["deduplicated"] = True
                    return JSONResponse(status_code=200, content=attach_review(conn, out))
                return JSONResponse(status_code=409, content={
                    "detail": "相同环境与发布号已存在不同内容",
                    "existing": row_to_dict(row),
                })
            cur = conn.execute(
                "INSERT INTO receipts (env, release_no, result, content_hash, received_at) VALUES (?,?,?,?,?)",
                (env, release_no, result, ch, now_iso()),
            )
            rid = cur.lastrowid
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        receipt = fetch_dict(conn, rid)
        # —— 故障注入（仅演示）：记录已提交入库，但调用方拿不到回执 ——
        if body.simulate == "lose_response":
            return JSONResponse(status_code=500, content={"detail": "模拟故障：记录已入库，响应丢失"})
        if body.simulate == "slow":
            time.sleep(6)  # 长于发送端 3s 超时，回执实际已生成
        return JSONResponse(status_code=201, content=attach_review(conn, receipt))
    except sqlite3.Error as exc:
        return JSONResponse(status_code=500, content={"detail": f"接收端写入失败：{exc}"})
    finally:
        conn.close()


@app.post("/api/receipts/review")
def submit_review(body: ReviewIn):
    """审计员对已送达回执提交人工复核。

    - 回执不存在 -> 404（不存在的回执不能复核）；
    - 同回执同内容重试 -> 200 幂等返回，复核只保留一条；
    - 同回执不同内容 -> 409 冲突，原复核保持不变；
    - 写入失败（含故障注入）-> 500，数据库内复核状态不变。
    """
    env = body.env.strip()
    release_no = body.release_no.strip()
    reviewer = body.reviewer.strip()
    conclusion = body.conclusion.strip()
    remark = (body.remark or "").strip()
    if not env or not release_no or not reviewer or not conclusion:
        return JSONResponse(status_code=400,
                            content={"detail": "env / release_no / reviewer / conclusion 均不能为空"})
    if len(env) > 64 or len(release_no) > 64 or len(reviewer) > 64:
        return JSONResponse(status_code=400,
                            content={"detail": "env / release_no / reviewer 长度不能超过 64 字符"})
    if conclusion not in VALID_CONCLUSIONS:
        return JSONResponse(status_code=400,
                            content={"detail": f"conclusion 仅支持：{sorted(VALID_CONCLUSIONS)}"})
    if len(remark) > MAX_REMARK_LEN:
        return JSONResponse(status_code=400,
                            content={"detail": f"remark 长度不能超过 {MAX_REMARK_LEN} 字符"})

    rh = review_hash(env, release_no, reviewer, conclusion, remark)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            receipt = conn.execute(
                "SELECT * FROM receipts WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if receipt is None:
                conn.execute("ROLLBACK")
                return JSONResponse(status_code=404, content={
                    "detail": "回执不存在：通知尚未送达，不能复核",
                })
            existing = conn.execute(
                "SELECT * FROM reviews WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if existing is not None:
                conn.execute("COMMIT")
                if existing["review_hash"] == rh:
                    out = row_to_dict(existing)
                    out["deduplicated"] = True
                    # 同内容重试：只保留一条，原样返回
                    return JSONResponse(status_code=200, content=out)
                return JSONResponse(status_code=409, content={
                    "detail": "该回执已存在内容不同的复核",
                    "existing": row_to_dict(existing),
                })
            # —— 故障注入（仅演示）：校验均通过，写入前失败 ——
            if body.review_write_fail:
                conn.execute("ROLLBACK")
                return JSONResponse(status_code=500, content={
                    "detail": "模拟故障：复核写入失败，复核状态保持不变",
                })
            cur = conn.execute(
                """INSERT INTO reviews
                   (receipt_id, env, release_no, reviewer, conclusion, remark, review_hash, reviewed_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (receipt["id"], env, release_no, reviewer, conclusion, remark, rh, now_iso()),
            )
            review_id = cur.lastrowid
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        row = conn.execute("SELECT * FROM reviews WHERE id=?", (review_id,)).fetchone()
        return JSONResponse(status_code=201, content=row_to_dict(row))
    except sqlite3.Error as exc:
        return JSONResponse(status_code=500, content={"detail": f"复核写入失败：{exc}"})
    finally:
        conn.close()


@app.get("/api/receipts")
def list_or_get_receipts(env: str | None = None, release_no: str | None = None):
    conn = connect()
    try:
        if env is not None or release_no is not None:
            if not env or not release_no:
                return JSONResponse(status_code=400, content={"detail": "env 与 release_no 须同时提供"})
            row = conn.execute(
                "SELECT * FROM receipts WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is None:
                return JSONResponse(status_code=404, content={"detail": "无此记录"})
            return attach_review(conn, row_to_dict(row))
        rows = conn.execute("SELECT * FROM receipts ORDER BY id DESC").fetchall()
        return {"items": [attach_review(conn, row_to_dict(r)) for r in rows]}
    finally:
        conn.close()


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
