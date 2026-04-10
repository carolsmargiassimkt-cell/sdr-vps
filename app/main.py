from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "inbound_queue.sqlite3"

app = FastAPI(title="whatsapp-inbound-vps")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inbound_messages (
                id TEXT PRIMARY KEY,
                phone TEXT NOT NULL,
                text TEXT NOT NULL,
                timestamp TEXT,
                status TEXT NOT NULL DEFAULT 'pending'
            )
            """
        )
        conn.commit()


class InboundPayload(BaseModel):
    id: str | None = None
    msg_id: str | None = None
    messageId: str | None = None
    phone: str
    text: str | None = None
    message: str | None = None
    timestamp: str | None = None


class AckPayload(BaseModel):
    id: str


class PendingPayload(BaseModel):
    id: str


@app.on_event("startup")
def on_startup() -> None:
    init_db()


@app.get("/")
def health() -> dict[str, Any]:
    return {"ok": True}


@app.post("/inbound")
def inbound(payload: InboundPayload) -> dict[str, Any]:
    msg_id = str(payload.id or payload.msg_id or payload.messageId or "").strip()
    phone = str(payload.phone or "").strip()
    text = str(payload.text or payload.message or "").strip()
    timestamp = str(payload.timestamp or "").strip() or None
    if not msg_id or not phone or not text:
        return {"ok": False, "error": "invalid_payload"}

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT status FROM inbound_messages WHERE id = ?",
            (msg_id,),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO inbound_messages (id, phone, text, timestamp, status)
            VALUES (?, ?, ?, ?, 'pending')
            ON CONFLICT(id) DO UPDATE SET
                phone=excluded.phone,
                text=excluded.text,
                timestamp=COALESCE(excluded.timestamp, inbound_messages.timestamp)
            """,
            (msg_id, phone, text, timestamp),
        )
        conn.commit()
    if existing:
        return {"ok": True, "id": msg_id, "status": str(existing["status"] or "pending"), "duplicate": True}
    return {"ok": True, "id": msg_id, "status": "pending", "duplicate": False}


@app.get("/fila")
def fila() -> dict[str, Any]:
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            """
            SELECT id, phone, text, timestamp, status
            FROM inbound_messages
            WHERE status = 'pending'
            ORDER BY COALESCE(timestamp, ''), id
            """
        ).fetchall()
        if rows:
            conn.executemany(
                "UPDATE inbound_messages SET status = 'processing' WHERE id = ? AND status = 'pending'",
                [(str(row["id"]),) for row in rows],
            )
        conn.commit()
    return {"ok": True, "items": [dict(row) for row in rows]}


@app.post("/ack")
def ack(payload: AckPayload) -> dict[str, Any]:
    msg_id = str(payload.id or "").strip()
    if not msg_id:
        return {"ok": False, "error": "invalid_id"}
    with get_conn() as conn:
        before = conn.execute(
            "SELECT status FROM inbound_messages WHERE id = ?",
            (msg_id,),
        ).fetchone()
        conn.execute(
            "UPDATE inbound_messages SET status = 'done' WHERE id = ? AND status != 'done'",
            (msg_id,),
        )
        conn.commit()
    return {
        "ok": True,
        "id": msg_id,
        "status": "done",
        "previous_status": str(before["status"] or "") if before else "",
    }


@app.post("/pending")
def pending(payload: PendingPayload) -> dict[str, Any]:
    msg_id = str(payload.id or "").strip()
    if not msg_id:
        return {"ok": False, "error": "invalid_id"}
    with get_conn() as conn:
        conn.execute(
            "UPDATE inbound_messages SET status = 'pending' WHERE id = ? AND status != 'done'",
            (msg_id,),
        )
        conn.commit()
    return {"ok": True, "id": msg_id, "status": "pending"}
