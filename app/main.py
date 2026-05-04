from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from email_cadence.routes import router as email_cadence_router
from email_cadence.forms_webhook import router as forms_router
from fastapi import FastAPI
from pydantic import BaseModel

from core.automation_freeze import should_drop_inbound


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "inbound_queue.sqlite3"
PROCESSING_LEASE_SECONDS = 300

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
        columns = {
            str(row["name"] or "").strip()
            for row in conn.execute("PRAGMA table_info(inbound_messages)").fetchall()
        }
        if "processing_started_at" not in columns:
            conn.execute("ALTER TABLE inbound_messages ADD COLUMN processing_started_at TEXT")
        conn.commit()


class InboundPayload(BaseModel):
    id: str | None = None
    msg_id: str | None = None
    messageId: str | None = None
    phone: str
    text: str | None = None
    message: str | None = None
    timestamp: str | None = None
    remoteJid: str | None = None
    remoteJidAlt: str | None = None
    source: str | None = None


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
    remote_jid = str(payload.remoteJid or "").strip()
    remote_jid_alt = str(payload.remoteJidAlt or "").strip()
    source = str(payload.source or "").strip() or "unknown"
    missing_fields: list[str] = []
    if not msg_id:
        missing_fields.append("id")
    if not phone:
        missing_fields.append("phone")
    if not text:
        missing_fields.append("text")
    if missing_fields:
        print(
            "[INBOUND_REJEITADO]",
            f"missing={','.join(missing_fields)}",
            f"id={msg_id or '-'}",
            f"phone={phone or '-'}",
            f"text_len={len(text)}",
            f"remote_jid={remote_jid or '-'}",
            f"remote_jid_alt={remote_jid_alt or '-'}",
            f"source={source}",
        )
        return {"ok": False, "error": "invalid_payload", "missing_fields": missing_fields}

    if should_drop_inbound():
        print(
            "[INBOUND_FREEZE_DROP]",
            f"id={msg_id}",
            f"phone={phone}",
            f"source={source}",
        )
        return {"ok": True, "id": msg_id, "status": "paused", "duplicate": False}

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
    print(
        "[INBOUND_ACEITO]",
        f"id={msg_id}",
        f"phone={phone}",
        f"duplicate={bool(existing)}",
        f"source={source}",
    )
    if existing:
        return {"ok": True, "id": msg_id, "status": str(existing["status"] or "pending"), "duplicate": True}
    return {"ok": True, "id": msg_id, "status": "pending", "duplicate": False}


@app.get("/fila")
def fila() -> dict[str, Any]:
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        lease_cutoff = (datetime.now(timezone.utc) - timedelta(seconds=PROCESSING_LEASE_SECONDS)).isoformat()
        reclaimed = conn.execute(
            """
            UPDATE inbound_messages
            SET status = 'pending', processing_started_at = NULL
            WHERE status = 'processing'
              AND (
                processing_started_at IS NULL
                OR processing_started_at = ''
                OR processing_started_at < ?
              )
            """,
            (lease_cutoff,),
        ).rowcount or 0
        rows = conn.execute(
            """
            SELECT id, phone, text, timestamp, status, processing_started_at
            FROM inbound_messages
            WHERE status = 'pending'
            ORDER BY COALESCE(timestamp, ''), id
            """
        ).fetchall()
        if rows:
            locked_at = datetime.now(timezone.utc).isoformat()
            conn.executemany(
                """
                UPDATE inbound_messages
                SET status = 'processing', processing_started_at = ?
                WHERE id = ? AND status = 'pending'
                """,
                [(locked_at, str(row["id"])) for row in rows],
            )
        conn.commit()
    if reclaimed:
        print(f"[FILA_REQUEUE_STALE] reclaimed={reclaimed}")
    if rows:
        print(f"[FILA_CLAIM] claimed={len(rows)}")
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
            """
            UPDATE inbound_messages
            SET status = 'done', processing_started_at = NULL
            WHERE id = ? AND status != 'done'
            """,
            (msg_id,),
        )
        conn.commit()
    print(
        "[ACK_RESULT]",
        f"id={msg_id}",
        f"previous_status={str(before['status'] or '') if before else ''}",
    )
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
            """
            UPDATE inbound_messages
            SET status = 'pending', processing_started_at = NULL
            WHERE id = ? AND status != 'done'
            """,
            (msg_id,),
        )
        conn.commit()
    print("[PENDING_RESULT]", f"id={msg_id}", "status=pending")
    return {"ok": True, "id": msg_id, "status": "pending"}


from app.webhooks_pipedrive import router as pipedrive_router
app.include_router(pipedrive_router)


app.include_router(email_cadence_router)
app.include_router(forms_router)
