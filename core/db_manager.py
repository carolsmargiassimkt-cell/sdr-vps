from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, List

from config.config_loader import get_database_path


class DBManager:
    def __init__(self, db_path: Path | str | None = None) -> None:
        self.db_path = Path(db_path or get_database_path())
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS leads (
                    record_key TEXT PRIMARY KEY,
                    lead_id TEXT,
                    phone TEXT,
                    email TEXT,
                    linkedin_profile TEXT,
                    status TEXT,
                    blocked INTEGER DEFAULT 0,
                    bot TEXT,
                    cadence_step INTEGER DEFAULT 0,
                    updated_at TEXT,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS queue (
                    id TEXT PRIMARY KEY,
                    channel TEXT,
                    phone TEXT,
                    email TEXT,
                    lead_id TEXT,
                    status TEXT,
                    timestamp TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    attempts INTEGER DEFAULT 0,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 0,
                    retry_after TEXT,
                    last_attempt_at TEXT,
                    last_error_at TEXT,
                    error TEXT,
                    source TEXT,
                    dedupe_key TEXT,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status, channel);
                CREATE INDEX IF NOT EXISTS idx_queue_dedupe ON queue(dedupe_key);
                CREATE TABLE IF NOT EXISTS dispatch_log (
                    dispatch_key TEXT PRIMARY KEY,
                    lead_key TEXT,
                    channel TEXT,
                    cadence_step TEXT,
                    created_at TEXT,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_dispatch_lead ON dispatch_log(lead_key, channel, cadence_step);
                CREATE TABLE IF NOT EXISTS enrichment_log (
                    enrichment_key TEXT PRIMARY KEY,
                    cnpj TEXT,
                    lead_id TEXT,
                    status TEXT,
                    created_at TEXT,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_enrichment_cnpj ON enrichment_log(cnpj);
                CREATE TABLE IF NOT EXISTS runtime_state (
                    state_key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT
                );
                """
            )

    def load_queue_items(self) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT payload_json FROM queue ORDER BY COALESCE(timestamp, created_at, updated_at, id)").fetchall()
        return [self._decode_payload(row["payload_json"]) for row in rows]

    def replace_queue_items(self, items: Iterable[Dict[str, Any]]) -> None:
        payload = [dict(item or {}) for item in items]
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM queue")
            for item in payload:
                conn.execute(
                    """
                    INSERT INTO queue (
                        id, channel, phone, email, lead_id, status, timestamp, created_at, updated_at,
                        attempts, retry_count, max_retries, retry_after, last_attempt_at, last_error_at,
                        error, source, dedupe_key, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    self._queue_tuple(item),
                )
            conn.execute("COMMIT")

    def load_leads_map(self) -> Dict[str, Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT record_key, payload_json FROM leads").fetchall()
        return {str(row["record_key"]): self._decode_payload(row["payload_json"]) for row in rows}

    def replace_leads_map(self, mapping: Dict[str, Dict[str, Any]]) -> None:
        payload = {str(key): dict(value or {}) for key, value in (mapping or {}).items()}
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM leads")
            for record_key, item in payload.items():
                conn.execute(
                    """
                    INSERT INTO leads (
                        record_key, lead_id, phone, email, linkedin_profile, status, blocked, bot,
                        cadence_step, updated_at, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record_key,
                        str(item.get("lead_id", "")).strip(),
                        str(item.get("telefone") or item.get("phone") or "").strip(),
                        str(item.get("email", "")).strip().lower(),
                        str(item.get("linkedin_profile", "")).strip().lower(),
                        str(item.get("status", "")).strip(),
                        1 if bool(item.get("blocked")) else 0,
                        str(item.get("bot", "")).strip(),
                        int(item.get("cadence_step", 0) or 0),
                        str(item.get("updated_at", "")).strip(),
                        json.dumps(item, ensure_ascii=False),
                    ),
                )
            conn.execute("COMMIT")

    def dispatch_exists(self, dispatch_key: str) -> bool:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT 1 FROM dispatch_log WHERE dispatch_key = ?", (str(dispatch_key).strip(),)).fetchone()
        return row is not None

    def load_dispatch_items(self) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT payload_json FROM dispatch_log WHERE channel != 'cadence' ORDER BY created_at, dispatch_key"
            ).fetchall()
        return [self._decode_payload(row["payload_json"]) for row in rows]

    def replace_dispatch_items(self, items: Iterable[Dict[str, Any]]) -> None:
        payload = [dict(item or {}) for item in items]
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM dispatch_log WHERE channel != 'cadence'")
            for item in payload:
                conn.execute(
                    """
                    INSERT INTO dispatch_log (
                        dispatch_key, lead_key, channel, cadence_step, created_at, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(item.get("id") or item.get("dispatch_key") or "").strip(),
                        str(item.get("identity") or item.get("lead_key") or "").strip(),
                        str(item.get("channel", "")).strip(),
                        str(item.get("cadence_step", "")).strip(),
                        str(item.get("created_at", "")).strip(),
                        json.dumps(item, ensure_ascii=False),
                    ),
                )
            conn.execute("COMMIT")

    def insert_dispatch_log(
        self,
        *,
        dispatch_key: str,
        lead_key: str,
        channel: str,
        cadence_step: str,
        created_at: str,
        payload: Dict[str, Any],
    ) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO dispatch_log (
                    dispatch_key, lead_key, channel, cadence_step, created_at, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(dispatch_key).strip(),
                    str(lead_key).strip(),
                    str(channel).strip(),
                    str(cadence_step).strip(),
                    str(created_at).strip(),
                    json.dumps(payload or {}, ensure_ascii=False),
                ),
            )

    def reserve_dispatch_log(
        self,
        *,
        dispatch_key: str,
        lead_key: str,
        channel: str,
        cadence_step: str,
        created_at: str,
        payload: Dict[str, Any],
    ) -> bool:
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO dispatch_log (
                    dispatch_key, lead_key, channel, cadence_step, created_at, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(dispatch_key).strip(),
                    str(lead_key).strip(),
                    str(channel).strip(),
                    str(cadence_step).strip(),
                    str(created_at).strip(),
                    json.dumps(payload or {}, ensure_ascii=False),
                ),
            )
        return int(getattr(cursor, "rowcount", 0) or 0) > 0

    def delete_dispatch_log(self, dispatch_key: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM dispatch_log WHERE dispatch_key = ?", (str(dispatch_key).strip(),))

    def get_latest_dispatch_created_at(self, channel: str) -> str:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT created_at
                FROM dispatch_log
                WHERE channel = ?
                ORDER BY created_at DESC, dispatch_key DESC
                LIMIT 1
                """,
                (str(channel or "").strip(),),
            ).fetchone()
        return str(row["created_at"] or "").strip() if row else ""

    def get_runtime_state(self, state_key: str) -> Dict[str, Any]:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT value_json FROM runtime_state WHERE state_key = ?",
                (str(state_key or "").strip(),),
            ).fetchone()
        if not row:
            return {}
        return self._decode_payload(row["value_json"])

    def set_runtime_state(self, state_key: str, payload: Dict[str, Any]) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runtime_state (state_key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(state_key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = excluded.updated_at
                """,
                (
                    str(state_key or "").strip(),
                    json.dumps(payload or {}, ensure_ascii=False),
                    str((payload or {}).get("updated_at") or ""),
                ),
            )

    @staticmethod
    def _decode_payload(raw: str) -> Dict[str, Any]:
        try:
            payload = json.loads(str(raw or "").strip() or "{}")
        except Exception:
            payload = {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _queue_tuple(item: Dict[str, Any]) -> tuple[Any, ...]:
        return (
            str(item.get("id", "")).strip(),
            str(item.get("channel", "")).strip(),
            str(item.get("phone", "")).strip(),
            str(item.get("email", "")).strip().lower(),
            str(item.get("lead_id", "")).strip(),
            str(item.get("status", "")).strip(),
            str(item.get("timestamp", "")).strip(),
            str(item.get("created_at", "")).strip(),
            str(item.get("updated_at", "")).strip(),
            int(item.get("attempts", 0) or 0),
            int(item.get("retry_count", 0) or 0),
            int(item.get("max_retries", 0) or 0),
            str(item.get("retry_after", "")).strip(),
            str(item.get("last_attempt_at", "")).strip(),
            str(item.get("last_error_at", "")).strip(),
            str(item.get("error", "")).strip(),
            str(item.get("source", "")).strip(),
            str(item.get("dedupe_key", "")).strip(),
            json.dumps(item, ensure_ascii=False),
        )


_MANAGER_LOCK = threading.Lock()
_MANAGER: DBManager | None = None


def get_db_manager(db_path: Path | str | None = None) -> DBManager:
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER is None or (db_path is not None and Path(db_path) != _MANAGER.db_path):
            _MANAGER = DBManager(db_path=db_path)
        return _MANAGER
