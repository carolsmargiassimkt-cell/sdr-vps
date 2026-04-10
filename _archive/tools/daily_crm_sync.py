from __future__ import annotations

import argparse
import hashlib
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.local_crm_cache import LocalCRMCache
from crm.pipedrive_client import PipedriveClient
from services.pipeline_stage_sync import sync_pipeline_stage_by_pair
from utils.safe_json import safe_read_json, safe_write_json

EVENTS_FILE = PROJECT_ROOT / "logs" / "events.log"
BLOCKLIST_FILE = PROJECT_ROOT / "logs" / "whatsapp_manual_blocklist.json"
LOCAL_BASE_FILE = PROJECT_ROOT / "data" / "local_crm_base.json"
STATE_FILE = PROJECT_ROOT / "runtime" / "daily_crm_sync_state.json"
REPORT_FILE = PROJECT_ROOT / "logs" / "daily_crm_sync_report.json"
PENDING_FILE = PROJECT_ROOT / "runtime" / "crm_sync_pending_queue.json"


@dataclass
class ParsedEvent:
    timestamp: str
    event: str
    phone: str
    bot: str
    message: str
    result: str
    raw_line: str

    @property
    def dedupe_id(self) -> str:
        return hashlib.sha1(self.raw_line.encode("utf-8")).hexdigest()


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_phone(value: Any) -> str:
    return LocalCRMCache.normalize_phone(value)


def load_state() -> Dict[str, Any]:
    payload = safe_read_json(STATE_FILE) if STATE_FILE.exists() else {}
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("processed_ids", [])
    payload.setdefault("updated_at", "")
    return payload


def save_state(state: Dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = utcnow_iso()
    safe_write_json(STATE_FILE, state)


def load_pending_queue() -> List[Dict[str, Any]]:
    payload = safe_read_json(PENDING_FILE) if PENDING_FILE.exists() else []
    return [dict(item) for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []


def save_pending_queue(items: List[Dict[str, Any]]) -> None:
    PENDING_FILE.parent.mkdir(parents=True, exist_ok=True)
    safe_write_json(PENDING_FILE, items)


def queue_pending(items: List[Dict[str, Any]], pending: List[Dict[str, Any]]) -> None:
    seen = {str(item.get("queue_id") or "").strip() for item in pending}
    for item in items:
        queue_id = str(item.get("queue_id") or "").strip()
        if not queue_id or queue_id in seen:
            continue
        seen.add(queue_id)
        pending.append(item)


def parse_event_line(line: str) -> ParsedEvent | None:
    parts = [segment.strip() for segment in str(line or "").split(" | ")]
    if len(parts) != 6:
        return None
    return ParsedEvent(
        timestamp=parts[0],
        event=parts[1],
        phone=normalize_phone(parts[2]),
        bot=parts[3],
        message=parts[4],
        result=parts[5].lower(),
        raw_line=str(line).rstrip("\n"),
    )


def iter_new_events(processed_ids: set[str]) -> List[ParsedEvent]:
    if not EVENTS_FILE.exists():
        return []
    output: List[ParsedEvent] = []
    with EVENTS_FILE.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            parsed = parse_event_line(line)
            if not parsed:
                continue
            if parsed.result != "success":
                continue
            if parsed.event not in {"SEND_MESSAGE", "REPLY_SENT"}:
                continue
            if parsed.dedupe_id in processed_ids:
                continue
            output.append(parsed)
    return output


def load_local_records() -> List[Dict[str, Any]]:
    payload = safe_read_json(LOCAL_BASE_FILE) if LOCAL_BASE_FILE.exists() else {}
    if not isinstance(payload, dict):
        return []
    return [dict(item) for item in payload.get("records", []) if isinstance(item, dict)]


def find_local_record(records: Iterable[Dict[str, Any]], phone: str) -> Dict[str, Any]:
    normalized = normalize_phone(phone)
    for record in records:
        primary = normalize_phone(record.get("telefone"))
        phones = [normalize_phone(item) for item in (record.get("phones") or [])]
        if normalized and (normalized == primary or normalized in phones):
            return dict(record)
    return {}


def detect_cadence_step(record: Dict[str, Any], event: ParsedEvent) -> int:
    tags = {str(item or "").strip().upper() for item in (record.get("tags") or [])}
    for candidate in range(6, 0, -1):
        if f"WHATSAPP_CAD{candidate}" in tags or f"CAD{candidate}" in tags:
            return candidate
    text = str(event.message or "").upper()
    match = re.search(r"CAD(\d)", text)
    if match:
        return max(1, min(6, int(match.group(1))))
    try:
        return max(1, min(6, int(record.get("sheet_day") or 1)))
    except Exception:
        return 1


def client_failed(client: PipedriveClient) -> bool:
    return int(getattr(client, "last_http_status", 0) or 0) not in {200, 201}


def client_rate_limited(client: PipedriveClient) -> bool:
    return int(getattr(client, "last_http_status", 0) or 0) == 429


def emit(message: str) -> None:
    print(message)


def build_pending_item(*, queue_type: str, queue_id: str, payload: Dict[str, Any], reason: str) -> Dict[str, Any]:
    return {
        "queue_type": queue_type,
        "queue_id": queue_id,
        "payload": payload,
        "reason": reason,
        "created_at": utcnow_iso(),
        "updated_at": utcnow_iso(),
        "attempts": 0,
    }


def sync_send_event(client: PipedriveClient, record: Dict[str, Any], event: ParsedEvent, *, dry_run: bool) -> Dict[str, Any]:
    deal_id = int(record.get("deal_id", 0) or 0)
    person_id = int(record.get("person_id", 0) or 0)
    step = detect_cadence_step(record, event)
    tags = [f"WHATSAPP_CAD{step}", f"CAD{step}"]
    note = f"WHATSAPP_SENT_SDR|CAD{step} | {event.timestamp}"
    outcome = {
        "phone": event.phone,
        "deal_id": deal_id,
        "person_id": person_id,
        "step": step,
        "tags": tags,
        "note": note,
        "dry_run": dry_run,
        "actions": [],
        "pending": None,
    }
    if not deal_id:
        outcome["actions"].append("queue_missing_deal_id")
        outcome["pending"] = build_pending_item(
            queue_type="send_event",
            queue_id=f"send:{event.dedupe_id}",
            payload={"event": event.__dict__, "record": record},
            reason="missing_deal_id",
        )
        return outcome
    if dry_run:
        outcome["actions"].extend(["note", "tag", "stage_sync"])
        return outcome
    note_ok = client.create_note(content=note, person_id=person_id, deal_id=deal_id)
    if note_ok:
        outcome["actions"].append("note")
    elif client_rate_limited(client):
        outcome["actions"].append("queue_rate_limit")
        outcome["pending"] = build_pending_item(
            queue_type="send_event",
            queue_id=f"send:{event.dedupe_id}",
            payload={"event": event.__dict__, "record": record},
            reason="pipedrive_429",
        )
        return outcome
    tag_ok = client.add_tag("deal", deal_id, tags)
    if tag_ok:
        outcome["actions"].append("tag")
    elif client_rate_limited(client):
        outcome["actions"].append("queue_rate_limit")
        outcome["pending"] = build_pending_item(
            queue_type="send_event",
            queue_id=f"send:{event.dedupe_id}",
            payload={"event": event.__dict__, "record": record},
            reason="pipedrive_429",
        )
        return outcome
    sync_pipeline_stage_by_pair(client=client, lead=record, emit=emit)
    outcome["actions"].append("stage_sync")
    return outcome


def sync_reply_event(client: PipedriveClient, record: Dict[str, Any], event: ParsedEvent, *, dry_run: bool) -> Dict[str, Any]:
    deal_id = int(record.get("deal_id", 0) or 0)
    person_id = int(record.get("person_id", 0) or 0)
    note = f"WHATSAPP_REPLY_SDR|{event.timestamp}"
    outcome = {
        "phone": event.phone,
        "deal_id": deal_id,
        "person_id": person_id,
        "note": note,
        "dry_run": dry_run,
        "actions": [],
        "pending": None,
    }
    if not deal_id:
        outcome["actions"].append("queue_missing_deal_id")
        outcome["pending"] = build_pending_item(
            queue_type="reply_event",
            queue_id=f"reply:{event.dedupe_id}",
            payload={"event": event.__dict__, "record": record},
            reason="missing_deal_id",
        )
        return outcome
    if dry_run:
        outcome["actions"].append("note")
        return outcome
    if client.create_note(content=note, person_id=person_id, deal_id=deal_id):
        outcome["actions"].append("note")
        return outcome
    if client_rate_limited(client):
        outcome["actions"].append("queue_rate_limit")
        outcome["pending"] = build_pending_item(
            queue_type="reply_event",
            queue_id=f"reply:{event.dedupe_id}",
            payload={"event": event.__dict__, "record": record},
            reason="pipedrive_429",
        )
        return outcome
    outcome["actions"].append("note_failed")
    return outcome


def sync_blacklist_entry(client: PipedriveClient, records: List[Dict[str, Any]], entry: Dict[str, Any], *, dry_run: bool) -> Dict[str, Any]:
    phone = normalize_phone(entry.get("telefone"))
    record = find_local_record(records, phone)
    outcome = {"phone": phone, "actions": [], "pending": None}
    if not record:
        outcome["actions"].append("queue_missing_local")
        outcome["pending"] = build_pending_item(
            queue_type="blacklist_entry",
            queue_id=f"blacklist:{phone}",
            payload={"entry": entry},
            reason="missing_local_record",
        )
        return outcome
    deal_id = int(record.get("deal_id", 0) or 0)
    person_id = int(record.get("person_id", 0) or 0)
    reason = str(entry.get("reason") or "manual_blacklist").strip().lower()
    outcome.update({"deal_id": deal_id, "person_id": person_id, "reason": reason})
    if not deal_id and not person_id:
        outcome["actions"].append("queue_missing_crm_ids")
        outcome["pending"] = build_pending_item(
            queue_type="blacklist_entry",
            queue_id=f"blacklist:{phone}",
            payload={"entry": entry, "record": record},
            reason="missing_crm_ids",
        )
        return outcome
    if dry_run:
        outcome["actions"].extend(["note", "status_bot"])
        return outcome
    if deal_id:
        client.create_note(content=f"BLACKLIST|{reason}|{utcnow_iso()}", person_id=person_id, deal_id=deal_id)
        outcome["actions"].append("note")
        client.update_deal(deal_id, {"status_bot": "perdido"})
        outcome["actions"].append("status_bot")
        if client_rate_limited(client):
            outcome["actions"].append("queue_rate_limit")
            outcome["pending"] = build_pending_item(
                queue_type="blacklist_entry",
                queue_id=f"blacklist:{phone}",
                payload={"entry": entry, "record": record},
                reason="pipedrive_429",
            )
            return outcome
    if person_id:
        client.update_person(person_id, {"label": ["BLACKLIST"]})
        outcome["actions"].append("person_label")
    return outcome


def sync_terminal_status(client: PipedriveClient, record: Dict[str, Any], *, dry_run: bool) -> Dict[str, Any]:
    status_bot = str(record.get("status_bot") or "").strip().lower()
    deal_id = int(record.get("deal_id", 0) or 0)
    person_id = int(record.get("person_id", 0) or 0)
    outcome = {
        "deal_id": deal_id,
        "person_id": person_id,
        "status_bot": status_bot,
        "actions": [],
        "pending": None,
    }
    if status_bot not in {"agendado", "perdido"}:
        outcome["actions"].append("skip_status")
        return outcome
    if not deal_id:
        outcome["actions"].append("queue_missing_deal_id")
        outcome["pending"] = build_pending_item(
            queue_type="terminal_status",
            queue_id=f"terminal:{status_bot}:{record.get('cnpj') or record.get('telefone') or deal_id}",
            payload={"record": record},
            reason="missing_deal_id",
        )
        return outcome
    if dry_run:
        outcome["actions"].append("status_sync")
        return outcome
    if status_bot == "agendado":
        client.create_note(content=f"AGENDAMENTO|{utcnow_iso()}", person_id=person_id, deal_id=deal_id)
        sync_pipeline_stage_by_pair(client=client, lead=record, emit=emit)
        outcome["actions"].extend(["note", "stage_sync"])
    elif status_bot == "perdido":
        client.update_deal(deal_id, {"status": "lost", "lost_reason": "cadencia_finalizada", "status_bot": "perdido"})
        outcome["actions"].append("deal_lost")
    if client_rate_limited(client):
        outcome["actions"].append("queue_rate_limit")
        outcome["pending"] = build_pending_item(
            queue_type="terminal_status",
            queue_id=f"terminal:{status_bot}:{deal_id}",
            payload={"record": record},
            reason="pipedrive_429",
        )
    return outcome


def replay_pending(client: PipedriveClient, records: List[Dict[str, Any]], pending_items: List[Dict[str, Any]], *, dry_run: bool) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    remaining: List[Dict[str, Any]] = []
    outcomes: List[Dict[str, Any]] = []
    for item in pending_items:
        queue_type = str(item.get("queue_type") or "").strip()
        payload = dict(item.get("payload") or {})
        if queue_type == "send_event":
            event = ParsedEvent(**payload.get("event", {}))
            record = find_local_record(records, event.phone) or dict(payload.get("record") or {})
            result = sync_send_event(client, record, event, dry_run=dry_run)
        elif queue_type == "reply_event":
            event = ParsedEvent(**payload.get("event", {}))
            record = find_local_record(records, event.phone) or dict(payload.get("record") or {})
            result = sync_reply_event(client, record, event, dry_run=dry_run)
        elif queue_type == "blacklist_entry":
            result = sync_blacklist_entry(client, records, dict(payload.get("entry") or {}), dry_run=dry_run)
        elif queue_type == "terminal_status":
            result = sync_terminal_status(client, dict(payload.get("record") or {}), dry_run=dry_run)
        else:
            result = {"actions": ["skip_unknown_pending_type"], "pending": None}
        outcomes.append({"queue_id": item.get("queue_id"), **result})
        if result.get("pending"):
            retry_item = dict(result["pending"])
            retry_item["attempts"] = int(item.get("attempts", 0) or 0) + 1
            retry_item["updated_at"] = utcnow_iso()
            remaining.append(retry_item)
    return remaining, outcomes


def save_report(report: Dict[str, Any]) -> None:
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    safe_write_json(REPORT_FILE, report)


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    state = load_state()
    processed_ids = set(str(item) for item in state.get("processed_ids", []))
    records = load_local_records()
    client = PipedriveClient()

    existing_pending = load_pending_queue()
    pending_after_replay, replay_outcomes = replay_pending(client, records, existing_pending, dry_run=args.dry_run)

    events = iter_new_events(processed_ids)
    event_outcomes: List[Dict[str, Any]] = []
    new_pending: List[Dict[str, Any]] = []
    for event in events:
        record = find_local_record(records, event.phone)
        if not record:
            result = {
                "phone": event.phone,
                "event": event.event,
                "actions": ["queue_missing_local"],
                "pending": build_pending_item(
                    queue_type="reply_event" if event.event == "REPLY_SENT" else "send_event",
                    queue_id=f"event:{event.dedupe_id}",
                    payload={"event": event.__dict__},
                    reason="missing_local_record",
                ),
            }
        elif event.event == "SEND_MESSAGE":
            result = sync_send_event(client, record, event, dry_run=args.dry_run)
        else:
            result = sync_reply_event(client, record, event, dry_run=args.dry_run)
        event_outcomes.append({key: value for key, value in result.items() if key != "pending"})
        if result.get("pending"):
            new_pending.append(result["pending"])
        processed_ids.add(event.dedupe_id)

    blacklist_outcomes: List[Dict[str, Any]] = []
    blocklist_payload = safe_read_json(BLOCKLIST_FILE) if BLOCKLIST_FILE.exists() else []
    for entry in blocklist_payload if isinstance(blocklist_payload, list) else []:
        if not isinstance(entry, dict):
            continue
        result = sync_blacklist_entry(client, records, entry, dry_run=args.dry_run)
        blacklist_outcomes.append({key: value for key, value in result.items() if key != "pending"})
        if result.get("pending"):
            new_pending.append(result["pending"])

    terminal_outcomes: List[Dict[str, Any]] = []
    for record in records:
        result = sync_terminal_status(client, record, dry_run=args.dry_run)
        if result.get("actions") == ["skip_status"]:
            continue
        terminal_outcomes.append({key: value for key, value in result.items() if key != "pending"})
        if result.get("pending"):
            new_pending.append(result["pending"])

    combined_pending = list(pending_after_replay)
    queue_pending(new_pending, combined_pending)
    save_pending_queue(combined_pending)

    trimmed_ids = list(processed_ids)[-5000:]
    save_state({"processed_ids": trimmed_ids, "last_event_count": len(events)})
    report = {
        "generated_at": utcnow_iso(),
        "dry_run": bool(args.dry_run),
        "events_processed": len(events),
        "pending_replayed": len(existing_pending),
        "pending_remaining": len(combined_pending),
        "replay_outcomes": replay_outcomes,
        "event_outcomes": event_outcomes,
        "blacklist_outcomes": blacklist_outcomes,
        "terminal_outcomes": terminal_outcomes,
    }
    save_report(report)
    print(
        f"[DAILY_CRM_SYNC] dry_run={args.dry_run} events={len(events)} "
        f"replayed={len(existing_pending)} pending={len(combined_pending)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
