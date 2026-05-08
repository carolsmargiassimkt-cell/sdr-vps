from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FILES = [
    ROOT / "logs" / "whatsapp_outbound_messages.wa1.json",
    ROOT / "logs" / "whatsapp_outbound_messages.wa2.json",
    ROOT / "logs" / "whatsapp_outbound_messages.json",
]


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) in {10, 11}:
        digits = "55" + digits
    return digits


def parse_ts(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def load_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8") or "{}")
    except Exception as exc:
        print(f"[WA_PENDING_AUDIT_READ_FAIL] file={path} error={exc}")
        return []
    rows = payload.get("outboundMessages") if isinstance(payload, dict) else payload
    return [dict(row) for row in rows] if isinstance(rows, list) else []


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only audit for WhatsApp outbound PENDING messages.")
    parser.add_argument("--file", default="", help="Specific outbound state JSON file.")
    parser.add_argument("--number", default="", help="Filter by phone number/JID.")
    parser.add_argument("--message-id", default="", help="Filter by WhatsApp message id.")
    parser.add_argument("--hours", type=int, default=24)
    args = parser.parse_args()

    files = [Path(args.file)] if args.file else DEFAULT_FILES
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, int(args.hours or 24)))
    number_filter = normalize_phone(args.number)
    message_filter = str(args.message_id or "").strip()

    total = 0
    pending = 0
    for file_path in files:
        rows = load_rows(file_path)
        if not rows:
            continue
        for row in rows:
            sent_at = parse_ts(row.get("sentAt"))
            if sent_at and sent_at < cutoff:
                continue
            message_id = str(row.get("message_id") or "").strip()
            jid = str(row.get("jid") or "").strip()
            if message_filter and message_id != message_filter:
                continue
            if number_filter and number_filter not in normalize_phone(jid):
                continue
            total += 1
            status = str(row.get("status") or "PENDING").upper()
            if status == "PENDING":
                pending += 1
                print(
                    "[WA_PENDING]",
                    f"file={file_path}",
                    f"jid={jid}",
                    f"message_id={message_id}",
                    f"status={status}",
                    f"sentAt={row.get('sentAt') or '-'}",
                    f"gateway_status={row.get('gateway_status') or '-'}",
                    f"preview={str(row.get('text_preview') or '')[:120]}",
                )
    print(f"[WA_PENDING_SUMMARY] total_scanned={total} pending={pending} hours={args.hours}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
