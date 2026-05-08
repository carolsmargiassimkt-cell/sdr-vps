from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.human_handoff import load_human_handoff_state
from core.locked_json_state import locked_load_json
from core.pipedrive_safe import safe_pd_request


REPORT_DIR = ROOT / "reports"
WATCH_FILE = ROOT / "data" / "wa_warm_delivery_watch.json"
MEETING_FILE = ROOT / "data" / "sdr_meeting_reminders.json"
WARM_FILE = ROOT / "data" / "whatsapp_warm_cadence.json"
WA_BLOCKLIST = ROOT / "data" / "whatsapp_manual_blocklist.json"


def parse_dt(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) in {10, 11}:
        digits = "55" + digits
    return digits


def load_dict(path: Path) -> dict[str, Any]:
    payload = locked_load_json(path, {})
    return payload if isinstance(payload, dict) else {}


def add(findings: list[dict[str, Any]], severity: str, check: str, detail: str, **extra: Any) -> None:
    findings.append({"severity": severity, "check": check, "detail": detail, **extra})
    tag = "[AUDIT_CRITICAL]" if severity == "CRITICAL" else "[AUDIT_WARNING]" if severity in {"HIGH", "MEDIUM"} else "[AUDIT_OK]"
    print(tag, f"check={check}", detail)


def audit_local(findings: list[dict[str, Any]]) -> None:
    watch = load_dict(WATCH_FILE)
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=30)
    for message_id, row in watch.items():
        if not isinstance(row, dict):
            continue
        sent_at = parse_dt(row.get("sent_at"))
        if str(row.get("status") or "").upper() == "PENDING" and sent_at and sent_at < cutoff and not row.get("fallback_done"):
            add(findings, "CRITICAL", "wa_pending_gt_30min", f"message_id={message_id} deal_id={row.get('deal_id')}", message_id=message_id, deal_id=row.get("deal_id"))
        if row.get("fallback_done") and not row.get("fallback_done_at"):
            add(findings, "MEDIUM", "fallback_without_timestamp", f"message_id={message_id}", message_id=message_id)

    handoff = load_human_handoff_state()
    blocklist = load_dict(WA_BLOCKLIST)
    block_phones = {normalize_phone(k) for k in blocklist.keys()} | {normalize_phone(v.get("phone")) for v in blocklist.values() if isinstance(v, dict)}
    for phone, row in handoff.items():
        if normalize_phone(phone) not in block_phones:
            add(findings, "MEDIUM", "human_handoff_without_blocklist", f"phone={phone}", phone=phone, deal_id=(row or {}).get("deal_id"))

    warm = load_dict(WARM_FILE)
    meeting = load_dict(MEETING_FILE)
    for deal_id, row in warm.items():
        if str(deal_id) in meeting and not (row or {}).get("stopped"):
            add(findings, "HIGH", "meeting_booked_warm_active", f"deal_id={deal_id}", deal_id=deal_id)


def audit_crm(findings: list[dict[str, Any]], limit: int = 500) -> None:
    response = safe_pd_request("GET", "/deals", params={"status": "open", "limit": limit, "pipeline_id": 7})
    deals = list((response or {}).get("data") or [])
    emails: dict[str, list[int]] = defaultdict(list)
    phones: dict[str, list[int]] = defaultdict(list)
    org_names: Counter[str] = Counter()
    for deal in deals:
        deal_id = int(deal.get("id") or 0)
        if not deal.get("person_id"):
            add(findings, "HIGH", "deal_without_person", f"deal_id={deal_id}", deal_id=deal_id)
        if not deal.get("org_id"):
            add(findings, "MEDIUM", "deal_without_org", f"deal_id={deal_id}", deal_id=deal_id)
        person = deal.get("person_id") if isinstance(deal.get("person_id"), dict) else {}
        org = deal.get("org_id") if isinstance(deal.get("org_id"), dict) else {}
        org_name = str(org.get("name") or "").strip()
        if org_name:
            org_names[org_name.lower()] += 1
            if any(token in org_name.lower() for token in ("<style", "{", "}", "padding:", "#outlook")):
                add(findings, "CRITICAL", "org_html_css", f"deal_id={deal_id} org={org_name[:80]}", deal_id=deal_id)
        for item in list(person.get("email") or []):
            value = str(item.get("value") if isinstance(item, dict) else item or "").strip().lower()
            if value:
                emails[value].append(deal_id)
        for item in list(person.get("phone") or []):
            value = normalize_phone(item.get("value") if isinstance(item, dict) else item)
            if value:
                phones[value].append(deal_id)
        labels = str(deal.get("label") or "").lower()
        status = str(deal.get("status_bot") or deal.get("status_sdr") or "").lower()
        if "conversando" in labels and "wa_respondeu" not in labels and "respondido" not in labels and "respond" not in status:
            add(findings, "MEDIUM", "conversando_without_wa_respondeu", f"deal_id={deal_id}", deal_id=deal_id)
    for email, deal_ids in emails.items():
        if len(set(deal_ids)) > 1:
            add(findings, "MEDIUM", "duplicate_person_email", f"email={email} deals={sorted(set(deal_ids))[:8]}", email=email)
    for phone, deal_ids in phones.items():
        if len(set(deal_ids)) > 1:
            add(findings, "HIGH", "duplicate_phone", f"phone={phone} deals={sorted(set(deal_ids))[:8]}", phone=phone)
    for org_name, count in org_names.items():
        if count > 3:
            add(findings, "MEDIUM", "duplicate_org_name", f"org={org_name} count={count}", org=org_name, count=count)


def main() -> int:
    findings: list[dict[str, Any]] = []
    audit_local(findings)
    audit_crm(findings)
    summary = Counter(item["severity"] for item in findings)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "summary": dict(summary), "findings": findings}
    (REPORT_DIR / "sdr_integrity_latest.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[AUDIT_OK]" if not findings else "[AUDIT_WARNING]", f"total_findings={len(findings)} summary={dict(summary)}")
    return 1 if summary.get("CRITICAL") else 0


if __name__ == "__main__":
    raise SystemExit(main())

