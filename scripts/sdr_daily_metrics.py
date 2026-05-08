from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.human_handoff import load_human_handoff_state
from core.locked_json_state import locked_load_json


REPORT_DIR = ROOT / "reports"
TODAY = datetime.now().date().isoformat()


def load_any(path: Path, default: Any) -> Any:
    return locked_load_json(path, default)


def parse_date(value: Any) -> str:
    raw = str(value or "").strip()
    return raw[:10] if len(raw) >= 10 else ""


def count_events(metrics: Counter[str]) -> None:
    events = load_any(ROOT / "data" / "email_cadence_events.json", [])
    for row in events if isinstance(events, list) else []:
        if parse_date(row.get("sent_at")) == TODAY:
            metrics["emails_enviados"] += 1


def count_wa(metrics: Counter[str]) -> None:
    for path in (ROOT / "logs" / "whatsapp_outbound_messages.wa1.json", ROOT / "logs" / "whatsapp_outbound_messages.wa2.json"):
        payload = load_any(path, {})
        rows = payload.get("outboundMessages") if isinstance(payload, dict) else []
        for row in rows if isinstance(rows, list) else []:
            if parse_date(row.get("sentAt")) != TODAY:
                continue
            status = str(row.get("status") or "PENDING").upper()
            metrics["warm_enviados"] += 1
            metrics[status] += 1


def count_fallback(metrics: Counter[str]) -> None:
    watch = load_any(ROOT / "data" / "wa_warm_delivery_watch.json", {})
    for row in watch.values() if isinstance(watch, dict) else []:
        if parse_date(row.get("fallback_done_at")) == TODAY:
            metrics["fallback_acionado"] += 1
        if str(row.get("status") or "").upper() == "PENDING":
            metrics["PENDING"] += 1


def count_handoff(metrics: Counter[str]) -> None:
    handoff = load_human_handoff_state()
    for row in handoff.values():
        if parse_date((row or {}).get("created_at")) == TODAY:
            metrics["human_handoff"] += 1


def count_signals(metrics: Counter[str]) -> None:
    path = ROOT / "data" / "sdr_signal_events.jsonl"
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        try:
            row = json.loads(line)
        except Exception:
            continue
        if parse_date(row.get("ts")) != TODAY:
            continue
        signal = str(row.get("signal_type") or "").lower()
        metrics["leads_novos"] += 1 if signal in {"forms_lead", "leadster_form", "mand_lp_form"} else 0
        metrics["reunioes"] += 1 if signal == "calendly_booked" else 0
        metrics["no_show"] += 1 if signal == "no_show" else 0
        metrics["opt_out"] += 1 if signal in {"opt_out", "negative_stop"} else 0
        metrics["invalid_numbers"] += 1 if signal in {"wrong_contact", "invalid_phone"} else 0


def main() -> int:
    metrics: Counter[str] = Counter()
    count_events(metrics)
    count_wa(metrics)
    count_fallback(metrics)
    count_handoff(metrics)
    count_signals(metrics)
    payload = {"date": TODAY, "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "metrics": dict(metrics)}
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = REPORT_DIR / f"sdr_daily_metrics_{TODAY}.json"
    csv_path = REPORT_DIR / f"sdr_daily_metrics_{TODAY}.csv"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value"])
        for key in sorted(metrics):
            writer.writerow([key, metrics[key]])
    print("[SDR_DAILY_METRICS]", json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

