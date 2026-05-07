from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


BASE_DIR = Path(__file__).resolve().parents[1]
EVENTS_FILE = BASE_DIR / "data" / "sdr_signal_events.jsonl"
TZ = ZoneInfo("America/Sao_Paulo")


def parse_ts(value):
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        return parsed.astimezone(TZ) if parsed.tzinfo else parsed.replace(tzinfo=TZ)
    except Exception:
        return None


def iter_events():
    if not EVENTS_FILE.exists():
        return
    with EVENTS_FILE.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            if isinstance(payload, dict):
                yield payload


def main() -> int:
    now = datetime.now(TZ)
    today = now.date()
    week_start = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)

    counts = Counter()
    deals_sem_match = []
    meetings_today = 0
    meetings_week = 0
    meetings_month = 0

    for event in iter_events() or []:
        ts = parse_ts(event.get("ts"))
        if ts and ts.date() == today:
            counts["total_sinais_hoje"] += 1
            counts[str(event.get("signal_type") or "unknown")] += 1
            counts[str(event.get("temperature") or "unknown")] += 1
        if int(event.get("deal_id") or 0) <= 0 and len(deals_sem_match) < 20:
            deals_sem_match.append(event)
        if str(event.get("signal_type") or "") == "calendly_booked" and ts:
            if ts.date() == today:
                meetings_today += 1
            if ts.date() >= week_start:
                meetings_week += 1
            if ts.date() >= month_start:
                meetings_month += 1

    for key in [
        "total_sinais_hoje",
        "email_open",
        "email_click",
        "forms_lead",
        "leadster_form",
        "calendly_booked",
        "calendly_canceled",
        "MORNO_LEVE",
        "MORNO_FORTE",
        "MORNO",
        "QUENTE",
        "QUENTE_PRIORIDADE",
    ]:
        print(f"{key}={counts.get(key, 0)}")
    print(f"reunioes_agendadas_hoje={meetings_today}")
    print(f"reunioes_agendadas_semana={meetings_week}")
    print(f"reunioes_agendadas_mes={meetings_month}")
    print(f"deals_sem_match={len(deals_sem_match)}")
    for event in deals_sem_match[:10]:
        print("deal_sem_match_exemplo=", json.dumps(event, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
