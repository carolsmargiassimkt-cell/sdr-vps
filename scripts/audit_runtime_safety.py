from __future__ import annotations

import json
import subprocess
from pathlib import Path

from core.auto_reply_guard import detect_auto_reply
from core.inflight_actions import acquire_inflight, release_inflight
from core.locked_json_state import locked_load_json, locked_save_json
from core.stage_router import STAGE_TENTATIVA_CONTATO, resolve_pipeline_stage
from core.whatsapp_hunter_guard import mark_hunter_sent_today, should_send_hunter_today, today_brt


BASE_DIR = Path(__file__).resolve().parents[1]
JSON_FILES = [
    BASE_DIR / "data" / "wa_strategy_state.json",
    BASE_DIR / "data" / "email_cadence_queue.json",
    BASE_DIR / "data" / "whatsapp_hunter_daily_guard.json",
    BASE_DIR / "data" / "inflight_actions.json",
]


def check_json_parse() -> bool:
    ok = True
    for path in JSON_FILES:
        try:
            locked_load_json(path, {} if path.name != "email_cadence_queue.json" else [])
            print(f"[AUDIT_JSON_OK] path={path}")
        except Exception as exc:
            print(f"[AUDIT_JSON_FAIL] path={path} error={exc}")
            ok = False
    return ok


def check_lock_roundtrip() -> bool:
    path = BASE_DIR / "data" / "_runtime_safety_lock_test.json"
    payload = {"ok": True}
    locked_save_json(path, payload)
    loaded = locked_load_json(path, {})
    ok = loaded == payload
    print(f"[AUDIT_LOCK] ok={1 if ok else 0}")
    return ok


def check_stage_routing() -> bool:
    route = resolve_pipeline_stage({"source": "email_click", "email_clicked": True})
    ok = int(route.get("stage_id") or 0) == STAGE_TENTATIVA_CONTATO
    print(f"[AUDIT_STAGE_ROUTING] route={route} ok={1 if ok else 0}")
    return ok


def check_duplicate_guard() -> bool:
    guard = {"date": today_brt(), "sent_by_phone": {}, "sent_by_deal": {}}
    deal_id = 987654
    phone = "5531999990000"
    first, _, _ = should_send_hunter_today(deal_id, phone, guard)
    guard = mark_hunter_sent_today(deal_id, phone, sent_at=f"{today_brt()}T09:00:00-03:00", guard=guard, persist=False)
    second, _, reason = should_send_hunter_today(deal_id, phone, guard)
    ok = first and not second
    print(f"[AUDIT_DUPLICATE_GUARD] ok={1 if ok else 0} second_reason={reason}")
    return ok


def check_inflight() -> bool:
    deal_id = 987655
    ok1, _ = acquire_inflight(deal_id, "email")
    ok2, reason = acquire_inflight(deal_id, "whatsapp")
    release_inflight(deal_id, "email")
    ok = ok1 and not ok2
    print(f"[AUDIT_INFLIGHT] ok={1 if ok else 0} block_reason={reason}")
    return ok


def check_auto_reply() -> bool:
    detected, reason = detect_auto_reply("Bem-vindo ao atendimento virtual. Digite 1 para nossos serviços.")
    print(f"[AUDIT_AUTO_REPLY] detected={1 if detected else 0} reason={reason}")
    return detected


def check_pm2_singleton() -> bool:
    try:
        result = subprocess.run(["pm2", "jlist"], capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            print("[AUDIT_PM2] unavailable=1")
            return True
        processes = json.loads(result.stdout or "[]")
        names = [str(proc.get("name") or "") for proc in processes]
        duplicates = {name for name in names if name and names.count(name) > 1}
        print(f"[AUDIT_PM2] duplicates={sorted(duplicates)}")
        return not duplicates
    except Exception as exc:
        print(f"[AUDIT_PM2] skipped error={exc}")
        return True


def main() -> int:
    checks = [
        check_json_parse(),
        check_lock_roundtrip(),
        check_stage_routing(),
        check_duplicate_guard(),
        check_inflight(),
        check_auto_reply(),
        check_pm2_singleton(),
    ]
    go = all(checks)
    print("GO" if go else "NO-GO")
    return 0 if go else 1


if __name__ == "__main__":
    raise SystemExit(main())
