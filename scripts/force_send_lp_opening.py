#!/usr/bin/env python3
"""Force a single LP/Leadster WA opening only after gateway confirms ACK."""
import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.locked_json_state import locked_load_json, locked_save_json
from core.pipedrive_safe import safe_pd_request
from scripts.whatsapp_warm_cadence import (
    STATE_FILE,
    already_sent_today,
    attempted_today,
    is_valid_warm_phone,
    now,
    phone_clean,
    render_warm_message,
)


GATEWAY_URL = "http://127.0.0.1:3000/send"


def post_gateway(phone, text):
    payload = {"number": phone, "text": text, "count_towards_daily_limit": True}
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(GATEWAY_URL, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            body = resp.read().decode("utf-8", "replace")
            return resp.status, json.loads(body or "{}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        try:
            parsed = json.loads(body or "{}")
        except Exception:
            parsed = {"raw": body}
        return exc.code, parsed


def add_note(deal_id, content):
    try:
        safe_pd_request("POST", "/notes", {"deal_id": int(deal_id), "content": content})
    except Exception as exc:
        print("[PIPEDRIVE_NOTE_FAIL]", deal_id, str(exc))


def resolve_deal_by_email(email_value):
    email_value = str(email_value or "").strip().lower()
    if not email_value:
        return None
    search = safe_pd_request("GET", "/persons/search", params={"term": email_value, "exact_match": True}) or {}
    items = (((search.get("data") or {}).get("items")) or [])
    if not items:
        print("[FORCE_DEAL_LOOKUP_NOT_FOUND]", "email=", email_value)
        return None
    person = items[0].get("item") or {}
    person_id = person.get("id")
    if not person_id:
        print("[FORCE_DEAL_LOOKUP_NO_PERSON_ID]", "email=", email_value)
        return None
    deals_payload = safe_pd_request("GET", f"/persons/{person_id}/deals", params={"status": "open"}) or {}
    deals = deals_payload.get("data") or []
    if not deals:
        print("[FORCE_DEAL_LOOKUP_NO_OPEN_DEAL]", "email=", email_value, "person_id=", person_id)
        return None
    deals = sorted(deals, key=lambda item: int(item.get("id") or 0), reverse=True)
    deal = deals[0]
    print("[FORCE_DEAL_LOOKUP_OK]", "email=", email_value, "person_id=", person_id, "deal_id=", deal.get("id"))
    return str(deal.get("id") or "").strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--deal-id")
    parser.add_argument("--email")
    parser.add_argument("--phone", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--send", action="store_true")
    args = parser.parse_args()

    deal_id = str(args.deal_id or "").strip()
    if not deal_id:
        deal_id = resolve_deal_by_email(args.email)
    if not deal_id:
        print("[FORCE_SEND_ABORT]", "reason=missing_deal_id", "email=", args.email or "")
        return 2
    phone = phone_clean(args.phone)
    if not is_valid_warm_phone(phone):
        print("[WA_INVALID_PHONE]", "deal_id=", deal_id, "phone=", args.phone)
        return 2

    state = locked_load_json(STATE_FILE, {})
    if not isinstance(state, dict):
        state = {}
    rec = state.get(deal_id) if isinstance(state.get(deal_id), dict) else {}
    blocked_today, last_sent_at, reason = already_sent_today(state, deal_id, phone)
    if blocked_today and rec.get("send_status") in {"sent_confirmed", "sent"}:
        print("[WA_SKIP_ALREADY_SENT]", "deal_id=", deal_id, "phone=", phone, "last_sent_at=", last_sent_at, "reason=", reason)
        return 0
    if attempted_today(rec):
        print("[WA_SKIP_ALREADY_SENT]", "deal_id=", deal_id, "phone=", phone, "last_attempt_at=", rec.get("last_attempt_at"), "reason=attempted_today")
        return 0

    fake_deal = {"id": int(deal_id), "title": f"Lead LP - {args.name}"}
    msg = render_warm_message(fake_deal, "trafego", 1, phone)
    print("[FORCE_WA_OPENING_TARGET]", "deal_id=", deal_id, "phone=", phone, "apply=", args.apply, "send=", args.send)
    print(msg)

    if not (args.apply and args.send):
        print("[DRY_RUN_SKIP_WRITE] force_send_lp_opening requires --apply --send for live send")
        return 0

    code, payload = post_gateway(phone, msg)
    status = str(payload.get("status") or "").strip().lower()
    message_id = str(payload.get("message_id") or payload.get("id") or "").strip()
    print("[FORCE_WA_GATEWAY_RESULT]", "code=", code, "status=", status, "message_id=", message_id)

    if code != 200 or status != "sent":
        print("[FORCE_WA_NOT_CONFIRMED]", "deal_id=", deal_id, "phone=", phone, "status=", status, "message_id=", message_id)
        if status == "pending_unconfirmed":
            new_rec = dict(rec)
            new_rec.update({
                "origin": "trafego",
                "phone": phone,
                "name": args.name,
                "step": int(rec.get("step") or 0),
                "wa1_sent": bool(rec.get("wa1_sent")),
                "message_id": message_id,
                "send_status": "pending_unconfirmed",
                "needs_manual_check": True,
                "last_attempt_at": now().strftime("%Y-%m-%d %H:%M:%S"),
                "require_reply_before_next_step": True,
                "do_not_advance_without_inbound": True,
                "last_message_preview": msg[:180],
                "title": fake_deal["title"],
            })
            state[deal_id] = new_rec
            locked_save_json(STATE_FILE, state)
        return 3

    now_str = now().strftime("%Y-%m-%d %H:%M:%S")
    new_rec = dict(rec)
    new_rec.update({
        "origin": "trafego",
        "phone": phone,
        "name": args.name,
        "step": 1,
        "wa1_sent": True,
        "last_sent_step_whatsapp": 1,
        "last_sent_at": now_str,
        "message_id": message_id,
        "send_status": "sent_confirmed",
        "require_reply_before_next_step": True,
        "do_not_advance_without_inbound": True,
        "last_message_preview": msg[:180],
        "title": fake_deal["title"],
    })
    state[deal_id] = new_rec
    locked_save_json(STATE_FILE, state)
    add_note(deal_id, "[WA_OPENING_CONFIRMED] abertura enviada e confirmada.<br>phone=%s<br>message_id=%s" % (phone, message_id))
    print("[WA_OPENING_CONFIRMED]", "deal_id=", deal_id, "phone=", phone, "message_id=", message_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
