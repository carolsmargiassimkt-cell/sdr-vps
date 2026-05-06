#!/usr/bin/env python3
"""Validate the WhatsApp inbound auto-reply guard contract.

This script intentionally exercises the FastAPI/Flask handler endpoint instead
of sending directly to the WhatsApp gateway, so the official intent/router/pitch
flow remains in the path.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError


REPO_ROOT = Path(__file__).resolve().parents[1]
INBOX_HANDLER = REPO_ROOT / "inbox_handler.py"
HANDLER_URL = "http://127.0.0.1:5001/inbound"


def post_json(url: str, payload: dict, timeout: int = 25) -> tuple[int, dict]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib_request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, json.loads(body or "{}")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body or "{}")
        except Exception:
            parsed = {"raw": body}
        return exc.code, parsed
    except URLError as exc:
        return 0, {"error": str(exc)}


def assert_gateway_contract_in_source() -> None:
    source = INBOX_HANDLER.read_text(encoding="utf-8", errors="replace")
    required = (
        '"number": number',
        '"text": clean_text',
        '"count_towards_daily_limit": False',
        "[INBOX_SEND_WHITELIST_RESULT]",
        "[OFFICIAL_REPLY_SELECTED]",
        "[SPINTAX_RENDERED]",
        "prepare_whatsapp_reply_text",
    )
    missing = [item for item in required if item not in source]
    if missing:
        raise AssertionError(f"inbox_handler.py missing gateway/official-flow markers: {missing}")


def run_live() -> None:
    now = int(time.time())
    whitelist_payload = {
        "phone": "35920002020",
        "message": "da onde fala?",
        "timestamp": now,
        "source": "manual_test",
        "force_process": True,
    }
    status, body = post_json(HANDLER_URL, whitelist_payload)
    print(f"WHITELIST status={status} body={json.dumps(body, ensure_ascii=False)}")
    if status != 200 or body.get("auto_reply_blocked") is True or body.get("ok") is not True:
        raise AssertionError("whitelist inbound did not pass auto-reply guards")

    non_whitelist_payload = {
        "phone": "31999999999",
        "message": "da onde fala?",
        "timestamp": now,
        "source": "manual_test",
        "force_process": True,
    }
    status, body = post_json(HANDLER_URL, non_whitelist_payload)
    print(f"NON_WHITELIST status={status} body={json.dumps(body, ensure_ascii=False)}")
    if status != 200 or body.get("auto_reply_blocked") is not True:
        raise AssertionError("non-whitelist no-CRM inbound was not blocked")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="POST to the local inbound handler")
    args = parser.parse_args()

    assert_gateway_contract_in_source()
    print("SOURCE_CONTRACT_OK")
    if args.live:
        run_live()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as exc:
        print(f"VALIDATION_FAILED: {exc}", file=sys.stderr)
        raise SystemExit(1)
