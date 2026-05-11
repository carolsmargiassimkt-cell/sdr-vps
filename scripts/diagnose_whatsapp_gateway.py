#!/usr/bin/env python3
"""Read-only WhatsApp gateway diagnostic, with optional explicit test send."""
import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


BASE_URL = "http://127.0.0.1:3000"


def request_json(method, path, payload=None, timeout=20):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE_URL + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", "replace")
            return resp.status, json.loads(body or "{}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        try:
            parsed = json.loads(body or "{}")
        except Exception:
            parsed = {"raw": body}
        return exc.code, parsed
    except Exception as exc:
        return 0, {"error": str(exc)}


def conclusion_from_status(status_code, status_payload):
    if status_code == 0:
        return "GATEWAY_OFFLINE"
    if status_payload.get("session_invalid") or status_payload.get("needs_qr"):
        return "SESSION_NEEDS_RECONNECT"
    if not status_payload.get("connected"):
        return "GATEWAY_OFFLINE"
    return "GATEWAY_ONLINE"


def poll_outbound(message_id, seconds=45):
    deadline = time.time() + seconds
    last = {}
    while time.time() < deadline:
        qs = urllib.parse.urlencode({"message_id": message_id})
        code, payload = request_json("GET", f"/outbound-status?{qs}", timeout=10)
        last = payload
        status = str(payload.get("status") or "").upper()
        print("[GATEWAY_ACK_POLL]", "code=", code, "message_id=", message_id, "status=", status)
        if status in {"SERVER_ACK", "DELIVERED", "READ", "PLAYED"}:
            return payload
        time.sleep(3)
    return last


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--send-test", nargs=2, metavar=("PHONE", "TEXT"), help="explicitly send a live test message")
    args = parser.parse_args()

    code, status = request_json("GET", "/status", timeout=10)
    print("[GATEWAY_STATUS]", "code=", code)
    print(json.dumps(status, indent=2, ensure_ascii=False))
    base_conclusion = conclusion_from_status(code, status)
    if not args.send_test:
        print(base_conclusion if base_conclusion != "GATEWAY_ONLINE" else "GATEWAY_OK_STATUS_ONLY")
        return 0 if base_conclusion == "GATEWAY_ONLINE" else 1

    if base_conclusion != "GATEWAY_ONLINE":
        print(base_conclusion)
        return 1

    phone, text = args.send_test
    send_code, send_payload = request_json(
        "POST",
        "/send",
        {"number": phone, "text": text, "count_towards_daily_limit": False},
        timeout=90,
    )
    print("[GATEWAY_SEND_TEST]", "code=", send_code)
    print(json.dumps(send_payload, indent=2, ensure_ascii=False))
    status_value = str(send_payload.get("status") or "").lower()
    message_id = str(send_payload.get("message_id") or "").strip()
    if status_value == "sent":
        print("GATEWAY_OK_CONFIRMED")
        return 0
    if not message_id:
        print("GATEWAY_PENDING_ONLY" if status_value == "pending_unconfirmed" else "GATEWAY_SEND_FAILED")
        return 2
    final = poll_outbound(message_id)
    final_status = str(final.get("status") or "").upper()
    if final_status in {"SERVER_ACK", "DELIVERED", "READ", "PLAYED"}:
        print("GATEWAY_OK_CONFIRMED")
        return 0
    print("GATEWAY_PENDING_ONLY")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
