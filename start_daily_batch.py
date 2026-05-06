from __future__ import annotations

import os
import time

import requests

from core.crm_hygiene import is_safe_deal_for_outbound
from core.sdr_state import PIPELINE_ID, STAGE_ENTRADA, STAGE_NUTRICAO, log_event


PIPEDRIVE_API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN") or os.getenv("PIPEDRIVE_TOKEN") or ""
BATCH_LIMIT = int(os.getenv("EMAIL_DAILY_BATCH_LIMIT", "50") or "50")
API_URL = os.getenv("API_URL", "http://127.0.0.1:8001").rstrip("/")
PAGE_LIMIT = int(os.getenv("EMAIL_BATCH_PAGE_LIMIT", "100") or "100")
MAX_SCAN = int(os.getenv("EMAIL_BATCH_MAX_SCAN", "1000") or "1000")


def pd(method, path, json_body=None, params=None):
    params = params or {}
    params["api_token"] = PIPEDRIVE_API_TOKEN
    response = requests.request(
        method,
        f"https://api.pipedrive.com/v1/{path.lstrip('/')}",
        params=params,
        json=json_body,
        timeout=60,
    )
    response.raise_for_status()
    return response.json() if response.text else {}


def move_to_nutricao(deal_id):
    pd("PUT", f"/deals/{int(deal_id)}", {"stage_id": STAGE_NUTRICAO})
    log_event("CRM_STAGE_UPDATE", deal_id=deal_id, stage_id=STAGE_NUTRICAO, reason="outbound_batch")


def main():
    if not PIPEDRIVE_API_TOKEN:
        raise SystemExit("PIPEDRIVE_API_TOKEN ausente")
    queued = 0
    already_queued = 0
    skipped = 0
    errors = 0
    scanned = 0
    start = 0
    seen_deals = set()

    while queued < BATCH_LIMIT and scanned < MAX_SCAN:
        payload = pd("GET", "/deals", params={
            "status": "open",
            "pipeline_id": PIPELINE_ID,
            "stage_id": STAGE_ENTRADA,
            "start": start,
            "limit": PAGE_LIMIT,
        })
        deals = payload.get("data") or []
        if not deals:
            break
        for deal in deals:
            if queued >= BATCH_LIMIT or scanned >= MAX_SCAN:
                break
            scanned += 1
            deal_id = int(deal.get("id") or 0)
            if not deal_id or deal_id in seen_deals:
                skipped += 1
                continue
            seen_deals.add(deal_id)
            if not is_safe_deal_for_outbound(deal):
                skipped += 1
                log_event("EMAIL_QUEUE_SKIP", deal_id=deal_id, reason="crm_hygiene_unsafe")
                continue
            try:
                response = requests.post(f"{API_URL}/webhooks/email-cadence", json={"deal_id": deal_id}, timeout=60)
                print(f"[BATCH_ITEM] deal={deal_id} status={response.status_code} body={response.text[:300]}")
                if not response.ok:
                    errors += 1
                    continue
                body = response.json()
                if body.get("queued"):
                    move_to_nutricao(deal_id)
                    queued += 1
                elif body.get("skip") == "already_queued":
                    move_to_nutricao(deal_id)
                    already_queued += 1
                else:
                    skipped += 1
            except Exception as exc:
                errors += 1
                print(f"[BATCH_FAIL] deal={deal_id} err={exc}")
            time.sleep(0.1)
        pagination = (payload.get("additional_data") or {}).get("pagination") or {}
        if not pagination.get("more_items_in_collection"):
            break
        start = int(pagination.get("next_start") or start + PAGE_LIMIT)

    print(
        f"[BATCH_SUMMARY] scanned={scanned} queued={queued} already_queued={already_queued} "
        f"skipped={skipped} errors={errors} target={BATCH_LIMIT}"
    )


if __name__ == "__main__":
    main()
