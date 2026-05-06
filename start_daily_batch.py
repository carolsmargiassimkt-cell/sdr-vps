from __future__ import annotations

import os
import random
import re
import time

import requests

from core.sdr_state import (
    PIPELINE_ID,
    STAGE_ENTRADA,
    STAGE_NUTRICAO,
    log_event,
)
from core.crm_hygiene import is_safe_deal_for_outbound, is_generic_email


PD = os.getenv("PIPEDRIVE_API_TOKEN") or os.getenv("PIPEDRIVE_TOKEN") or ""
LIMIT = int(os.getenv("OUTBOUND_DAILY_BATCH_LIMIT", "50") or "50")
API = "https://api.pipedrive.com/v1"

BAD_EMAIL_PREFIXES = (
    "fiscal@", "contabilidade@", "tributario@", "paralegal@", "legalizacao@",
    "nfe@", "financeiro@", "adm@", "administrativo@", "atendimento@",
    "sac@", "rh@", "dp@", "postmaster@", "no-reply@", "noreply@", "naoresponda@",
)
BAD_EMAIL_PARTS = ("contabil", "tributario", "fiscal", "legalizacao", "paralegal", "financeiro", "nfe", "cobranca", "compras")


def email_is_bad(email):
    e = (email or "").strip().lower()
    if not e or "@" not in e or "." not in e.split("@")[-1]:
        return True
    if e in {"teste@teste.com", "test@test.com", "email@email.com"}:
        return True
    if e.startswith(BAD_EMAIL_PREFIXES):
        return True
    if is_generic_email(e):
        return True
    local, domain = e.split("@", 1)
    if "manddigital.com.br" in domain:
        return True
    return any(part in local for part in BAD_EMAIL_PARTS)


def pd(method, path, json_body=None, params=None):
    if not PD:
        raise RuntimeError("PIPEDRIVE_API_TOKEN ausente")
    params = params or {}
    params["api_token"] = PD
    r = requests.request(method, API + path, params=params, json=json_body, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Pipedrive {method} {path} {r.status_code}: {r.text[:200]}")
    return r.json() if r.text else {}


def deal_email(deal):
    person = deal.get("person_id") or {}
    if not isinstance(person, dict):
        return ""
    emails = person.get("email") or []
    return next((item.get("value") for item in emails if item.get("value")), "")


def deal_phone(deal):
    person = deal.get("person_id") or {}
    if not isinstance(person, dict):
        return ""
    phones = person.get("phone") or []
    return next((item.get("value") for item in phones if item.get("value")), "")


def start_cadence(deal_id, email="", phone=""):
    try:
        r = requests.post(
            "http://127.0.0.1:8001/webhooks/email-cadence",
            json={"deal_id": int(deal_id), "email": email, "phone": phone},
            timeout=15,
        )
        return r.json() if r.status_code == 200 else {"ok": False, "error": r.text}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def move_to_nutricao(deal_id):
    pd("PUT", f"/deals/{int(deal_id)}", {"stage_id": STAGE_NUTRICAO})
    log_event("CRM_STAGE_UPDATE", deal_id=deal_id, stage_id=STAGE_NUTRICAO, reason="outbound_batch")


def run_batch():
    log_event("OUTBOUND_BATCH_START", limit=LIMIT, pipeline=PIPELINE_ID, stage=STAGE_ENTRADA)
    resp = pd("GET", "/deals", params={"stage_id": STAGE_ENTRADA, "status": "open", "limit": 100})
    deals = resp.get("data") or []
    random.shuffle(deals)
    count = 0
    for deal in deals:
        if count >= LIMIT:
            break
        if int(deal.get("pipeline_id") or PIPELINE_ID) != PIPELINE_ID:
            continue
        deal_id = int(deal.get("id") or 0)
        email = deal_email(deal)
        phone = deal_phone(deal)
        if not is_safe_deal_for_outbound(deal):
            log_event("EMAIL_QUEUE_SKIP", deal_id=deal_id, email=email, reason="crm_hygiene_unsafe")
            continue
        if email_is_bad(email):
            log_event("EMAIL_QUEUE_SKIP", deal_id=deal_id, email=email, reason="bad_email")
            continue
        res = start_cadence(deal_id, email=email, phone=phone)
        if res.get("skip") == "already_queued":
            move_to_nutricao(deal_id)
            log_event("EMAIL_QUEUE_ADD", deal_id=deal_id, email=email, skip="already_queued")
            continue
        if res.get("ok") and res.get("queued"):
            move_to_nutricao(deal_id)
            count += 1
            time.sleep(1)
        else:
            log_event("EMAIL_QUEUE_FAIL", deal_id=deal_id, error=res.get("error") or res)
    log_event("OUTBOUND_BATCH_DONE", processed=count)


if __name__ == "__main__":
    run_batch()
