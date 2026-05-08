from __future__ import annotations

import os
import time
from typing import Any

import requests


API = os.getenv("PIPEDRIVE_BASE_URL", "https://api.pipedrive.com/v1").rstrip("/")
RETRY_STATUS = {429, 502, 503}
BACKOFFS = (1, 2, 5, 10, 10)
LEAD_TRAFEGO_LABEL_ID = 193


def _retry_after(response: requests.Response) -> float:
    headers = response.headers or {}
    for name in ("Retry-After", "retry-after", "X-RateLimit-Reset", "x-ratelimit-reset"):
        raw = str(headers.get(name) or "").strip()
        if not raw:
            continue
        try:
            return max(0.0, float(raw))
        except Exception:
            continue
    return 0.0


def safe_pd_request(method: str, path: str, json_body: Any = None, params: dict[str, Any] | None = None, *, timeout: int = 30) -> dict[str, Any] | None:
    token = os.getenv("PIPEDRIVE_API_TOKEN") or os.getenv("PIPEDRIVE_TOKEN") or ""
    if not token:
        print(f"[PD_REQUEST_FAIL_FINAL] method={method} path={path} reason=missing_token")
        return None
    request_params = dict(params or {})
    request_params["api_token"] = token
    url = f"{API}/{str(path or '').lstrip('/')}"
    last_error = ""
    for attempt in range(1, 6):
        try:
            response = requests.request(method, url, params=request_params, json=json_body, timeout=timeout)
            if response.status_code < 400:
                return response.json() if response.text else {}
            last_error = f"status={response.status_code} body={response.text[:180]}"
            if response.status_code not in RETRY_STATUS:
                break
            wait = _retry_after(response) or BACKOFFS[min(attempt - 1, len(BACKOFFS) - 1)]
            log = "[PD_RATE_LIMIT]" if response.status_code == 429 else "[PD_RETRY]"
            print(f"{log} method={method} path={path} attempt={attempt} wait={wait}")
            time.sleep(wait)
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = str(exc)
            wait = BACKOFFS[min(attempt - 1, len(BACKOFFS) - 1)]
            print(f"[PD_RETRY] method={method} path={path} attempt={attempt} wait={wait} error={exc}")
            time.sleep(wait)
    print(f"[PD_REQUEST_FAIL_FINAL] method={method} path={path} error={last_error}")
    return None


def _label_key(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("id") or value.get("value") or value.get("label") or value.get("name")
    return str(value or "").strip()


def _normalize_labels(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return list(raw)
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def merge_deal_labels(deal_id: int, wanted_labels: list[Any] | tuple[Any, ...] | set[Any]) -> bool:
    if int(deal_id or 0) <= 0:
        return False
    payload = safe_pd_request("GET", f"/deals/{int(deal_id)}")
    deal = (payload or {}).get("data") or {}
    if not deal:
        return False
    merged = _normalize_labels(deal.get("label"))
    keys = {_label_key(item) for item in merged if _label_key(item)}
    if str(LEAD_TRAFEGO_LABEL_ID) in keys:
        preserve_193 = True
    else:
        preserve_193 = False
    for label in list(wanted_labels or []):
        key = _label_key(label)
        if key and key not in keys:
            merged.append(int(label) if str(label).isdigit() else label)
            keys.add(key)
    if preserve_193 and str(LEAD_TRAFEGO_LABEL_ID) not in keys:
        merged.append(LEAD_TRAFEGO_LABEL_ID)
    if not wanted_labels and preserve_193:
        merged.append(LEAD_TRAFEGO_LABEL_ID)
    ok = safe_pd_request("PUT", f"/deals/{int(deal_id)}", {"label": merged}) is not None
    print(f"[CRM_LABEL_UPDATE] deal_id={deal_id} preserve_193={1 if preserve_193 else 0} ok={1 if ok else 0}")
    return ok
