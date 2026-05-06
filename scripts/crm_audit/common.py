from __future__ import annotations

import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from core.crm_hygiene import severity_from_score


API = "https://api.pipedrive.com/v1"
TOKEN = os.getenv("PIPEDRIVE_API_TOKEN") or os.getenv("PIPEDRIVE_TOKEN") or ""
REPORT_DIR = ROOT / "reports" / "crm_audit"


def ensure_report_dir() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)


def pd_get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    if not TOKEN:
        raise RuntimeError("PIPEDRIVE_API_TOKEN ausente")
    params = dict(params or {})
    params["api_token"] = TOKEN
    response = requests.get(API + path, params=params, timeout=30)
    response.raise_for_status()
    return response.json() if response.text else {}


def paged_get(path: str, params: dict[str, Any] | None = None, limit: int = 500) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    start = 0
    while True:
        payload = pd_get(path, {**(params or {}), "start": start, "limit": limit})
        rows.extend(payload.get("data") or [])
        pagination = (payload.get("additional_data") or {}).get("pagination") or {}
        if not pagination.get("more_items_in_collection"):
            break
        start = int(pagination.get("next_start") or start + limit)
        time.sleep(0.15)
    return rows


def all_orgs() -> list[dict[str, Any]]:
    return paged_get("/organizations", {"status": "all"}, limit=500)


def all_persons() -> list[dict[str, Any]]:
    return paged_get("/persons", {}, limit=500)


def all_deals() -> list[dict[str, Any]]:
    return paged_get("/deals", {"status": "all"}, limit=500)


def org_persons(org_id: int) -> list[dict[str, Any]]:
    try:
        return paged_get(f"/organizations/{int(org_id)}/persons", {}, limit=500)
    except Exception as exc:
        print(f"[ORG_PERSONS_FAIL] org={org_id} error={exc}")
        return []


def person_details(person_id: int) -> dict[str, Any]:
    try:
        return (pd_get(f"/persons/{int(person_id)}").get("data") or {})
    except Exception as exc:
        print(f"[PERSON_FAIL] person={person_id} error={exc}")
        return {}


def write_reports(name: str, rows: list[dict[str, Any]]) -> None:
    ensure_report_dir()
    json_path = REPORT_DIR / f"{name}.json"
    csv_path = REPORT_DIR / f"{name}.csv"
    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    fields: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fields:
                fields.append(key)
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields or ["empty"])
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})
    print(f"[REPORT_WRITTEN] {csv_path} rows={len(rows)}")
    print(f"[REPORT_WRITTEN] {json_path} rows={len(rows)}")


def severity(score: int) -> str:
    return severity_from_score(int(score or 0))
