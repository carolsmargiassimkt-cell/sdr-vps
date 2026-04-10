from __future__ import annotations

import argparse
import csv
import json
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List

from crm.pipedrive_client import PipedriveClient


INPUT_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_writeback_clean_unique.csv")
STATE_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\runtime\crm_safe_clean_writeback_state.json")
REPORT_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\crm_safe_clean_writeback_report.json")
CNPJ_FIELD_KEY = "aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0"
PIPELINE_ID = 2
BLOCKED_PHONES = {
    "18043309723",
    "1804330972",
    "11999999999",
    "11999998888",
    "00999999977",
    "1155555555",
    "999999999",
}


def log(message: str) -> None:
    print(str(message), flush=True)


def read_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def load_state() -> Dict[str, Any]:
    if not STATE_JSON.exists():
        return {"done": []}
    try:
        payload = json.loads(STATE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {"done": []}
    return payload if isinstance(payload, dict) else {"done": []}


def save_state(state: Dict[str, Any]) -> None:
    STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
    STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_cnpj(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    return digits if len(digits) == 14 else ""


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    if len(digits) not in {10, 11}:
        return ""
    if digits in BLOCKED_PHONES or len(set(digits)) <= 2:
        return ""
    return digits


def normalize_email(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if "@" in text and "." in text.split("@")[-1] else ""


def normalize_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def clean_text(value: Any, limit: int = 200) -> str:
    return str(value or "").strip()[:limit]


def row_key(row: Dict[str, Any]) -> str:
    return str(row.get("deal_id") or "").strip()


def split_pipe(value: Any) -> List[str]:
    return [str(item).strip() for item in str(value or "").split("|") if str(item).strip()]


def extract_id(value: Any) -> int:
    if isinstance(value, dict):
        try:
            return int(value.get("value") or value.get("id") or 0)
        except Exception:
            return 0
    try:
        return int(value or 0)
    except Exception:
        return 0


def first_valid_phone(*values: Any) -> str:
    for value in values:
        phone = normalize_phone(value)
        if phone:
            return phone
    return ""


def collect_phones(*values: Any) -> List[str]:
    phones: List[str] = []
    for value in values:
        if isinstance(value, list):
            raw_items = value
        else:
            raw_items = split_pipe(value)
            if not raw_items:
                raw_items = [value]
        for raw in raw_items:
            phone = normalize_phone(raw)
            if phone and phone not in phones:
                phones.append(phone)
    return phones


def collect_emails(*values: Any) -> List[str]:
    emails: List[str] = []
    for value in values:
        if isinstance(value, list):
            raw_items = value
        else:
            raw_items = split_pipe(value)
            if not raw_items:
                raw_items = [value]
        for raw in raw_items:
            email = normalize_email(raw)
            if email and email not in emails:
                emails.append(email)
    return emails


def build_org_payload(row: Dict[str, Any], current_org: Dict[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    target_name = clean_text(row.get("org_name_clean"), 120)
    target_cnpj = normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))
    target_address = clean_text(row.get("org_address_clean"), 255)
    trusted_cnpj = str(row.get("casa_status_code") or "") == "200" or str(row.get("opp_status_code") or "") == "200"
    current_name = clean_text(current_org.get("name"), 120)
    current_address = clean_text(current_org.get("address"), 255)
    current_cnpj = normalize_cnpj(current_org.get(CNPJ_FIELD_KEY))
    if target_name and normalize_name(target_name) != normalize_name(current_name):
        payload["name"] = target_name
    if trusted_cnpj and target_address and normalize_name(target_address) != normalize_name(current_address):
        payload["address"] = target_address
    if trusted_cnpj and target_cnpj and target_cnpj != current_cnpj:
        payload[CNPJ_FIELD_KEY] = target_cnpj
    return payload


def build_person_payload(row: Dict[str, Any], current_person: Dict[str, Any], target_org_id: int) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    target_name = clean_text(row.get("person_name_clean") or "Responsável", 120)
    target_phones = collect_phones(row.get("person_phone_clean"), row.get("telefones_exclusivos"), row.get("telefone_exclusivo"))
    target_emails = collect_emails(row.get("person_email_clean"), row.get("emails_exclusivos"), row.get("email_exclusivo_principal"))

    current_name = clean_text(current_person.get("name"), 120)
    if target_name and normalize_name(target_name) != normalize_name(current_name):
        payload["name"] = target_name
    if target_org_id and extract_id(current_person.get("org_id")) != int(target_org_id):
        payload["org_id"] = int(target_org_id)

    current_phones = []
    for item in current_person.get("phone") or []:
        current_phones.extend(collect_phones(item.get("value") if isinstance(item, dict) else item))
    current_emails = []
    for item in current_person.get("email") or []:
        current_emails.extend(collect_emails(item.get("value") if isinstance(item, dict) else item))

    if target_phones and target_phones[:5] != current_phones[:5]:
        payload["phone"] = [{"value": phone, "primary": idx == 0, "label": "work"} for idx, phone in enumerate(target_phones[:5])]
    if target_emails and target_emails[:5] != current_emails[:5]:
        payload["email"] = [{"value": email, "primary": idx == 0, "label": "work"} for idx, email in enumerate(target_emails[:5])]
    return payload


def create_person_payload(row: Dict[str, Any], org_id: int) -> Dict[str, Any]:
    target_name = clean_text(row.get("person_name_clean") or "Responsável", 120)
    phones = collect_phones(row.get("person_phone_clean"), row.get("telefones_exclusivos"), row.get("telefone_exclusivo"))
    emails = collect_emails(row.get("person_email_clean"), row.get("emails_exclusivos"), row.get("email_exclusivo_principal"))
    if not phones and not emails:
        return {}
    payload: Dict[str, Any] = {"name": target_name, "org_id": int(org_id)}
    if phones:
        payload["phone"] = [{"value": phone, "primary": idx == 0, "label": "work"} for idx, phone in enumerate(phones[:5])]
    if emails:
        payload["email"] = [{"value": email, "primary": idx == 0, "label": "work"} for idx, email in enumerate(emails[:5])]
    return payload


def choose_target_org(client: PipedriveClient, row: Dict[str, Any], current_org_id: int) -> Dict[str, Any]:
    target_cnpj = normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))
    target_name = clean_text(row.get("org_name_clean"), 120)
    current_org = client.get_organization(current_org_id) if current_org_id else {}
    current_cnpj = normalize_cnpj(current_org.get(CNPJ_FIELD_KEY)) if current_org else ""
    if current_org and target_cnpj and current_cnpj == target_cnpj:
        return current_org
    if target_cnpj:
        found = client.find_organization_by_cnpj(target_cnpj)
        found_id = extract_id(found.get("id") or found)
        if found_id:
            full = client.get_organization(found_id)
            if full and normalize_cnpj(full.get(CNPJ_FIELD_KEY)) == target_cnpj:
                return full
    if current_org and target_name and normalize_name(current_org.get("name")) == normalize_name(target_name):
        return current_org
    if target_name:
        for item in client.find_organization_by_name(target_name):
            org = dict(item.get("item") or {})
            org_id = extract_id(org.get("id"))
            if not org_id:
                continue
            full = client.get_organization(org_id)
            if not full:
                continue
            full_cnpj = normalize_cnpj(full.get(CNPJ_FIELD_KEY))
            if normalize_name(full.get("name")) != normalize_name(target_name):
                continue
            if target_cnpj and full_cnpj and full_cnpj != target_cnpj:
                continue
            if full:
                return full
    if current_org:
        return current_org
    created = client.create_organization(
        {
            "name": target_name or clean_text(row.get("nome_final"), 120),
            CNPJ_FIELD_KEY: target_cnpj,
            "address": clean_text(row.get("org_address_clean"), 255),
        }
    )
    created_id = extract_id(created.get("id"))
    return client.get_organization(created_id) if created_id else {}


def find_matching_person(client: PipedriveClient, row: Dict[str, Any], target_org_id: int) -> Dict[str, Any]:
    target_emails = collect_emails(row.get("person_email_clean"), row.get("emails_exclusivos"), row.get("email_exclusivo_principal"))
    target_phones = collect_phones(row.get("person_phone_clean"), row.get("telefones_exclusivos"), row.get("telefone_exclusivo"))
    target_name = normalize_name(row.get("person_name_clean"))
    for email in target_emails:
        found = client.find_person(org_id=target_org_id, email=email)
        found_id = extract_id(found.get("id"))
        if found_id:
            full = client.get_person_details(found_id)
            if full and extract_id(full.get("org_id")) == int(target_org_id):
                return full
    for phone in target_phones:
        found = client.find_person(org_id=target_org_id, phone=phone)
        found_id = extract_id(found.get("id"))
        if found_id:
            full = client.get_person_details(found_id)
            if full and extract_id(full.get("org_id")) == int(target_org_id):
                return full
    for person in client.get_organization_persons(target_org_id, limit=100):
        if target_name and normalize_name(person.get("name")) == target_name:
            return dict(person)
    return {}


def remove_bad_participants(
    client: PipedriveClient,
    deal_id: int,
    target_person_id: int,
    target_org_id: int,
    counters: Counter[str],
) -> None:
    participants = client.get_deal_participants(deal_id, limit=100)
    seen_person_ids = set()
    for participant in participants:
        participant_id = extract_id(participant.get("id"))
        person_id = extract_id(participant.get("person_id"))
        if not participant_id or not person_id:
            continue
        person = client.get_person_details(person_id)
        person_org_id = extract_id(person.get("org_id"))
        remove = False
        if target_person_id and person_id == int(target_person_id):
            if person_id in seen_person_ids:
                remove = True
            else:
                seen_person_ids.add(person_id)
        elif target_org_id and person_org_id != int(target_org_id):
            remove = True
        elif person_id in seen_person_ids:
            remove = True
        else:
            seen_person_ids.add(person_id)
        if remove and client.remove_deal_participant(deal_id, participant_id):
            counters["participants_removed"] += 1
            log(f"[PARTICIPANT_REMOVED] deal={deal_id} participant={participant_id} person={person_id}")


def maybe_add_target_participant(client: PipedriveClient, deal_id: int, target_person_id: int, counters: Counter[str]) -> None:
    if not deal_id or not target_person_id:
        return
    participants = client.get_deal_participants(deal_id, limit=100)
    for participant in participants:
        if extract_id(participant.get("person_id")) == int(target_person_id):
            return
    if client.add_deal_participant(deal_id, target_person_id):
        counters["participants_added"] += 1
        log(f"[PARTICIPANT_ADDED] deal={deal_id} person={target_person_id}")


def build_deal_payload(row: Dict[str, Any], current_deal: Dict[str, Any], target_org_id: int, target_person_id: int) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    target_title = clean_text(row.get("deal_title_clean"), 120)
    if target_title and normalize_name(target_title) != normalize_name(current_deal.get("title")):
        payload["title"] = target_title
    if target_org_id and extract_id(current_deal.get("org_id")) != int(target_org_id):
        payload["org_id"] = int(target_org_id)
    if target_person_id and extract_id(current_deal.get("person_id")) != int(target_person_id):
        payload["person_id"] = int(target_person_id)
    return payload


def save_report(counters: Counter[str], done_count: int) -> None:
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(
        json.dumps(
            {
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "done_count": done_count,
                "counters": dict(sorted(counters.items())),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def body_has(code: str, client: PipedriveClient) -> bool:
    return str(code or "").strip().upper() in str(client.last_http_body or "").upper()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=40)
    parser.add_argument("--delay", type=float, default=1.2)
    parser.add_argument("--filter", default="", help="Filtra por texto em nome/deal/org")
    args = parser.parse_args()

    rows = read_csv(INPUT_CSV)
    if args.filter:
        needle = str(args.filter).strip().lower()
        rows = [
            row
            for row in rows
            if needle in " | ".join([str(row.get("nome_final") or ""), str(row.get("org_name_clean") or ""), str(row.get("deal_title_clean") or "")]).lower()
        ]
    rows.sort(key=lambda row: int(str(row.get("deal_id") or "0") or 0))
    state = load_state()
    done = set(str(item) for item in list(state.get("done") or []))
    client = PipedriveClient()
    counters: Counter[str] = Counter()

    if not client.test_connection():
        log(f"[PIPEDRIVE_FAIL] status={client.last_http_status} body={client.last_http_body[:200]}")
        return 2

    planned_rows = [row for row in rows if row_key(row) not in done]
    log(f"[CRM_SAFE_WRITEBACK_START] rows={len(planned_rows)} apply={int(args.apply)}")
    for index, row in enumerate(planned_rows[: max(1, int(args.limit or 1))], start=1):
        deal_id = extract_id(row.get("deal_id"))
        if not deal_id:
            counters["skip_no_deal"] += 1
            done.add(row_key(row))
            continue
        deal = client.get_deal_details(deal_id)
        if not deal:
            if body_has("ERR_DEAL_DELETED", client):
                counters["skip_deleted_deal"] += 1
                done.add(row_key(row))
                continue
            counters["deal_error"] += 1
            log(f"[DEAL_ERROR] deal={deal_id} status={client.last_http_status}")
            if client.last_http_status == 429:
                break
            continue
        if int(deal.get("pipeline_id") or 0) != PIPELINE_ID:
            counters["skip_pipeline"] += 1
            done.add(row_key(row))
            continue

        current_org_id = extract_id(deal.get("org_id"))
        target_org = choose_target_org(client, row, current_org_id)
        target_org_id = extract_id(target_org.get("id"))
        if not target_org_id:
            counters["org_error"] += 1
            log(f"[ORG_ERROR] deal={deal_id}")
            continue

        org_payload = build_org_payload(row, target_org)
        current_person_id = extract_id(deal.get("person_id"))
        current_person = client.get_person_details(current_person_id) if current_person_id else {}
        target_person = current_person if current_person and extract_id(current_person.get("org_id")) == target_org_id else {}
        if not target_person:
            target_person = find_matching_person(client, row, target_org_id)
        person_payload = build_person_payload(row, target_person, target_org_id) if target_person else create_person_payload(row, target_org_id)

        log(
            f"[PLAN] {index}/{min(len(planned_rows), int(args.limit or 1))} deal={deal_id} "
            f"current_org={current_org_id} target_org={target_org_id} "
            f"person={'UPDATE' if target_person else ('CREATE' if person_payload else 'SKIP_CREATE')} "
            f"org_fields={list(org_payload.keys())} person_fields={list(person_payload.keys())}"
        )

        if not args.apply:
            counters["dry_run"] += 1
            continue

        if org_payload:
            if client.update_organization(target_org_id, org_payload):
                counters["org_updated"] += 1
                target_org = client.get_organization(target_org_id) or target_org
            elif body_has("ERR_ORGANIZATION_DELETED", client):
                counters["skip_deleted_org"] += 1
                done.add(row_key(row))
                continue

        if target_person:
            target_person_id = extract_id(target_person.get("id"))
            if person_payload and client.update_person(target_person_id, person_payload):
                counters["person_updated"] += 1
        else:
            target_person_id = 0
            if person_payload:
                created = client.create_person(person_payload)
                target_person_id = extract_id(created.get("id"))
                if target_person_id:
                    counters["person_created"] += 1
                else:
                    counters["person_error"] += 1
                    log(f"[PERSON_CREATE_ERROR] deal={deal_id} status={client.last_http_status}")
                    if client.last_http_status == 429:
                        break
                    continue
            else:
                counters["person_skipped_create"] += 1

        deal_payload = build_deal_payload(row, deal, target_org_id, target_person_id)
        if deal_payload:
            if client.update_deal(deal_id, deal_payload):
                counters["deal_updated"] += 1
                deal = client.get_deal_details(deal_id) or deal
            elif body_has("ERR_DEAL_DELETED", client):
                counters["skip_deleted_deal"] += 1
                done.add(row_key(row))
                continue
            elif client.last_http_status == 429:
                break

        maybe_add_target_participant(client, deal_id, target_person_id, counters)
        remove_bad_participants(client, deal_id, target_person_id, target_org_id, counters)

        if client.last_http_status == 429:
            break
        counters["ok"] += 1
        done.add(row_key(row))
        if counters["ok"] % 10 == 0:
            save_state({"done": sorted(done)})
            save_report(counters, len(done))
        time.sleep(float(args.delay))

    save_state({"done": sorted(done)})
    save_report(counters, len(done))
    log("[CRM_SAFE_WRITEBACK_END] " + " ".join(f"{k}={v}" for k, v in sorted(counters.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
