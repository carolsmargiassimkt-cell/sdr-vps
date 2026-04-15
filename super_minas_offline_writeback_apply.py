from __future__ import annotations

import argparse
import csv
import json
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

from crm.pipedrive_client import PipedriveClient


INPUT_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\super_minas_offline_writeback_20260410.csv")
STATE_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\runtime\super_minas_offline_writeback_apply_state.json")
REPORT_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\super_minas_offline_writeback_apply_report.json")
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


def row_key(row: Dict[str, Any]) -> str:
    return str(row.get("deal_id") or "").strip()


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
    if not text or text == "nan":
        return ""
    return text if "@" in text and "." in text.split("@")[-1] else ""


def split_pipe(value: Any) -> List[str]:
    return [str(item).strip() for item in str(value or "").split("|") if str(item).strip() and str(item).strip().lower() != "nan"]


def collect_phones(*values: Any) -> List[str]:
    output: List[str] = []
    for value in values:
        for item in split_pipe(value) or [value]:
            phone = normalize_phone(item)
            if phone and phone not in output:
                output.append(phone)
    return output


def collect_emails(*values: Any) -> List[str]:
    output: List[str] = []
    for value in values:
        for item in split_pipe(value) or [value]:
            email = normalize_email(item)
            if email and email not in output:
                output.append(email)
    return output


def current_phones(person: Dict[str, Any]) -> List[str]:
    output: List[str] = []
    for item in person.get("phone") or []:
        raw = item.get("value") if isinstance(item, dict) else item
        output.extend([phone for phone in collect_phones(raw) if phone not in output])
    return output


def current_emails(person: Dict[str, Any]) -> List[str]:
    output: List[str] = []
    for item in person.get("email") or []:
        raw = item.get("value") if isinstance(item, dict) else item
        output.extend([email for email in collect_emails(raw) if email not in output])
    return output


def find_person_for_org(client: PipedriveClient, org_id: int, phones: List[str], emails: List[str]) -> Dict[str, Any]:
    for email in emails:
        found = client.find_person(org_id=org_id, email=email)
        found_id = extract_id(found.get("id"))
        if found_id:
            person = client.get_person_details(found_id)
            if person and extract_id(person.get("org_id")) == org_id:
                return person
    for phone in phones:
        found = client.find_person(org_id=org_id, phone=phone)
        found_id = extract_id(found.get("id"))
        if found_id:
            person = client.get_person_details(found_id)
            if person and extract_id(person.get("org_id")) == org_id:
                return person
    people = client.get_organization_persons(org_id, limit=100)
    return dict(people[0]) if people else {}


def build_org_payload(row: Dict[str, Any], org: Dict[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    target_cnpj = normalize_cnpj(row.get("cnpj"))
    target_address = str(row.get("address") or "").strip()
    current_cnpj = normalize_cnpj(org.get(CNPJ_FIELD_KEY))
    current_address = str(org.get("address") or "").strip()
    if target_cnpj and target_cnpj != current_cnpj:
        payload[CNPJ_FIELD_KEY] = target_cnpj
    if target_address and target_address.lower() != current_address.lower():
        payload["address"] = target_address[:255]
    return payload


def build_person_payload(phones: List[str], emails: List[str], person: Dict[str, Any], org_id: int) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    existing_phones = current_phones(person)
    existing_emails = current_emails(person)
    merged_phones = existing_phones[:]
    for phone in phones:
        if phone not in merged_phones:
            merged_phones.append(phone)
    merged_emails = existing_emails[:]
    for email in emails:
        if email not in merged_emails:
            merged_emails.append(email)
    if merged_phones[:5] != existing_phones[:5]:
        payload["phone"] = [{"value": phone, "primary": idx == 0, "label": "work"} for idx, phone in enumerate(merged_phones[:5])]
    if merged_emails[:5] != existing_emails[:5]:
        payload["email"] = [{"value": email, "primary": idx == 0, "label": "work"} for idx, email in enumerate(merged_emails[:5])]
    if org_id and extract_id(person.get("org_id")) != org_id:
        payload["org_id"] = org_id
    return payload


def create_person_payload(row: Dict[str, Any], phones: List[str], emails: List[str], org_id: int) -> Dict[str, Any]:
    if not phones and not emails:
        return {}
    payload: Dict[str, Any] = {
        "name": str(row.get("company_input") or "Responsável").strip()[:120],
        "org_id": org_id,
    }
    if phones:
        payload["phone"] = [{"value": phone, "primary": idx == 0, "label": "work"} for idx, phone in enumerate(phones[:5])]
    if emails:
        payload["email"] = [{"value": email, "primary": idx == 0, "label": "work"} for idx, email in enumerate(emails[:5])]
    return payload


def build_note(row: Dict[str, Any]) -> str:
    lines: List[str] = []
    websites = split_pipe(row.get("websites"))
    decision_makers = split_pipe(row.get("decision_makers"))
    notes = str(row.get("manual_public_notes") or "").strip()
    if not websites and not decision_makers and (not notes or notes.lower() == "nan"):
        return ""
    lines.append("Enriquecimento offline Super Minas 2026-04-10")
    if websites:
        lines.append("Sites: " + " | ".join(websites[:15]))
    if decision_makers:
        lines.append("Sócios/decisores: " + " | ".join(decision_makers[:10]))
    if notes and notes.lower() != "nan":
        lines.append("Notas: " + notes[:800])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--delay", type=float, default=1.2)
    args = parser.parse_args()

    rows = read_csv(INPUT_CSV)
    rows = [
        row
        for row in rows
        if str(row.get("writeback_ready") or "").strip().lower() == "yes" and str(row.get("deal_id") or "").strip()
    ]
    rows.sort(key=lambda row: extract_id(row.get("deal_id")))
    state = load_state()
    done = set(str(item) for item in list(state.get("done") or []))
    client = PipedriveClient()
    counters: Counter[str] = Counter()

    if not client.test_connection():
        log(f"[PIPEDRIVE_FAIL] status={client.last_http_status} body={client.last_http_body[:200]}")
        return 2

    planned_rows = [row for row in rows if row_key(row) not in done]
    log(f"[SUPER_MINAS_WRITEBACK_START] rows={len(planned_rows)} apply={int(args.apply)}")
    for index, row in enumerate(planned_rows[: max(1, int(args.limit or 1))], start=1):
        deal_id = extract_id(row.get("deal_id"))
        org_id = extract_id(row.get("org_id"))
        if not deal_id or not org_id:
            counters["skip_missing_ids"] += 1
            done.add(row_key(row))
            continue

        deal = client.get_deal_details(deal_id)
        if not deal:
            counters["deal_error"] += 1
            log(f"[DEAL_ERROR] deal={deal_id} status={client.last_http_status}")
            if client.last_http_status == 429:
                break
            continue
        if extract_id(deal.get("pipeline_id")) != PIPELINE_ID and int(deal.get("pipeline_id") or 0) != PIPELINE_ID:
            counters["skip_pipeline"] += 1
            done.add(row_key(row))
            continue

        current_org_id = extract_id(deal.get("org_id"))
        org = client.get_organization(org_id)
        if not org and current_org_id and current_org_id != org_id:
            fallback_org = client.get_organization(current_org_id)
            if fallback_org:
                log(f"[ORG_FALLBACK] deal={deal_id} csv_org={org_id} current_org={current_org_id}")
                org_id = current_org_id
                org = fallback_org
        if not org:
            counters["org_error"] += 1
            log(f"[ORG_ERROR] deal={deal_id} org={org_id} status={client.last_http_status}")
            if client.last_http_status == 429:
                break
            continue

        phones = collect_phones(row.get("phones_exclusive"))
        emails = collect_emails(row.get("emails_exclusive"))
        org_payload = build_org_payload(row, org)

        current_person_id = extract_id(deal.get("person_id"))
        person = client.get_person_details(current_person_id) if current_person_id else {}
        if person and extract_id(person.get("org_id")) != org_id:
            person = {}
        if not person:
            person = find_person_for_org(client, org_id, phones, emails)

        person_payload = build_person_payload(phones, emails, person, org_id) if person else create_person_payload(row, phones, emails, org_id)
        note_content = build_note(row)

        log(
            f"[PLAN] {index}/{min(len(planned_rows), int(args.limit or 1))} "
            f"deal={deal_id} org={org_id} person={'UPDATE' if person else ('CREATE' if person_payload else 'SKIP')} "
            f"org_fields={list(org_payload.keys())} phone_count={len(phones)} email_count={len(emails)} "
            f"note={int(bool(note_content.strip()))}"
        )

        if not args.apply:
            counters["dry_run"] += 1
            continue

        if org_payload:
            if client.update_organization(org_id, org_payload):
                counters["org_updated"] += 1
            elif body_has("ERR_ORGANIZATION_DELETED", client) and current_org_id and current_org_id != org_id:
                fallback_org = client.get_organization(current_org_id)
                if fallback_org:
                    log(f"[ORG_UPDATE_FALLBACK] deal={deal_id} deleted_org={org_id} current_org={current_org_id}")
                    org_id = current_org_id
                    org = fallback_org
                    org_payload = build_org_payload(row, org)
                    if not org_payload:
                        counters["org_unchanged"] += 1
                    elif client.update_organization(org_id, org_payload):
                        counters["org_updated"] += 1
                    else:
                        counters["org_update_error"] += 1
                        log(f"[ORG_UPDATE_ERROR] org={org_id} status={client.last_http_status}")
                        if client.last_http_status == 429:
                            break
                        continue
                else:
                    counters["org_update_error"] += 1
                    log(f"[ORG_UPDATE_ERROR] org={org_id} status={client.last_http_status}")
                    if client.last_http_status == 429:
                        break
                    continue
            else:
                counters["org_update_error"] += 1
                log(f"[ORG_UPDATE_ERROR] org={org_id} status={client.last_http_status}")
                if client.last_http_status == 429:
                    break
                continue

        target_person_id = extract_id(person.get("id")) if person else 0
        if person:
            if person_payload:
                if client.update_person(target_person_id, person_payload):
                    counters["person_updated"] += 1
                else:
                    counters["person_update_error"] += 1
                    log(f"[PERSON_UPDATE_ERROR] person={target_person_id} status={client.last_http_status}")
                    if client.last_http_status == 429:
                        break
                    continue
        elif person_payload:
            created = client.create_person(person_payload)
            target_person_id = extract_id(created.get("id"))
            if target_person_id:
                counters["person_created"] += 1
            else:
                counters["person_create_error"] += 1
                log(f"[PERSON_CREATE_ERROR] deal={deal_id} status={client.last_http_status}")
                if client.last_http_status == 429:
                    break
                continue
        else:
            counters["person_skipped"] += 1

        if note_content.strip():
            if client.add_note(deal_id=deal_id, content=note_content):
                counters["notes_added"] += 1
            else:
                counters["note_error"] += 1
                log(f"[NOTE_ERROR] deal={deal_id} status={client.last_http_status}")
                if client.last_http_status == 429:
                    break
                continue

        counters["ok"] += 1
        done.add(row_key(row))
        if counters["ok"] % 10 == 0:
            save_state({"done": sorted(done)})
            save_report(counters, len(done))
        time.sleep(float(args.delay))

    save_state({"done": sorted(done)})
    save_report(counters, len(done))
    log("[SUPER_MINAS_WRITEBACK_END] " + " ".join(f"{k}={v}" for k, v in sorted(counters.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
