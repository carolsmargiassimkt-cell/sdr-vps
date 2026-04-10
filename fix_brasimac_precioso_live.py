from __future__ import annotations

import json
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

from crm.pipedrive_client import PipedriveClient


STATE_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\runtime\fix_brasimac_precioso_live_state.json")
REPORT_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\fix_brasimac_precioso_live_report.json")
CNPJ_FIELD_KEY = "aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0"
ARCHIVE_STAGE_ID = 50

BRASIMAC_DEALS = [553, 554, 556, 562, 564, 565, 566, 568, 577, 578, 581, 582, 590]
PRECIOSO_MASTER = 383
PRECIOSO_DUPLICATES = [508, 510, 511, 512, 523]
PRECIOSO_ORG_CANONICAL = 744


def log(message: str) -> None:
    print(str(message), flush=True)


def load_state() -> Dict[str, Any]:
    if not STATE_JSON.exists():
        return {"done": []}
    try:
        return json.loads(STATE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {"done": []}


def save_state(state: Dict[str, Any]) -> None:
    STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
    STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def save_report(counters: Counter[str], done: List[str]) -> None:
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(
        json.dumps({"done": done, "counters": dict(counters)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def normalize_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


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


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    return digits if len(digits) in {10, 11} else ""


def normalize_email(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if "@" in text and "." in text.split("@")[-1] else ""


def get_person_contacts(person: Dict[str, Any]) -> tuple[List[str], List[str]]:
    phones: List[str] = []
    emails: List[str] = []
    for item in person.get("phone") or []:
        raw = item.get("value") if isinstance(item, dict) else item
        phone = normalize_phone(raw)
        if phone and phone not in phones:
            phones.append(phone)
    for item in person.get("email") or []:
        raw = item.get("value") if isinstance(item, dict) else item
        email = normalize_email(raw)
        if email and email not in emails:
            emails.append(email)
    return phones[:5], emails[:5]


def build_person_payload(name: str, org_id: int, phones: List[str], emails: List[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"name": str(name or "Responsável").strip()[:120], "org_id": int(org_id)}
    if phones:
        payload["phone"] = [{"value": phone, "primary": idx == 0, "label": "work"} for idx, phone in enumerate(phones[:5])]
    if emails:
        payload["email"] = [{"value": email, "primary": idx == 0, "label": "work"} for idx, email in enumerate(emails[:5])]
    return payload


def ensure_org(client: PipedriveClient, name: str) -> Dict[str, Any]:
    clean = str(name or "").strip()
    for item in client.find_organization_by_name(clean):
        org = dict(item.get("item") or {})
        org_id = extract_id(org.get("id"))
        if not org_id:
            continue
        full = client.get_organization(org_id)
        if full and normalize_name(full.get("name")) == normalize_name(clean) and bool(full.get("active_flag", True)):
            return full
    created = client.create_organization({"name": clean})
    created_id = extract_id(created.get("id"))
    return client.get_organization(created_id) if created_id else {}


def remove_wrong_participants(client: PipedriveClient, deal_id: int, keep_person_id: int, keep_org_id: int, counters: Counter[str]) -> None:
    participants = client.get_deal_participants(deal_id, limit=100)
    seen = set()
    for participant in participants:
        participant_id = extract_id(participant.get("id"))
        person_id = extract_id(participant.get("person_id"))
        if not participant_id or not person_id:
            continue
        person = client.get_person_details(person_id)
        person_org_id = extract_id(person.get("org_id"))
        remove = False
        if keep_person_id and person_id == keep_person_id:
            if person_id in seen:
                remove = True
            else:
                seen.add(person_id)
        elif keep_org_id and person_org_id != keep_org_id:
            remove = True
        elif person_id in seen:
            remove = True
        else:
            seen.add(person_id)
        if remove and client.remove_deal_participant(deal_id, participant_id):
            counters["participants_removed"] += 1
            log(f"[PARTICIPANT_REMOVED] deal={deal_id} participant={participant_id} person={person_id}")


def ensure_participant(client: PipedriveClient, deal_id: int, person_id: int, counters: Counter[str]) -> None:
    if not deal_id or not person_id:
        return
    for participant in client.get_deal_participants(deal_id, limit=100):
        if extract_id(participant.get("person_id")) == int(person_id):
            return
    if client.add_deal_participant(deal_id, person_id):
        counters["participants_added"] += 1
        log(f"[PARTICIPANT_ADDED] deal={deal_id} person={person_id}")


def fix_brasimac(client: PipedriveClient, done: set[str], counters: Counter[str]) -> None:
    for deal_id in BRASIMAC_DEALS:
        key = f"brasimac:{deal_id}"
        if key in done:
            continue
        deal = client.get_deal_details(deal_id)
        if not deal:
            done.add(key)
            counters["deal_missing"] += 1
            continue
        title = str(deal.get("title") or "").strip()
        target_org = ensure_org(client, title.upper())
        target_org_id = extract_id(target_org.get("id"))
        if not target_org_id:
            counters["org_error"] += 1
            continue
        payload = {}
        if extract_id(deal.get("org_id")) != target_org_id:
            payload["org_id"] = target_org_id
        if title and title.upper() != title:
            payload["title"] = title.upper()
        current_person_id = extract_id(deal.get("person_id"))
        target_person_id = 0
        if current_person_id:
            person = client.get_person_details(current_person_id)
            phones, emails = get_person_contacts(person)
            person_payload = build_person_payload(title.upper(), target_org_id, phones, emails)
            if client.update_person(current_person_id, person_payload):
                counters["person_updated"] += 1
            target_person_id = current_person_id
            if extract_id(person.get("org_id")) != target_org_id:
                client.update_person(current_person_id, {"org_id": target_org_id})
        if target_person_id and extract_id(deal.get("person_id")) != target_person_id:
            payload["person_id"] = target_person_id
        if payload and client.update_deal(deal_id, payload):
            counters["deal_updated"] += 1
        ensure_participant(client, deal_id, target_person_id, counters)
        remove_wrong_participants(client, deal_id, target_person_id, target_org_id, counters)
        done.add(key)
        save_state({"done": sorted(done)})
        save_report(counters, sorted(done))
        time.sleep(1.0)


def fix_precioso(client: PipedriveClient, done: set[str], counters: Counter[str]) -> None:
    master_key = f"precioso:{PRECIOSO_MASTER}"
    if master_key not in done:
        master = client.get_deal_details(PRECIOSO_MASTER)
        if master:
            payload = {}
            if extract_id(master.get("org_id")) != PRECIOSO_ORG_CANONICAL:
                payload["org_id"] = PRECIOSO_ORG_CANONICAL
            title = str(master.get("title") or "").strip().upper()
            if title and str(master.get("title") or "") != title:
                payload["title"] = title
            if payload and client.update_deal(PRECIOSO_MASTER, payload):
                counters["deal_updated"] += 1
            remove_wrong_participants(client, PRECIOSO_MASTER, extract_id(master.get("person_id")), PRECIOSO_ORG_CANONICAL, counters)
        done.add(master_key)
        save_state({"done": sorted(done)})
        save_report(counters, sorted(done))
    for deal_id in PRECIOSO_DUPLICATES:
        key = f"precioso:{deal_id}"
        if key in done:
            continue
        deal = client.get_deal_details(deal_id)
        if not deal:
            done.add(key)
            counters["deal_missing"] += 1
            continue
        participants = client.get_deal_participants(deal_id, limit=100)
        master_person_id = extract_id(client.get_deal_details(PRECIOSO_MASTER).get("person_id"))
        for participant in participants:
            person_id = extract_id(participant.get("person_id"))
            if person_id and person_id != master_person_id:
                person = client.get_person_details(person_id)
                phones, emails = get_person_contacts(person)
                if phones or emails:
                    target = client.get_person_details(master_person_id) if master_person_id else {}
                    merged_phones, merged_emails = get_person_contacts(target)
                    for phone in phones:
                        if phone not in merged_phones:
                            merged_phones.append(phone)
                    for email in emails:
                        if email not in merged_emails:
                            merged_emails.append(email)
                    if master_person_id:
                        client.update_person(
                            master_person_id,
                            build_person_payload(
                                str(target.get("name") or "PRECIOSO FRUTO COMERCIO E DISTRIBUICAO DE ALIMENTOS LTDA"),
                                PRECIOSO_ORG_CANONICAL,
                                merged_phones,
                                merged_emails,
                            ),
                        )
        if client.update_stage(deal_id=deal_id, stage_id=ARCHIVE_STAGE_ID):
            counters["archived_duplicates"] += 1
        done.add(key)
        save_state({"done": sorted(done)})
        save_report(counters, sorted(done))
        time.sleep(1.0)


def main() -> int:
    client = PipedriveClient()
    if not client.test_connection():
        log(f"[PIPEDRIVE_FAIL] status={client.last_http_status}")
        return 2
    state = load_state()
    done = set(str(item) for item in list(state.get("done") or []))
    counters: Counter[str] = Counter()
    fix_brasimac(client, done, counters)
    fix_precioso(client, done, counters)
    save_state({"done": sorted(done)})
    save_report(counters, sorted(done))
    log("[FIX_BRASIMAC_PRECIOSO_END] " + " ".join(f"{k}={v}" for k, v in sorted(counters.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
