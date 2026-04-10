from __future__ import annotations

import argparse
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

from enrich_missing_cnpj_from_xlsx import (
    bucket,
    fetch_casa,
    load_cache,
    normalize_cnpj,
    normalize_phone,
    parse_xlsx,
    save_cache,
    search_casa_by_name,
    validate_casa_auth,
)
from supervisor import SDRSupervisor


CNPJ_FIELD_KEY = "aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0"
WRITEBACK_XLSX = Path(r"C:\Users\Asus\Downloads\deals_enriquecido_writeback_ready.xlsx")
SENT_LABEL_IDS = {"176", "195", "196", "197"}
SENT_LABEL_NAMES = {"CAD1", "RESPONDIDO", "WHATSAPP_CAD1", "EMAIL_CAD1"}
BLOCKED_GENERIC_PHONES = {"1804330972", "18043309723", "8731990856"}


def log(message: str) -> None:
    print(str(message), flush=True)


def extract_id(value: Any) -> int:
    if isinstance(value, dict):
        for key in ("value", "id"):
            try:
                parsed = int(value.get(key) or 0)
            except Exception:
                parsed = 0
            if parsed:
                return parsed
        return 0
    try:
        return int(value or 0)
    except Exception:
        return 0


def org_name(deal: Dict[str, Any], organization: Dict[str, Any] | None = None) -> str:
    organization = organization or {}
    if str(organization.get("name") or "").strip():
        return str(organization.get("name") or "").strip()
    org = deal.get("org_id")
    if isinstance(org, dict) and str(org.get("name") or "").strip():
        return str(org.get("name") or "").strip()
    return str(deal.get("org_name") or deal.get("title") or "").strip()


def person_phones(person: Dict[str, Any], bot) -> List[str]:
    phones: List[str] = []
    for item in person.get("phone") or []:
        raw = item.get("value") if isinstance(item, dict) else item
        phone = bot.normalize_phone(raw)
        if phone in BLOCKED_GENERIC_PHONES:
            continue
        if phone and phone not in phones:
            phones.append(phone)
    return phones


def person_emails(person: Dict[str, Any]) -> List[str]:
    emails: List[str] = []
    for item in person.get("email") or []:
        raw = item.get("value") if isinstance(item, dict) else item
        email = str(raw or "").strip().lower()
        if email and "@" in email and email not in emails:
            emails.append(email)
    return emails


def choose_phone(raw_phones: List[Any], bot, current_counts: Counter[str]) -> str:
    phones: List[str] = []
    for raw in raw_phones:
        phone = normalize_phone(raw)
        if not phone or phone in phones:
            continue
        if phone in BLOCKED_GENERIC_PHONES:
            continue
        if not bot.is_valid_phone(phone):
            continue
        if not bot.can_send(phone):
            continue
        if current_counts.get(phone, 0) > 0:
            continue
        phones.append(phone)
    phones.sort(key=lambda phone: (0 if len(phone) == 11 and phone[2] == "9" else 1, phone))
    return phones[0] if phones else ""


def merge_phones(new_phone: str, existing: List[str], bot) -> List[str]:
    merged: List[str] = []
    for phone in [new_phone] + list(existing or []):
        normalized = bot.normalize_phone(phone)
        if normalized in BLOCKED_GENERIC_PHONES:
            continue
        if normalized and normalized not in merged:
            merged.append(normalized)
    return merged[:5]


def merge_emails(new_email: str, existing: List[str]) -> List[str]:
    merged: List[str] = []
    for email in [new_email] + list(existing or []):
        normalized = str(email or "").strip().lower()
        if normalized and "@" in normalized and normalized not in merged:
            merged.append(normalized)
    return merged[:5]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    supervisor = SDRSupervisor()
    crm = supervisor.crm
    bot = supervisor.whatsapp
    cache = load_cache()
    casa_cache = bucket(cache, "casa")
    casa_search_cache = bucket(cache, "casa_search")
    if not validate_casa_auth(casa_cache):
        save_cache(cache)
        raise SystemExit("[CASA_AUTH_FAIL]")
    sheet_by_deal: Dict[str, Dict[str, Any]] = {}
    if WRITEBACK_XLSX.exists():
        for row in parse_xlsx(WRITEBACK_XLSX):
            deal_key = str(row.get("deal_id") or "").strip()
            if deal_key:
                sheet_by_deal[deal_key] = row

    deals = crm.get_deals(status="open", limit=1000, pipeline_id=2)
    current_counts: Counter[str] = Counter()
    for deal in deals:
        person = supervisor._embedded_person(deal)
        for phone in person_phones(person, bot):
            current_counts[phone] += 1

    targets: List[Dict[str, Any]] = []
    for deal in deals:
        if not supervisor._is_super_minas(deal):
            continue
        tokens = supervisor._deal_tokens(deal)
        label_ids = supervisor._raw_label_ids(deal)
        if tokens & SENT_LABEL_NAMES or label_ids & SENT_LABEL_IDS:
            continue
        person_id = supervisor._extract_person_id(deal)
        embedded_person = supervisor._embedded_person(deal)
        embedded_phone = supervisor._extract_phone(embedded_person)
        if person_id and embedded_phone and bot.can_send(embedded_phone):
            continue
        targets.append(deal)

    log(f"[SUPER_MINAS_TARGETS] total={len(targets)} apply={int(args.apply)}")
    counters = Counter()

    for index, deal in enumerate(targets, start=1):
        deal_id = int(deal.get("id") or 0)
        org_id = extract_id(deal.get("org_id"))
        person_id = supervisor._extract_person_id(deal)
        organization = crm.get_organization(org_id) if org_id else {}
        name = org_name(deal, organization)
        log(f"[SUPER_MINAS_PROCESSANDO] {index}/{len(targets)} deal={deal_id} org={org_id} person={person_id} nome={name}")

        raw_org_people = crm.get_organization_persons(org_id, limit=20) if org_id else []
        org_people = [
            person
            for person in raw_org_people
            if extract_id((person or {}).get("org_id")) == org_id
        ]
        if raw_org_people and not org_people:
            counters["org_people_filtrado"] += 1
            log(f"[ORG_PEOPLE_FILTRADO] deal={deal_id} org={org_id} raw={len(raw_org_people)}")
        existing_person = crm.get_person(int(person_id)) if person_id else {}
        if not existing_person and org_people:
            for candidate in org_people:
                phones = person_phones(candidate, bot)
                if any(bot.can_send(phone) for phone in phones):
                    existing_person = candidate
                    break
            if not existing_person:
                existing_person = org_people[0]

        existing_phones = person_phones(existing_person, bot) if existing_person else []
        existing_emails = person_emails(existing_person) if existing_person else []
        existing_usable_phone = next((phone for phone in existing_phones if bot.can_send(phone)), "")

        sheet_row = sheet_by_deal.get(str(deal_id), {})
        sheet_cnpj = normalize_cnpj(sheet_row.get("cnpj"))
        sheet_phone = normalize_phone(sheet_row.get("telefone"))
        sheet_email = str(sheet_row.get("email") or "").strip().lower()
        cnpj = sheet_cnpj or normalize_cnpj(organization.get(CNPJ_FIELD_KEY)) or normalize_cnpj(organization.get("cnpj"))
        if not cnpj and name:
            cnpj = search_casa_by_name(name, casa_search_cache)
            save_cache(cache)
            if cnpj:
                counters["cnpj_found"] += 1
                log(f"[CNPJ_ENCONTRADO] deal={deal_id} cnpj={cnpj}")

        payload: Dict[str, Any] = {}
        casa_phones: List[str] = []
        casa_emails: List[str] = []
        if cnpj:
            payload = fetch_casa(cnpj, casa_cache)
            save_cache(cache)
            if int(payload.get("status_code") or 0) == 200:
                counters["casa_ok"] += 1
                casa_phones = list(payload.get("telefones") or [])
                casa_emails = [str(email or "").strip().lower() for email in list(payload.get("emails") or []) if "@" in str(email or "")]

        chosen_phone = ""
        if not existing_usable_phone:
            chosen_phone = choose_phone([sheet_phone] + casa_phones, bot, current_counts)
        chosen_email = "" if existing_emails else (sheet_email if "@" in sheet_email else (casa_emails[0] if casa_emails else ""))

        updates = {}
        if existing_person and chosen_phone:
            updates["phone"] = merge_phones(chosen_phone, existing_phones, bot)
        if existing_person and chosen_email:
            updates["email"] = merge_emails(chosen_email, existing_emails)

        org_updates = {}
        if updates and cnpj and org_id and not normalize_cnpj(organization.get(CNPJ_FIELD_KEY)):
            org_updates[CNPJ_FIELD_KEY] = cnpj

        target_person_id = extract_id(existing_person.get("id")) if existing_person else 0
        deal_updates = {}
        if target_person_id and not person_id:
            deal_updates["person_id"] = target_person_id

        if not updates and not org_updates and not deal_updates:
            counters["no_safe_update"] += 1
            log(
                f"[SEM_UPDATE_SEGURO] deal={deal_id} cnpj={cnpj or 'na'} "
                f"existing_phone={','.join(existing_phones) or 'na'} casa_phones={','.join(map(str, casa_phones[:5])) or 'na'}"
            )
            continue

        log(
            f"[UPDATE_PLANEJADO] deal={deal_id} person={target_person_id or person_id or 0} "
            f"telefone={chosen_phone or existing_usable_phone or 'na'} email={chosen_email or 'na'} "
            f"org_updates={list(org_updates.keys())} deal_updates={deal_updates}"
        )

        if not args.apply:
            counters["dry_updates"] += 1
            continue

        if org_updates:
            if crm.update_organization(org_id, org_updates):
                counters["org_updated"] += 1
                log(f"[ORG_UPDATE_OK] org={org_id}")
        if updates and target_person_id:
            if crm.update_person(target_person_id, updates):
                counters["person_updated"] += 1
                if chosen_phone:
                    current_counts[chosen_phone] += 1
                log(f"[PERSON_UPDATE_OK] person={target_person_id}")
        if deal_updates:
            if crm.update_deal(deal_id, deal_updates):
                counters["deal_update_ok"] += 1
                log(f"[DEAL_PERSON_OK] deal={deal_id} person={target_person_id}")
        time.sleep(0.5)

    save_cache(cache)
    log("[SUPER_MINAS_RESUMO] " + " ".join(f"{key}={value}" for key, value in sorted(counters.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
