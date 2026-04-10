from __future__ import annotations

import argparse
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List

from crm.pipedrive_client import PipedriveClient
from enrich_missing_cnpj_from_xlsx import (
    bucket,
    fetch_casa,
    load_cache,
    normalize_cnpj,
    normalize_phone,
    parse_xlsx,
    save_cache,
    validate_casa_auth,
)


INPUT_XLSX = Path(r"C:\Users\Asus\Downloads\deals_enriquecido_final.xlsx")
CNPJ_FIELD_KEY = "aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0"
PIPELINE_ID = 2
REQUEST_DELAY_SEC = 2.0
BLOCKED_PHONES = {"1804330972", "18043309723", "8731990856", "11999999999", "00999999977", "1155555555"}


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


def clean_email(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if "@" in text and "." in text else ""


def clean_phone(value: Any) -> str:
    phone = normalize_phone(value)
    return "" if phone in BLOCKED_PHONES else phone


def phones_from_payload(payload: Dict[str, Any]) -> List[str]:
    phones: List[str] = []
    for raw in payload.get("telefones") or []:
        phone = clean_phone(raw)
        if phone and phone not in phones:
            phones.append(phone)
    return phones


def emails_from_payload(payload: Dict[str, Any]) -> List[str]:
    emails: List[str] = []
    for raw in payload.get("emails") or []:
        email = clean_email(raw)
        if email and email not in emails:
            emails.append(email)
    return emails


def person_phones(person: Dict[str, Any]) -> List[str]:
    output: List[str] = []
    for item in person.get("phone") or []:
        raw = item.get("value") if isinstance(item, dict) else item
        phone = clean_phone(raw)
        if phone and phone not in output:
            output.append(phone)
    return output


def person_emails(person: Dict[str, Any]) -> List[str]:
    output: List[str] = []
    for item in person.get("email") or []:
        raw = item.get("value") if isinstance(item, dict) else item
        email = clean_email(raw)
        if email and email not in output:
            output.append(email)
    return output


def format_phone_payload(phones: Iterable[str]) -> List[Dict[str, Any]]:
    return [{"value": phone, "primary": idx == 0, "label": "work"} for idx, phone in enumerate(phones)]


def format_email_payload(emails: Iterable[str]) -> List[Dict[str, Any]]:
    return [{"value": email, "primary": idx == 0, "label": "work"} for idx, email in enumerate(emails)]


def build_person_payload(person: Dict[str, Any], casa_phones: List[str], casa_emails: List[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    current_phones = person_phones(person)
    current_emails = person_emails(person)
    merged_phones = []
    for phone in list(casa_phones or []) + current_phones:
        if phone and phone not in merged_phones:
            merged_phones.append(phone)
    merged_emails = []
    for email in list(casa_emails or []) + current_emails:
        if email and email not in merged_emails:
            merged_emails.append(email)
    if merged_phones and merged_phones[:5] != current_phones[:5]:
        payload["phone"] = format_phone_payload(merged_phones[:5])
    if merged_emails and merged_emails[:5] != current_emails[:5]:
        payload["email"] = format_email_payload(merged_emails[:5])
    return payload


def normalize_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def safe_person_name(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text[:120]
    return "Contato"


def person_org_id(person: Dict[str, Any]) -> int:
    return extract_id(person.get("org_id"))


def is_person_contaminated(person: Dict[str, Any], expected_org_id: int) -> bool:
    pid = int(person.get("id") or 0)
    if not pid:
        return False
    org_id = person_org_id(person)
    return bool(expected_org_id and org_id and org_id != int(expected_org_id))


def pick_existing_org_person(client: PipedriveClient, org_id: int, casa_phones: List[str], casa_emails: List[str]) -> Dict[str, Any]:
    for phone in casa_phones:
        found = client.find_person(org_id=org_id, phone=phone)
        found_id = int(found.get("id") or 0) if found else 0
        full = client.get_person_details(found_id) if found_id else {}
        if full and person_org_id(full) == int(org_id):
            return full
    for email in casa_emails:
        found = client.find_person(org_id=org_id, email=email)
        found_id = int(found.get("id") or 0) if found else 0
        full = client.get_person_details(found_id) if found_id else {}
        if full and person_org_id(full) == int(org_id):
            return full
    return {}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Aplica alterações no CRM. Sem isso, só diagnostica.")
    parser.add_argument("--limit", type=int, default=40)
    parser.add_argument("--delay", type=float, default=REQUEST_DELAY_SEC)
    parser.add_argument("--only-contaminated", action="store_true", help="Só aplica/planeja correção quando a person atual pertence a outro org.")
    args = parser.parse_args()

    rows = parse_xlsx(INPUT_XLSX)
    targets = [
        row
        for row in rows
        if str(row.get("acao") or "").strip().upper() in {"UPDATE", "RESGATAR"}
        and int(str(row.get("deal_id") or "0") or 0) > 0
        and int(str(row.get("org_id") or "0") or 0) > 0
    ]
    targets.sort(key=lambda row: int(str(row.get("deal_id") or "0") or 0))
    client = PipedriveClient()
    cache = load_cache()
    casa_cache = bucket(cache, "casa")
    casa_ok = validate_casa_auth(casa_cache)
    save_cache(cache)
    if not casa_ok:
        log("[CASA_AUTH_FAIL]")
        return 2

    counters: Counter[str] = Counter()
    log(f"[FIX_CONTAMINACAO_INICIO] targets={len(targets)} apply={int(args.apply)} limit={args.limit}")
    for index, row in enumerate(targets[: max(1, int(args.limit or 40))], start=1):
        deal_id = int(str(row.get("deal_id") or "0") or 0)
        expected_org_id = int(str(row.get("org_id") or "0") or 0)
        cnpj = normalize_cnpj(row.get("cnpj"))
        nome_final = str(row.get("nome_final") or "").strip()
        log(f"[CHECANDO] {index}/{min(len(targets), int(args.limit or 40))} deal={deal_id} org={expected_org_id}")

        deal = client.get_deal_details(deal_id)
        if not deal:
            counters["deal_erro"] += 1
            log(f"[DEAL_ERRO] deal={deal_id} status={client.last_http_status}")
            time.sleep(float(args.delay))
            continue
        if int(deal.get("pipeline_id") or 0) != PIPELINE_ID:
            counters["fora_pipeline"] += 1
            log(f"[SKIP_FORA_PIPELINE] deal={deal_id} pipeline={deal.get('pipeline_id')}")
            continue
        actual_org_id = extract_id(deal.get("org_id"))
        if actual_org_id and actual_org_id != expected_org_id:
            counters["org_mismatch"] += 1
            log(f"[SKIP_ORG_MISMATCH] deal={deal_id} planilha_org={expected_org_id} crm_org={actual_org_id}")
            continue

        organization = client.get_organization(expected_org_id)
        if not organization:
            counters["org_erro"] += 1
            log(f"[ORG_ERRO] org={expected_org_id} status={client.last_http_status}")
            time.sleep(float(args.delay))
            continue
        cnpj = cnpj or normalize_cnpj(organization.get(CNPJ_FIELD_KEY))
        if not cnpj:
            counters["sem_cnpj"] += 1
            log(f"[SKIP_SEM_CNPJ] deal={deal_id} org={expected_org_id}")
            continue

        payload = fetch_casa(cnpj, casa_cache)
        save_cache(cache)
        if int(payload.get("status_code") or 0) != 200:
            counters["casa_erro"] += 1
            log(f"[CASA_DADOS_ERRO] deal={deal_id} cnpj={cnpj} status={payload.get('status_code')}")
            time.sleep(float(args.delay))
            continue
        counters["casa_ok"] += 1
        log(f"[CASA_DADOS_OK] deal={deal_id} cnpj={cnpj}")

        casa_phones = phones_from_payload(payload)
        casa_emails = emails_from_payload(payload)
        current_person_id = extract_id(deal.get("person_id"))
        current_person = client.get_person_details(current_person_id) if current_person_id else {}
        contaminated = is_person_contaminated(current_person, expected_org_id)
        if contaminated:
            counters["contaminado"] += 1
            log(
                f"[CONTAMINADO] deal={deal_id} person={current_person_id} "
                f"person_org={person_org_id(current_person)} expected_org={expected_org_id}"
            )
        elif args.only_contaminated:
            counters["skip_nao_contaminado"] += 1
            log(f"[SKIP_NAO_CONTAMINADO] deal={deal_id} person={current_person_id or 0}")
            time.sleep(float(args.delay))
            continue

        target_person = {} if contaminated else current_person
        if not target_person:
            target_person = pick_existing_org_person(client, expected_org_id, casa_phones, casa_emails)
        if target_person and person_org_id(target_person) not in {0, expected_org_id}:
            log(
                f"[PERSON_REJEITADA_ORG_MISMATCH] deal={deal_id} "
                f"person={int(target_person.get('id') or 0)} person_org={person_org_id(target_person)} expected_org={expected_org_id}"
            )
            target_person = {}

        person_payload: Dict[str, Any] = {}
        target_person_id = int(target_person.get("id") or 0)
        if target_person_id:
            person_payload = build_person_payload(target_person, casa_phones, casa_emails)
        else:
            person_name = safe_person_name(payload.get("nome_final"), nome_final, organization.get("name"), f"Contato org {expected_org_id}")
            person_payload = {
                "name": person_name,
                "org_id": expected_org_id,
            }
            if casa_phones:
                person_payload["phone"] = format_phone_payload(casa_phones[:5])
            if casa_emails:
                person_payload["email"] = format_email_payload(casa_emails[:5])

        org_payload: Dict[str, Any] = {}
        if cnpj and not normalize_cnpj(organization.get(CNPJ_FIELD_KEY)):
            org_payload[CNPJ_FIELD_KEY] = cnpj
        if nome_final and normalize_name(str(organization.get("name") or "")) != normalize_name(nome_final):
            current_name = str(organization.get("name") or "").strip().lower()
            if current_name.startswith("lead mand digital") or current_name in {"negocio", "negócio"}:
                org_payload["name"] = nome_final[:120]

        log(
            f"[PLANO] deal={deal_id} contaminated={int(contaminated)} "
            f"target_person={target_person_id or ('CREATE' if person_payload else 'NONE')} phones={len(casa_phones)} emails={len(casa_emails)} "
            f"person_fields={list(person_payload.keys())} org_fields={list(org_payload.keys())}"
        )

        if not args.apply:
            counters["dry_run"] += 1
            time.sleep(float(args.delay))
            continue

        if org_payload:
            if client.update_organization(expected_org_id, org_payload):
                counters["org_update_ok"] += 1
        if target_person_id:
            if person_payload and client.update_person(target_person_id, person_payload):
                counters["person_corrigida"] += 1
                log(f"[PERSON_CORRIGIDA] person={target_person_id}")
        else:
            created = client.create_person(person_payload)
            target_person_id = int(created.get("id") or 0)
            if target_person_id:
                counters["person_criada"] += 1
                log(f"[PERSON_CORRIGIDA] person={target_person_id}")

        if target_person_id and current_person_id != target_person_id:
            if client.update_deal(deal_id, {"person_id": target_person_id}):
                counters["vinculo_ok"] += 1
                log(f"[VINCULO_OK] deal={deal_id} person={target_person_id}")

        participants = client.get_deal_participants(deal_id, limit=50)
        for participant in participants:
            participant_person_id = extract_id(participant.get("person_id"))
            participant_id = int(participant.get("id") or 0)
            if participant_id and participant_person_id and participant_person_id != target_person_id:
                person = client.get_person_details(participant_person_id)
                if is_person_contaminated(person, expected_org_id):
                    if client.remove_deal_participant(deal_id, participant_id):
                        counters["person_removida"] += 1
                        log(f"[PERSON_REMOVIDA] deal={deal_id} participant={participant_id} person={participant_person_id}")
        time.sleep(float(args.delay))

    save_cache(cache)
    log("[FIX_CONTAMINACAO_RESUMO] " + " ".join(f"{key}={value}" for key, value in sorted(counters.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
