from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List

from crm.pipedrive_client import PipedriveClient
from enrich_missing_cnpj_from_xlsx import normalize_cnpj, normalize_phone, parse_xlsx
from reset_pipeline2_participants import (
    BLOCKED_PHONES,
    CACHE_JSON,
    PIPELINE_ID,
    clean_email,
    clean_phone,
    contact_candidates,
    create_or_update_person,
    extract_id,
    load_cache,
    participant_id,
    participant_person_id,
    person_emails,
    person_org_id,
    person_phones,
    preload_casa,
    save_cache,
)


INPUT_XLSX = Path(r"C:\Users\Asus\Downloads\LISTAS-MAND\deals_enriquecido_final.xlsx")
ORG_CNPJ_FIELD_KEY = "aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0"
PIPEDRIVE_DELAY_SEC = 1.2
MAX_PARTICIPANTS = 3


def log(message: str) -> None:
    print(str(message), flush=True)


def normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    replacements = {
        "á": "a", "à": "a", "ã": "a", "â": "a",
        "é": "e", "ê": "e",
        "í": "i",
        "ó": "o", "ô": "o", "õ": "o",
        "ú": "u",
        "ç": "c",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return re.sub(r"[^a-z0-9]+", "", text)


def display_name(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"^lead\s+mand\s+digital\s*-\s*", "", text, flags=re.I)
    text = re.sub(r"^lead\s+mand\s+digital\s*", "", text, flags=re.I)
    text = re.sub(r"\s*-\s*neg[oó]cio\s*$", "", text, flags=re.I)
    text = re.sub(r"\bneg[oó]cio\b", " ", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip(" -")
    return text[:180]


def title_case_company(value: Any) -> str:
    text = display_name(value)
    if not text:
        return ""
    small = {"de", "da", "do", "das", "dos", "e", "em", "a", "o"}
    parts = []
    for token in text.lower().split():
        parts.append(token if token in small else token[:1].upper() + token[1:])
    return " ".join(parts).strip()


def walk(node: Any) -> Iterable[Any]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from walk(item)


def casa_address(casa_payload: Dict[str, Any]) -> Dict[str, str]:
    raw = casa_payload.get("raw") or {}
    endereco = raw.get("endereco") if isinstance(raw, dict) else {}
    if not isinstance(endereco, dict):
        endereco = {}
    route = " ".join(
        part
        for part in [
            str(endereco.get("tipo_logradouro") or "").strip(),
            str(endereco.get("logradouro") or "").strip(),
        ]
        if part
    ).strip()
    number = str(endereco.get("numero") or "").strip()
    complement = str(endereco.get("complemento") or "").strip()
    district = str(endereco.get("bairro") or "").strip()
    city = str(endereco.get("municipio") or "").strip()
    uf = str(endereco.get("uf") or "").strip()
    cep = re.sub(r"\D+", "", str(endereco.get("cep") or ""))
    formatted_parts = [route, number, complement, district, city, uf]
    formatted = ", ".join(part for part in formatted_parts if part)
    return {
        "address": formatted[:255],
        "address_route": route[:255],
        "address_street_number": number[:255],
        "address_subpremise": complement[:255],
        "address_sublocality": district[:255],
        "address_locality": city[:255],
        "address_admin_area_level_1": uf[:255],
        "address_country": "Brasil" if formatted else "",
        "address_postal_code": cep[:255],
    }


def raw_org_cnpj(org: Dict[str, Any]) -> str:
    return normalize_cnpj(org.get(ORG_CNPJ_FIELD_KEY) or org.get("cnpj"))


def same_clean(a: Any, b: Any) -> bool:
    return normalize_text(a) == normalize_text(b)


def valid_contact_phone(value: Any) -> str:
    phone = normalize_phone(value)
    if len(phone) == 11 and phone.startswith("0"):
        phone = phone[1:]
    if not phone or phone in BLOCKED_PHONES:
        return ""
    if len(phone) < 10 or phone.startswith("00") or len(set(phone)) <= 2:
        return ""
    return phone


def allowed_from_row_and_casa(row: Dict[str, Any], casa: Dict[str, Any]) -> tuple[list[dict[str, Any]], set[str], set[str]]:
    base_name = title_case_company(row.get("nome_final")) or str(casa.get("company_name") or "Contato").strip()
    candidates = contact_candidates(casa, base_name)
    row_phone = valid_contact_phone(row.get("telefone"))
    row_email = clean_email(row.get("email"))
    if row_phone or row_email:
        candidates.insert(0, {"name": base_name or "Contato", "phone": row_phone, "email": row_email})
    deduped: list[dict[str, Any]] = []
    seen = set()
    for item in candidates:
        phone = valid_contact_phone(item.get("phone"))
        email = clean_email(item.get("email"))
        key = phone or email
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append({"name": str(item.get("name") or base_name or "Contato").strip(), "phone": phone, "email": email})
    limited = deduped[:MAX_PARTICIPANTS]
    phones = {valid_contact_phone(item.get("phone")) for item in limited if valid_contact_phone(item.get("phone"))}
    emails = {clean_email(item.get("email")) for item in limited if clean_email(item.get("email"))}
    return limited, phones, emails


def is_generic_person_name(name: Any) -> bool:
    clean = normalize_text(name)
    return clean in {
        "juridico", "financeiro", "comercial", "contato", "fiscal", "administrativo",
        "vendas", "compras", "rh", "recepcao", "atendimento",
    }


def participant_is_contaminated(
    person: Dict[str, Any],
    expected_org_id: int,
    allowed_phones: set[str],
    allowed_emails: set[str],
) -> bool:
    if not person:
        return False
    org_id = person_org_id(person)
    phones = set(person_phones(person))
    emails = set(person_emails(person))
    if org_id and expected_org_id and org_id != expected_org_id:
        return True
    if any(not valid_contact_phone(phone) for phone in phones):
        return True
    if allowed_phones and phones and not (phones & allowed_phones):
        return True
    if not phones and allowed_emails and emails and not (emails & allowed_emails):
        return True
    if is_generic_person_name(person.get("name")):
        return True
    return False


def build_org_payload(org: Dict[str, Any], row: Dict[str, Any], casa: Dict[str, Any], address_counts: Counter[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    clean_name = title_case_company(row.get("nome_final"))
    current_name = str(org.get("name") or "")
    if clean_name and (not current_name or "neg" in current_name.lower() or not same_clean(current_name, clean_name)):
        payload["name"] = clean_name
    cnpj = normalize_cnpj(row.get("cnpj"))
    if cnpj and raw_org_cnpj(org) != cnpj:
        payload[ORG_CNPJ_FIELD_KEY] = cnpj
    address = casa_address(casa)
    current_address = str(org.get("address") or org.get("address_formatted_address") or "").strip()
    current_key = normalize_text(current_address)
    casa_key = normalize_text(address.get("address"))
    duplicate_current = bool(current_key and address_counts[current_key] >= 3)
    if casa_key and (not current_key or duplicate_current or current_key != casa_key and duplicate_current):
        for key, value in address.items():
            if value:
                payload[key] = value
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--delay", type=float, default=PIPEDRIVE_DELAY_SEC)
    args = parser.parse_args()

    rows = [
        row for row in parse_xlsx(INPUT_XLSX)
        if str(row.get("acao") or "").strip().upper() in {"UPDATE", "RESGATAR"}
        and int(str(row.get("deal_id") or "0") or 0) > 0
        and int(str(row.get("org_id") or "0") or 0) > 0
    ]
    rows.sort(key=lambda item: int(str(item.get("deal_id") or "0") or 0))
    if args.limit:
        rows = rows[max(0, args.offset):max(0, args.offset) + max(1, args.limit)]
    elif args.offset:
        rows = rows[max(0, args.offset):]

    cnpjs = sorted({normalize_cnpj(row.get("cnpj")) for row in rows if normalize_cnpj(row.get("cnpj"))})
    casa_cache = preload_casa(cnpjs, workers=args.workers)
    PipedriveClient.GLOBAL_MIN_INTERVAL_SEC = max(float(PipedriveClient.GLOBAL_MIN_INTERVAL_SEC or 0.0), 1.1)
    client = PipedriveClient()
    counters: Counter[str] = Counter()

    org_cache: Dict[int, Dict[str, Any]] = {}
    deal_cache: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        org_id = int(str(row.get("org_id") or "0") or 0)
        deal_id = int(str(row.get("deal_id") or "0") or 0)
        if org_id and org_id not in org_cache:
            org_cache[org_id] = client.get_organization(org_id)
            time.sleep(0.05)
        if deal_id and deal_id not in deal_cache:
            deal_cache[deal_id] = client.get_deal_details(deal_id)
            time.sleep(0.05)

    address_counts: Counter[str] = Counter(
        normalize_text(org.get("address") or org.get("address_formatted_address") or "")
        for org in org_cache.values()
        if normalize_text(org.get("address") or org.get("address_formatted_address") or "")
    )
    log(f"[AUDIT_INICIO] rows={len(rows)} orgs={len(org_cache)} cnpjs={len(cnpjs)} apply={int(args.apply)}")

    for index, row in enumerate(rows, start=1):
        deal_id = int(str(row.get("deal_id") or "0") or 0)
        expected_org_id = int(str(row.get("org_id") or "0") or 0)
        cnpj = normalize_cnpj(row.get("cnpj"))
        deal = deal_cache.get(deal_id) or {}
        if not deal:
            counters["deal_nao_encontrado"] += 1
            continue
        if int(deal.get("pipeline_id") or 0) != PIPELINE_ID:
            counters["fora_pipeline"] += 1
            continue
        live_org_id = extract_id(deal.get("org_id"))
        if live_org_id != expected_org_id:
            counters["org_mismatch"] += 1
            log(f"[ORG_MISMATCH] deal={deal_id} live_org={live_org_id} expected_org={expected_org_id}")
            if args.apply and client.update_deal(deal_id, {"org_id": expected_org_id}):
                counters["org_vinculo_ok"] += 1
                live_org_id = expected_org_id

        casa = casa_cache.get(cnpj) or {}
        if cnpj and int(casa.get("status_code") or 0) != 200:
            counters["casa_skip"] += 1
            casa = {}

        org = org_cache.get(expected_org_id) or {}
        if org:
            org_payload = build_org_payload(org, row, casa, address_counts)
            if org_payload:
                counters["org_update_plan"] += 1
                log(f"[ORG_UPDATE] deal={deal_id} org={expected_org_id} fields={','.join(org_payload.keys())}")
                if args.apply and client.update_organization(expected_org_id, org_payload):
                    counters["org_update_ok"] += 1
        else:
            counters["org_nao_encontrada"] += 1

        clean_title = title_case_company(row.get("nome_final"))
        current_title = str(deal.get("title") or "")
        if clean_title and (not current_title or "neg" in current_title.lower() or not same_clean(current_title, clean_title)):
            counters["deal_title_plan"] += 1
            if args.apply and client.update_deal(deal_id, {"title": clean_title}):
                counters["deal_title_ok"] += 1
                log(f"[DEAL_NOME_OK] deal={deal_id}")

        candidates, allowed_phones, allowed_emails = allowed_from_row_and_casa(row, casa)
        target_person_ids: List[int] = []
        for candidate in candidates:
            person_id = create_or_update_person(client, expected_org_id, candidate, apply=args.apply)
            if person_id > 0 and person_id not in target_person_ids:
                target_person_ids.append(person_id)
                counters["person_ok"] += 1

        if target_person_ids and extract_id(deal.get("person_id")) != target_person_ids[0]:
            counters["primary_plan"] += 1
            if args.apply and client.update_deal(deal_id, {"person_id": target_person_ids[0]}):
                counters["primary_ok"] += 1
                log(f"[VINCULO_OK] deal={deal_id} primary_person={target_person_ids[0]}")

        participants = client.get_deal_participants(deal_id, limit=100)
        existing_participants = {participant_person_id(participant) for participant in participants}
        if args.apply:
            for person_id in target_person_ids[1:MAX_PARTICIPANTS]:
                if person_id and person_id not in existing_participants:
                    if client.add_deal_participant(deal_id, person_id):
                        counters["participant_add"] += 1
                        log(f"[VINCULO_OK] deal={deal_id} participant_person={person_id}")

        for participant in participants:
            p_id = participant_person_id(participant)
            part_id = participant_id(participant)
            if not p_id or p_id in target_person_ids:
                continue
            person = client.get_person_details(p_id)
            if participant_is_contaminated(person, expected_org_id, allowed_phones, allowed_emails):
                counters["participant_remove_plan"] += 1
                if args.apply and part_id and client.remove_deal_participant(deal_id, part_id):
                    counters["participant_removed"] += 1
                    log(f"[REMOVIDO_CONTAMINADO] deal={deal_id} participant={part_id} person={p_id}")

        counters["deal_auditado"] += 1
        if index % 25 == 0:
            log(f"[PROGRESSO] {index}/{len(rows)}")
        time.sleep(float(args.delay))

    save_cache(load_cache())
    log("[AUDIT_RESUMO] " + " ".join(f"{key}={value}" for key, value in sorted(counters.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
