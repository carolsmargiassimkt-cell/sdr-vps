from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List

import requests

from config.config_loader import get_config_value
from crm.pipedrive_client import PipedriveClient
from enrich_missing_cnpj_from_xlsx import normalize_cnpj, normalize_phone, parse_xlsx


INPUT_XLSX = Path(r"C:\Users\Asus\Downloads\deals_enriquecido_final.xlsx")
CACHE_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\pipeline2_participants_casa_cache.json")
PIPELINE_ID = 2
MAX_PARTICIPANTS_PER_DEAL = 3
CASA_TIMEOUT_SEC = 10
PIPEDRIVE_DELAY_SEC = 1.5
BLOCKED_PHONES = {"1804330972", "18043309723", "8731990856", "11999999999", "00999999977", "1155555555"}


def log(message: str) -> None:
    print(str(message), flush=True)


def load_cache() -> Dict[str, Any]:
    if not CACHE_JSON.exists():
        return {}
    try:
        payload = json.loads(CACHE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def save_cache(cache: Dict[str, Any]) -> None:
    CACHE_JSON.parent.mkdir(parents=True, exist_ok=True)
    CACHE_JSON.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


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


def clean_phone(value: Any) -> str:
    phone = normalize_phone(value)
    if not phone or phone in BLOCKED_PHONES:
        return ""
    if len(phone) < 10 or phone.startswith("00") or len(set(phone)) <= 2:
        return ""
    return phone


def clean_email(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if "@" in text and "." in text else ""


def normalize_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def walk(node: Any) -> Iterable[Any]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from walk(item)


def collect_casa_fields(raw: Dict[str, Any]) -> Dict[str, Any]:
    phones: List[str] = []
    emails: List[str] = []
    socios: List[str] = []
    company_name = ""
    for node in walk(raw):
        if not isinstance(node, dict):
            continue
        lowered = {str(k).lower(): v for k, v in node.items()}
        if not company_name:
            for key in ("razao_social", "nome_empresarial", "nome", "nome_fantasia", "fantasia"):
                value = str(lowered.get(key) or "").strip()
                if value:
                    company_name = value
                    break
        for key, value in lowered.items():
            if any(token in key for token in ("telefone", "celular", "fone", "whatsapp")):
                for match in re.findall(r"\d[\d\s().+-]{8,}\d", str(value)):
                    phone = clean_phone(match)
                    if phone and phone not in phones:
                        phones.append(phone)
            if "mail" in key:
                for match in re.findall(r"[\w.+-]+@[\w.-]+\.[a-zA-Z]{2,}", str(value)):
                    email = clean_email(match)
                    if email and email not in emails:
                        emails.append(email)
            if any(token in key for token in ("socio", "administrador", "nome_socio")):
                text = str(value or "").strip()
                if text and len(text) <= 120 and not re.search(r"[\{\}\[\]@]", text) and text not in socios:
                    socios.append(text)
    return {"phones": phones, "emails": emails, "socios": socios[:5], "company_name": company_name}


def fetch_casa_cnpj(cnpj: str, api_key: str) -> Dict[str, Any]:
    cnpj = normalize_cnpj(cnpj)
    if not cnpj:
        return {"status_code": 0, "error": "cnpj_invalido"}
    headers_v2 = {"Authorization": f"Bearer {api_key}", "accept": "application/json", "user-agent": "bot-sdr-ai/1.0"}
    headers_v4 = {"api-key": api_key, "accept": "application/json", "user-agent": "bot-sdr-ai/1.0"}
    last_payload: Dict[str, Any] = {}
    for endpoint, url, headers in (
        ("v2", f"https://api.casadosdados.com.br/v2/cnpj/{cnpj}", headers_v2),
        ("v4", f"https://api.casadosdados.com.br/v4/cnpj/{cnpj}", headers_v4),
    ):
        for attempt in range(2):
            try:
                response = requests.get(url, headers=headers, timeout=CASA_TIMEOUT_SEC)
                if response.status_code == 200:
                    raw = response.json()
                    fields = collect_casa_fields(raw if isinstance(raw, dict) else {})
                    return {"status_code": 200, "endpoint": endpoint, "raw": raw, **fields}
                last_payload = {"status_code": response.status_code, "error": response.text[:300], "url": url, "endpoint": endpoint}
                if response.status_code not in {408, 429, 500, 502, 503, 504}:
                    break
            except Exception as exc:
                last_payload = {"status_code": 0, "error": str(exc)[:300], "url": url, "endpoint": endpoint}
            time.sleep(0.1)
    return last_payload


def preload_casa(cnpjs: List[str], workers: int = 5) -> Dict[str, Any]:
    cache = load_cache()
    api_key = str(get_config_value("casa_dos_dados_api_key", "") or "").strip()
    missing = [cnpj for cnpj in cnpjs if cnpj and cnpj not in cache]
    if missing:
        log(f"[CASA_PRELOAD] missing={len(missing)} workers={workers}")
    with ThreadPoolExecutor(max_workers=max(1, int(workers or 5))) as executor:
        futures = {executor.submit(fetch_casa_cnpj, cnpj, api_key): cnpj for cnpj in missing}
        for future in as_completed(futures):
            cnpj = futures[future]
            try:
                cache[cnpj] = future.result()
            except Exception as exc:
                cache[cnpj] = {"status_code": 0, "error": str(exc)[:300]}
            if int((cache.get(cnpj) or {}).get("status_code") or 0) == 200:
                log(f"[CASA_OK] cnpj={cnpj} endpoint={(cache.get(cnpj) or {}).get('endpoint') or 'cache_antigo'}")
            else:
                log(f"[CASA_ERRO] cnpj={cnpj} status={(cache.get(cnpj) or {}).get('status_code')}")
            save_cache(cache)
    return cache


def person_phones(person: Dict[str, Any]) -> List[str]:
    phones: List[str] = []
    for item in person.get("phone") or []:
        value = item.get("value") if isinstance(item, dict) else item
        phone = clean_phone(value)
        if phone and phone not in phones:
            phones.append(phone)
    return phones


def person_emails(person: Dict[str, Any]) -> List[str]:
    emails: List[str] = []
    for item in person.get("email") or []:
        value = item.get("value") if isinstance(item, dict) else item
        email = clean_email(value)
        if email and email not in emails:
            emails.append(email)
    return emails


def person_org_id(person: Dict[str, Any]) -> int:
    return extract_id(person.get("org_id"))


def participant_person_id(participant: Dict[str, Any]) -> int:
    return extract_id(participant.get("person_id") or participant.get("person"))


def participant_id(participant: Dict[str, Any]) -> int:
    return int(participant.get("id") or participant.get("participant_id") or 0)


def format_phone_payload(phones: List[str]) -> List[Dict[str, Any]]:
    return [{"value": phone, "primary": idx == 0, "label": "work"} for idx, phone in enumerate(phones[:5])]


def format_email_payload(emails: List[str]) -> List[Dict[str, Any]]:
    return [{"value": email, "primary": idx == 0, "label": "work"} for idx, email in enumerate(emails[:5])]


def contact_candidates(casa_payload: Dict[str, Any], fallback_name: str) -> List[Dict[str, Any]]:
    phones = list(casa_payload.get("phones") or [])
    emails = list(casa_payload.get("emails") or [])
    socios = [str(x or "").strip() for x in list(casa_payload.get("socios") or []) if str(x or "").strip()]
    company_name = str(casa_payload.get("company_name") or fallback_name or "Contato").strip()[:120]
    candidates: List[Dict[str, Any]] = []
    max_count = max(len(phones), len(emails), 1)
    for idx in range(min(MAX_PARTICIPANTS_PER_DEAL, max_count)):
        phone = phones[idx] if idx < len(phones) else ""
        email = emails[idx] if idx < len(emails) else ""
        name = socios[idx] if idx < len(socios) else company_name
        if not phone and not email:
            continue
        candidates.append({"name": name or company_name, "phone": phone, "email": email})
    return candidates[:MAX_PARTICIPANTS_PER_DEAL]


def find_reusable_person(client: PipedriveClient, org_id: int, phone: str, email: str) -> Dict[str, Any]:
    for term_kind, term in (("phone", phone), ("email", email)):
        if not term:
            continue
        for item in client.find_person_by_term(term):
            found = dict(item.get("item") or {})
            found_id = int(found.get("id") or 0) if found else 0
            full = client.get_person_details(found_id) if found_id else {}
            if not full:
                continue
            if term_kind == "phone" and term not in set(person_phones(full)):
                log(f"[PERSON_SEARCH_INEXATO] person={found_id} phone_busca={term}")
                continue
            if term_kind == "email" and term not in set(person_emails(full)):
                log(f"[PERSON_SEARCH_INEXATO] person={found_id} email_busca={term}")
                continue
            found_org = person_org_id(full)
            if found_org == int(org_id):
                return full
            log(f"[PERSON_CONFLITO_OUTRO_ORG] person={found_id} person_org={found_org} expected_org={org_id} {term_kind}={term}")
            if term_kind == "phone":
                return {"__conflict__": True, "id": found_id, "org_id": found_org}
            continue
    return {}


def create_or_update_person(client: PipedriveClient, org_id: int, candidate: Dict[str, Any], *, apply: bool) -> int:
    phone = clean_phone(candidate.get("phone"))
    email = clean_email(candidate.get("email"))
    name = str(candidate.get("name") or "Contato").strip()[:120] or "Contato"
    reusable = find_reusable_person(client, org_id, phone, email)
    if reusable.get("__conflict__"):
        log(f"[PERSON_SKIP_CONFLITO] existing_person={reusable.get('id')} existing_org={reusable.get('org_id')} expected_org={org_id}")
        return 0
    payload: Dict[str, Any] = {}
    if phone:
        payload["phone"] = format_phone_payload([phone])
    if email:
        payload["email"] = format_email_payload([email])
    if reusable:
        person_id = int(reusable.get("id") or 0)
        current_phones = person_phones(reusable)
        current_emails = person_emails(reusable)
        update_payload: Dict[str, Any] = {}
        merged_phones = [x for x in [phone] + current_phones if x]
        merged_emails = [x for x in [email] + current_emails if x]
        if merged_phones and merged_phones[:5] != current_phones[:5]:
            update_payload["phone"] = format_phone_payload(list(dict.fromkeys(merged_phones))[:5])
        if merged_emails and merged_emails[:5] != current_emails[:5]:
            update_payload["email"] = format_email_payload(list(dict.fromkeys(merged_emails))[:5])
        if apply and update_payload:
            client.update_person(person_id, update_payload)
        log(f"[PERSON_OK] person={person_id} reused=1")
        return person_id
    payload["name"] = name
    payload["org_id"] = int(org_id)
    if not (phone or email):
        return 0
    if not apply:
        log(f"[PERSON_OK] person=DRY_CREATE reused=0")
        return -1
    created = client.create_person(payload)
    person_id = int(created.get("id") or 0)
    if person_id:
        log(f"[PERSON_OK] person={person_id} reused=0")
    return person_id


def should_remove_person(person: Dict[str, Any], expected_org_id: int, allowed_phones: set[str], allowed_emails: set[str]) -> bool:
    if not person:
        return False
    phones = set(person_phones(person))
    emails = set(person_emails(person))
    org_id = person_org_id(person)
    if org_id and expected_org_id and org_id != expected_org_id:
        return True
    if phones and not (phones & allowed_phones):
        return True
    if not phones and emails and allowed_emails and not (emails & allowed_emails):
        return True
    name = normalize_text(person.get("name"))
    if name in {"juridico", "financeiro", "comercial", "contato", "fiscal"}:
        return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--delay", type=float, default=PIPEDRIVE_DELAY_SEC)
    args = parser.parse_args()

    rows = [
        row
        for row in parse_xlsx(INPUT_XLSX)
        if str(row.get("acao") or "").strip().upper() == "UPDATE"
        and int(str(row.get("deal_id") or "0") or 0) > 0
        and int(str(row.get("org_id") or "0") or 0) > 0
        and normalize_cnpj(row.get("cnpj"))
    ]
    rows.sort(key=lambda row: int(str(row.get("deal_id") or "0") or 0))
    selected = rows[max(0, args.offset): max(0, args.offset) + max(1, args.limit)]
    cnpjs = sorted({normalize_cnpj(row.get("cnpj")) for row in selected if normalize_cnpj(row.get("cnpj"))})
    casa_cache = preload_casa(cnpjs, workers=args.workers)
    client = PipedriveClient()
    counters: Counter[str] = Counter()
    log(f"[RESET_PARTICIPANTS_INICIO] selected={len(selected)} apply={int(args.apply)} offset={args.offset} limit={args.limit}")

    for idx, row in enumerate(selected, start=1):
        deal_id = int(str(row.get("deal_id") or "0") or 0)
        org_id = int(str(row.get("org_id") or "0") or 0)
        cnpj = normalize_cnpj(row.get("cnpj"))
        fallback_name = str(row.get("nome_final") or "").strip()
        casa = casa_cache.get(cnpj) or {}
        if int(casa.get("status_code") or 0) != 200:
            counters["skip_casa"] += 1
            continue
        candidates = contact_candidates(casa, fallback_name)
        allowed_phones = {clean_phone(item.get("phone")) for item in candidates if clean_phone(item.get("phone"))}
        allowed_emails = {clean_email(item.get("email")) for item in candidates if clean_email(item.get("email"))}
        log(f"[DEAL_RESET] {idx}/{len(selected)} deal={deal_id} org={org_id} candidates={len(candidates)}")
        deal = client.get_deal_details(deal_id)
        if not deal:
            counters["deal_erro"] += 1
            continue
        if int(deal.get("pipeline_id") or 0) != PIPELINE_ID:
            counters["fora_pipeline"] += 1
            continue
        live_org_id = extract_id(deal.get("org_id"))
        if live_org_id != org_id:
            counters["org_mismatch"] += 1
            log(f"[ORG_MISMATCH] deal={deal_id} live_org={live_org_id} expected_org={org_id}")
            if not args.apply:
                continue
            if client.update_deal(deal_id, {"org_id": org_id}):
                counters["org_vinculo_ok"] += 1
                log(f"[ORG_VINCULO_OK] deal={deal_id} org={org_id}")
            else:
                counters["org_vinculo_erro"] += 1
                continue

        target_person_ids: List[int] = []
        for candidate in candidates:
            person_id = create_or_update_person(client, org_id, candidate, apply=args.apply)
            if person_id > 0 and person_id not in target_person_ids:
                target_person_ids.append(person_id)
            elif person_id == -1:
                target_person_ids.append(-1)
        if not target_person_ids:
            counters["sem_person_ok"] += 1
        elif args.apply:
            first_id = target_person_ids[0]
            if first_id > 0 and extract_id(deal.get("person_id")) != first_id:
                if client.update_deal(deal_id, {"person_id": first_id}):
                    log(f"[VINCULO_OK] deal={deal_id} primary_person={first_id}")
                    counters["primary_ok"] += 1

        participants = client.get_deal_participants(deal_id, limit=100)
        existing_participant_persons = {participant_person_id(item) for item in participants}
        if args.apply:
            for person_id in target_person_ids[1:]:
                if person_id > 0 and person_id not in existing_participant_persons:
                    if client.add_deal_participant(deal_id, person_id):
                        log(f"[VINCULO_OK] deal={deal_id} participant_person={person_id}")
                        counters["participant_add"] += 1

        for participant in participants:
            p_id = participant_person_id(participant)
            part_id = participant_id(participant)
            if not p_id or p_id in set(x for x in target_person_ids if x > 0):
                continue
            person = client.get_person_details(p_id)
            if should_remove_person(person, org_id, allowed_phones, allowed_emails):
                counters["remover"] += 1
                if args.apply and part_id:
                    if client.remove_deal_participant(deal_id, part_id):
                        log(f"[REMOVIDO_CONTAMINADO] deal={deal_id} participant={part_id} person={p_id}")
                        counters["removido"] += 1
                else:
                    log(f"[REMOVIDO_CONTAMINADO] deal={deal_id} participant={part_id} person={p_id} dry=1")

        counters["deal_corrigido"] += 1
        time.sleep(float(args.delay))

    log("[RESET_PARTICIPANTS_RESUMO] " + " ".join(f"{key}={value}" for key, value in sorted(counters.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
