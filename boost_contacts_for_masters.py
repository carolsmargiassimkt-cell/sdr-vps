from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List

from enrich_missing_cnpj_from_xlsx import (
    bucket,
    fetch_casa,
    load_cache,
    normalize_cnpj,
    normalize_name,
    normalize_phone,
    parse_xlsx,
    save_cache,
    search_casa_by_name,
    validate_casa_auth,
    write_xlsx,
)


INPUT_XLSX = Path(r"C:\Users\Asus\Downloads\deals_enriquecido_final.xlsx")
OUTPUT_XLSX = Path(r"C:\Users\Asus\Downloads\deals_enriquecido_final.xlsx")
BLOCKED_GENERIC_PHONES = {"18043309723", "11999999999", "00999999977"}


def log(message: str) -> None:
    print(str(message), flush=True)


def clean_phone(value: Any) -> str:
    phone = normalize_phone(value)
    return "" if phone in BLOCKED_GENERIC_PHONES else phone


def phone_is_mobile(phone: str) -> bool:
    return len(phone) == 11 and phone[2] == "9"


def collect_phones(node: Any) -> Iterable[str]:
    if isinstance(node, dict):
        lowered = {str(k).lower(): v for k, v in node.items()}
        if any(any(token in key for token in ("telefone", "celular", "fone", "whatsapp", "ddd", "numero")) for key in lowered):
            combined_fields: List[str] = []
            for key in ("ddd", "telefone", "numero", "celular", "fone", "whatsapp"):
                value = lowered.get(key)
                if value is not None:
                    combined_fields.append(str(value))
            if combined_fields:
                candidate = clean_phone("".join(combined_fields))
                if candidate:
                    yield candidate
            for value in lowered.values():
                candidate = clean_phone(value)
                if candidate:
                    yield candidate
        for value in node.values():
            yield from collect_phones(value)
    elif isinstance(node, list):
        for item in node:
            yield from collect_phones(item)
    else:
        candidate = clean_phone(node)
        if candidate:
            yield candidate


def collect_emails(node: Any) -> Iterable[str]:
    if isinstance(node, dict):
        for key, value in node.items():
            lowered = str(key).lower()
            if "mail" in lowered or "email" in lowered:
                text = str(value or "").strip().lower()
                if "@" in text:
                    yield text
            yield from collect_emails(value)
    elif isinstance(node, list):
        for item in node:
            yield from collect_emails(item)


def best_phone(candidates: Iterable[str], current_counts: Counter[str]) -> str:
    phones = []
    seen = set()
    for raw in candidates:
        phone = clean_phone(raw)
        if not phone or phone in seen:
            continue
        seen.add(phone)
        phones.append(phone)
    if not phones:
        return ""
    mobiles = [phone for phone in phones if phone_is_mobile(phone)]
    source = mobiles or phones
    source.sort(key=lambda phone: (current_counts.get(phone, 0), 0 if phone_is_mobile(phone) else 1, phone))
    chosen = source[0]
    return "" if current_counts.get(chosen, 0) > 0 else chosen


def payload_contact_fields(payload: Dict[str, Any]) -> tuple[List[str], List[str]]:
    phones = list(payload.get("telefones") or [])
    emails = list(payload.get("emails") or [])
    raw = payload.get("raw")
    if raw:
        phones.extend(list(collect_phones(raw)))
        emails.extend(list(collect_emails(raw)))
    clean_phones: List[str] = []
    seen_phone = set()
    for phone in phones:
        normalized = clean_phone(phone)
        if normalized and normalized not in seen_phone:
            seen_phone.add(normalized)
            clean_phones.append(normalized)
    clean_emails: List[str] = []
    seen_email = set()
    for email in emails:
        normalized = str(email or "").strip().lower()
        if "@" in normalized and normalized not in seen_email:
            seen_email.add(normalized)
            clean_emails.append(normalized)
    return clean_phones, clean_emails


def main() -> int:
    rows = parse_xlsx(INPUT_XLSX)
    masters = [row for row in rows if str(row.get("acao") or "").strip().upper() in {"UPDATE", "RESGATAR"}]
    phone_counts = Counter(clean_phone(row.get("telefone")) for row in masters if clean_phone(row.get("telefone")))
    cache = load_cache()
    casa_cache = bucket(cache, "casa")
    casa_search_cache = bucket(cache, "casa_search")
    if not validate_casa_auth(casa_cache):
        raise SystemExit("[CASA_AUTH_FAIL]")
    targets = [
        row
        for row in masters
        if not clean_phone(row.get("telefone")) or not str(row.get("email") or "").strip() or not normalize_cnpj(row.get("cnpj"))
    ]
    targets.sort(key=lambda row: (0 if not clean_phone(row.get("telefone")) else 1, 0 if not normalize_cnpj(row.get("cnpj")) else 1, int(str(row.get("deal_id") or "0") or 0)))
    log(f"[FILTRO_CONTATOS] masters={len(masters)} targets={len(targets)}")
    phone_added = 0
    email_added = 0
    cnpj_added = 0
    casa_ok = 0
    for idx, row in enumerate(targets, start=1):
        deal_id = row.get("deal_id")
        name = normalize_name(row.get("nome_final"))
        cnpj = normalize_cnpj(row.get("cnpj"))
        log(f"[PROCESSANDO_CONTATO] {idx}/{len(targets)} deal={deal_id} empresa={name}")
        if not cnpj:
            cnpj = search_casa_by_name(name, casa_search_cache)
            if cnpj:
                row["cnpj"] = cnpj
                cnpj_added += 1
                log(f"[CNPJ_ENCONTRADO] deal={deal_id} cnpj={cnpj}")
        if not cnpj:
            save_cache(cache)
            continue
        payload = fetch_casa(cnpj, casa_cache)
        save_cache(cache)
        if int(payload.get("status_code") or 0) != 200:
            continue
        casa_ok += 1
        phones, emails = payload_contact_fields(payload)
        if not clean_phone(row.get("telefone")):
            chosen = best_phone(phones, phone_counts)
            if chosen:
                row["telefone"] = chosen
                phone_counts[chosen] += 1
                phone_added += 1
                log(f"[TELEFONE_CASA] deal={deal_id} telefone={chosen}")
        if not str(row.get("email") or "").strip() and emails:
            row["email"] = emails[0]
            email_added += 1
            log(f"[EMAIL_CASA] deal={deal_id} email={emails[0]}")
    for row in masters:
        phone = clean_phone(row.get("telefone"))
        if phone and phone_counts.get(phone, 0) > 1:
            row["telefone"] = ""
            log(f"[TELEFONE_DUPLICADO_REMOVIDO] deal={row.get('deal_id')} telefone={phone}")
    write_xlsx(rows, OUTPUT_XLSX)
    final_masters = [row for row in rows if str(row.get("acao") or "").strip().upper() in {"UPDATE", "RESGATAR"}]
    final_phone = sum(1 for row in final_masters if clean_phone(row.get("telefone")))
    final_email = sum(1 for row in final_masters if str(row.get("email") or "").strip())
    final_cnpj = sum(1 for row in final_masters if normalize_cnpj(row.get("cnpj")))
    log(f"[RESUMO_CONTATOS] casa_ok={casa_ok} cnpj_added={cnpj_added} phone_added={phone_added} email_added={email_added}")
    log(f"[RESUMO_FINAL] masters={len(final_masters)} com_cnpj={final_cnpj} com_telefone={final_phone} com_email={final_email}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
