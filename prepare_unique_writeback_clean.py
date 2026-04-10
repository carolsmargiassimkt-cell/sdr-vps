from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set

from enrich_missing_cnpj_from_xlsx import BLOCKED_GENERIC_PHONES, best_phone, normalize_cnpj, normalize_name, normalize_phone, parse_xlsx
from offline_final_sheet_enrichment import canonical_title, dynamic_write_xlsx


CASA_INPUT_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_oportunidados_enriched.csv")
PEOPLE_XLSX = Path(r"C:\Users\Asus\Downloads\people-25681240-15.xlsx")
OUTPUT_XLSX = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_writeback_clean_unique.xlsx")
OUTPUT_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_writeback_clean_unique.csv")
SUMMARY_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\deals_writeback_clean_unique_summary.json")


def read_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    headers: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in headers:
                headers.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def split_pipe(value: Any) -> List[str]:
    return [str(item).strip() for item in str(value or "").split("|") if str(item).strip()]


def normalize_email(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if "@" in text and "." in text.split("@")[-1] else ""


def email_bad(value: str) -> bool:
    text = normalize_email(value)
    if not text:
        return True
    if text.startswith("contato@empresa") or text.endswith("@example.com"):
        return True
    return False


def choose_primary_email(emails: List[str]) -> str:
    clean = []
    for email in emails:
        normalized = normalize_email(email)
        if normalized and not email_bad(normalized) and normalized not in clean:
            clean.append(normalized)
    if not clean:
        return ""
    clean.sort(key=lambda item: (item.startswith("contato@"), item.startswith("comercial@"), len(item), item))
    return clean[0]


def choose_org_name(row: Dict[str, Any]) -> str:
    for key in ("opp_razao_social", "casa_razao_social", "opp_nome_fantasia", "casa_nome_fantasia", "nome_padronizado", "nome_final"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def choose_deal_name(row: Dict[str, Any]) -> str:
    for key in ("opp_nome_fantasia", "casa_nome_fantasia", "nome_padronizado", "nome_final", "opp_razao_social", "casa_razao_social"):
        value = str(row.get(key) or "").strip()
        if value:
            return canonical_title(value)
    return ""


def valid_deal_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output = []
    for row in rows:
        cnpj = normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))
        action = str(row.get("acao") or "").strip().upper()
        if not cnpj or action == "ARCHIVE":
            continue
        output.append(dict(row))
    return output


def collect_company_candidates(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    companies: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        cnpj = normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))
        company = companies.setdefault(
            cnpj,
            {
                "rows": [],
                "phones": set(),
                "emails": set(),
                "name_candidates": set(),
                "address_candidates": set(),
                "aliases": set(),
            },
        )
        company["rows"].append(row)
        for phone in [row.get("telefone"), row.get("telefone_principal_sugerido"), *split_pipe(row.get("casa_telefones"))]:
            normalized = normalize_phone(phone)
            if normalized and normalized not in BLOCKED_GENERIC_PHONES:
                company["phones"].add(normalized)
        for phone in split_pipe(row.get("opp_company_phones")):
            normalized = normalize_phone(phone)
            if normalized and normalized not in BLOCKED_GENERIC_PHONES:
                company["phones"].add(normalized)
        for email in [row.get("email"), row.get("email_principal_sugerido"), *split_pipe(row.get("casa_emails")), *split_pipe(row.get("opp_company_emails"))]:
            normalized = normalize_email(email)
            if normalized and not email_bad(normalized):
                company["emails"].add(normalized)
        for candidate in [row.get("opp_razao_social"), row.get("casa_razao_social"), row.get("opp_nome_fantasia"), row.get("casa_nome_fantasia"), row.get("nome_padronizado"), row.get("nome_final")]:
            candidate = str(candidate or "").strip()
            if candidate:
                company["name_candidates"].add(candidate)
                company["aliases"].add(normalize_name(candidate))
        address = str(row.get("opp_endereco") or row.get("casa_endereco") or "").strip()
        if address:
            company["address_candidates"].add(address)
    return companies


def suspicious_cnpjs(companies: Dict[str, Dict[str, Any]]) -> Set[str]:
    flagged: Set[str] = set()
    for cnpj, company in companies.items():
        distinct_names = {normalize_name(row.get("nome_final")) for row in company["rows"] if normalize_name(row.get("nome_final"))}
        if len(distinct_names) >= 3:
            flagged.add(cnpj)
    return flagged


def load_people() -> List[Dict[str, Any]]:
    rows = parse_xlsx(PEOPLE_XLSX)
    output: List[Dict[str, Any]] = []
    for row in rows:
        phones = []
        for key in ("Pessoa - Telefone - Trabalho", "Pessoa - Telefone - residencial", "Pessoa - Telefone - celular", "Pessoa - Telefone - outros"):
            phone = normalize_phone(row.get(key))
            if phone and phone not in BLOCKED_GENERIC_PHONES and phone not in phones:
                phones.append(phone)
        emails = []
        for key in ("Pessoa - E-mail - trabalho", "Pessoa - E-mail - residencial", "Pessoa - E-mail - outros"):
            email = normalize_email(row.get(key))
            if email and not email_bad(email) and email not in emails:
                emails.append(email)
        output.append(
            {
                "name": str(row.get("Pessoa - Nome") or "").strip(),
                "org_name": str(row.get("Pessoa - Organização") or "").strip(),
                "org_norm": normalize_name(row.get("Pessoa - Organização")),
                "emails": emails,
                "phones": phones,
            }
        )
    return output


def choose_people(companies: Dict[str, Dict[str, Any]], people_rows: List[Dict[str, Any]], exclusive_phone_owner: Dict[str, str], exclusive_email_owner: Dict[str, str]) -> Dict[str, Dict[str, str]]:
    result: Dict[str, Dict[str, str]] = {}
    by_org: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for person in people_rows:
        if person["org_norm"]:
            by_org[person["org_norm"]].append(person)

    for cnpj, company in companies.items():
        aliases = set(company["aliases"])
        candidates: List[Dict[str, Any]] = []
        for alias in aliases:
            candidates.extend(by_org.get(alias, []))
        best = {"person_name": "", "person_email": "", "person_phone": ""}
        best_score = -1
        for person in candidates:
            if not person["name"] or person["name"].lower().startswith("contato - "):
                continue
            exclusive_emails = [email for email in person["emails"] if exclusive_email_owner.get(email) == cnpj]
            exclusive_phones = [phone for phone in person["phones"] if exclusive_phone_owner.get(phone) == cnpj]
            score = 0
            if exclusive_emails:
                score += 2
            if exclusive_phones:
                score += 2
            if person["name"]:
                score += 1
            if score > best_score:
                best_score = score
                best = {
                    "person_name": person["name"],
                    "person_email": choose_primary_email(exclusive_emails),
                    "person_phone": best_phone(exclusive_phones),
                }
        result[cnpj] = best
    return result


def main() -> int:
    rows = valid_deal_rows(read_csv(CASA_INPUT_CSV))
    companies = collect_company_candidates(rows)
    flagged_cnpjs = suspicious_cnpjs(companies)
    if flagged_cnpjs:
        rows = [row for row in rows if normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj")) not in flagged_cnpjs]
        companies = collect_company_candidates(rows)

    phone_to_cnpjs: Dict[str, Set[str]] = defaultdict(set)
    email_to_cnpjs: Dict[str, Set[str]] = defaultdict(set)
    for cnpj, company in companies.items():
        for phone in company["phones"]:
            phone_to_cnpjs[phone].add(cnpj)
        for email in company["emails"]:
            email_to_cnpjs[email].add(cnpj)

    exclusive_phone_owner = {phone: next(iter(cnpjs)) for phone, cnpjs in phone_to_cnpjs.items() if len(cnpjs) == 1}
    exclusive_email_owner = {email: next(iter(cnpjs)) for email, cnpjs in email_to_cnpjs.items() if len(cnpjs) == 1}

    people_rows = load_people()
    people_by_cnpj = choose_people(companies, people_rows, exclusive_phone_owner, exclusive_email_owner)

    output: List[Dict[str, Any]] = []
    for cnpj, company in companies.items():
        row0 = company["rows"][0]
        exclusive_phones = sorted([phone for phone in company["phones"] if exclusive_phone_owner.get(phone) == cnpj])
        exclusive_emails = sorted([email for email in company["emails"] if exclusive_email_owner.get(email) == cnpj])
        chosen_phone = best_phone(exclusive_phones)
        chosen_email = choose_primary_email(exclusive_emails)
        person = people_by_cnpj.get(cnpj, {})
        canonical_org_name = choose_org_name(row0)
        canonical_deal_name = choose_deal_name(row0)
        address = str(row0.get("opp_endereco") or row0.get("casa_endereco") or "").strip()
        socios_roles = str(row0.get("opp_socios_roles") or row0.get("opp_socios") or "").strip()
        for row in company["rows"]:
            current = dict(row)
            current["deal_title_clean"] = canonical_deal_name
            current["org_name_clean"] = canonical_org_name
            current["org_address_clean"] = address
            current["telefone_exclusivo"] = chosen_phone
            current["telefones_exclusivos"] = " | ".join(exclusive_phones)
            current["emails_exclusivos"] = " | ".join(exclusive_emails)
            current["email_exclusivo_principal"] = chosen_email
            current["person_name_clean"] = person.get("person_name", "")
            current["person_email_clean"] = person.get("person_email", "") or chosen_email
            current["person_phone_clean"] = person.get("person_phone", "") or chosen_phone
            current["decision_makers_hint"] = socios_roles
            current["phone_candidates_exclusive_count"] = str(len(exclusive_phones))
            current["email_candidates_exclusive_count"] = str(len(exclusive_emails))
            current["cnpj_root_company_key"] = cnpj
            output.append(current)

    dynamic_write_xlsx(output, OUTPUT_XLSX)
    write_csv(output, OUTPUT_CSV)
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "valid_rows": len(output),
        "unique_companies": len(companies),
        "flagged_suspicious_cnpjs": len(flagged_cnpjs),
        "rows_with_exclusive_phone": sum(1 for row in output if str(row.get("telefone_exclusivo") or "").strip()),
        "rows_with_exclusive_email": sum(1 for row in output if str(row.get("email_exclusivo_principal") or "").strip()),
        "rows_with_clean_person": sum(1 for row in output if str(row.get("person_name_clean") or "").strip()),
        "unique_exclusive_phones": len(exclusive_phone_owner),
        "unique_exclusive_emails": len(exclusive_email_owner),
    }
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
