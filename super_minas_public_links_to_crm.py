from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, unquote, urlparse

import requests

from crm.pipedrive_client import PipedriveClient
from enrich_missing_cnpj_from_xlsx import bucket, fetch_casa, load_cache, save_cache, validate_casa_auth


OPORTUNIDADOS_TOKEN = "6a15950c356566f697425b11567f391e5993e544d3030bc836e41fd07a2599dd"
OPORTUNIDADOS_BASE = "https://app.oportunidados.com.br"
CNPJ_FIELD_KEY = "aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0"
OUT_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\super_minas_public_links_report.json")
REQUEST_TIMEOUT = 20
OPORTUNIDADOS_DELAY_SEC = 3.2
SEARCH_DELAY_SEC = 1.0
ROLE_PATTERNS = (
    "CEO",
    "Head de Marketing",
    "Gerente de Marketing",
    "Head de Vendas",
    "Gerente de Vendas",
)
GENERIC_EMAILS = {"n/a", "na", "none", "null"}


def log(message: str) -> None:
    print(str(message), flush=True)


def normalize_cnpj(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    return digits if len(digits) == 14 else ""


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    return digits if 10 <= len(digits) <= 11 else ""


def phone_is_valid(value: str) -> bool:
    if not value:
        return False
    if value in {"11999999999", "11999998888", "1804330972", "18043309723", "00999999977"}:
        return False
    if len(set(value)) <= 2:
        return False
    if value[-8:] in {"99999999", "00000000", "12345678"}:
        return False
    return True


def email_is_valid(value: str) -> bool:
    email = str(value or "").strip().lower()
    if not email or email in GENERIC_EMAILS:
        return False
    return "@" in email and "." in email


def extract_id(value: Any) -> int:
    if isinstance(value, dict):
        return int(value.get("value") or value.get("id") or 0)
    try:
        return int(value or 0)
    except Exception:
        return 0


def existing_person_phones(person: Dict[str, Any]) -> List[str]:
    output: List[str] = []
    for item in person.get("phone") or []:
        raw = item.get("value") if isinstance(item, dict) else item
        phone = normalize_phone(raw)
        if phone and phone_is_valid(phone) and phone not in output:
            output.append(phone)
    return output


def existing_person_emails(person: Dict[str, Any]) -> List[str]:
    output: List[str] = []
    for item in person.get("email") or []:
        raw = item.get("value") if isinstance(item, dict) else item
        email = str(raw or "").strip().lower()
        if email_is_valid(email) and email not in output:
            output.append(email)
    return output


def get_super_minas_deals(client: PipedriveClient, limit: int, offset: int = 0) -> List[Dict[str, Any]]:
    labels = client.get_deal_labels()
    output: List[Dict[str, Any]] = []
    skipped = 0
    for deal in client.get_deals(status="open", limit=3000, pipeline_id=2):
        tokens = client.resolve_label_tokens(deal.get("label"), labels) if deal.get("label") else set()
        upper = {str(token or "").strip().upper() for token in tokens}
        if "SUPER_MINAS" not in upper and "SUPER MINAS" not in upper:
            continue
        if skipped < max(0, int(offset)):
            skipped += 1
            continue
        output.append(deal)
        if len(output) >= max(1, int(limit)):
            break
    return output


def oportunidades_company(cnpj: str) -> Dict[str, Any]:
    if not cnpj:
        return {}
    response = requests.get(
        f"{OPORTUNIDADOS_BASE}/api/v1/brazilian_companies/{cnpj}/company",
        headers={"Authorization": f"Bearer {OPORTUNIDADOS_TOKEN}"},
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code != 200:
        return {"status_code": response.status_code, "raw": response.text[:300]}
    payload = response.json()
    payload["status_code"] = 200
    return payload


def ddg_html(query: str) -> str:
    response = requests.get(
        "https://html.duckduckgo.com/html/",
        params={"q": query},
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.text


def extract_urls(html: str, host_filter: str = "") -> List[str]:
    urls: List[str] = []
    seen = set()
    for raw in re.findall(r'href="([^"]+)"', str(html or "")):
        candidate = raw
        if raw.startswith("/l/?"):
            parsed = urlparse(raw)
            candidate = parse_qs(parsed.query).get("uddg", [""])[0]
        candidate = unquote(candidate)
        if not candidate.startswith("http"):
            continue
        host = urlparse(candidate).netloc.lower()
        if host_filter and host_filter not in host:
            continue
        if candidate not in seen:
            seen.add(candidate)
            urls.append(candidate)
    return urls


def linkedin_public_links(company_name: str) -> List[str]:
    links: List[str] = []
    company_queries = [
        f'"{company_name}" site:linkedin.com/company',
        f'"{company_name}" site:linkedin.com/in',
    ]
    for query in company_queries:
        try:
            html = ddg_html(query)
            for url in extract_urls(html, host_filter="linkedin.com"):
                if url not in links:
                    links.append(url)
            time.sleep(SEARCH_DELAY_SEC)
        except Exception:
            continue
        if len(links) >= 2:
            break
    for role in ROLE_PATTERNS:
        query = f'"{company_name}" "{role}" site:linkedin.com/in'
        try:
            html = ddg_html(query)
            for url in extract_urls(html, host_filter="linkedin.com"):
                if url not in links:
                    links.append(url)
            time.sleep(SEARCH_DELAY_SEC)
        except Exception:
            continue
        if len(links) >= 4:
            break
    return links[:4]


def best_new_phone(existing: List[str], payload: Dict[str, Any]) -> str:
    company = payload.get("company") if isinstance(payload.get("company"), dict) else {}
    candidates = [
        normalize_phone("".join([str(company.get("ddd_1") or ""), str(company.get("telefone_1") or "")])),
        normalize_phone(company.get("telefone_1")),
    ]
    for phone in candidates:
        if phone and phone_is_valid(phone) and phone not in existing:
            return phone
    return ""


def best_new_email(existing: List[str], payload: Dict[str, Any]) -> str:
    company = payload.get("company") if isinstance(payload.get("company"), dict) else {}
    direct = str(company.get("email") or "").strip().lower()
    if email_is_valid(direct) and direct not in existing:
        return direct
    for item in payload.get("contatos_extras") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("tipo_contato") or "").strip().lower() != "email":
            continue
        email = str(item.get("contato") or "").strip().lower()
        if email_is_valid(email) and email not in existing:
            return email
    return ""


def casa_phones(payload: Dict[str, Any]) -> List[str]:
    phones: List[str] = []
    for raw in list(payload.get("telefones") or []):
        phone = normalize_phone(raw)
        if phone and phone_is_valid(phone) and phone not in phones:
            phones.append(phone)
    return phones


def casa_emails(payload: Dict[str, Any]) -> List[str]:
    emails: List[str] = []
    for raw in list(payload.get("emails") or []):
        email = str(raw or "").strip().lower()
        if email_is_valid(email) and email not in emails:
            emails.append(email)
    return emails


def website_from_payload(payload: Dict[str, Any]) -> str:
    for item in payload.get("contatos_extras") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("tipo_contato") or "").strip().lower() != "website":
            continue
        url = str(item.get("contato") or "").strip()
        if url.startswith("http"):
            return url
    return ""


def build_note(company_name: str, cnpj: str, payload: Dict[str, Any], linkedin_links: List[str], new_phone: str, new_email: str) -> str:
    company = payload.get("company") if isinstance(payload.get("company"), dict) else {}
    socios = []
    for item in payload.get("socios") or []:
        if not isinstance(item, dict):
            continue
        nome = str(item.get("nome_socio") or "").strip()
        qual = str(item.get("qualificacao_socio") or "").strip()
        if nome:
            socios.append(f"{nome} - {qual}".strip(" -"))
    lines = [
        "Enriquecimento Super Minas",
        f"Empresa: {company_name}",
        f"CNPJ: {cnpj}",
    ]
    if new_phone:
        lines.append(f"Telefone novo sugerido: {new_phone}")
    if new_email:
        lines.append(f"Email novo sugerido: {new_email}")
    website = website_from_payload(payload)
    if website:
        lines.append(f"Website: {website}")
    if socios:
        lines.append("Sócios/decisores:")
        lines.extend(f"- {item}" for item in socios[:5])
    if linkedin_links:
        lines.append("Links públicos relevantes:")
        lines.extend(f"- {url}" for url in linkedin_links)
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    client = PipedriveClient()
    PipedriveClient.GLOBAL_MIN_INTERVAL_SEC = 0.4
    cache = load_cache()
    casa_cache = bucket(cache, "casa")
    casa_auth_ok = validate_casa_auth(casa_cache)
    deals = get_super_minas_deals(client, limit=max(1, int(args.limit)), offset=max(0, int(args.offset)))
    counters: Counter[str] = Counter()
    results: List[Dict[str, Any]] = []

    for index, deal in enumerate(deals, start=1):
        deal_id = int(deal.get("id") or 0)
        org_id = extract_id(deal.get("org_id"))
        person_id = extract_id(deal.get("person_id"))
        organization = client.get_organization(org_id) if org_id else {}
        person = client.get_person_details(person_id) if person_id else {}
        company_name = str(organization.get("name") or (deal.get("org_id") or {}).get("name") or deal.get("title") or "").strip()
        cnpj = normalize_cnpj(organization.get(CNPJ_FIELD_KEY))
        log(f"[SUPER_MINAS_PUBLIC] {index}/{len(deals)} deal={deal_id} org={org_id} nome={company_name}")

        result: Dict[str, Any] = {
            "deal_id": deal_id,
            "org_id": org_id,
            "person_id": person_id,
            "company_name": company_name,
            "cnpj": cnpj,
        }
        if not cnpj:
            result["status"] = "skip_no_cnpj"
            results.append(result)
            counters["skip_no_cnpj"] += 1
            continue

        payload = oportunidades_company(cnpj)
        result["oportunidados_status"] = int(payload.get("status_code") or 0)
        if int(payload.get("status_code") or 0) != 200:
            result["status"] = "skip_oportunidados_error"
            results.append(result)
            counters["skip_oportunidados_error"] += 1
            time.sleep(OPORTUNIDADOS_DELAY_SEC)
            continue

        casa_payload: Dict[str, Any] = {}
        if casa_auth_ok:
            casa_payload = fetch_casa(cnpj, casa_cache)
            save_cache(cache)
            result["casa_status"] = int(casa_payload.get("status_code") or 0)
            if int(casa_payload.get("status_code") or 0) == 200:
                counters["casa_ok"] += 1

        existing_phones = existing_person_phones(person)
        existing_emails = existing_person_emails(person)
        new_phone = best_new_phone(existing_phones, payload)
        new_email = best_new_email(existing_emails, payload)
        if not new_phone:
            for phone in casa_phones(casa_payload):
                if phone not in existing_phones:
                    new_phone = phone
                    break
        if not new_email:
            for email in casa_emails(casa_payload):
                if email not in existing_emails:
                    new_email = email
                    break
        linkedin_links = linkedin_public_links(company_name)

        result["new_phone"] = new_phone
        result["new_email"] = new_email
        result["linkedin_links"] = linkedin_links
        result["socios_count"] = len(payload.get("socios") or [])
        result["website"] = website_from_payload(payload)

        note_saved = False
        person_updated = False
        person_payload: Dict[str, Any] = {}
        if person_id:
            if new_phone:
                merged = existing_phones + ([new_phone] if new_phone not in existing_phones else [])
                person_payload["phone"] = [{"value": phone, "primary": idx == 0, "label": "work"} for idx, phone in enumerate(merged[:5])]
            if new_email:
                merged = existing_emails + ([new_email] if new_email not in existing_emails else [])
                person_payload["email"] = [{"value": email, "primary": idx == 0, "label": "work"} for idx, email in enumerate(merged[:5])]
            if args.apply and person_payload:
                person_updated = client.update_person(person_id, person_payload)

        note = build_note(company_name, cnpj, payload, linkedin_links, new_phone, new_email)
        if args.apply and (linkedin_links or new_phone or new_email or result["website"] or result["socios_count"]):
            note_saved = client.add_note(deal_id=deal_id, content=note)

        result["person_updated"] = bool(person_updated)
        result["note_saved"] = bool(note_saved)
        results.append(result)

        if new_phone:
            counters["phones_found"] += 1
        if new_email:
            counters["emails_found"] += 1
        if linkedin_links:
            counters["linkedin_rows"] += 1
            counters["linkedin_links"] += len(linkedin_links)
        if result["website"]:
            counters["websites_found"] += 1
        if result["socios_count"]:
            counters["socios_rows"] += 1
        if person_updated:
            counters["person_updated"] += 1
        if note_saved:
            counters["notes_saved"] += 1

        time.sleep(OPORTUNIDADOS_DELAY_SEC)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps({"summary": dict(counters), "results": results}, ensure_ascii=False, indent=2), encoding="utf-8")
    log("[SUPER_MINAS_PUBLIC_SUMMARY] " + " ".join(f"{k}={v}" for k, v in sorted(counters.items())))
    log(f"[SUPER_MINAS_PUBLIC_REPORT] {OUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
