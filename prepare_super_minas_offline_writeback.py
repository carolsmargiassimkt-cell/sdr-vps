from __future__ import annotations

import argparse
import json
import re
import time
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd
import requests

from crm.pipedrive_client import PipedriveClient
from enrich_missing_cnpj_from_xlsx import (
    bucket,
    fetch_casa,
    load_cache,
    save_cache,
    search_casa_by_name,
    validate_casa_auth,
)


OPORTUNIDADOS_TOKEN = "6a15950c356566f697425b11567f391e5993e544d3030bc836e41fd07a2599dd"
OPORTUNIDADOS_BASE = "https://app.oportunidados.com.br"
REQUEST_TIMEOUT = 20
OPORTUNIDADOS_DELAY_SEC = 3.2
INPUT_LIST = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\super_minas_target_companies_20260410.txt")
SOURCE_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_writeback_clean_unique.csv")
MANUAL_CNPJ_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\super_minas_manual_cnpj_overrides_20260410.csv")
MANUAL_PUBLIC_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\super_minas_manual_public_enrichment_20260410.csv")
OUT_XLSX = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\super_minas_offline_writeback_20260410.xlsx")
OUT_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\super_minas_offline_writeback_20260410.csv")
OUT_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\super_minas_offline_writeback_20260410_summary.json")

BLOCKED_PHONES = {
    "11999999999",
    "11999998888",
    "1804330972",
    "18043309723",
    "00999999977",
}
BLOCKED_EMAILS = {"n/a", "na", "none", "null", "info@example.com"}


def log(message: str) -> None:
    print(str(message), flush=True)


def normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " e ")
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\b(ltda|sa|s a|eireli|me|mei)\b", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_cnpj(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    return digits if len(digits) == 14 else ""


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    return digits if 10 <= len(digits) <= 11 else ""


def phone_is_valid(value: str) -> bool:
    if not value or value in BLOCKED_PHONES:
        return False
    if len(set(value)) <= 2:
        return False
    if value[-8:] in {"99999999", "00000000", "12345678"}:
        return False
    return True


def email_is_valid(value: str) -> bool:
    email = str(value or "").strip().lower()
    return bool(email and email not in BLOCKED_EMAILS and "@" in email and "." in email)


def split_multi(value: Any) -> List[str]:
    if value is None:
        return []
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return []
    parts = re.split(r"[|;,]\s*|\n+", text)
    return [part.strip() for part in parts if str(part or "").strip()]


def unique_keep_order(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def load_manual_overrides() -> Dict[str, Dict[str, str]]:
    if not MANUAL_CNPJ_CSV.exists():
        return {}
    df = pd.read_csv(MANUAL_CNPJ_CSV)
    output: Dict[str, Dict[str, str]] = {}
    for row in df.to_dict(orient="records"):
        key = normalize_text(row.get("company_input"))
        if not key:
            continue
        output[key] = {
            "cnpj": normalize_cnpj(row.get("cnpj")),
            "matched_name": str(row.get("matched_name") or "").strip(),
        }
    return output


def load_manual_public() -> Dict[str, Dict[str, str]]:
    if not MANUAL_PUBLIC_CSV.exists():
        return {}
    df = pd.read_csv(MANUAL_PUBLIC_CSV)
    output: Dict[str, Dict[str, str]] = {}
    for row in df.to_dict(orient="records"):
        key = normalize_text(row.get("company_input"))
        if not key:
            continue
        output[key] = {
            "phones": str(row.get("phones") or "").strip(),
            "emails": str(row.get("emails") or "").strip(),
            "websites": str(row.get("websites") or "").strip(),
            "notes": str(row.get("notes") or "").strip(),
        }
    return output


def oportunidades_company(cnpj: str) -> Dict[str, Any]:
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


def oportunidades_phones(payload: Dict[str, Any]) -> List[str]:
    company = payload.get("company") if isinstance(payload.get("company"), dict) else {}
    candidates = [
        normalize_phone(f"{company.get('ddd_1') or ''}{company.get('telefone_1') or ''}"),
        normalize_phone(company.get("telefone_1")),
    ]
    return unique_keep_order([phone for phone in candidates if phone_is_valid(phone)])


def oportunidades_emails(payload: Dict[str, Any]) -> List[str]:
    emails: List[str] = []
    company = payload.get("company") if isinstance(payload.get("company"), dict) else {}
    direct = str(company.get("email") or "").strip().lower()
    if email_is_valid(direct):
        emails.append(direct)
    for item in payload.get("contatos_extras") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("tipo_contato") or "").strip().lower() != "email":
            continue
        email = str(item.get("contato") or "").strip().lower()
        if email_is_valid(email):
            emails.append(email)
    return unique_keep_order(emails)


def oportunidades_websites(payload: Dict[str, Any]) -> List[str]:
    output: List[str] = []
    for item in payload.get("contatos_extras") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("tipo_contato") or "").strip().lower() != "website":
            continue
        url = str(item.get("contato") or "").strip()
        if url.startswith("http"):
            output.append(url)
    return unique_keep_order(output)


def oportunidades_socios(payload: Dict[str, Any]) -> List[str]:
    output: List[str] = []
    for item in payload.get("socios") or []:
        if not isinstance(item, dict):
            continue
        nome = str(item.get("nome_socio") or "").strip()
        qual = str(item.get("qualificacao_socio") or "").strip()
        if nome:
            output.append(f"{nome} ({qual})" if qual else nome)
    return unique_keep_order(output)


def candidate_strings_from_row(row: Dict[str, Any]) -> List[str]:
    fields = [
        row.get("nome_final"),
        row.get("deal_title_clean"),
        row.get("org_name_clean"),
        row.get("casa_razao_social"),
        row.get("casa_nome_fantasia"),
        row.get("opp_razao_social"),
        row.get("opp_nome_fantasia"),
    ]
    return [normalize_text(value) for value in fields if normalize_text(value)]


def score_company_match(query: str, candidates: List[str]) -> float:
    q_tokens = set(query.split())
    if not q_tokens or not candidates:
        return 0.0
    best = 0.0
    for cand in candidates:
        c_tokens = set(cand.split())
        if not c_tokens:
            continue
        if query == cand:
            return 1.0
        if query in cand or cand in query:
            best = max(best, 0.90)
        inter = len(q_tokens & c_tokens)
        union = len(q_tokens | c_tokens)
        if union:
            overlap = inter / union
            coverage = inter / len(q_tokens)
            cand_coverage = inter / len(c_tokens)
            best = max(best, overlap, (coverage * 0.7 + cand_coverage * 0.3))
    return best


def name_fit_score(query: str, matched_name: str) -> float:
    return score_company_match(normalize_text(query), [normalize_text(matched_name)])


def extract_live_super_minas(client: PipedriveClient) -> List[Dict[str, Any]]:
    labels = client.get_deal_labels()
    output: List[Dict[str, Any]] = []
    for deal in client.get_deals(status="open", limit=3000, pipeline_id=2):
        tokens = client.resolve_label_tokens(deal.get("label"), labels) if deal.get("label") else set()
        tokens = {str(token or "").strip().upper() for token in tokens}
        if "SUPER_MINAS" not in tokens and "SUPER MINAS" not in tokens:
            continue
        org = deal.get("org_id") if isinstance(deal.get("org_id"), dict) else {}
        output.append(
            {
                "deal_id": int(deal.get("id") or 0),
                "org_id": int((org or {}).get("value") or (org or {}).get("id") or 0),
                "deal_title": str(deal.get("title") or "").strip(),
                "org_name": str((org or {}).get("name") or "").strip(),
                "person_name": str((deal.get("person_id") or {}).get("name") or "").strip() if isinstance(deal.get("person_id"), dict) else "",
                "match_candidates": [
                    normalize_text(deal.get("title")),
                    normalize_text((org or {}).get("name")),
                ],
            }
        )
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit-missing", type=int, default=9999)
    args = parser.parse_args()

    companies = [line.strip() for line in INPUT_LIST.read_text(encoding="utf-8").splitlines() if line.strip()]
    df = pd.read_csv(SOURCE_CSV)
    rows = df.to_dict(orient="records")
    manual_overrides = load_manual_overrides()
    manual_public = load_manual_public()
    client = PipedriveClient()
    live_super_minas = extract_live_super_minas(client)

    cache = load_cache()
    casa_cache = bucket(cache, "casa")
    casa_search_cache = bucket(cache, "casa_search")
    opp_cache = bucket(cache, "super_minas_opp_company")
    if not validate_casa_auth(casa_cache):
        raise SystemExit("[CASA_AUTH_FAIL]")

    row_records = []
    missing_queue = []
    for company in companies:
        norm = normalize_text(company)
        best_row = None
        best_score = 0.0
        for row in rows:
            score = score_company_match(norm, candidate_strings_from_row(row))
            if score > best_score:
                best_score = score
                best_row = row
        best_live = None
        best_live_score = 0.0
        for item in live_super_minas:
            score = score_company_match(norm, item.get("match_candidates") or [])
            if score > best_live_score:
                best_live_score = score
                best_live = item
        record = {
            "company_input": company,
            "company_norm": norm,
            "csv_match_score": round(best_score, 4),
            "crm_match_score": round(best_live_score, 4),
            "status": "",
            "deal_id": "",
            "org_id": "",
            "matched_name": "",
            "cnpj": "",
            "crm_phone": "",
            "crm_email": "",
            "casa_phones": "",
            "casa_emails": "",
            "opp_phones": "",
            "opp_emails": "",
            "websites": "",
            "decision_makers": "",
            "address": "",
            "source": "",
            "manual_public_notes": "",
        }
        if best_row and best_score >= 0.78:
            record["status"] = "matched_existing"
            record["source"] = "csv"
            record["deal_id"] = str(best_row.get("deal_id") or "")
            record["org_id"] = str(best_row.get("org_id") or "")
            record["matched_name"] = str(best_row.get("org_name_clean") or best_row.get("nome_final") or "")
            record["cnpj"] = normalize_cnpj(best_row.get("cnpj") or best_row.get("cnpj_enriquecido"))
            record["crm_phone"] = str(best_row.get("telefone_exclusivo") or best_row.get("telefone") or "")
            record["crm_email"] = str(best_row.get("email_exclusivo_principal") or best_row.get("email") or "")
            record["casa_phones"] = str(best_row.get("casa_telefones") or "")
            record["casa_emails"] = str(best_row.get("casa_emails") or "")
            record["opp_phones"] = str(best_row.get("opp_company_phones") or "")
            record["opp_emails"] = str(best_row.get("opp_company_emails") or "")
            record["websites"] = str(best_row.get("opp_websites") or "")
            record["decision_makers"] = str(best_row.get("decision_makers_hint") or best_row.get("opp_socios") or "")
            record["address"] = str(best_row.get("org_address_clean") or best_row.get("casa_endereco") or best_row.get("opp_endereco") or "")
        elif best_live and best_live_score >= 0.82:
            record["status"] = "matched_live_only"
            record["source"] = "crm"
            record["deal_id"] = str(best_live.get("deal_id") or "")
            record["org_id"] = str(best_live.get("org_id") or "")
            record["matched_name"] = str(best_live.get("org_name") or best_live.get("deal_title") or "")
        else:
            record["status"] = "missing_lookup"
            missing_queue.append(record)
        manual = manual_overrides.get(norm)
        if manual and manual.get("cnpj"):
            record["status"] = "resolved_manual"
            record["cnpj"] = manual["cnpj"]
            if manual.get("matched_name"):
                record["matched_name"] = manual["matched_name"]
            record["source"] = "manual"
        public = manual_public.get(norm)
        if public:
            record["casa_phones"] = public.get("phones", "")
            record["casa_emails"] = public.get("emails", "")
            record["websites"] = public.get("websites", "")
            record["manual_public_notes"] = public.get("notes", "")
        row_records.append(record)

    processed_missing = 0
    for record in row_records:
        if record["status"] not in {"missing_lookup", "resolved_manual"}:
            continue
        if record["status"] == "missing_lookup" and processed_missing >= max(0, int(args.limit_missing)):
            break
        company = record["company_input"]
        cnpj = normalize_cnpj(record.get("cnpj"))
        if not cnpj:
            cnpj = search_casa_by_name(company, casa_search_cache)
            save_cache(cache)
            if not cnpj:
                record["status"] = "missing_cnpj"
                continue
            record["cnpj"] = cnpj
            record["status"] = "resolved_by_name"
            processed_missing += 1

        casa_payload = fetch_casa(cnpj, casa_cache)
        save_cache(cache)
        opp_payload = opp_cache.get(cnpj)
        if not isinstance(opp_payload, dict) or "status_code" not in opp_payload:
            try:
                opp_payload = oportunidades_company(cnpj)
            except Exception as exc:
                opp_payload = {"status_code": 0, "raw": str(exc)[:300]}
            opp_cache[cnpj] = opp_payload
            save_cache(cache)
            time.sleep(OPORTUNIDADOS_DELAY_SEC)

        casa_phones = [phone for phone in split_multi(casa_payload.get("telefones")) if phone_is_valid(normalize_phone(phone))]
        casa_phones = [normalize_phone(phone) for phone in casa_phones if normalize_phone(phone)]
        casa_emails = [email for email in split_multi(casa_payload.get("emails")) if email_is_valid(email)]
        opp_phones = oportunidades_phones(opp_payload if isinstance(opp_payload, dict) else {})
        opp_emails = oportunidades_emails(opp_payload if isinstance(opp_payload, dict) else {})
        websites = oportunidades_websites(opp_payload if isinstance(opp_payload, dict) else {})
        decision_makers = oportunidades_socios(opp_payload if isinstance(opp_payload, dict) else {})
        company_data = opp_payload.get("company") if isinstance(opp_payload.get("company"), dict) else {}
        record["matched_name"] = (
            str(company_data.get("nome_fantasia") or "").strip()
            or str(company_data.get("razao_social") or "").strip()
            or str(casa_payload.get("nome_fantasia") or "").strip()
            or str(casa_payload.get("razao_social") or "").strip()
            or company
        )
        existing_manual_phones = split_multi(record.get("casa_phones"))
        existing_manual_emails = split_multi(record.get("casa_emails"))
        existing_manual_sites = split_multi(record.get("websites"))
        record["casa_phones"] = " | ".join(unique_keep_order(existing_manual_phones + casa_phones))
        record["casa_emails"] = " | ".join(unique_keep_order(existing_manual_emails + casa_emails))
        record["opp_phones"] = " | ".join(unique_keep_order(opp_phones))
        record["opp_emails"] = " | ".join(unique_keep_order(opp_emails))
        record["websites"] = " | ".join(unique_keep_order(existing_manual_sites + websites))
        record["decision_makers"] = " | ".join(unique_keep_order(decision_makers[:10]))
        address = str(casa_payload.get("endereco") or "").strip()
        if not address:
            parts = [
                str(casa_payload.get("logradouro") or "").strip(),
                str(casa_payload.get("numero") or "").strip(),
                str(casa_payload.get("bairro") or "").strip(),
                str(casa_payload.get("cidade") or "").strip(),
                str(casa_payload.get("uf") or "").strip(),
                str(casa_payload.get("cep") or "").strip(),
            ]
            address = ", ".join([part for part in parts if part])
        if not address and company_data:
            parts = [
                str(company_data.get("tipo_logradouro") or "").strip(),
                str(company_data.get("logradouro") or "").strip(),
                str(company_data.get("numero") or "").strip(),
                str(company_data.get("bairro") or "").strip(),
                str(company_data.get("municipio") or "").strip(),
                str(company_data.get("uf") or "").strip(),
                str(company_data.get("cep") or "").strip(),
            ]
            address = ", ".join([part for part in parts if part])
        record["address"] = address

    phone_counts = Counter()
    email_counts = Counter()
    for record in row_records:
        for value in split_multi(record.get("crm_phone")) + split_multi(record.get("casa_phones")) + split_multi(record.get("opp_phones")):
            phone = normalize_phone(value)
            if phone and phone_is_valid(phone):
                phone_counts[phone] += 1
        for value in split_multi(record.get("crm_email")) + split_multi(record.get("casa_emails")) + split_multi(record.get("opp_emails")):
            email = str(value or "").strip().lower()
            if email_is_valid(email):
                email_counts[email] += 1

    export_rows = []
    for record in row_records:
        phones = []
        for value in split_multi(record.get("crm_phone")) + split_multi(record.get("casa_phones")) + split_multi(record.get("opp_phones")):
            phone = normalize_phone(value)
            if phone and phone_is_valid(phone) and phone_counts[phone] == 1:
                phones.append(phone)
        emails = []
        for value in split_multi(record.get("crm_email")) + split_multi(record.get("casa_emails")) + split_multi(record.get("opp_emails")):
            email = str(value or "").strip().lower()
            if email_is_valid(email) and email_counts[email] == 1:
                emails.append(email)
        export = dict(record)
        fit_score = name_fit_score(export["company_input"], export.get("matched_name", ""))
        review_reasons: List[str] = []
        if export.get("status") == "missing_cnpj":
            review_reasons.append("missing_cnpj")
        if fit_score < 0.78:
            review_reasons.append("low_name_fit")
        export["phones_exclusive"] = " | ".join(unique_keep_order(phones))
        export["emails_exclusive"] = " | ".join(unique_keep_order(emails))
        export["name_fit_score"] = round(fit_score, 4)
        export["review_reason"] = " | ".join(review_reasons)
        export["writeback_ready"] = (
            "yes"
            if export.get("cnpj")
            and not review_reasons
            and (export["phones_exclusive"] or export["emails_exclusive"] or export.get("decision_makers") or export.get("address"))
            else "review"
        )
        export_rows.append(export)

    out_df = pd.DataFrame(export_rows)
    out_df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    xlsx_status = "skipped"
    xlsx_error = ""
    try:
        out_df.to_excel(OUT_XLSX, index=False)
        xlsx_status = "ok"
    except Exception as exc:
        xlsx_status = "failed"
        xlsx_error = str(exc)

    summary = {
        "input_companies": len(companies),
        "matched_existing": int((out_df["status"] == "matched_existing").sum()),
        "matched_live_only": int((out_df["status"] == "matched_live_only").sum()),
        "resolved_by_name": int((out_df["status"] == "resolved_by_name").sum()),
        "missing_cnpj": int((out_df["status"] == "missing_cnpj").sum()),
        "writeback_ready": int((out_df["writeback_ready"] == "yes").sum()),
        "exclusive_phone_rows": int(out_df["phones_exclusive"].fillna("").astype(str).str.len().gt(0).sum()),
        "exclusive_email_rows": int(out_df["emails_exclusive"].fillna("").astype(str).str.len().gt(0).sum()),
        "decision_maker_rows": int(out_df["decision_makers"].fillna("").astype(str).str.len().gt(0).sum()),
        "xlsx_status": xlsx_status,
        "xlsx_error": xlsx_error,
    }
    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"[SUPER_MINAS_OFFLINE_WRITEBACK] csv={OUT_CSV} xlsx={OUT_XLSX} summary={OUT_JSON}")
    log(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
