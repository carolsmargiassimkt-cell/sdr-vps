from __future__ import annotations

import csv
import json
import re
import sys
import unicodedata
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.local_crm_cache import LocalCRMCache

DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "local_crm_base.json"
DOWNLOADS_DIR = Path.home() / "Downloads"
SUPPLEMENTAL_SOURCES = [
    PROJECT_ROOT / "data" / "manual_cnpj_seed.csv",
]


def _latest_existing(paths: Iterable[Path]) -> Path:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return list(paths)[0]
    return max(existing, key=lambda path: path.stat().st_mtime)


def _discover_default_sheets() -> List[Path]:
    sales_candidates = [
        DOWNLOADS_DIR / "LISTA_MAND_FINAL_VENDAS.csv",
    ]
    cnpj_candidates = sorted(DOWNLOADS_DIR.rglob("lista-mand*.csv"))
    deals_candidates = sorted(DOWNLOADS_DIR.glob("deals-*.xlsx"))
    sales_file = _latest_existing(sales_candidates)
    cnpj_file = _latest_existing(cnpj_candidates or [DOWNLOADS_DIR / "lista-mand.csv"])
    deals_file = _latest_existing(deals_candidates or [DOWNLOADS_DIR / "deals.xlsx"])
    return [sales_file, cnpj_file, deals_file]


DEFAULT_SHEETS = _discover_default_sheets()


def normalize_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\s+", " ", text).strip().upper()
    return text


def normalize_phone(value: Any) -> str:
    return LocalCRMCache.normalize_phone(value)


def normalize_email(value: Any) -> str:
    return LocalCRMCache.normalize_email(value)


def normalize_cnpj(value: Any) -> str:
    return LocalCRMCache.normalize_cnpj(value)


def split_multi(value: Any, *, kind: str) -> List[str]:
    raw = str(value or "").replace("\n", ",").replace("/", ",")
    if not raw:
        return []
    items = re.split(r"[;,]", raw)
    output: List[str] = []
    seen = set()
    for item in items:
        candidate = item.strip()
        if not candidate:
            continue
        if kind == "phone":
            candidate = normalize_phone(candidate)
        elif kind == "email":
            candidate = normalize_email(candidate)
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        output.append(candidate)
    return output


def read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", errors="replace") as handle:
        sample = handle.read(4096)
        handle.seek(0)
        delimiter = ";"
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,")
            delimiter = dialect.delimiter or ";"
        except Exception:
            delimiter = ";"
        reader = csv.DictReader(handle, delimiter=delimiter)
        return [dict(row) for row in reader]


def read_xlsx_rows(path: Path) -> List[Dict[str, Any]]:
    ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    shared_strings: List[str] = []
    rows_out: List[Dict[str, Any]] = []
    with zipfile.ZipFile(path) as workbook:
        if "xl/sharedStrings.xml" in workbook.namelist():
            root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
            shared_strings = [
                "".join(node.text or "" for node in item.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t"))
                for item in root.findall("a:si", ns)
            ]
        worksheet = ET.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
        rows = worksheet.find("a:sheetData", ns).findall("a:row", ns)
        headers: List[str] = []
        for idx, row in enumerate(rows):
            values: List[str] = []
            for cell in row.findall("a:c", ns):
                cell_type = cell.attrib.get("t")
                value_node = cell.find("a:v", ns)
                value = ""
                if value_node is not None:
                    value = value_node.text or ""
                    if cell_type == "s":
                        value = shared_strings[int(value)]
                values.append(value)
            if idx == 0:
                headers = values
                continue
            if not any(values):
                continue
            item = {}
            for col_idx, header in enumerate(headers):
                item[header] = values[col_idx] if col_idx < len(values) else ""
            rows_out.append(item)
    return rows_out


def make_key(record: Dict[str, Any]) -> str:
    cnpj = normalize_cnpj(record.get("cnpj"))
    if cnpj:
        return f"cnpj:{cnpj}"
    phones = record.get("phones") or []
    if phones:
        return f"phone:{phones[0]}"
    emails = record.get("emails") or []
    if emails:
        return f"email:{emails[0]}"
    company = normalize_text(record.get("empresa") or record.get("razao_social"))
    if company:
        return f"company:{company}"
    return f"fallback:{normalize_text(record.get('deal_title') or record.get('lead_id') or '')}"


def base_record() -> Dict[str, Any]:
    return {
        "lead_id": "",
        "deal_id": 0,
        "person_id": 0,
        "nome": "",
        "empresa": "",
        "telefone": "",
        "email": "",
        "phones": [],
        "emails": [],
        "tags": [],
        "status_bot": "",
        "stage_id": 0,
        "sheet_day": "1",
        "origem_oficial": "",
        "cnpj": "",
        "source": "local_base",
        "super_minas": False,
        "blacklisted": False,
        "blacklist_reason": "",
        "deal_title": "",
        "aliases": [],
        "decision_makers": [],
        "websites": [],
        "enriched_source": "",
        "enriched_at": "",
        "raw_enrichment": {},
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def merge_record(current: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(current or base_record())
    for field in (
        "lead_id",
        "deal_id",
        "person_id",
        "nome",
        "empresa",
        "telefone",
        "email",
        "status_bot",
        "stage_id",
        "sheet_day",
        "origem_oficial",
        "cnpj",
        "source",
        "deal_title",
        "blacklist_reason",
        "enriched_source",
        "enriched_at",
    ):
        value = incoming.get(field)
        if value not in (None, "", 0):
            merged[field] = value
    merged["phones"] = dedupe([normalize_phone(item) for item in (merged.get("phones") or []) + (incoming.get("phones") or [])])
    merged["emails"] = dedupe([normalize_email(item) for item in (merged.get("emails") or []) + (incoming.get("emails") or [])])
    merged["tags"] = dedupe([str(item or "").strip() for item in (merged.get("tags") or []) + (incoming.get("tags") or []) if str(item or "").strip()])
    merged["aliases"] = dedupe([str(item or "").strip() for item in (merged.get("aliases") or []) + (incoming.get("aliases") or []) if str(item or "").strip()])
    merged["decision_makers"] = dedupe([str(item or "").strip() for item in (merged.get("decision_makers") or []) + (incoming.get("decision_makers") or []) if str(item or "").strip()])
    merged["websites"] = dedupe([str(item or "").strip() for item in (merged.get("websites") or []) + (incoming.get("websites") or []) if str(item or "").strip()])
    merged["super_minas"] = bool(merged.get("super_minas")) or bool(incoming.get("super_minas"))
    merged["blacklisted"] = bool(merged.get("blacklisted")) or bool(incoming.get("blacklisted"))
    raw_enrichment = incoming.get("raw_enrichment")
    if isinstance(raw_enrichment, dict) and raw_enrichment:
        merged["raw_enrichment"] = raw_enrichment
    if not merged.get("telefone") and merged["phones"]:
        merged["telefone"] = merged["phones"][0]
    if not merged.get("email") and merged["emails"]:
        merged["email"] = merged["emails"][0]
    if merged.get("telefone"):
        merged["telefone"] = normalize_phone(merged.get("telefone"))
    if merged.get("email"):
        merged["email"] = normalize_email(merged.get("email"))
    if merged.get("cnpj"):
        merged["cnpj"] = normalize_cnpj(merged.get("cnpj"))
    merged["updated_at"] = datetime.now(timezone.utc).isoformat()
    return merged


def dedupe(values: Iterable[str]) -> List[str]:
    output: List[str] = []
    seen = set()
    for value in values:
        clean = str(value or "").strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        output.append(clean)
    return output


def records_from_existing_targets(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    records: List[Dict[str, Any]] = []
    for item in data if isinstance(data, list) else []:
        if not isinstance(item, dict):
            continue
        tags = [str(tag) for tag in (item.get("tags") or [])]
        super_minas = bool(
            item.get("super_minas")
            or "SUPER_MINAS" in {normalize_text(tag) for tag in tags}
            or "175" in {str(tag) for tag in tags}
            or normalize_text(item.get("origem_oficial")) == "SUPER_MINAS"
        )
        cnpj = normalize_cnpj(
            item.get("cnpj")
            or ((item.get("crm_person") or {}).get("56a8827b133e76ca3c7f8dd1147a7d40a83d90c5"))
            or ((item.get("crm_deal") or {}).get("org_name"))
        )
        record = {
            "lead_id": item.get("lead_id") or "",
            "deal_id": int(item.get("deal_id", 0) or 0),
            "person_id": int(item.get("person_id", 0) or 0),
            "nome": str(item.get("nome") or "").strip(),
            "empresa": str(item.get("empresa") or "").strip(),
            "telefone": normalize_phone(item.get("telefone") or item.get("phone")),
            "email": normalize_email(item.get("email")),
            "phones": [normalize_phone(item.get("telefone") or item.get("phone"))] if normalize_phone(item.get("telefone") or item.get("phone")) else [],
            "emails": [normalize_email(item.get("email"))] if normalize_email(item.get("email")) else [],
            "tags": tags,
            "status_bot": str(item.get("status_bot") or "").strip().lower(),
            "stage_id": int(item.get("stage_id", 0) or 0),
            "sheet_day": str(item.get("sheet_day") or 1),
            "origem_oficial": str(item.get("origem_oficial") or "").strip(),
            "cnpj": cnpj,
            "source": "pipedrive_cache",
            "super_minas": super_minas,
            "deal_title": str(((item.get("crm_deal") or {}).get("title")) or "").strip(),
        }
        records.append(record)
    return records


def records_from_sales_csv(path: Path) -> List[Dict[str, Any]]:
    records = []
    for row in read_csv_rows(path):
        company = str(row.get("Empresa") or "").strip()
        email = normalize_email(row.get("Email"))
        phone = normalize_phone(row.get("Telefone"))
        records.append(
            {
                "empresa": company,
                "nome": company,
                "telefone": phone,
                "email": email,
                "phones": [phone] if phone else [],
                "emails": [email] if email else [],
                "cnpj": normalize_cnpj(row.get("CNPJ")),
                "source": "sheet_sales",
            }
        )
    return records


def records_from_cnpj_csv(path: Path) -> List[Dict[str, Any]]:
    records = []
    for row in read_csv_rows(path):
        company = str(row.get("Razao Social") or row.get("Nome Fantasia") or "").strip()
        phones = split_multi(row.get("Telefones"), kind="phone")
        emails = split_multi(row.get("E-mail"), kind="email")
        records.append(
            {
                "empresa": company,
                "nome": company,
                "telefone": phones[0] if phones else "",
                "email": emails[0] if emails else "",
                "phones": phones,
                "emails": emails,
                "cnpj": normalize_cnpj(row.get("CNPJ")),
                "source": "sheet_cnpj",
            }
        )
    return records


def records_from_xlsx(path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    rows = read_xlsx_rows(path)
    columns = [str(col or "").strip() for col in rows[0].keys()] if rows else []
    print(f"[COLUNAS_SUPER_MINAS] {columns}")
    possible_cols = ['telefone', 'Telefone', 'phone', 'Phone', 'celular', 'Celular', 'whatsapp', 'WhatsApp', 'TEL', 'tel']
    col_tel = None
    for header in columns:
        if not header:
            continue
        if any(key.lower() in header.lower() for key in possible_cols):
            col_tel = header
            break
    print(f"[COLUNA_TELEFONE] {col_tel}")

    valid_nums: List[str] = []
    seen_nums = set()
    for row in rows:
        title = str(row.get("NegÃ³cio - TÃ­tulo") or "").strip()
        company_hint = str(row.get("NegÃ³cio - OrganizaÃ§Ã£o") or "").strip()
        cnpj = normalize_cnpj(row.get("Pessoa - CNPJ"))
        empresa = (
            company_hint.split("|")[0].strip()
            if company_hint
            else re.sub(r"\s+neg[oÃ³]cio.*$", "", title, flags=re.IGNORECASE).strip()
        )

        row_numbers: List[str] = []
        if col_tel:
            value = str(row.get(col_tel, "") or "")
            digits = re.sub(r"\D", "", value)
            if digits.startswith("55") and len(digits) >= 12:
                row_numbers.append(digits)
                if digits not in seen_nums:
                    seen_nums.add(digits)
                    valid_nums.append(digits)

        record_phone = normalize_phone(row_numbers[0]) if row_numbers else ""
        record_phones = [record_phone] if record_phone else []

        records.append(
            {
                "empresa": empresa,
                "nome": "",
                "cnpj": cnpj,
                "aliases": [empresa] if empresa else [],
                "deal_title": title,
                "origem_oficial": "SUPER_MINAS",
                "super_minas": True,
                "tags": ["SUPER_MINAS"],
                "source": "sheet_super_minas",
                "telefone": record_phone,
                "phones": record_phones,
            }
        )

    print(f"[LEADS_VALIDOS] {len(valid_nums)}")
    print(valid_nums[:5])
    return records

def records_from_manual_seed_csv(path: Path) -> List[Dict[str, Any]]:
    records = []
    for row in read_csv_rows(path):
        company = str(row.get("Empresa") or row.get("empresa") or "").strip()
        cnpj = normalize_cnpj(row.get("CNPJ") or row.get("cnpj"))
        if not company and not cnpj:
            continue
        records.append(
            {
                "empresa": company,
                "nome": company,
                "cnpj": cnpj,
                "aliases": [company] if company else [],
                "source": "manual_cnpj_seed",
            }
        )
    return records


def main() -> int:
    output = DEFAULT_OUTPUT
    cache = LocalCRMCache(output)
    payload = cache.load()
    by_key: Dict[str, Dict[str, Any]] = {}

    for record in payload.get("records", []):
        if isinstance(record, dict):
            by_key[make_key(record)] = merge_record(base_record(), record)

    for record in records_from_existing_targets(PROJECT_ROOT / "config" / "whatsapp_targets.json"):
        by_key[make_key(record)] = merge_record(by_key.get(make_key(record), base_record()), record)

    source_builders = [
        (DEFAULT_SHEETS[0], records_from_sales_csv),
        (DEFAULT_SHEETS[1], records_from_cnpj_csv),
        (DEFAULT_SHEETS[2], records_from_xlsx),
    ] + [(path, records_from_manual_seed_csv) for path in SUPPLEMENTAL_SOURCES]
    for path, builder in source_builders:
        if not path.exists():
            continue
        for record in builder(path):
            key = make_key(record)
            current = by_key.get(key)
            if not current and record.get("cnpj"):
                for existing_key, existing in list(by_key.items()):
                    if normalize_cnpj(existing.get("cnpj")) and normalize_cnpj(existing.get("cnpj")) == normalize_cnpj(record.get("cnpj")):
                        current = existing
                        key = existing_key
                        break
            if not current and record.get("empresa"):
                target_company = normalize_text(record.get("empresa"))
                for existing_key, existing in list(by_key.items()):
                    if normalize_text(existing.get("empresa")) == target_company:
                        current = existing
                        key = existing_key
                        break
            by_key[key] = merge_record(current or base_record(), record)

    final_records = sorted(
        [record for record in by_key.values() if isinstance(record, dict) and (record.get("empresa") or record.get("telefone") or record.get("email"))],
        key=lambda item: (
            0 if item.get("super_minas") else 1,
            0 if item.get("deal_id") else 1,
            normalize_text(item.get("empresa")),
        ),
    )
    payload = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "record_count": len(final_records),
            "source_files": [str(path) for path in DEFAULT_SHEETS + SUPPLEMENTAL_SOURCES if path.exists()],
        },
        "records": final_records,
    }
    cache.save(payload)
    print(f"[LOCAL_CRM_BASE] registros={len(final_records)} arquivo={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
