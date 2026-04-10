from __future__ import annotations

import argparse
import csv
import json
import os
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests

from enrich_missing_cnpj_from_xlsx import normalize_cnpj, normalize_phone, xml_escape, excel_column_name


INPUT_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_casa_focus_enriched.csv")
OUTPUT_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_oportunidados_enriched.csv")
OUTPUT_XLSX = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_oportunidados_enriched.xlsx")
SUMMARY_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\deals_oportunidados_enriched_summary.json")
CACHE_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\oportunidados_enrichment_cache.json")
BASE_URL = "https://app.oportunidados.com.br/api/v1/brazilian_companies/{cnpj}/company"
REQUEST_DELAY_SEC = 1.35


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


def dynamic_write_xlsx(rows: List[Dict[str, Any]], path: Path) -> None:
    headers: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in headers:
                headers.append(key)
    data = [headers]
    for row in rows:
        data.append([row.get(header, "") for header in headers])

    sheet_rows: List[str] = []
    for row_idx, row in enumerate(data, start=1):
        cells: List[str] = []
        for col_idx, value in enumerate(row, start=1):
            ref = f"{excel_column_name(col_idx)}{row_idx}"
            as_text = str(value or "")
            if row_idx > 1 and as_text.isdigit() and len(as_text) < 12 and col_idx <= 3:
                cells.append(f'<c r="{ref}"><v>{int(as_text)}</v></c>')
            else:
                cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{xml_escape(as_text)}</t></is></c>')
        sheet_rows.append(f'<row r="{row_idx}">{"".join(cells)}</row>')

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>"""
    rels_root = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>"""
    workbook = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="oportunidados" sheetId="1" r:id="rId1"/></sheets>
</workbook>"""
    workbook_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>"""
    worksheet = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>{''.join(sheet_rows)}</sheetData></worksheet>"""
    core = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>Codex</dc:creator><cp:lastModifiedBy>Codex</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{now}</dcterms:created><dcterms:modified xsi:type="dcterms:W3CDTF">{now}</dcterms:modified>
</cp:coreProperties>"""
    app = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"><Application>Codex</Application></Properties>"""

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", rels_root)
        zf.writestr("xl/workbook.xml", workbook)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        zf.writestr("xl/worksheets/sheet1.xml", worksheet)
        zf.writestr("docProps/core.xml", core)
        zf.writestr("docProps/app.xml", app)


def read_cache() -> Dict[str, Any]:
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


def fetch_company(cnpj: str, token: str, cache: Dict[str, Any]) -> Dict[str, Any]:
    cached = cache.get(cnpj)
    if isinstance(cached, dict) and int(cached.get("status_code") or 0) in {200, 404}:
        payload = dict(cached)
        payload["_from_cache"] = True
        return payload
    try:
        response = requests.get(
            BASE_URL.format(cnpj=cnpj),
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=30,
        )
        payload = {"status_code": response.status_code, "_from_cache": False}
        if response.status_code == 200:
            payload["raw"] = response.json()
        else:
            payload["error"] = response.text[:500]
    except Exception as exc:
        payload = {"status_code": 0, "error": str(exc)[:500], "_from_cache": False}
    cache[cnpj] = payload
    return payload


def contacts_from_payload(payload: Dict[str, Any]) -> Dict[str, str]:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    company = raw.get("company") if isinstance(raw.get("company"), dict) else {}
    socios = raw.get("socios") if isinstance(raw.get("socios"), list) else []
    extras = raw.get("contatos_extras") if isinstance(raw.get("contatos_extras"), list) else []

    phones: List[str] = []
    for idx in (1, 2, 3):
        ddd = str(company.get(f"ddd_{idx}") or "").strip()
        number = str(company.get(f"telefone_{idx}") or "").strip()
        full = normalize_phone(f"{ddd}{number}")
        if full and full not in phones:
            phones.append(full)

    emails: List[str] = []
    main_email = str(company.get("email") or "").strip().lower()
    if "@" in main_email and main_email not in emails:
        emails.append(main_email)
    websites: List[str] = []
    for item in extras:
        if not isinstance(item, dict):
            continue
        contact = str(item.get("contato") or "").strip()
        contact_type = str(item.get("tipo_contato") or "").strip().lower()
        if contact_type == "email":
            lowered = contact.lower()
            if "@" in lowered and lowered not in emails:
                emails.append(lowered)
        elif contact_type in {"website", "url"}:
            if contact and contact not in websites:
                websites.append(contact)

    socios_names: List[str] = []
    socios_roles: List[str] = []
    for socio in socios:
        if not isinstance(socio, dict):
            continue
        name = str(socio.get("nome_socio") or "").strip()
        role = str(socio.get("qualificacao_socio") or "").strip()
        if name and name not in socios_names:
            socios_names.append(name)
        if name and role:
            socios_roles.append(f"{name} ({role})")

    return {
        "opp_razao_social": str(company.get("razao_social") or "").strip(),
        "opp_nome_fantasia": str(company.get("nome_fantasia") or "").strip(),
        "opp_situacao": str(company.get("situacao_cadastral") or "").strip(),
        "opp_company_phones": " | ".join(phones),
        "opp_company_emails": " | ".join(emails),
        "opp_websites": " | ".join(websites),
        "opp_socios": " | ".join(socios_names),
        "opp_socios_roles": " | ".join(socios_roles),
        "opp_logradouro": str(company.get("logradouro") or "").strip(),
        "opp_numero": str(company.get("numero") or "").strip(),
        "opp_bairro": str(company.get("bairro") or "").strip(),
        "opp_municipio": str(company.get("municipio") or "").strip(),
        "opp_uf": str(company.get("uf") or "").strip(),
        "opp_cep": str(company.get("cep") or "").strip(),
        "opp_endereco": ", ".join(
            part
            for part in [
                " ".join(part for part in [str(company.get("tipo_logradouro") or "").strip(), str(company.get("logradouro") or "").strip()] if part).strip(),
                str(company.get("numero") or "").strip(),
                str(company.get("complemento") or "").strip(),
                str(company.get("bairro") or "").strip(),
                str(company.get("municipio") or "").strip(),
                str(company.get("uf") or "").strip(),
                str(company.get("cep") or "").strip(),
            ]
            if part
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(INPUT_CSV))
    parser.add_argument("--output-csv", default=str(OUTPUT_CSV))
    parser.add_argument("--output-xlsx", default=str(OUTPUT_XLSX))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--delay", type=float, default=REQUEST_DELAY_SEC)
    parser.add_argument("--retry-429-only", action="store_true")
    args = parser.parse_args()

    token = os.environ.get("OPORTUNIDADOS_API_TOKEN", "").strip()
    if not token:
        raise SystemExit("Missing OPORTUNIDADOS_API_TOKEN")

    rows = read_csv(Path(args.input))
    output = [dict(row) for row in rows]
    target_rows = [row for row in output if normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))]
    if args.retry_429_only:
        target_rows = [row for row in target_rows if str(row.get("opp_status_code") or "").strip() == "429"]
    if args.limit:
        target_rows = target_rows[: max(1, int(args.limit))]
    cache = read_cache()
    ok = 0
    for index, row in enumerate(target_rows, start=1):
        cnpj = normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))
        payload = fetch_company(cnpj, token, cache)
        row["opp_status_code"] = str(payload.get("status_code") or "")
        row["opp_error"] = str(payload.get("error") or "")
        if int(payload.get("status_code") or 0) == 200:
            row.update(contacts_from_payload(payload))
        if int(payload.get("status_code") or 0) == 200:
            ok += 1
        if index % 25 == 0:
            print(f"[OPP_PROGRESS] {index}/{len(target_rows)}", flush=True)
        if not bool(payload.get("_from_cache")):
            save_cache(cache)
            time.sleep(max(0.0, float(args.delay)))
    save_cache(cache)
    write_csv(output, Path(args.output_csv))
    dynamic_write_xlsx(output, Path(args.output_xlsx))
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_rows": len(target_rows),
        "ok": ok,
        "with_phone": sum(1 for row in output if str(row.get("opp_company_phones") or "").strip()),
        "with_email": sum(1 for row in output if str(row.get("opp_company_emails") or "").strip()),
        "with_website": sum(1 for row in output if str(row.get("opp_websites") or "").strip()),
        "with_socios": sum(1 for row in output if str(row.get("opp_socios") or "").strip()),
    }
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
