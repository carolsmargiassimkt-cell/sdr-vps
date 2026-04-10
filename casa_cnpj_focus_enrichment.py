from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from enrich_missing_cnpj_from_xlsx import (
    BLOCKED_GENERIC_PHONES,
    best_phone,
    bucket,
    fetch_casa,
    load_cache,
    normalize_cnpj,
    normalize_name,
    normalize_phone,
    parse_xlsx,
    save_cache,
    validate_casa_auth,
    xml_escape,
    excel_column_name,
)
from offline_final_sheet_enrichment import extract_casa_address


INPUT_XLSX = Path(r"C:\Users\Asus\Downloads\LISTAS-MAND\deals_enriquecido_writeback_ready.xlsx")
OUTPUT_XLSX = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_casa_focus_enriched.xlsx")
OUTPUT_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_casa_focus_enriched.csv")
SUMMARY_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\deals_casa_focus_enriched_summary.json")


def dynamic_write_xlsx(rows: List[Dict[str, Any]], path: Path) -> None:
    import zipfile

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
  <sheets><sheet name="casa_focus" sheetId="1" r:id="rId1"/></sheets>
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


def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    import csv

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


def casa_phone_list(payload: Dict[str, Any]) -> List[str]:
    phones = [normalize_phone(item) for item in list(payload.get("telefones") or [])]
    return [item for item in phones if item and item not in BLOCKED_GENERIC_PHONES]


def run(input_path: Path, output_path: Path, limit: int = 0, with_cnpj_only: bool = False, missing_phone_only: bool = False) -> Dict[str, Any]:
    rows = parse_xlsx(input_path)
    if with_cnpj_only:
        rows = [row for row in rows if normalize_cnpj(row.get("cnpj"))]
    if missing_phone_only:
        rows = [
            row
            for row in rows
            if not normalize_phone(row.get("telefone")) or normalize_phone(row.get("telefone")) in BLOCKED_GENERIC_PHONES
        ]
    if limit:
        rows = rows[: max(1, int(limit))]

    cache = load_cache()
    casa_cache = bucket(cache, "casa")
    auth_ok = validate_casa_auth(casa_cache)
    save_cache(cache)
    if not auth_ok:
        raise RuntimeError("Casa dos Dados auth failed")

    before_with_phone = 0
    after_with_phone = 0
    before_with_address = 0
    after_with_address = 0
    casa_consulted = 0
    casa_ok = 0
    output: List[Dict[str, Any]] = []

    for index, row in enumerate(rows, start=1):
        current = dict(row)
        cnpj = normalize_cnpj(current.get("cnpj"))
        original_phone = normalize_phone(current.get("telefone"))
        if original_phone and original_phone not in BLOCKED_GENERIC_PHONES:
            before_with_phone += 1
        if str(current.get("casa_endereco") or current.get("endereco") or "").strip():
            before_with_address += 1

        casa_payload: Dict[str, Any] = {}
        if cnpj:
            casa_consulted += 1
            casa_payload = fetch_casa(cnpj, casa_cache)
            save_cache(cache)
            if int(casa_payload.get("status_code") or 0) == 200:
                casa_ok += 1

        raw_casa = casa_payload.get("raw") if isinstance(casa_payload.get("raw"), dict) else {}
        casa_phones = casa_phone_list(casa_payload)
        casa_emails = list(casa_payload.get("emails") or [])
        current["cnpj_enriquecido"] = cnpj
        current["casa_status_code"] = str(casa_payload.get("status_code") or "")
        current["casa_razao_social"] = str(casa_payload.get("razao_social") or raw_casa.get("razao_social") or "")
        current["casa_nome_fantasia"] = str(casa_payload.get("nome_fantasia") or raw_casa.get("nome_fantasia") or "")
        current["casa_telefones"] = " | ".join(casa_phones)
        current["casa_emails"] = " | ".join(casa_emails)
        current["telefone_principal_sugerido"] = best_phone([original_phone, *casa_phones])
        current["telefone_novo_enriquecido"] = (
            current["telefone_principal_sugerido"]
            if current["telefone_principal_sugerido"]
            and current["telefone_principal_sugerido"] != original_phone
            else ""
        )
        current["email_principal_sugerido"] = casa_emails[0] if casa_emails else ""
        current.update(extract_casa_address(casa_payload))
        current["nome_base"] = normalize_name(
            current.get("nome_padronizado") or current.get("nome_final") or current.get("nome_original") or ""
        )

        if current["telefone_principal_sugerido"]:
            after_with_phone += 1
        if str(current.get("casa_endereco") or "").strip():
            after_with_address += 1

        output.append(current)
        if index % 25 == 0:
            print(f"[CASA_FOCUS_PROGRESS] {index}/{len(rows)}", flush=True)

    dynamic_write_xlsx(output, output_path)
    write_csv(output, OUTPUT_CSV)
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_rows": len(rows),
        "casa_consulted": casa_consulted,
        "casa_ok": casa_ok,
        "before_with_phone": before_with_phone,
        "after_with_phone": after_with_phone,
        "phone_gain": after_with_phone - before_with_phone,
        "before_with_address": before_with_address,
        "after_with_address": after_with_address,
        "address_gain": after_with_address - before_with_address,
        "sem_cnpj": sum(1 for row in output if not row.get("cnpj_enriquecido")),
        "com_cnpj": sum(1 for row in output if row.get("cnpj_enriquecido")),
        "novos_telefones": sum(1 for row in output if str(row.get("telefone_novo_enriquecido") or "").strip()),
        "emails_casa": sum(1 for row in output if str(row.get("casa_emails") or "").strip()),
    }
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[CASA_FOCUS_SUMMARY] " + " ".join(f"{k}={v}" for k, v in summary.items() if k != "generated_at"), flush=True)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(INPUT_XLSX))
    parser.add_argument("--output", default=str(OUTPUT_XLSX))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--with-cnpj-only", action="store_true")
    parser.add_argument("--missing-phone-only", action="store_true")
    args = parser.parse_args()
    run(
        Path(args.input),
        Path(args.output),
        limit=args.limit,
        with_cnpj_only=bool(args.with_cnpj_only),
        missing_phone_only=bool(args.missing_phone_only),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
