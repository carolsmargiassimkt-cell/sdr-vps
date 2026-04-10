from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, Tuple

from enrich_missing_cnpj_from_xlsx import normalize_cnpj, normalize_phone, parse_xlsx, write_xlsx


BASE_XLSX = Path(r"C:\Users\Asus\Downloads\deals_enriquecido_final.xlsx")
V2_XLSX = Path(r"C:\Users\Asus\Downloads\deals_enriquecido_final_v2.xlsx")
BACKUP_XLSX = Path(r"C:\Users\Asus\Downloads\deals_enriquecido_final_v2_google_raw_backup.xlsx")
OUTPUT_XLSX = Path(r"C:\Users\Asus\Downloads\deals_enriquecido_writeback_ready.xlsx")

BLOCKED_PHONES = {"18043309723", "11999999999", "00999999977", "1155555555"}


def log(message: str) -> None:
    print(str(message), flush=True)


def row_key(row: Dict[str, Any]) -> Tuple[str, str]:
    return (str(row.get("deal_id") or "").strip(), str(row.get("org_id") or "").strip())


def clean_phone(value: Any) -> str:
    phone = normalize_phone(value)
    return "" if phone in BLOCKED_PHONES else phone


def has_text(value: Any) -> bool:
    return bool(str(value or "").strip())


def merge_rows(base: Dict[str, Any], v2: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    base_cnpj = normalize_cnpj(base.get("cnpj"))
    v2_cnpj = normalize_cnpj(v2.get("cnpj"))
    if not base_cnpj and v2_cnpj:
        merged["cnpj"] = v2_cnpj
    elif base_cnpj:
        merged["cnpj"] = base_cnpj
    base_phone = clean_phone(base.get("telefone"))
    v2_phone = clean_phone(v2.get("telefone"))
    if v2_phone:
        merged["telefone"] = v2_phone
    elif base_phone:
        merged["telefone"] = base_phone
    else:
        merged["telefone"] = ""
    if not has_text(base.get("email")) and has_text(v2.get("email")):
        merged["email"] = str(v2.get("email") or "").strip()
    return merged


def main() -> int:
    base_rows = parse_xlsx(BASE_XLSX)
    v2_rows = parse_xlsx(V2_XLSX)
    backup_rows = parse_xlsx(BACKUP_XLSX) if BACKUP_XLSX.exists() else []
    v2_by_key = {row_key(row): row for row in v2_rows if row_key(row) != ("", "")}
    output = []
    seen = set()
    merged_count = 0
    for row in base_rows:
        key = row_key(row)
        if key == ("", "") or key in seen:
            continue
        seen.add(key)
        v2 = v2_by_key.get(key)
        if v2:
            output.append(merge_rows(row, v2))
            merged_count += 1
        else:
            current = dict(row)
            current["telefone"] = clean_phone(current.get("telefone"))
            current["cnpj"] = normalize_cnpj(current.get("cnpj"))
            output.append(current)
    for row in v2_rows:
        key = row_key(row)
        if key == ("", "") or key in seen:
            continue
        seen.add(key)
        current = dict(row)
        current["telefone"] = clean_phone(current.get("telefone"))
        current["cnpj"] = normalize_cnpj(current.get("cnpj"))
        output.append(current)

    masters = [row for row in output if str(row.get("acao") or "").strip().upper() in {"UPDATE", "RESGATAR"}]
    phone_counts = Counter(clean_phone(row.get("telefone")) for row in masters if clean_phone(row.get("telefone")))
    removed_repeated = 0
    for row in masters:
        phone = clean_phone(row.get("telefone"))
        if phone and phone_counts[phone] > 1:
            row["telefone"] = ""
            removed_repeated += 1
            log(f"[TELEFONE_DUPLICADO_REMOVIDO] deal={row.get('deal_id')} telefone={phone}")

    write_xlsx(output, OUTPUT_XLSX)
    masters = [row for row in output if str(row.get("acao") or "").strip().upper() in {"UPDATE", "RESGATAR"}]
    archives = [row for row in output if str(row.get("acao") or "").strip().upper() == "ARCHIVE"]
    phones = [clean_phone(row.get("telefone")) for row in masters if clean_phone(row.get("telefone"))]
    phone_counts = Counter(phones)
    log(f"[MERGE_OK] arquivo={OUTPUT_XLSX}")
    log(f"[RESUMO] linhas={len(output)} masters={len(masters)} archives={len(archives)} merged_com_v2={merged_count} backup_lido={len(backup_rows)}")
    log(
        "[QUALIDADE] "
        f"com_cnpj={sum(1 for row in masters if normalize_cnpj(row.get('cnpj')))} "
        f"sem_cnpj={sum(1 for row in masters if not normalize_cnpj(row.get('cnpj')))} "
        f"com_telefone={len(phones)} "
        f"sem_telefone={len(masters)-len(phones)} "
        f"com_email={sum(1 for row in masters if has_text(row.get('email')))} "
        f"telefones_repetidos={sum(1 for _, count in phone_counts.items() if count > 1)} "
        f"telefones_repetidos_removidos={removed_repeated}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
