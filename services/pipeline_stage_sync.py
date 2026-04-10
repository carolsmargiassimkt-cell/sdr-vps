from __future__ import annotations

import re
from typing import Any, Callable, Dict, List

from crm.pipedrive_client import PipedriveClient


PROSPECCAO_PIPELINE_ID = 2
PIPELINE_PAIR_STAGE_NAMES = {
    1: "CONTATO 1 E 2",
    2: "CONTATO 3 E 4",
    3: "CONTATO 5 E 6",
}
FINAL_STAGE_NAMES = {
    "agendado": "AGENDADO",
    "perdido": "PERDIDO",
}
PIPELINE_STAGE_ORDER = {
    "ENTRADA": 0,
    "CONTATO 1 E 2": 1,
    "CONTATO 3 E 4": 2,
    "CONTATO 5 E 6": 3,
}
BLOCKING_STATUS_BOT = {"conversando", "conversation", "sem_interesse", "encerrado", "stop", "failed"}


def _normalize_stage_name(value: Any) -> str:
    text = str(value or "").strip().upper()
    text = re.sub(r"\s+", " ", text)
    return text


def _extract_label_tokens(raw_labels: Any) -> set[str]:
    items = raw_labels if isinstance(raw_labels, list) else [raw_labels]
    tokens: set[str] = set()
    for item in items:
        if isinstance(item, dict):
            candidates = (
                item.get("id"),
                item.get("label"),
                item.get("name"),
                item.get("value"),
                item.get("text"),
            )
        else:
            candidates = (item,)
        for candidate in candidates:
            clean = _normalize_stage_name(candidate)
            if clean:
                tokens.add(clean)
    return tokens


def get_par_cadencia(cadencia: Any) -> int:
    try:
        current = int(cadencia or 0)
    except Exception:
        current = 0
    if current <= 0:
        return 0
    if current <= 2:
        return 1
    if current <= 4:
        return 2
    return 3


def _highest_stage_from_tokens(tokens: set[str], prefix: str) -> int:
    highest = 0
    clean_prefix = _normalize_stage_name(prefix)
    for stage in range(1, 6):
        if f"{clean_prefix}{stage}" in tokens:
            highest = stage
    return highest


def _highest_stage_from_notes(notes: List[Dict[str, Any]], marker_prefix: str) -> int:
    highest = 0
    clean_prefix = _normalize_stage_name(marker_prefix)
    for note in notes or []:
        content = _normalize_stage_name((note or {}).get("content"))
        for stage in range(1, 6):
            if f"{clean_prefix}{stage}" in content:
                highest = max(highest, stage)
    return highest


def _pick_target_deal(client: PipedriveClient, lead: Dict[str, Any]) -> Dict[str, Any]:
    deal_id = int(lead.get("deal_id", 0) or 0)
    if deal_id:
        current = client.get_deal_details(deal_id)
        return current if isinstance(current, dict) else {}

    person_id = int(lead.get("person_id", lead.get("id", 0)) or 0)
    if not person_id:
        return {}
    for deal in client.find_open_deals_by_person_or_org(person_id=person_id):
        if int(deal.get("pipeline_id", 0) or 0) == PROSPECCAO_PIPELINE_ID:
            return dict(deal)
    return {}


def _notes_for_entities(client: PipedriveClient, *, deal_id: int, person_id: int) -> List[Dict[str, Any]]:
    if not hasattr(client, "_request"):
        return []
    endpoint_parts: List[str] = []
    if person_id:
        endpoint_parts.append(f"person_id={person_id}")
    if deal_id:
        endpoint_parts.append(f"deal_id={deal_id}")
    if not endpoint_parts:
        return []
    response = client._request("GET", f"notes?{'&'.join(endpoint_parts)}")
    notes = list((response or {}).get("data") or [])
    return [item for item in notes if isinstance(item, dict)]


def _target_stage_id_for_pair(client: PipedriveClient, pair_number: int) -> int:
    target_name = PIPELINE_PAIR_STAGE_NAMES.get(int(pair_number or 0), "")
    if not target_name:
        return 0
    for stage in client.get_stages():
        if not isinstance(stage, dict):
            continue
        if int(stage.get("pipeline_id", 0) or 0) != PROSPECCAO_PIPELINE_ID:
            continue
        if _normalize_stage_name(stage.get("name")) == target_name:
            return int(stage.get("id", 0) or 0)
    return 0


def _target_stage_id_by_name(client: PipedriveClient, stage_name: str) -> int:
    normalized_target = _normalize_stage_name(stage_name)
    if not normalized_target:
        return 0
    fallback_aliases = {
        "AGENDADO": {"AGENDADA", "REUNIAO MARCADA", "REUNIÃO MARCADA", "MEETING"},
        "PERDIDO": {"LOST", "PERDIDA", "PERDIDOS"},
    }
    for stage in client.get_stages():
        if not isinstance(stage, dict):
            continue
        if int(stage.get("pipeline_id", 0) or 0) != PROSPECCAO_PIPELINE_ID:
            continue
        candidate = _normalize_stage_name(stage.get("name"))
        if candidate == normalized_target or candidate in fallback_aliases.get(normalized_target, set()):
            return int(stage.get("id", 0) or 0)
    return 0


def _stage_name_from_id(client: PipedriveClient, stage_id: Any) -> str:
    try:
        target_id = int(stage_id or 0)
    except Exception:
        target_id = 0
    if target_id <= 0:
        return ""
    for stage in client.get_stages():
        if not isinstance(stage, dict):
            continue
        if int(stage.get("id", 0) or 0) == target_id:
            return str(stage.get("name") or "").strip()
    return ""


def _current_stage_rank(current_stage_name: str) -> int:
    return int(PIPELINE_STAGE_ORDER.get(_normalize_stage_name(current_stage_name), 99))


def sync_pipeline_stage_by_pair(
    *,
    client: PipedriveClient,
    lead: Dict[str, Any],
    emit: Callable[[str], None],
) -> bool:
    if not callable(emit):
        emit = print
    deal = _pick_target_deal(client, lead)
    if not deal:
        return False
    deal_id = int(deal.get("id", 0) or 0)
    if not deal_id:
        return False
    if int(deal.get("pipeline_id", 0) or 0) != PROSPECCAO_PIPELINE_ID:
        return False

    person_id = int(lead.get("person_id", lead.get("id", 0)) or 0)
    notes = _notes_for_entities(client, deal_id=deal_id, person_id=person_id)
    state_status = _normalize_stage_name(lead.get("status_bot") or lead.get("status") or "")
    deal_status = _normalize_stage_name(deal.get("status_bot") or "")
    final_state = ""
    if state_status == "AGENDADO" or deal_status == "AGENDADO":
        final_state = "agendado"
    elif state_status in {"PERDIDO", "ENCERRADO", "LOST"} or deal_status in {"PERDIDO", "ENCERRADO", "LOST"}:
        final_state = "perdido"

    target_stage_id = 0
    if final_state:
        target_stage_id = _target_stage_id_by_name(client, FINAL_STAGE_NAMES[final_state])
    else:
        tokens = _extract_label_tokens(deal.get("label"))
        highest_stage = max(
            _highest_stage_from_tokens(tokens, "CAD"),
            _highest_stage_from_tokens(tokens, "WHATSAPP_CAD"),
            _highest_stage_from_tokens(tokens, "EMAIL_CAD"),
            _highest_stage_from_notes(notes, "WHATSAPP_SENT_SDR|CAD"),
            _highest_stage_from_notes(notes, "EMAIL_SENT_SDR|CAD"),
        )
        pair_number = get_par_cadencia(highest_stage)
        if pair_number <= 0:
            return True
        target_stage_id = _target_stage_id_for_pair(client, pair_number)

    if not target_stage_id:
        emit(f"[PIPELINE_SYNC_SKIP] deal={deal_id} motivo=stage_nao_encontrado")
        return False

    current_stage_id = int(deal.get("stage_id", 0) or 0)
    if current_stage_id == target_stage_id:
        return True

    current_stage_name = _stage_name_from_id(client, current_stage_id)
    target_stage_name = _stage_name_from_id(client, target_stage_id)
    if not final_state:
        current_rank = _current_stage_rank(current_stage_name)
        target_rank = _current_stage_rank(target_stage_name)
        if target_rank < current_rank:
            return True

    ok = client.update_stage(deal_id=deal_id, stage_id=target_stage_id)
    if ok:
        emit(f"[CADENCIA_AVANCO] deal={deal_id} stage={target_stage_name or target_stage_id}")
    else:
        emit(f"[ERRO_CRM_CRITICO] operacao=update_stage | deal={deal_id} | stage={target_stage_id}")
    return ok
