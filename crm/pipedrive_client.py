from __future__ import annotations

import logging
import re
import socket
import time
from threading import Lock
from typing import Any, Dict, List, Optional

import requests
from requests.exceptions import ConnectionError as RequestsConnectionError
from urllib3.exceptions import MaxRetryError

from config.config_loader import get_config_value


class PipedriveClient:
    STATUS_BOT_FIELD = "status_bot"
    CNPJ_FIELD_KEY = "cnpj"
    CNAE_FIELD_KEY = "cnae"
    CRM_WRITE_RETRIES = 1
    REQUEST_RETRIES = 3
    REQUEST_RETRY_DELAYS_SEC = (5.0, 10.0, 20.0)
    REQUEST_TIMEOUT_SEC = 10
    RATE_LIMIT_COOLDOWN_SEC = 60
    GLOBAL_MIN_INTERVAL_SEC = 1.5
    _rate_limit_until_ts = 0.0
    _global_last_request_ts = 0.0
    _global_rate_lock = Lock()

    def __init__(
        self,
        token: str = "",
        domain: str = "",
        *,
        base_url: str = "",
        logger: logging.Logger | None = None,
    ) -> None:
        resolved_token = str(
            token or get_config_value("pipedrive_api_token", "") or get_config_value("pipedrive_token", "") or ""
        ).strip()
        resolved_base_url = str(
            base_url or domain or get_config_value("pipedrive_base_url", "https://api.pipedrive.com/v1") or "https://api.pipedrive.com/v1"
        ).rstrip("/")
        self.token = resolved_token
        self.api_token = resolved_token
        self.base_url = resolved_base_url
        self.logger = logger or logging.getLogger(__name__)
        self.last_http_status = 0
        self.last_http_body = ""
        self.last_http_endpoint = ""
        self.last_http_method = ""

    def _perform_request_with_retry(
        self,
        method: str,
        url: str,
        *,
        params: Dict[str, Any],
        json: Optional[Dict[str, Any]],
    ):
        for attempt in range(1, self.REQUEST_RETRIES + 1):
            try:
                return requests.request(
                    method.upper(),
                    url,
                    params=params,
                    json=json,
                    timeout=min(max(int(self.REQUEST_TIMEOUT_SEC or 10), 1), 15),
                )
            except (RequestsConnectionError, MaxRetryError, socket.gaierror) as exc:
                self.last_http_status = 0
                self.last_http_body = str(exc)
                self.logger.warning(
                    f"[PIPEDRIVE_DNS_FAIL] metodo={method.upper()} url={url} tentativa={attempt}/{self.REQUEST_RETRIES}"
                )
                if attempt >= self.REQUEST_RETRIES:
                    return None
                self.logger.warning(
                    f"[PIPEDRIVE_RETRY] metodo={method.upper()} url={url} tentativa={attempt}/{self.REQUEST_RETRIES}"
                )
                delay = self.REQUEST_RETRY_DELAYS_SEC[min(attempt - 1, len(self.REQUEST_RETRY_DELAYS_SEC) - 1)]
                time.sleep(delay)
            except Exception as exc:
                if attempt >= self.REQUEST_RETRIES:
                    raise
                self.logger.warning(
                    f"[PIPEDRIVE_RETRY] metodo={method.upper()} url={url} tentativa={attempt}/{self.REQUEST_RETRIES}"
                )
                delay = self.REQUEST_RETRY_DELAYS_SEC[min(attempt - 1, len(self.REQUEST_RETRY_DELAYS_SEC) - 1)]
                time.sleep(delay)
        return None

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if not self.api_token:
            self.last_http_status = 0
            self.last_http_body = ""
            self.last_http_endpoint = str(endpoint or "")
            self.last_http_method = str(method or "").upper()
            return None
        now = time.time()
        if now < float(self.__class__._rate_limit_until_ts or 0.0):
            remaining = max(0, int(float(self.__class__._rate_limit_until_ts) - now))
            self.logger.warning(
                f"[CRM_RATE_LIMIT] aguardando_cooldown={remaining}s metodo={str(method or '').upper()} endpoint={str(endpoint or '')}"
            )
            time.sleep(remaining + 1)
        with self.__class__._global_rate_lock:
            now = time.time()
            elapsed = now - float(self.__class__._global_last_request_ts or 0.0)
            if elapsed < float(self.GLOBAL_MIN_INTERVAL_SEC):
                time.sleep(float(self.GLOBAL_MIN_INTERVAL_SEC) - elapsed)
            self.__class__._global_last_request_ts = time.time()
        request_params = dict(params or {})
        request_params["api_token"] = self.api_token
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        self.last_http_endpoint = str(endpoint or "")
        self.last_http_method = str(method or "").upper()
        try:
            response = self._perform_request_with_retry(method, url, params=request_params, json=json)
            if response is None:
                self.last_http_status = int(self.last_http_status or 0)
                if not self.last_http_body:
                    self.last_http_body = "request_failed_after_retries"
                return None
        except (RequestsConnectionError, MaxRetryError, socket.gaierror) as exc:
            self.last_http_status = 0
            self.last_http_body = str(exc)
            self.logger.warning(
                f"[PIPEDRIVE_DNS_FAIL] metodo={self.last_http_method} endpoint={self.last_http_endpoint}"
            )
            return None
        except Exception as exc:
            self.last_http_status = 0
            self.last_http_body = str(exc)
            self.logger.error(f"[ERRO_ENRIQUECIMENTO] {str(exc)}", exc_info=True)
            return None
        self.last_http_status = int(response.status_code or 0)
        self.last_http_body = str(response.text or "")
        if self.last_http_status == 429:
            self.__class__._rate_limit_until_ts = time.time() + float(self.RATE_LIMIT_COOLDOWN_SEC)
            self.logger.warning(
                f"[CRM_RATE_LIMIT] cooldown={self.RATE_LIMIT_COOLDOWN_SEC}s metodo={method.upper()} endpoint={endpoint}"
            )
            return None
        if response.status_code not in (200, 201):
            self.logger.error(
                f"Erro Pipedrive API | {method.upper()} {endpoint} | Status: {response.status_code} | Resposta: {response.text}"
            )
            return None
        try:
            payload = response.json()
        except Exception as exc:
            self.logger.error(f"[ERRO_ENRIQUECIMENTO] {str(exc)}", exc_info=True)
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _extract_id(field: Any) -> int:
        if isinstance(field, dict):
            return int(field.get("value") or field.get("id") or 0)
        try:
            return int(field or 0)
        except Exception:
            return 0

    @staticmethod
    def _as_list(value: Any) -> List[Any]:
        if isinstance(value, list):
            return value
        if value in (None, "", 0):
            return []
        return [value]

    @staticmethod
    def _normalize_label_token(value: Any) -> str:
        return str(value or "").strip().upper()

    @classmethod
    def _label_candidate_tokens(cls, value: Any) -> List[str]:
        token = cls._normalize_label_token(value)
        if not token:
            return []
        if "," in token:
            return [part.strip() for part in token.split(",") if part.strip()]
        return [token]

    @classmethod
    def _build_label_option_maps(cls, options: List[Dict[str, Any]]) -> tuple[Dict[int, str], Dict[str, int]]:
        id_to_name: Dict[int, str] = {}
        name_to_id: Dict[str, int] = {}
        for option in options or []:
            if not isinstance(option, dict):
                continue
            try:
                option_id = int(option.get("id") or 0)
            except Exception:
                option_id = 0
            option_name = cls._normalize_label_token(option.get("label") or option.get("name") or option.get("value"))
            if option_id > 0:
                id_to_name[option_id] = option_name
            if option_id > 0 and option_name:
                name_to_id[option_name] = option_id
        return id_to_name, name_to_id

    @classmethod
    def resolve_label_tokens(cls, raw_labels: Any, options: List[Dict[str, Any]]) -> set[str]:
        id_to_name, _ = cls._build_label_option_maps(options)
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
                for token in cls._label_candidate_tokens(candidate):
                    if token.isdigit():
                        mapped = id_to_name.get(int(token), "")
                        if mapped:
                            tokens.add(mapped)
                    tokens.add(token)
        return tokens

    @classmethod
    def resolve_label_ids(cls, raw_labels: Any, options: List[Dict[str, Any]]) -> List[int]:
        _, name_to_id = cls._build_label_option_maps(options)
        items = raw_labels if isinstance(raw_labels, list) else [raw_labels]
        resolved: set[int] = set()
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
                for token in cls._label_candidate_tokens(candidate):
                    if token.isdigit():
                        value = int(token)
                        if value > 0:
                            resolved.add(value)
                            continue
                    option_id = name_to_id.get(token, 0)
                    if option_id > 0:
                        resolved.add(option_id)
        return sorted(resolved)

    @staticmethod
    def _normalize_phone(value: Any) -> str:
        digits = re.sub(r"\D+", "", str(value or ""))
        if not digits:
            return ""
        if digits.startswith("55"):
            return digits
        if len(digits) in {10, 11, 12, 13}:
            return f"55{digits}"
        return digits

    def test_connection(self) -> bool:
        response = self._request("GET", "users/me")
        return bool(response and response.get("data"))

    def get_stages(self) -> List[Dict[str, Any]]:
        response = self._request("GET", "stages")
        return list(response.get("data") or []) if response else []

    def get_leads(self, filter_id: int | None = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if filter_id:
            params["filter_id"] = int(filter_id)
        response = self._request("GET", "leads", params=params)
        return list(response.get("data") or []) if response else []

    def get_deals(
        self,
        status: str = "open",
        limit: int = 100,
        updated_since: float | None = None,
        pipeline_id: int | None = None,
    ) -> List[Dict[str, Any]]:
        target_total = max(1, int(limit or 100))
        page_limit = max(1, min(target_total, 100))
        start = 0
        deals: List[Dict[str, Any]] = []
        seen_ids = set()
        while len(deals) < target_total:
            params = {"status": status, "start": start, "limit": page_limit}
            if updated_since:
                params["start_modified_time"] = int(updated_since)
            if int(pipeline_id or 0) > 0:
                params["pipeline_id"] = int(pipeline_id)
            response = self._request("GET", "deals", params=params)
            if not response:
                break
            chunk = response.get("data", []) or []
            if not isinstance(chunk, list) or not chunk:
                break
            added = 0
            for deal in chunk:
                if not isinstance(deal, dict):
                    continue
                if "days_in_stage" not in deal:
                    deal["days_in_stage"] = 0
                deal_id = int(deal.get("id", 0) or 0)
                if deal_id and deal_id in seen_ids:
                    continue
                if deal_id:
                    seen_ids.add(deal_id)
                deals.append(deal)
                added += 1
                if len(deals) >= target_total:
                    break
            if len(deals) >= target_total:
                break
            pagination = response.get("additional_data", {}).get("pagination", {})
            if not pagination.get("more_items_in_collection"):
                break
            next_start = pagination.get("next_start")
            start = int(next_start or (start + page_limit))
            if added == 0:
                break
        return deals[:target_total]

    def get_person(self, person_id: int) -> Dict[str, Any]:
        return self.get_person_details(person_id)

    def get_person_details(self, person_id: int) -> Dict[str, Any]:
        response = self._request("GET", f"persons/{int(person_id)}")
        return dict(response.get("data") or {}) if response else {}

    def get_deal_details(self, deal_id: int) -> Dict[str, Any]:
        response = self._request("GET", f"deals/{int(deal_id)}")
        return dict(response.get("data") or {}) if response else {}

    def get_deal_persons(self, deal_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        response = self._request("GET", f"deals/{int(deal_id)}/persons", params={"limit": max(1, int(limit))})
        return list(response.get("data") or []) if response else []

    def get_deal_participants(self, deal_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        response = self._request("GET", f"deals/{int(deal_id)}/participants", params={"limit": max(1, int(limit))})
        return list(response.get("data") or []) if response else []

    def get_organization(self, org_id: int) -> Dict[str, Any]:
        response = self._request("GET", f"organizations/{int(org_id)}")
        return dict(response.get("data") or {}) if response else {}

    def get_organization_persons(self, org_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        if not int(org_id or 0):
            return []
        response = self._request("GET", "persons", params={"org_id": int(org_id), "limit": max(1, int(limit))})
        return list(response.get("data") or []) if response else []

    def _write_with_retry(self, operation: str, fn) -> bool:
        last_ok = False
        for attempt in range(self.CRM_WRITE_RETRIES + 1):
            try:
                last_ok = bool(fn())
            except Exception:
                last_ok = False
            if last_ok:
                return True
            if attempt < self.CRM_WRITE_RETRIES:
                time.sleep(0.5)
        self.logger.error(f"[ERRO_CRM_CRITICO] operacao={operation}")
        return False

    def update_deal(self, deal_id: int, data: Dict[str, Any]) -> bool:
        payload = dict(data or {})
        label = payload.get("label")
        if label:
            payload["label"] = self.resolve_label_ids(label, self.get_deal_labels())
        return self._write_with_retry(
            f"update_deal:{int(deal_id)}",
            lambda: self._request("PUT", f"deals/{int(deal_id)}", json=payload),
        )

    def delete_deal(self, deal_id: int) -> bool:
        if not int(deal_id or 0):
            return False
        return self._write_with_retry(
            f"delete_deal:{int(deal_id)}",
            lambda: self._request("DELETE", f"deals/{int(deal_id)}"),
        )

    def update_stage(self, *, deal_id: int, stage_id: int) -> bool:
        if not int(deal_id or 0) or not int(stage_id or 0):
            return False
        return self.update_deal(int(deal_id), {"stage_id": int(stage_id)})

    def update_person(self, person_id: int, data: Dict[str, Any]) -> bool:
        payload = dict(data or {})
        label = payload.get("label")
        if label:
            resolved = self.resolve_label_ids(label, self.get_person_labels())
            payload["label"] = resolved[0] if resolved else None
            if payload["label"] is None:
                payload.pop("label", None)
        return self._write_with_retry(
            f"update_person:{int(person_id)}",
            lambda: self._request("PUT", f"persons/{int(person_id)}", json=payload),
        )

    def update_organization(self, org_id: int, data: Dict[str, Any]) -> bool:
        return bool(self._request("PUT", f"organizations/{int(org_id)}", json=dict(data or {})))

    def add_deal_participant(self, deal_id: int, person_id: int) -> bool:
        if not int(deal_id or 0) or not int(person_id or 0):
            return False
        return bool(
            self._request(
                "POST",
                f"deals/{int(deal_id)}/participants",
                json={"person_id": int(person_id)},
            )
        )

    def remove_deal_participant(self, deal_id: int, participant_id: int) -> bool:
        if not int(deal_id or 0) or not int(participant_id or 0):
            return False
        return self._write_with_retry(
            f"remove_deal_participant:{int(deal_id)}:{int(participant_id)}",
            lambda: self._request(
                "DELETE",
                f"deals/{int(deal_id)}/participants/{int(participant_id)}",
            ),
        )

    def find_person_by_term(self, term: str) -> List[Dict[str, Any]]:
        clean = str(term or "").strip()
        if not clean:
            return []
        response = self._request("GET", "persons/search", params={"term": clean, "limit": 10})
        return list(((response or {}).get("data") or {}).get("items") or [])

    def find_person(self, *, org_id: int = 0, phone: str = "", email: str = "") -> Dict[str, Any]:
        for term in (self._normalize_phone(phone), str(email or "").strip().lower()):
            if not term:
                continue
            for item in self.find_person_by_term(term):
                person = dict(item.get("item") or {})
                if not person:
                    continue
                if org_id and self._extract_id(person.get("org_id")) not in {0, int(org_id)}:
                    continue
                return person
        return {}

    def create_person(self, *args, **kwargs) -> Dict[str, Any]:
        payload: Dict[str, Any]
        if args and isinstance(args[0], dict):
            payload = dict(args[0])
        else:
            payload = dict(kwargs)
        if "phone" in payload:
            payload["phone"] = self._as_list(payload.get("phone"))
        if "email" in payload:
            payload["email"] = self._as_list(payload.get("email"))
        response = self._request("POST", "persons", json=payload)
        return dict(response.get("data") or {}) if response else {}

    def search_organizations(self, term: str, field_key: Optional[str] = None) -> List[Dict[str, Any]]:
        clean = str(term or "").strip()
        if not clean:
            return []
        params: Dict[str, Any] = {"term": clean, "fields": "name", "limit": 10}
        if field_key:
            params["fields"] = "name,custom_fields"
        response = self._request("GET", "organizations/search", params=params)
        return list(((response or {}).get("data") or {}).get("items") or [])

    def find_organization_by_name(self, name: str) -> List[Dict[str, Any]]:
        return self.search_organizations(name)

    def find_organization_by_cnpj(self, cnpj: str) -> Dict[str, Any]:
        clean = re.sub(r"\D+", "", str(cnpj or ""))
        if not clean:
            return {}
        for item in self.search_organizations(clean, field_key=self.CNPJ_FIELD_KEY):
            organization = dict(item.get("item") or {})
            if organization:
                return organization
        return {}

    def create_organization(self, data: Dict[str, Any]) -> Dict[str, Any]:
        response = self._request("POST", "organizations", json=dict(data or {}))
        return dict(response.get("data") or {}) if response else {}

    def find_open_deals_by_person_or_org(self, *, person_id: int = 0, org_id: int = 0) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        seen_ids = set()
        for deal in self.get_deals(status="open", limit=500):
            current_person_id = self._extract_id(deal.get("person_id"))
            current_org_id = self._extract_id(deal.get("org_id"))
            if person_id and current_person_id == int(person_id):
                deal_id = self._extract_id(deal.get("id"))
                if deal_id and deal_id not in seen_ids:
                    seen_ids.add(deal_id)
                    results.append(deal)
                continue
            if org_id and current_org_id == int(org_id):
                deal_id = self._extract_id(deal.get("id"))
                if deal_id and deal_id not in seen_ids:
                    seen_ids.add(deal_id)
                    results.append(deal)
        return results

    def find_deal_by_title(self, title: str) -> Dict[str, Any]:
        clean = str(title or "").strip().lower()
        if not clean:
            return {}
        for deal in self.get_deals(status="all_not_deleted", limit=500):
            if str(deal.get("title", "")).strip().lower() == clean:
                return deal
        return {}

    def add_note(self, deal_id: int = 0, content: str = "", **kwargs) -> bool:
        payload = dict(kwargs)
        if deal_id:
            payload["deal_id"] = int(deal_id)
        if content:
            payload["content"] = str(content)
        return self.create_note(**payload)

    def create_note(self, *, content: str, person_id: int = 0, deal_id: int = 0) -> bool:
        payload = {"content": str(content or "").strip()}
        if int(person_id or 0):
            payload["person_id"] = int(person_id)
        if int(deal_id or 0):
            payload["deal_id"] = int(deal_id)
        return self._write_with_retry("create_note", lambda: self._request("POST", "notes", json=payload))

    def add_tag(self, entity_type: str, entity_id: int, tag: Any) -> bool:
        if str(entity_type or "").strip().lower() != "deal":
            return False
        if not entity_id:
            return False
        self.ensure_deal_labels([str(tag or "").strip()])
        
        try:
            deal = self.get_deal_details(entity_id)
            current_labels = deal.get("label") or []
            if isinstance(current_labels, str):
                current_labels = current_labels.split(",")
            elif not isinstance(current_labels, list):
                current_labels = [current_labels]
            
            new_labels = list(current_labels)
            if tag not in new_labels:
                new_labels.append(tag)
            
            return self.update_deal(int(entity_id), {"label": new_labels})
        except Exception as e:
            self.logger.error(f"[ERRO_ADD_TAG] {e}")
            return self.update_deal(int(entity_id), {"label": [tag]})

    def create_activity(self, *args, **kwargs) -> bool:
        payload: Dict[str, Any]
        if len(args) == 3 and not kwargs:
            deal_id, person_id, email = args
            payload = {
                "subject": f"Contato via LinkedIn - {str(email or '').strip()}",
                "type": "meeting",
                "deal_id": int(deal_id or 0),
                "person_id": int(person_id or 0),
                "note": f"Email capturado pelo LinkedIn: {str(email or '').strip()}",
                "done": 0,
            }
        else:
            payload = dict(kwargs)
        if not payload.get("subject"):
            payload["subject"] = "Atividade SDR"
        response = self._request("POST", "activities", json=payload)
        return bool(response and response.get("data"))

    def create_task(self, *, person_id: int, subject: str, due_date: str) -> bool:
        return self.create_activity(
            person_id=int(person_id),
            subject=str(subject or "").strip() or "Tarefa SDR",
            due_date=str(due_date or "").strip(),
            type="task",
            done=0,
        )

    def get_activities(self, **params) -> List[Dict[str, Any]]:
        response = self._request("GET", "activities", params=dict(params or {}))
        data = list(response.get("data") or []) if response else []
        return data if isinstance(data, list) else []

    def delete_activity(self, activity_id: int) -> bool:
        target_id = int(activity_id or 0)
        if target_id <= 0:
            return False
        response = self._request("DELETE", f"activities/{target_id}")
        return bool(response and response.get("success") is True)

    def cleanup_bot_call_activities_for_date(self, target_date: str = "") -> int:
        date_str = str(target_date or time.strftime("%Y-%m-%d")).strip() or time.strftime("%Y-%m-%d")
        deleted = 0
        seen_ids = set()
        for _pass in range(1, 11):
            try:
                activities = self.get_activities(due_date=date_str, limit=500)
            except Exception:
                activities = []
            batch_ids = []
            for activity in activities:
                if not isinstance(activity, dict):
                    continue
                activity_type_value = str(activity.get("type") or "").strip().lower()
                if activity_type_value != "call":
                    continue
                subject = str(activity.get("subject") or "").strip()
                if not subject.startswith("Ligar - ") or "[DEAL " not in subject:
                    continue
                activity_id = self._extract_id(activity.get("id"))
                if activity_id <= 0 or activity_id in seen_ids:
                    continue
                batch_ids.append(activity_id)
            if not batch_ids:
                break
            for activity_id in batch_ids:
                seen_ids.add(activity_id)
                try:
                    if self.delete_activity(activity_id):
                        deleted += 1
                except Exception:
                    continue
        return deleted

    def has_open_activity_today(self, *, deal_id: int, activity_type: str = "", subject: str = "") -> bool:
        target_deal_id = int(deal_id or 0)
        if target_deal_id <= 0:
            return False
        target_type = str(activity_type or "").strip().lower()
        target_subject = str(subject or "").strip().lower()
        today = time.strftime("%Y-%m-%d")
        try:
            activities = self.get_activities(
                deal_id=target_deal_id,
                due_date=today,
                done=0,
                limit=100,
            )
        except Exception:
            activities = []
        for activity in activities:
            if not isinstance(activity, dict):
                continue
            try:
                activity_deal_id = self._extract_id(activity.get("deal_id"))
            except Exception:
                activity_deal_id = 0
            if activity_deal_id != target_deal_id:
                continue
            activity_type_value = str(activity.get("type") or "").strip().lower()
            if target_type and activity_type_value != target_type:
                continue
            activity_due = str(activity.get("due_date") or "").strip()
            if activity_due and activity_due != today:
                continue
            activity_subject = str(activity.get("subject") or "").strip().lower()
            if target_subject and activity_subject != target_subject:
                continue
            return True
        return False

    def get_deal_labels(self) -> List[Dict[str, Any]]:
        response = self._request("GET", "dealFields")
        fields = list(response.get("data") or []) if response else []
        for field in fields:
            if str(field.get("key", "")).strip() == "label":
                options = field.get("options") or []
                return list(options) if isinstance(options, list) else []
        return []

    def get_person_labels(self) -> List[Dict[str, Any]]:
        response = self._request("GET", "personFields")
        fields = list(response.get("data") or []) if response else []
        for field in fields:
            if str(field.get("key", "")).strip() == "label":
                options = field.get("options") or []
                return list(options) if isinstance(options, list) else []
        return []

    def get_person_label_field(self) -> Dict[str, Any]:
        response = self._request("GET", "personFields")
        fields = list(response.get("data") or []) if response else []
        for field in fields:
            if str(field.get("key", "")).strip() == "label":
                return dict(field or {})
        return {}

    def ensure_person_labels(self, labels: List[str]) -> bool:
        target_labels = [self._normalize_label_token(item) for item in labels if self._normalize_label_token(item)]
        if not target_labels:
            return True
        field = self.get_person_label_field()
        if not field:
            return False
        field_id = int(field.get("id") or 0)
        existing_options = list(field.get("options") or [])
        existing_tokens = self.resolve_label_tokens(existing_options, existing_options)
        missing = [label for label in target_labels if label not in existing_tokens]
        if not missing:
            return True
        options_payload: List[Dict[str, Any]] = []
        for option in existing_options:
            if not isinstance(option, dict):
                continue
            item: Dict[str, Any] = {}
            if int(option.get("id") or 0):
                item["id"] = int(option.get("id") or 0)
            if str(option.get("label") or "").strip():
                item["label"] = str(option.get("label") or "").strip()
            if str(option.get("color") or "").strip():
                item["color"] = str(option.get("color") or "").strip()
            if item.get("label"):
                options_payload.append(item)
        for label in missing:
            options_payload.append({"label": label, "color": "blue"})
        payload = {"options": options_payload}
        return bool(self._request("PUT", f"personFields/{field_id}", json=payload))

    def get_deal_label_field(self) -> Dict[str, Any]:
        response = self._request("GET", "dealFields")
        fields = list(response.get("data") or []) if response else []
        for field in fields:
            if str(field.get("key", "")).strip() == "label":
                return dict(field or {})
        return {}

    def ensure_deal_labels(self, labels: List[str]) -> bool:
        target_labels = [self._normalize_label_token(item) for item in labels if self._normalize_label_token(item)]
        if not target_labels:
            return True
        field = self.get_deal_label_field()
        if not field:
            return False
        field_id = int(field.get("id") or 0)
        existing_options = list(field.get("options") or [])
        existing_tokens = self.resolve_label_tokens(existing_options, existing_options)
        missing = [label for label in target_labels if label not in existing_tokens]
        if not missing:
            return True
        options_payload: List[Dict[str, Any]] = []
        for option in existing_options:
            if not isinstance(option, dict):
                continue
            item: Dict[str, Any] = {}
            if int(option.get("id") or 0) > 0:
                item["id"] = int(option.get("id") or 0)
            if str(option.get("label") or "").strip():
                item["label"] = str(option.get("label") or "").strip()
            if str(option.get("color") or "").strip():
                item["color"] = str(option.get("color") or "").strip()
            if item.get("label"):
                options_payload.append(item)
        for label in missing:
            options_payload.append({"label": label, "color": "green"})
        payload = {"options": options_payload}
        return bool(self._request("PUT", f"dealFields/{field_id}", json=payload))

    @staticmethod
    def build_org_fields(cnpj: str, cnae: str) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        clean_cnpj = re.sub(r"\D+", "", str(cnpj or ""))
        if clean_cnpj:
            payload[PipedriveClient.CNPJ_FIELD_KEY] = clean_cnpj
        if str(cnae or "").strip():
            payload[PipedriveClient.CNAE_FIELD_KEY] = str(cnae or "").strip()
        return payload

    @staticmethod
    def build_company_note(lead: Dict[str, Any], mode: str = "create") -> str:
        header = "ENRIQUECIMENTO" if str(mode or "").strip().lower() == "enrich" else "CRIACAO"
        lines = [f"--- {header} EMPRESA ---"]
        for key in ("empresa", "cnpj", "telefone", "email", "endereco", "cnae", "fonte", "segmento_consultado"):
            value = str(lead.get(key, "")).strip()
            if value:
                lines.append(f"{key.upper()}: {value}")
        return "\n".join(lines)

    @staticmethod
    def extract_cnpj(organization: Dict[str, Any]) -> str:
        raw = organization.get(PipedriveClient.CNPJ_FIELD_KEY) or organization.get("cnpj") or ""
        return re.sub(r"\D+", "", str(raw or ""))
