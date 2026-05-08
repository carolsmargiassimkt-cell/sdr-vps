from __future__ import annotations

import logging
import re
import socket
import time
from threading import Lock
from typing import Any, Dict, List, Optional

import requests
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout
from urllib3.exceptions import MaxRetryError

from config.config_loader import get_config_value

try:
    from sdr_config import STATUS_FIELD as SDR_STATUS_FIELD, STATUS_MAP as SDR_STATUS_MAP
except Exception:
    SDR_STATUS_FIELD = "status_bot"
    SDR_STATUS_MAP = {}


class PipedriveClient:
    STATUS_BOT_FIELD = str(SDR_STATUS_FIELD or "status_bot").strip() or "status_bot"
    STATUS_BOT_MAP = {str(key or "").strip().lower(): int(value or 0) for key, value in dict(SDR_STATUS_MAP or {}).items()}
    STATUS_BOT_SYNONYMS = {
        "contato_iniciado": "lead_sem_contato",
        "bloqueado": "lead_sem_interesse",
        "sem_interesse": "lead_sem_interesse",
        "hostil": "lead_sem_interesse",
        "indicacao": "lead_sem_interesse",
        "interesse": "lead_interessado",
        "respondido": "lead_interessado",
        "wrong_number": "numero_nao_corresponde",
        "unknown_person": "numero_nao_corresponde",
        "numero_errado": "numero_nao_corresponde",
        "aguardando_horario": "lead_sem_contato",
    }
    PERSON_TAGS_FIELD = "label_ids"
    CNPJ_FIELD_KEY = "cnpj"
    CNAE_FIELD_KEY = "cnae"
    
    # Audit Tags
    TAG_FILTER_163 = 163
    TAG_FILTER_166 = 166
    TAG_LOCK_168 = 168
    
    CRM_WRITE_RETRIES = 2
    REQUEST_RETRIES = 5
    REQUEST_RETRY_DELAYS_SEC = (1.0, 2.0, 5.0, 10.0, 10.0)
    REQUEST_TIMEOUT_SEC = 10
    RETRIABLE_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}
    RATE_LIMIT_COOLDOWN_SEC = 60
    GLOBAL_MIN_INTERVAL_SEC = 0.5
    DEAL_FIELDS_CACHE_TTL_SEC = 3600
    _rate_limit_until_ts = 0.0
    _global_last_request_ts = 0.0
    _global_rate_lock = Lock()
    _deals_list_cache: Dict[str, tuple[float, List[Dict[str, Any]]]] = {}

    def __init__(
        self,
        token: str = "",
        domain: str = "",
        *,
        base_url: str = "",
        logger: logging.Logger | None = None,
    ) -> None:
        resolved_token = "119ada66d2216a66981ec95a1ec983ec8255e31c"
        # resolved_token = str(
        #     token or get_config_value("pipedrive_api_token", "") or get_config_value("pipedrive_token", "") or ""
        # ).strip()
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
        self._deal_fields_cache = None
        self._deal_fields_cache_time = 0.0

    @staticmethod
    def _body_preview(body: Any, limit: int = 200) -> str:
        compact = re.sub(r"\s+", " ", str(body or "")).strip()
        return compact[: max(1, int(limit or 200))]

    @classmethod
    def _body_looks_like_html(cls, body: Any) -> bool:
        preview = cls._body_preview(body, limit=64).lower()
        return preview.startswith("<!doctype html") or preview.startswith("<html") or preview.startswith("<head") or preview.startswith("<body")

    @classmethod
    def _response_looks_like_html(cls, response: Any) -> bool:
        try:
            content_type = str((response.headers or {}).get("Content-Type") or "").lower()
        except Exception:
            content_type = ""
        if "text/html" in content_type:
            return True
        return cls._body_looks_like_html(getattr(response, "text", ""))

    @classmethod
    def _is_transient_http_status(cls, status: Any) -> bool:
        try:
            return int(status or 0) in cls.RETRIABLE_HTTP_STATUS_CODES
        except Exception:
            return False

    @staticmethod
    def _header_int(headers: Any, *names: str) -> int:
        if not hasattr(headers, "items"):
            return 0
        lowered = {str(key).strip().lower(): value for key, value in dict(headers).items()}
        for name in names:
            raw = str(lowered.get(str(name).strip().lower(), "") or "").strip()
            match = re.search(r"-?\d+", raw)
            if match:
                try:
                    return int(match.group(0))
                except Exception:
                    continue
        return 0

    @classmethod
    def _rate_limit_cooldown_for_response(cls, response: Any) -> int:
        headers = getattr(response, "headers", {}) or {}
        retry_after = cls._header_int(headers, "retry-after")
        reset_window = cls._header_int(headers, "x-ratelimit-reset")
        cooldown = max(2, int(cls.RATE_LIMIT_COOLDOWN_SEC or 60))
        if retry_after > 0:
            cooldown = max(cooldown, retry_after + 1)
        elif reset_window > 0:
            cooldown = max(2, reset_window + 1)
        return cooldown

    @classmethod
    def _rate_limit_headers_text(cls, response: Any) -> str:
        headers = getattr(response, "headers", {}) or {}
        details = {
            "limit": cls._header_int(headers, "x-ratelimit-limit"),
            "remaining": cls._header_int(headers, "x-ratelimit-remaining"),
            "reset": cls._header_int(headers, "x-ratelimit-reset"),
            "daily_left": cls._header_int(headers, "x-daily-requests-left"),
            "retry_after": cls._header_int(headers, "retry-after"),
        }
        parts = [f"{key}={value}" for key, value in details.items() if value > 0]
        return " ".join(parts)

    def _last_call_was_transient(self, endpoint_prefix: str = "") -> bool:
        status = int(getattr(self, "last_http_status", 0) or 0)
        endpoint = str(getattr(self, "last_http_endpoint", "") or "")
        if endpoint_prefix and not endpoint.startswith(str(endpoint_prefix or "")):
            return False
        if status == 0:
            return True
        if self._is_transient_http_status(status):
            return True
        return self._body_looks_like_html(getattr(self, "last_http_body", ""))

    def _perform_request_with_retry(
        self,
        method: str,
        url: str,
        endpoint: str,
        *,
        params: Dict[str, Any],
        json: Optional[Dict[str, Any]],
    ):
        for attempt in range(1, self.REQUEST_RETRIES + 1):
            response = None
            try:
                response = requests.request(
                    method.upper(),
                    url,
                    params=params,
                    json=json,
                    timeout=float(self.REQUEST_TIMEOUT_SEC or 10),
                )
            except (RequestsConnectionError, RequestsTimeout, MaxRetryError, socket.gaierror) as exc:
                self.last_http_status = 0
                self.last_http_body = str(exc)
                self.logger.warning(
                    f"[PIPEDRIVE_RETRY] metodo={method.upper()} endpoint={endpoint} "
                    f"status=0 tentativa={attempt}/{self.REQUEST_RETRIES} motivo={exc.__class__.__name__} "
                    f"timeout={float(self.REQUEST_TIMEOUT_SEC or 10):.0f}s"
                )
                if attempt >= self.REQUEST_RETRIES:
                    return None
                delay = self.REQUEST_RETRY_DELAYS_SEC[min(attempt - 1, len(self.REQUEST_RETRY_DELAYS_SEC) - 1)]
                time.sleep(delay)
                continue
            except Exception as exc:
                if attempt >= self.REQUEST_RETRIES:
                    raise
                self.logger.warning(
                    f"[PIPEDRIVE_RETRY] metodo={method.upper()} endpoint={endpoint} "
                    f"status=0 tentativa={attempt}/{self.REQUEST_RETRIES} motivo={exc.__class__.__name__}"
                )
                delay = self.REQUEST_RETRY_DELAYS_SEC[min(attempt - 1, len(self.REQUEST_RETRY_DELAYS_SEC) - 1)]
                time.sleep(delay)
                continue
            response_status = int(getattr(response, "status_code", 0) or 0)
            response_body = str(getattr(response, "text", "") or "")
            self.last_http_status = response_status
            self.last_http_body = response_body
            if self._is_transient_http_status(response_status):
                self.logger.warning(
                    f"[PIPEDRIVE_RETRY] metodo={method.upper()} endpoint={endpoint} "
                    f"status={response_status} tentativa={attempt}/{self.REQUEST_RETRIES} motivo=http_{response_status}"
                )
                if attempt >= self.REQUEST_RETRIES:
                    return response
                
                delay = self.REQUEST_RETRY_DELAYS_SEC[min(attempt - 1, len(self.REQUEST_RETRY_DELAYS_SEC) - 1)]
                if response_status == 429:
                    cooldown = self._rate_limit_cooldown_for_response(response)
                    delay = max(delay, cooldown)
                
                time.sleep(delay)
                continue
            if self._response_looks_like_html(response):
                self.logger.warning(
                    f"[PIPEDRIVE_RETRY] metodo={method.upper()} endpoint={endpoint} "
                    f"status={response_status} tentativa={attempt}/{self.REQUEST_RETRIES} motivo=html_unexpected"
                )
                if attempt >= self.REQUEST_RETRIES:
                    return response
                delay = self.REQUEST_RETRY_DELAYS_SEC[min(attempt - 1, len(self.REQUEST_RETRY_DELAYS_SEC) - 1)]
                time.sleep(delay)
                continue
            return response
        return None

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        return self._request_once(method, endpoint, params=params, json=json)

    def _request_once(
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
            time.sleep(0.3)
            self.__class__._global_last_request_ts = time.time()
        request_params = dict(params or {})
        request_params["api_token"] = self.api_token
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        self.last_http_endpoint = str(endpoint or "")
        self.last_http_method = str(method or "").upper()
        try:
            response = self._perform_request_with_retry(method, url, endpoint=endpoint, params=request_params, json=json)
            if response is None:
                self.last_http_status = int(self.last_http_status or 0)
                if not self.last_http_body:
                    self.last_http_body = "request_failed_after_retries"
                return None
        except (RequestsConnectionError, RequestsTimeout, MaxRetryError, socket.gaierror) as exc:
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
            cooldown = self._rate_limit_cooldown_for_response(response)
            self.__class__._rate_limit_until_ts = time.time() + float(cooldown)
            extra = self._rate_limit_headers_text(response)
            self.logger.warning(
                f"[CRM_RATE_LIMIT] cooldown={cooldown}s metodo={method.upper()} endpoint={endpoint}"
                + (f" {extra}" if extra else "")
            )
            return None
        if self._response_looks_like_html(response):
            self.logger.error(
                f"[PIPEDRIVE_HTML_UNEXPECTED] metodo={method.upper()} endpoint={endpoint} "
                f"status={self.last_http_status} body={self._body_preview(self.last_http_body)}"
            )
            return None
        if response.status_code not in (200, 201):
            self.logger.error(
                f"[PIPEDRIVE_HTTP_ERROR] metodo={method.upper()} endpoint={endpoint} "
                f"status={response.status_code} body={self._body_preview(self.last_http_body)}"
            )
            return None
        try:
            payload = response.json()
        except Exception as exc:
            self.logger.error(
                f"[PIPEDRIVE_JSON_INVALID] metodo={method.upper()} endpoint={endpoint} "
                f"status={self.last_http_status} erro={exc.__class__.__name__} body={self._body_preview(self.last_http_body)}"
            )
            return None
        return payload if isinstance(payload, dict) else None

    def get_deal_fields(self) -> List[Dict[str, Any]]:
        if self._deal_fields_cache and (time.time() - float(self._deal_fields_cache_time or 0.0) < self.DEAL_FIELDS_CACHE_TTL_SEC):
            self.logger.info("[CACHE_DEAL_FIELDS_ATIVO]")
            return list(self._deal_fields_cache or [])
        response = self._request("GET", "dealFields")
        fields = list(response.get("data") or []) if response else []
        self._deal_fields_cache = list(fields)
        self._deal_fields_cache_time = time.time()
        self.logger.info("[CACHE_DEAL_FIELDS_ATIVO]")
        return list(fields)

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

    @staticmethod
    def _compact_digits(value: Any) -> str:
        return re.sub(r"\D+", "", str(value or ""))

    @classmethod
    def _phone_search_terms(cls, phone: Any) -> List[str]:
        raw = cls._compact_digits(phone)
        if not raw:
            return []
        terms: List[str] = []

        def add(term: str) -> None:
            clean = cls._compact_digits(term)
            if clean and clean not in terms:
                terms.append(clean)

        add(raw)
        if raw.startswith("55") and len(raw) > 11:
            local = raw[2:]
            add(local)
        else:
            local = raw
            add(f"55{raw}")

        if len(local) == 11 and local[2] == "9":
            add(local[:2] + local[3:])
            add(f"55{local[:2]}{local[3:]}")
        elif len(local) == 10:
            add(local[:2] + "9" + local[2:])
            add(f"55{local[:2]}9{local[2:]}")
        return terms

    @classmethod
    def _person_phone_tokens(cls, person: Dict[str, Any]) -> set[str]:
        tokens: set[str] = set()
        for source in (person.get("phones"), person.get("phone")):
            if isinstance(source, list):
                for item in source:
                    if isinstance(item, dict):
                        tokens.update(cls._phone_search_terms(item.get("value")))
                    else:
                        tokens.update(cls._phone_search_terms(item))
            elif isinstance(source, dict):
                tokens.update(cls._phone_search_terms(source.get("value")))
            elif source:
                tokens.update(cls._phone_search_terms(source))
        return {token for token in tokens if token}

    def _decorate_deal_status(self, deal: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(deal, dict):
            return {}
        payload = dict(deal)
        resolved = self.resolve_status_bot_token(payload.get(self.STATUS_BOT_FIELD, payload.get("status_bot")))
        if resolved:
            payload["status_bot"] = resolved
        return payload

    def resolve_status_bot_token(self, value: Any) -> str:
        token = str(value or "").strip()
        if not token:
            return ""
        normalized = token.lower()
        mapped = self.STATUS_BOT_SYNONYMS.get(normalized, normalized)
        if mapped in self.STATUS_BOT_MAP:
            return mapped
        if token.isdigit():
            target_id = int(token)
            for field in self.get_deal_fields():
                if str(field.get("key") or "").strip() != self.STATUS_BOT_FIELD:
                    continue
                for option in list(field.get("options") or []):
                    if int(option.get("id") or 0) == target_id:
                        return str(option.get("label") or "").strip().lower()
        return normalized

    def resolve_status_bot_value(self, value: Any) -> Any:
        if value in (None, ""):
            return value
        if isinstance(value, int):
            return value
        token = self.resolve_status_bot_token(value)
        mapped_id = int(self.STATUS_BOT_MAP.get(token, 0) or 0)
        if mapped_id > 0:
            return mapped_id
        if str(value).isdigit():
            return int(str(value))
        return value
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
        cache_key = f"{status}_{pipeline_id}_{limit}"
        now = time.time()
        if not updated_since and cache_key in self.__class__._deals_list_cache:
            cached_time, cached_data = self.__class__._deals_list_cache[cache_key]
            if now - cached_time < 60:
                self.logger.info(f"[CACHE_GET_DEALS_ATIVO] key={cache_key}")
                return list(cached_data)

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
                deals.append(self._decorate_deal_status(deal))
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
        
        final_deals = deals[:target_total]
        if not updated_since:
            self.__class__._deals_list_cache[cache_key] = (now, list(final_deals))
        return final_deals

    def get_person(self, person_id: int) -> Dict[str, Any]:
        return self.get_person_details(person_id)

    def get_person_details(self, person_id: int) -> Dict[str, Any]:
        response = self._request("GET", f"persons/{int(person_id)}")
        return dict(response.get("data") or {}) if response else {}

    def get_deal_details(self, deal_id: int) -> Dict[str, Any]:
        response = self._request("GET", f"deals/{int(deal_id)}")
        return self._decorate_deal_status(dict(response.get("data") or {})) if response else {}

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
        """Retorna pessoas vinculadas diretamente a uma organização."""
        if not int(org_id or 0):
            return []
        response = self._request(
            "GET",
            f"organizations/{int(org_id)}/persons",
            params={"limit": max(1, int(limit))},
        )
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

    def is_deal_locked(self, deal_id: int) -> bool:
        """Verifica se o deal tem a trava absoluta (Tag 168)."""
        try:
            deal = self.get_deal_details(deal_id)
            if not deal:
                if self._last_call_was_transient(f"deals/{int(deal_id)}"):
                    self.logger.warning(
                        f"[TRAVA_CHECK_INDETERMINADA] deal={int(deal_id)} status={int(self.last_http_status or 0)} "
                        f"endpoint={self.last_http_endpoint}"
                    )
                    return True
                return False
            labels = self.resolve_label_ids(deal.get("label"), self.get_deal_labels())
            if self.TAG_LOCK_168 in labels:
                self.logger.info(f"[TRAVA_ATIVA] Deal {deal_id} possui Tag 168. Operação abortada.")
                return True
        except Exception as e:
            self.logger.error(f"[ERRO_CHECK_LOCK] {e}")
        return False

    def update_deal(self, deal_id: int, data: Dict[str, Any]) -> bool:
        payload = dict(data or {})

        if "status_bot" in payload and self.STATUS_BOT_FIELD != "status_bot":
            payload[self.STATUS_BOT_FIELD] = payload.pop("status_bot")
        if self.STATUS_BOT_FIELD in payload:
            payload[self.STATUS_BOT_FIELD] = self.resolve_status_bot_value(payload.get(self.STATUS_BOT_FIELD))
        
        # Se estiver tentando atualizar algo além do status_bot ou se for uma atualização crítica,
        # verificamos a trava.
        if self.is_deal_locked(deal_id):
            return False

        label = payload.get("label")
        if label:
            wanted = self.resolve_label_ids(label, self.get_deal_labels())
            try:
                current_deal = self.get_deal_details(deal_id) or {}
                current = self.resolve_label_ids(current_deal.get("label"), self.get_deal_labels())
            except Exception:
                current = []
            merged = []
            seen = set()
            for item in list(current or []) + list(wanted or []):
                key = str(item or "").strip()
                if not key or key in seen:
                    continue
                seen.add(key)
                merged.append(item)
            if 193 in set(int(x) for x in seen if str(x).isdigit()) and 193 not in merged:
                merged.append(193)
            payload["label"] = merged
        
        ok = self._write_with_retry(
            f"update_deal:{int(deal_id)}",
            lambda: self._request("PUT", f"deals/{int(deal_id)}", json=payload),
        )
        if not ok:
            self.logger.error(f"[FALHA_UPDATE_DEAL] deal_id={deal_id} payload={payload}")
        return ok

    def create_deal(self, data: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(data or {})
        if "status_bot" in payload and self.STATUS_BOT_FIELD != "status_bot":
            payload[self.STATUS_BOT_FIELD] = payload.pop("status_bot")
        if self.STATUS_BOT_FIELD in payload:
            payload[self.STATUS_BOT_FIELD] = self.resolve_status_bot_value(payload.get(self.STATUS_BOT_FIELD))
        label = payload.get("label")
        if label:
            payload["label"] = self.resolve_label_ids(label, self.get_deal_labels())
        response = self._request("POST", "deals", json=payload)
        return self._decorate_deal_status(dict(response.get("data") or {})) if response else {}

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
        
        # 1. Ajustar Label
        label = payload.get("label")
        if label:
            resolved = self.resolve_label_ids(label, self.get_person_labels())
            payload["label"] = resolved[0] if resolved else None
            if payload["label"] is None:
                payload.pop("label", None)

        # 2. Garantir formato correto para telefone (array of objects)
        if "phone" in payload:
            raw_phone = payload["phone"]
            phones_list = []
            if isinstance(raw_phone, str) and raw_phone.strip():
                phones_list.append({"value": raw_phone.strip(), "primary": True})
            elif isinstance(raw_phone, list):
                for item in raw_phone:
                    if isinstance(item, dict) and item.get("value"):
                        phones_list.append(item)
                    elif isinstance(item, str) and item.strip():
                        phones_list.append({"value": item.strip(), "primary": len(phones_list) == 0})
            
            if phones_list:
                payload["phone"] = phones_list
            else:
                payload.pop("phone", None)

        # 3. Garantir formato correto para email (array of objects)
        if "email" in payload:
            raw_email = payload["email"]
            emails_list = []
            if isinstance(raw_email, str) and raw_email.strip():
                emails_list.append({"value": raw_email.strip(), "primary": True})
            elif isinstance(raw_email, list):
                for item in raw_email:
                    if isinstance(item, dict) and item.get("value"):
                        emails_list.append(item)
                    elif isinstance(item, str) and item.strip():
                        emails_list.append({"value": item.strip(), "primary": len(emails_list) == 0})
            
            if emails_list:
                payload["email"] = emails_list
            else:
                payload.pop("email", None)

        # 4. Remover campos None ou vazios ""
        payload = {k: v for k, v in payload.items() if v not in (None, "")}

        # 5. Log antes do PUT
        print("[DEBUG_PERSON_PAYLOAD]", payload)

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
        phone_terms = self._phone_search_terms(phone)
        expected_phones = set(phone_terms)
        for term in [*phone_terms, str(email or "").strip().lower()]:
            if not term:
                continue
            fallback_person = {}
            for item in self.find_person_by_term(term):
                person = dict(item.get("item") or {})
                if not person:
                    continue
                if org_id and self._extract_id(person.get("org_id")) not in {0, int(org_id)}:
                    continue
                if expected_phones and self._person_phone_tokens(person) & expected_phones:
                    return person
                if not fallback_person:
                    fallback_person = person
            if fallback_person:
                return fallback_person
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
        if person_id and int(person_id) > 0:
            response = self._request("GET", f"persons/{int(person_id)}/deals", params={"status": "open"})
            return [self._decorate_deal_status(dict(item or {})) for item in list(response.get("data") or [])] if response else []

        results: List[Dict[str, Any]] = []
        seen_ids = set()
        for deal in self.get_deals(status="open", limit=500):
            current_person_id = self._extract_id(deal.get("person_id"))
            current_org_id = self._extract_id(deal.get("org_id"))
            if person_id and current_person_id == int(person_id):
                deal_id = self._extract_id(deal.get("id"))
                if deal_id and deal_id not in seen_ids:
                    seen_ids.add(deal_id)
                    results.append(self._decorate_deal_status(deal))
                continue
            if org_id and current_org_id == int(org_id):
                deal_id = self._extract_id(deal.get("id"))
                if deal_id and deal_id not in seen_ids:
                    seen_ids.add(deal_id)
                    results.append(self._decorate_deal_status(deal))
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
        
        if self.is_deal_locked(entity_id):
            self.logger.warning(f"[ADD_TAG_ABORTADO] Deal {entity_id} está travado (Tag 168).")
            return False

        self.ensure_deal_labels([str(tag or "").strip()])
        
        try:
            deal = self.get_deal_details(entity_id)
            if not deal:
                self.logger.error(f"[ADD_TAG_FALHA] Não foi possível obter detalhes do deal {entity_id}")
                return False
                
            current_labels = deal.get("label") or []
            if isinstance(current_labels, str):
                current_labels = current_labels.split(",")
            elif not isinstance(current_labels, list):
                current_labels = [current_labels]

            normalized_labels = []
            normalized_seen = set()
            for item in list(current_labels or []):
                if isinstance(item, dict):
                    candidate = item.get("name") or item.get("label") or item.get("value") or item.get("id")
                else:
                    candidate = item
                clean = str(candidate or "").strip()
                token = clean.upper()
                if not clean or token in normalized_seen:
                    continue
                normalized_seen.add(token)
                normalized_labels.append(clean)

            target_tag = str(tag or "").strip()
            if target_tag and target_tag.upper() not in normalized_seen:
                normalized_labels.append(target_tag)
            
            ok = self.update_deal(int(entity_id), {"label": normalized_labels})
            if not ok:
                self.logger.error(f"[ADD_TAG_FALHA_UPDATE] deal_id={entity_id} tag={tag}")
            return ok
        except Exception as e:
            self.logger.error(f"[ERRO_ADD_TAG] deal_id={entity_id} erro={e}")
            # Tenta fallback de overwrite apenas com a tag nova se falhar a mesclagem
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

    def update_activity(self, activity_id: int, **fields) -> bool:
        target_id = int(activity_id or 0)
        payload = {k: v for k, v in dict(fields or {}).items() if v is not None}
        if target_id <= 0 or not payload:
            return False
        response = self._request("PUT", f"activities/{target_id}", json=payload)
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
        fields = self.get_deal_fields()
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

    def get_person_text_field(self, field_key: str) -> Dict[str, Any]:
        target_key = str(field_key or "").strip()
        if not target_key:
            return {}
        response = self._request("GET", "personFields")
        fields = list(response.get("data") or []) if response else []
        for field in fields:
            if str(field.get("key", "")).strip() == target_key:
                return dict(field or {})
        return {}

    def add_person_text_tag_incremental(self, person_id: int, field_key: str, new_tag: str) -> bool:
        target_person_id = int(person_id or 0)
        target_field_key = str(field_key or "").strip()
        normalized_tag = str(new_tag or "").strip()
        if target_person_id <= 0 or not target_field_key or not normalized_tag:
            return False
        if target_field_key == "tags_sdr":
            return self.add_person_label_incremental(target_person_id, normalized_tag)
        try:
            person = self.get_person(target_person_id)
        except Exception:
            person = {}
        if not person and self._last_call_was_transient(f"persons/{target_person_id}"):
            self.logger.warning(
                f"[PIPEDRIVE_PERSON_TAG_SKIP] person_id={target_person_id} field={target_field_key} "
                f"tag={normalized_tag} status={int(self.last_http_status or 0)} endpoint={self.last_http_endpoint}"
            )
            return False
        current_raw = str((person or {}).get(target_field_key) or "").strip()
        tokens = [item.strip() for item in re.split(r"[,\n;|]+", current_raw) if item.strip()]
        normalized_tokens = {item.lower(): item for item in tokens}
        if normalized_tag.lower() not in normalized_tokens:
            tokens.append(normalized_tag)
        merged = ", ".join(tokens)
        return self.update_person(target_person_id, {target_field_key: merged})

    def add_person_label_incremental(self, person_id: int, new_tag: str) -> bool:
        target_person_id = int(person_id or 0)
        normalized_tag = str(new_tag or "").strip()
        if target_person_id <= 0 or not normalized_tag:
            return False
        if not self.ensure_person_labels([normalized_tag]):
            return False
        try:
            person = self.get_person(target_person_id)
        except Exception:
            person = {}
        if not person and self._last_call_was_transient(f"persons/{target_person_id}"):
            self.logger.warning(
                f"[PIPEDRIVE_PERSON_TAG_SKIP] person_id={target_person_id} tag={normalized_tag} "
                f"status={int(self.last_http_status or 0)} endpoint={self.last_http_endpoint}"
            )
            return False
        current_labels = self.resolve_label_ids(
            (person or {}).get(self.PERSON_TAGS_FIELD) or (person or {}).get("label"),
            self.get_person_labels(),
        )
        target_labels = self.resolve_label_ids([normalized_tag], self.get_person_labels())
        merged = sorted({*current_labels, *target_labels})
        if not merged:
            return False
        return self.update_person(target_person_id, {self.PERSON_TAGS_FIELD: merged})

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
        fields = self.get_deal_fields()
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
