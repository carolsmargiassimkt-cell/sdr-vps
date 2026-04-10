from __future__ import annotations

import os
from pathlib import Path
from threading import Lock
from typing import Any, Dict

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_FILE = PROJECT_ROOT / "config" / "config.json"
RUNTIME_DIR = PROJECT_ROOT / "runtime"
DEFAULTS: Dict[str, Any] = {
    "api4com_base_url": "https://api4com.com.br/api/v1",
    "pipedrive_base_url": "https://api.pipedrive.com/v1",
    "voice_provider": "openvoice",
    "voice_reference_path": "audio/voice_reference.ogg",
    "request_timeout_sec": 15,
    "retry_attempts": 3,
    "retry_backoff_sec": 1.2,
    "target_latency_ms": 1500,
    "gemini_model": "gemini-1.5-flash",
    "linkedin_edge_debug_port": 9223,
    "whatsapp_edge_debug_port": 9222,
}
ENV_ALIASES = {
    "api4com_token": "API4COM_TOKEN",
    "pipedrive_token": "PIPEDRIVE_TOKEN",
    "pipedrive_api_token": "PIPEDRIVE_API_TOKEN",
    "gemini_api_key": "GEMINI_API_KEY",
    "gemini_api_key_primary": "GEMINI_API_KEY_PRIMARY",
    "gemini_api_key_fallback": "GEMINI_API_KEY_FALLBACK",
    "openrouter_api_key": "OPENROUTER_API_KEY",
    "casa_dos_dados_api_key": "CASA_DOS_DADOS_API_KEY",
    "smtp_password": "SMTP_PASSWORD",
    "master_sheet_url": "MASTER_SHEET_URL",
    "voice_provider": "VOICE_PROVIDER",
    "pipedrive_base_url": "PIPEDRIVE_BASE_URL",
    "api4com_base_url": "API4COM_BASE_URL",
    "whatsapp_web_cdp_url": "WHATSAPP_WEB_CDP_URL",
    "whatsapp_web_debug_port": "WHATSAPP_WEB_DEBUG_PORT",
    "linkedin_edge_cdp_url": "LINKEDIN_EDGE_CDP_URL",
    "linkedin_edge_debug_port": "LINKEDIN_EDGE_DEBUG_PORT",
}
RUNTIME_KEY_OVERRIDES = {
    "pipedrive_token": "pipedrive_token",
    "pipedrive_api_token": "pipedrive_token",
    "gemini_api_key": "gemini_api_key",
    "gemini_api_key_primary": "gemini_api_key_primary",
    "gemini_api_key_fallback": "gemini_api_key_fallback",
    "openrouter_api_key": "openrouter_api_key",
    "casa_dos_dados_api_key": "casa_dos_dados_api_key",
}
_CACHE_LOCK = Lock()
_CACHE: Dict[str, Any] | None = None


def _load_env() -> None:
    if load_dotenv is not None:
        load_dotenv(PROJECT_ROOT / ".env")


def _read_config_json() -> Dict[str, Any]:
    if not CONFIG_FILE.exists():
        return {}
    try:
        import json

        payload = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_value(
    key: str, file_data: Dict[str, Any], runtime_overrides: Dict[str, str]
) -> Any:
    env_name = ENV_ALIASES.get(key, key.upper())
    runtime_key = RUNTIME_KEY_OVERRIDES.get(key)
    if runtime_key:
        override = runtime_overrides.get(runtime_key, "")
        if override:
            return override
    env_value = os.getenv(env_name, "").strip()
    if env_value:
        return env_value
    if key in file_data:
        return file_data[key]
    return DEFAULTS.get(key)


def load_config(*, force_reload: bool = False) -> Dict[str, Any]:
    from core.runtime_config import load_runtime_config
    global _CACHE
    with _CACHE_LOCK:
        if _CACHE is not None and not force_reload:
            return dict(_CACHE)
        _load_env()
        file_data = _read_config_json()
        merged = dict(DEFAULTS)
        for key, value in file_data.items():
            merged[key] = value
        runtime_overrides = load_runtime_config()
        for key in set(DEFAULTS) | set(file_data) | set(ENV_ALIASES):
            resolved = _resolve_value(key, file_data, runtime_overrides)
            if resolved is not None:
                merged[key] = resolved
        if not merged.get("gemini_api_key"):
            merged["gemini_api_key"] = merged.get("gemini_api_key_primary") or merged.get("gemini_api_key_fallback") or ""
        _CACHE = merged
        return dict(_CACHE)


def get_config_value(key: str, default: Any = None) -> Any:
    payload = load_config()
    value = payload.get(key, default)
    return default if value is None else value


def get_env(key: str, default: str = "") -> str:
    value = get_config_value(key, default)
    return str(default if value is None else value).strip()


def get_runtime_dir() -> Path:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    return RUNTIME_DIR


def get_browser_profile_dir(name: str) -> Path:
    profile_dir = get_runtime_dir() / "browser_profiles" / str(name or "default").strip().lower()
    profile_dir.mkdir(parents=True, exist_ok=True)
    return profile_dir


def get_database_path() -> Path:
    db_path = PROJECT_ROOT / "data" / "system.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return db_path
