from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_CONFIG_PATH = PROJECT_ROOT / "config" / "runtime_config.json"

DEFAULT_RUNTIME_KEYS = {
    "pipedrive_token": "",
    "gemini_api_key_primary": "",
    "gemini_api_key_fallback": "",
    "casa_dos_dados_api_key": "",
    "gemini_api_key": "",
}


def _read_runtime_file() -> Dict[str, str]:
    if not RUNTIME_CONFIG_PATH.exists():
        return {}
    try:
        payload = json.loads(RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_runtime_config() -> Dict[str, str]:
    data = _read_runtime_file()
    runtime_values: Dict[str, str] = {}
    for key, default in DEFAULT_RUNTIME_KEYS.items():
        runtime_values[key] = str(data.get(key, default) or "").strip()
    if not runtime_values["gemini_api_key"]:
        runtime_values["gemini_api_key"] = runtime_values["gemini_api_key_primary"] or runtime_values["gemini_api_key_fallback"]
    return runtime_values


def save_runtime_config(values: Dict[str, str]) -> None:
    existing = _read_runtime_file()
    merged = {key: str(existing.get(key, default) or default).strip() for key, default in DEFAULT_RUNTIME_KEYS.items()}
    for key, default in DEFAULT_RUNTIME_KEYS.items():
        if key in values:
            merged[key] = str(values.get(key, "") or "").strip()
    merged["gemini_api_key"] = merged["gemini_api_key_primary"] or merged["gemini_api_key_fallback"] or merged.get("gemini_api_key", "")
    RUNTIME_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_CONFIG_PATH.write_text(json.dumps(merged, indent=2), encoding="utf-8")


def get_effective_gemini_key(runtime_values: Dict[str, str]) -> str:
    primary = runtime_values.get("gemini_api_key_primary", "")
    fallback = runtime_values.get("gemini_api_key_fallback", "")
    legacy = runtime_values.get("gemini_api_key", "")
    return primary or legacy or fallback
