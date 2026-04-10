from __future__ import annotations

from typing import Any, Callable, Dict


_registry_callback: Callable[..., Dict[str, Any]] | None = None


def configure_registrar(callback: Callable[..., Dict[str, Any]]) -> None:
    global _registry_callback
    _registry_callback = callback


def registrar_atividade(lead_id: str, descricao: str, **kwargs) -> Dict[str, Any]:
    if not callable(_registry_callback):
        raise RuntimeError("registrar_atividade nao configurado")
    return _registry_callback(lead_id, descricao, **kwargs)
