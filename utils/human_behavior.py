from __future__ import annotations

import random
import time
from typing import Callable


def _default_log(message: str) -> None:
    print(message)


def choose_human_delay() -> float:
    bucket = random.random()
    if bucket < 0.70:
        return random.uniform(2.0, 5.0)
    if bucket < 0.90:
        return random.uniform(5.0, 10.0)
    return random.uniform(10.0, 20.0)


def delay_humano(min_s: float = 2.0, max_s: float = 6.0, *, logger: Callable[[str], None] | None = None) -> float:
    log = logger or _default_log
    if float(min_s) == 2.0 and float(max_s) == 6.0:
        tempo = choose_human_delay()
    else:
        tempo = random.uniform(float(min_s), float(max_s))
    log(f"[DELAY] aguardando {tempo:.2f}s")
    time.sleep(tempo)
    return tempo


def pausa_seguranca(*, logger: Callable[[str], None] | None = None) -> float:
    log = logger or _default_log
    tempo = random.uniform(60.0, 180.0)
    log("[ANTI-BAN] pausa de segurança ativada")
    log(f"[DELAY] aguardando {tempo:.2f}s")
    time.sleep(tempo)
    return tempo


def digitar_como_humano(elemento, mensagem: str, *, logger: Callable[[str], None] | None = None) -> None:
    log = logger or _default_log
    texto = str(mensagem or "")
    if not texto:
        return
    log("[DIGITANDO] simulando digitação humana")
    for char in texto:
        if char == "\n":
            try:
                elemento.press("Shift+Enter")
            except Exception:
                elemento.type("\n")
        else:
            try:
                elemento.type(char)
            except Exception:
                elemento.press_sequentially(char)
        time.sleep(random.uniform(0.03, 0.12))
