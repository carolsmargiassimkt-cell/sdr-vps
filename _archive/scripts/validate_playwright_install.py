import os
import sys
from pathlib import Path

print('[PLAYWRIGHT][INFO] python=', sys.version.split()[0])

try:
    import playwright  # noqa: F401
    from playwright.sync_api import sync_playwright
except Exception as exc:
    print(f'[ERRO] [PLAYWRIGHT] import falhou: {exc}')
    raise SystemExit(1)

print('[OK] Playwright instalado')

try:
    with sync_playwright() as p:
        chromium_path = p.chromium.executable_path
    if chromium_path and Path(chromium_path).exists():
        print('[OK] Chromium disponível')
        print(f'[PLAYWRIGHT][INFO] chromium_path={chromium_path}')
        raise SystemExit(0)
    print('[ERRO] [PLAYWRIGHT] Chromium não encontrado')
    raise SystemExit(2)
except Exception as exc:
    print(f'[ERRO] [PLAYWRIGHT] engine falhou: {exc}')
    raise SystemExit(3)
