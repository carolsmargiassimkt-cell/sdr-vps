import atexit
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parent
AUTH_DIR = ROOT / "auth_info_baileys"
LOGS_DIR = ROOT / "logs"
PYTHON_EXE = Path(r"C:\Users\Asus\Python311\python.exe")
N8N_START_BAT = Path(r"C:\Users\Asus\start_n8n.bat")
WHATSAPP_STATUS_URL = "http://127.0.0.1:3000/status"
N8N_STATUS_URL = "http://127.0.0.1:5678"
QR_IMAGE_URL = "http://127.0.0.1:3000/qr-image"
QR_JSON_URL = "http://127.0.0.1:3000/qr"
STARTED_PROCS = []
LAST_PRINTED_QR = ""


def http_get_json(url, timeout=3):
    try:
        with urlopen(url, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="ignore")
            if not body:
                return {}
            return json.loads(body)
    except Exception:
        return None


def http_ok(url, timeout=3):
    try:
        with urlopen(url, timeout=timeout) as response:
            return 200 <= int(response.status or 0) < 400
    except (OSError, URLError):
        return False


def wait_for_http(url, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if http_ok(url):
            return True
        time.sleep(2)
    return False


def wait_for_whatsapp_ready(timeout=60):
    deadline = time.time() + timeout
    last_status = {}
    while time.time() < deadline:
        status = http_get_json(WHATSAPP_STATUS_URL) or {}
        last_status = status
        if status.get("connected") is True or status.get("needs_qr") is True or status.get("qr_available") is True:
            return status
        time.sleep(2)
    return last_status


def render_terminal_qr(qr_text):
    if not qr_text:
        return False
    script = r"""
const QRCode = require('qrcode-terminal/vendor/QRCode')
const qr = new QRCode(-1, require('qrcode-terminal').error)
qr.addData(process.argv[1])
qr.make()
const count = qr.getModuleCount()
const quiet = 4
const size = count + quiet * 2
for (let row = 0; row < size; row += 1) {
  let line = ''
  for (let col = 0; col < size; col += 1) {
    const r = row - quiet
    const c = col - quiet
    const dark = r >= 0 && r < count && c >= 0 && c < count && qr.modules?.[r]?.[c]
    line += dark ? '\x1b[40m  ' : '\x1b[47m  '
  }
  console.log(line + '\x1b[0m')
}
"""
    try:
        result = subprocess.run(
            [
                "node",
                "-e",
                script,
                str(qr_text),
            ],
            cwd=str(ROOT),
            text=True,
            capture_output=True,
            timeout=15,
        )
    except Exception as exc:
        print(f"[QR_TERMINAL_ERRO] {exc}")
        return False
    if result.returncode != 0:
        print(f"[QR_TERMINAL_ERRO] {result.stderr.strip()}")
        return False
    print("[QR_CODE_ASCII_INICIO]")
    print(result.stdout.rstrip())
    print("[QR_CODE_ASCII_FIM]")
    return True


def open_qr_in_browser():
    try:
        if os.name == "nt":
            subprocess.Popen(["cmd", "/c", "start", "", QR_IMAGE_URL])
        else:
            subprocess.Popen(["xdg-open", QR_IMAGE_URL])
        print(f"[QR_ABERTO_NAVEGADOR] {QR_IMAGE_URL}")
    except Exception as exc:
        print(f"[QR_ABRIR_NAVEGADOR_ERRO] {exc}")


def start_process(name, command, stdout_name, stderr_name):
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    stdout = open(LOGS_DIR / stdout_name, "a", encoding="utf-8")
    stderr = open(LOGS_DIR / stderr_name, "a", encoding="utf-8")
    print(f"[SUBINDO_{name}] {' '.join(map(str, command))}")
    proc = subprocess.Popen(
        [str(item) for item in command],
        cwd=str(ROOT),
        stdout=stdout,
        stderr=stderr,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    STARTED_PROCS.append(proc)
    return proc


def cleanup():
    for proc in STARTED_PROCS:
        try:
            if proc.poll() is None:
                proc.terminate()
        except Exception:
            pass


def ensure_whatsapp():
    global LAST_PRINTED_QR
    AUTH_DIR.mkdir(parents=True, exist_ok=True)
    if http_get_json(WHATSAPP_STATUS_URL):
        print("[WHATSAPP_JA_ATIVO]")
    else:
        start_process(
            "WHATSAPP",
            ["node", "central_whatsapp.mjs"],
            "whatsapp_email_supervisor.whatsapp.out.log",
            "whatsapp_email_supervisor.whatsapp.err.log",
        )
        if not wait_for_http(WHATSAPP_STATUS_URL, timeout=120):
            print("[WHATSAPP_OFFLINE] falha ao iniciar API na porta 3000")
            return

    status = wait_for_whatsapp_ready(timeout=60)
    if status.get("connected") is True:
        print("[WHATSAPP_ONLINE]")
        return

    print(f"[WHATSAPP_STATUS] {json.dumps(status, ensure_ascii=False)}")
    if status.get("needs_qr") or status.get("qr_available"):
        qr_payload = http_get_json(QR_JSON_URL) or {}
        LAST_PRINTED_QR = str(qr_payload.get("qr") or "")
        terminal_width = shutil.get_terminal_size((120, 30)).columns
        if terminal_width < 130:
            print(f"[AVISO_TERMINAL_ESTREITO] largura={terminal_width}; QR pode quebrar no terminal.")
        print("[QR_POWER_SHELL] Escaneie o QR abaixo:")
        if not render_terminal_qr(LAST_PRINTED_QR):
            print("[QR_TERMINAL_INDISPONIVEL]")
        print(f"[QR_NECESSARIO] Se preferir, abra no navegador: {QR_IMAGE_URL}")
        open_qr_in_browser()
        print(f"[SESSAO] Depois de escanear, o Baileys salva em: {AUTH_DIR}")


def ensure_email():
    if http_ok(N8N_STATUS_URL):
        print("[EMAIL_N8N_JA_ATIVO]")
        return
    if not N8N_START_BAT.exists():
        print(f"[EMAIL_N8N_STARTER_AUSENTE] {N8N_START_BAT}")
        return
    start_process(
        "EMAIL_N8N",
        ["cmd", "/c", str(N8N_START_BAT)],
        "whatsapp_email_supervisor.n8n.out.log",
        "whatsapp_email_supervisor.n8n.err.log",
    )
    if wait_for_http(N8N_STATUS_URL, timeout=120):
        print("[EMAIL_N8N_ONLINE]")
    else:
        print("[EMAIL_N8N_OFFLINE] confira logs/whatsapp_email_supervisor.n8n.err.log")


def main():
    global LAST_PRINTED_QR
    atexit.register(cleanup)
    ensure_whatsapp()
    ensure_email()
    print("[SUPERVISOR_WHATSAPP_EMAIL_ATIVO] Ctrl+C para encerrar os processos iniciados por este supervisor.")
    while True:
        time.sleep(30)
        wa_status = http_get_json(WHATSAPP_STATUS_URL) or {}
        n8n_status = "online" if http_ok(N8N_STATUS_URL) else "offline"
        print(
            "[HEALTH]",
            f"whatsapp_connected={wa_status.get('connected') is True}",
            f"needs_qr={wa_status.get('needs_qr') is True}",
            f"email_n8n={n8n_status}",
        )
        if wa_status.get("needs_qr") is True or wa_status.get("qr_available") is True:
            qr_payload = http_get_json(QR_JSON_URL) or {}
            qr_text = str(qr_payload.get("qr") or "")
            if qr_text and qr_text != LAST_PRINTED_QR:
                LAST_PRINTED_QR = qr_text
                print("[QR_ATUALIZADO_POWER_SHELL]")
                print("[QR_POWER_SHELL] Escaneie o QR abaixo:")
                if not render_terminal_qr(qr_text):
                    print("[QR_TERMINAL_INDISPONIVEL]")
                print(f"[QR_NECESSARIO] Se preferir, abra no navegador: {QR_IMAGE_URL}")
            elif qr_text:
                print("[QR_AINDA_AGUARDANDO_SCAN] se o WhatsApp recusou, reinicie para gerar QR novo.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("[SUPERVISOR_ENCERRANDO]")
        sys.exit(0)
