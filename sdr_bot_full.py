import requests
import time
import os
import sys
from services.whatsapp_service import WhatsAppService

print("[SDR RODANDO CONTINUO - VERSÃO SEGURA]")

LOCK_FILE = "sdr_process.lock"
whatsapp = WhatsAppService()

def check_lock():
    """Lock resiliente: expira após 10 minutos se o processo morrer."""
    if os.path.exists(LOCK_FILE):
        try:
            file_age = time.time() - os.path.getmtime(LOCK_FILE)
            if file_age < 600: # 10 minutos
                print("[ERRO] Outro loop SDR ativo recentemente. Abortando.")
                sys.exit(0)
            else:
                print("[AVISO] Lock antigo detectado (>10min). Sobrescrevendo...")
        except: pass
    
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))

def release_lock():
    if os.path.exists(LOCK_FILE):
        try: os.remove(LOCK_FILE)
        except: pass

def enviar(numero):
    if not whatsapp.can_send(numero):
        print("[JA_ENVIADO_HOJE]", numero)
        return

    try:
        r = requests.post(
            "http://127.0.0.1:3000/send",
            json={
                "number": numero,
                "text": "Oi, tudo bem? Vi seu perfil e queria falar rapidinho"
            },
            timeout=60
        )
        if r.status_code == 200:
            print("[ENVIO_OK]", numero)
            whatsapp.mark_sent(numero)
        else:
            print("[FALHA_ENVIO]", numero, r.text)
    except Exception as e:
        print("[ERRO]", numero, str(e))


def pegar_lista():
    # 👉 lista de contingência
    return []


while True:
    check_lock()
    try:
        numeros = pegar_lista()
        if not numeros:
            print("[INFO] Lista vazia no bot_full")
            time.sleep(60)
            continue

        print("TOTAL:", len(numeros))

        for i, n in enumerate(numeros):
            print(f"[{i+1}/{len(numeros)}]", n)
            enviar(n)
            time.sleep(30)
    finally:
        release_lock()

    print("CICLO FINALIZADO - REINICIANDO EM 60s")
    time.sleep(60)
