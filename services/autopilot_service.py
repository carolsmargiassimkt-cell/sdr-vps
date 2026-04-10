# -*- coding: utf-8 -*-
import time
import requests
from whatsapp_bot import WhatsAppBot

class AutopilotService:

    def __init__(self, *args, **kwargs):
        self.bot = WhatsAppBot()
        self.leads = []

    def run_forever(self):
        print("[WHATSAPP_WORKER_LOOP] autopilot iniciado")

        try:
            self.leads = self.bot._fetch_super_minas_targets(limit=200)
            print(f"[AUDIT_LEADS_CARREGADOS] total={len(self.leads)}")
        except Exception as e:
            print("[ERRO_CARREGAR_LEADS]", e)
            self.leads = []

        index = 0

        while True:
            print("[LOOP_INICIO]")

            if index < len(self.leads):
                lead = self.leads[index]
                telefone = lead.get("telefone")

                if telefone:
                    print(f"[PROCESSANDO] telefone={telefone}")

                    try:
                        r = requests.post(
                            "http://127.0.0.1:3000/message/send",
                            json={
                                "number": telefone,
                                "message": "Oi, tudo bem? Aqui é da MAND Digital 👋"
                            },
                            timeout=5
                        )

                        print(f"[ENVIADO] status={r.status_code}")

                    except Exception as e:
                        print(f"[ERRO_ENVIO] {e}")

                    index += 1
                    time.sleep(12)

            else:
                print("[FIM_DA_LISTA] aguardando...")
                time.sleep(10)
