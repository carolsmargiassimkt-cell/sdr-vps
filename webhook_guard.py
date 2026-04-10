from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json, requests, os, datetime

HOST = "127.0.0.1"
PORT = 8011

BAILEYS_SEND_URL = os.getenv("BAILEYS_SEND_URL", "http://127.0.0.1:3000/send")

def log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path.rstrip("/") != "/webhook":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"

        try:
            payload = json.loads(raw.decode("utf-8"))
        except:
            payload = {}

        log(f"?? RECEBIDO: {payload}")

        # tenta extrair numero + mensagem (compatível com Evolution/formatos comuns)
        phone = (
            payload.get("number")
            or payload.get("phone")
            or payload.get("from")
            or (payload.get("data") or {}).get("from")
        )

        message = (
            payload.get("text")
            or payload.get("message")
            or (payload.get("data") or {}).get("body")
        )

        if not phone or not message:
            log("⚠️ PAYLOAD SEM TELEFONE OU TEXTO")
        else:
            log(f"📥 CAPTURADO: {phone} | {message}")
            # webhook_guard AGORA APENAS MONITORA. 
            # A resposta real é dada pelo inbox_handler.py para evitar duplicidade.
            log("[INFO] Guard processou, resposta delegada ao inbox_handler.")

        body = json.dumps({"ok": True}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

def run():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    log(f" WEBHOOK ATIVO: http://{HOST}:{PORT}/webhook")
    server.serve_forever()

if __name__ == "__main__":
    run()
import sys
sys.stdout.reconfigure(encoding='utf-8')
