
import uvicorn
from app.main import app
import os
import sys

if __name__ == "__main__":
    # Garante que o diretório raiz está no path
    sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
    print("[INFO] Iniciando API de Fila na porta 8001...")
    uvicorn.run(app, host="127.0.0.1", port=8001)
