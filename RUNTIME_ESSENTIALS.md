# SDR Runtime Essentials

Arquivos ativos do runtime:

- `run/start_handler.sh`
- `run/start_supervisor.sh`
- `run/start_api.sh`
- `run_api.py`
- `app/main.py`
- `inbox_handler.py`
- `supervisor.py`
- `logic/whatsapp_pitch_engine.py`
- `logic/whatsapp_conversation_memory.py`
- `services/whatsapp_service.py`
- `crm/pipedrive_client.py`
- `central_whatsapp.mjs`

Contratos externos ativos:

- `POST /inbound`: entrada oficial de WhatsApp inbound no handler
- `POST /agent/decide`: decisao segura para AgentGraph/Botpress
- `POST /email/callback`: confirmacao real de envio de email pelo n8n
- `POST /email/inbound`: commit seguro de resposta inbound por email

Regras do gatekeeper VPS:

- AgentGraph/Botpress decide, mas nao atualiza CRM diretamente.
- n8n envia email, mas o CRM so avanca depois do callback confirmado.
- Inbound terminal limpa fila/pendencia de email e trava novas cadencias.
- Tags canonicas: `WHATSAPP_CAD1..6` e `EMAIL_CAD1..6`.
