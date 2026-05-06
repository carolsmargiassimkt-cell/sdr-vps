# SDR Runtime Essentials

Arquivos ativos do runtime:

- `run/start_handler.sh`
- `run/start_supervisor.sh`
- `run/start_api.sh`
- `run_api.py`
- `app/main.py`
- `inbox_handler.py`
- `email_cadence/engine.py`
- `email_cadence/routes.py`
- `email_cadence/forms_webhook.py`
- `scripts/whatsapp_warm_cadence.py`
- `logic/whatsapp_pitch_engine.py`
- `logic/whatsapp_conversation_memory.py`
- `services/whatsapp_service.py`
- `crm/pipedrive_client.py`
- `central_whatsapp.mjs`

Contratos externos ativos:

- `POST /inbound`: entrada oficial de WhatsApp inbound no handler
- `POST /agent/decide`: decisao segura para AgentGraph/Botpress
- `POST /webhooks/email-cadence`: entrada nativa para enfileirar cadencia de email
- `GET /t/{deal_id}/{step}`: tracking nativo de clique e aquecimento
- `POST /email/inbound`: commit seguro de resposta inbound por email

Regras do gatekeeper VPS:

- AgentGraph/Botpress decide, mas nao atualiza CRM diretamente.
- Email cadence envia email nativamente via SMTP e tracking FastAPI.
- WhatsApp automatico sai apenas por `scripts/whatsapp_warm_cadence.py`.
- Inbound terminal limpa fila/pendencia de email e trava novas cadencias.
- Tags canonicas: `WHATSAPP_CAD1..6` e `EMAIL_CAD1..6`.
