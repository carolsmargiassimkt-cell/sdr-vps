# MAND Central Unified Audit

Data da auditoria: 2026-05-14

## 1. Estrutura encontrada

- `crm-frontend/`: Vite + React + TypeScript + Supabase.
- `crm-frontend/src/modules/crm/services/api.ts`: servico principal do CRM para stages, deals, contacts, activities, profiles, integrations e interactions.
- `crm-frontend/src/modules/crm/pages/DealsListPage.tsx`: lista de deals/leads com filtros locais.
- `crm-frontend/src/modules/crm/pages/PipelinePage.tsx`: kanban por stage.
- `crm-frontend/src/modules/crm/layouts/CrmLayout.tsx`: layout com guarda de autenticacao.
- `crm-frontend/public/`: contem `favicon.ico`, `placeholder.svg`, `robots.txt`; nao contem `data/leads.json`.
- `sdr-backend/`: snapshot parcial do SDR com `inbox_handler.py`, `supervisor.py`, scripts Waalaxy, webhook Pipedrive e estado WA.

## 2. Problemas encontrados

### P0 critico

- `public/data/leads.json` nao existe.
- `data/leads.json` tambem nao existe.
- O CRM nao possuia fallback local antes do patch; `dealsApi.list()` lia somente `supabase.from("crm_deals")`.
- Sem usuario Supabase, `CrmLayout` redirecionava para `/crm/auth` antes de renderizar as telas do CRM.
- Os 1128 leads nao estao materializados neste repositorio. Foram encontrados apenas:
  - `sdr-backend/waalaxy_writeback_plan.json`: 657 entradas.
  - `sdr-backend/waalaxy_reconcile_report.json`: 690 entradas.
  - `sdr-backend/waalaxy_writeback_score50_preview.csv`: 167 entradas.
- O SDR unificado esta incompleto para execucao local: imports referenciam modulos ausentes nesta pasta (`crm.*`, `logic.*`, `services.*`, `core.stage_router`, `core.sdr_state`, `core.agent_router`).
- `python` e `py` nao estao disponiveis no ambiente, entao nao foi possivel rodar `compileall`.

### P1 importante

- O arquivo esperado pelo pedido (`src/services/dealsApi.ts`) nao existe; o equivalente real e `src/modules/crm/services/api.ts`.
- Scripts Waalaxy usam caminhos absolutos externos:
  - `C:\Users\Asus\Downloads`
  - `C:\Users\Asus\Downloads\deals-25681240-26.csv`
- Rotinas operacionais do SDR usam caminhos de VPS:
  - `/root/sdr-vps/.env`
  - `/root/sdr-vps/data/...`
  - `/root/sdr-vps/logs/...`
- `WhatsAppPage.tsx` exibe QR Code como placeholder; nao consome endpoint real de status/QR/envio.
- `SettingsPage.tsx` lista integracoes, mas nao ha fluxo seguro completo para salvar API keys mascaradas.
- `npm install` reportou 19 vulnerabilidades: 3 low, 7 moderate, 9 high. Nao foi aplicado `npm audit fix`.

### P2 melhorias

- Bundle de producao acima de 500 kB apos minificacao; Vite sugere code splitting/manual chunks.
- Browserslist/caniuse-lite esta 11 meses desatualizado.
- Textos do frontend aparentam problemas de encoding em varias telas.

## 3. Leads encontrados

Nao foi possivel confirmar 1128 leads neste workspace.

Planilha gerada com os leads efetivamente encontrados:

- `docs/MAND_CENTRAL_LEADS_AUDIT.csv`
- Total na planilha: 657 leads, derivados de `sdr-backend/waalaxy_writeback_plan.json`.

Campos exportados:

- `id`
- `name`
- `company`
- `title`
- `status`
- `stage`
- `email`
- `phone`
- `linkedin`
- `source_file`
- `action`
- `confidence`
- `tipo`
- `waalaxy_score`

Validacao dos campos no plano Waalaxy:

- Possui nome/titulo: sim, via `name` e `title`.
- Possui stage/status CRM final: nao; o arquivo e um plano de writeback, nao uma fonte ja importada no CRM. Foram preenchidos `status=pending_import` e `stage=action` na planilha para auditoria.
- Telefone/email: existem campos `phone` e `email`, mas muitos registros estao vazios.
- `id`: o plano nao possui `id` CRM nativo; a planilha usa `linkedin` quando disponivel ou `waalaxy-N` como identificador de auditoria.

## 4. Origem dos leads

Origem real encontrada:

- `sdr-backend/waalaxy_writeback_plan.json`, gerado por `sdr-backend/scripts/build_waalaxy_writeback_plan.py`.
- O script busca CSVs em `C:\Users\Asus\Downloads` e compara contra export do Pipedrive em `C:\Users\Asus\Downloads\deals-25681240-26.csv`.

Origem nao encontrada:

- `crm-frontend/public/data/leads.json`
- `crm-frontend/data/leads.json`
- `crm-frontend/src/services/dealsApi.ts`

## 5. Status da renderizacao

Antes do patch:

- `DealsListPage.tsx` renderizava apenas o retorno de `dealsApi.list()`.
- `dealsApi.list()` dependia exclusivamente de `crm_deals` no Supabase.
- Filtros default (`q=""`, `stageId="all"`, `temp="all"`) nao escondiam leads por si so.
- A autenticacao podia impedir renderizacao sem usuario.

Depois do patch seguro:

- `dealsApi.list()` registra:
  - `[LEADS_LOAD_START]`
  - `[LEADS_LOAD_SUCCESS]`
  - `[LEADS_LOAD_EMPTY]`
  - `[LEADS_LOAD_ERROR]`
- `DealsListPage.tsx` e `PipelinePage.tsx` registram `[LEADS_RENDER_TOTAL]`.
- Se Supabase falhar ou vier vazio em modo local, o CRM tenta carregar `/data/leads.json`.
- Modo local fica habilitado em `localhost`, `127.0.0.1` ou quando `VITE_CRM_LOCAL_MODE=true`.
- Nao foi criado `public/data/leads.json`, porque os 1128 leads prometidos nao existem no workspace e criar dados incompletos como fonte oficial mascararia o problema.

## 6. Status do build

Comandos executados:

- `npm install`: passou fora do sandbox apos falha inicial por permissao no cache `AppData`.
- `npm run build`: passou antes e depois do patch.

Build final:

- Sucesso.
- Chunk JS final reportado: aproximadamente 1.178 MB minificado, 340 kB gzip.
- Alertas: chunk grande e Browserslist desatualizado.

## 7. Status do SDR

### FastAPI / Flask

- `webhooks_pipedrive.py` usa FastAPI `APIRouter` e expõe `POST /webhooks/pipedrive`, mas apenas retorna disabled:
  - `skip=root_webhook_disabled_use_app_webhooks_pipedrive`
- `inbox_handler.py` usa Flask, nao FastAPI, e define a app principal `app = Flask(__name__)`.

### Endpoints encontrados

Em `sdr-backend/inbox_handler.py`:

- `GET /`
- `POST /agent/decide`
- `POST /agent/decide/botpress`
- `POST /email/callback`
- `POST /email/confirm`
- `POST /email/inbound`
- `POST /history/outbound`
- `POST /inbound`
- `POST /inbox`

Em `sdr-backend/webhooks_pipedrive.py`:

- `POST /webhooks/pipedrive`

### Logica preservada por arquivo

- `inbox_handler.py`: inbound WhatsApp/email, agent decision, Botpress fallback, opt-out, horario comercial, historico, anti-loop, warm logic, tags/status CRM, agendamento, indicacao, cache de lead, email callback/inbound.
- `supervisor.py`: hunter WhatsApp com cap diario, delay, horario comercial, blocklist, warm detection, Pipedrive notes, WhatsApp gateway.
- `scripts/whatsapp_warm_cadence.py`: cadencia warm de WhatsApp com 6 passos, blocklists, horarios, labels e Pipedrive.
- `scripts/reconcile_waalaxy_from_downloads.py`: reconciliacao Waalaxy/Pipedrive.
- `scripts/build_waalaxy_writeback_plan.py`: construcao do plano de importacao.
- `scripts/apply_waalaxy_writeback.py`: aplicacao controlada do plano no Pipedrive.
- `core/wa_strategy_state.py`: estado persistente com lock file para estrategias WA.

### Arquivos/runtime/filas/blocklists/metricas/webhooks

- Runtime local citado:
  - `data/wa_strategy_state.json`
  - `data/whatsapp_conversation_state.json`
  - `data/email_pending_confirmations.json`
  - `data/email_handoff_queue.json`
  - `data/whatsapp_warm_cadence.json`
  - `logs/whatsapp_message_history.json`
  - `logs/whatsapp_manual_blocklist.json`
  - `inbound_processed.json`
- Blocklists:
  - `data/whatsapp_manual_blocklist.json`
  - `data/whatsapp_blocklist.json`
  - `logs/whatsapp_manual_blocklist.json`
  - `invalidos.json`
- Webhooks:
  - Flask inbound/inbox/email/history.
  - FastAPI Pipedrive router placeholder/disabled.
- Metricas:
  - Nao ha endpoint dedicado de metricas no snapshot.
  - Existem logs estruturados no stdout, por exemplo `[WA_HUNTER_SUMMARY]`, `[WA_WARM_CADENCE]`, `[LEADS_LOAD_*]`.

### Imports quebrados provaveis

Ausentes no workspace unificado:

- `crm.sdr_field_updater`
- `crm.pipedrive_client`
- `logic.whatsapp_pitch_engine`
- `services.whatsapp_service`
- `core.stage_router`
- `core.sdr_state`
- `core.agent_router`
- `config.config_loader` tem fallback parcial em `inbox_handler.py`, mas nao resolve os demais.

## 8. Riscos da unificacao

- O frontend e o SDR nao compartilham uma API de integracao estavel ainda.
- CRM le dados do Supabase, enquanto a massa Waalaxy/Pipedrive fica em arquivos e scripts separados.
- Parte do SDR parece ter sido copiada sem seus pacotes internos (`crm`, `logic`, `services`, parte de `core`).
- Caminhos absolutos de maquina local e VPS tornam o runtime fragil fora do ambiente original.
- O QR Code WhatsApp no CRM e apenas UI placeholder.
- API keys e configuracoes sensiveis ainda precisam de backend proprio com storage seguro; nao devem ser persistidas em client-side/localStorage.

## 9. Plano

### P0 critico

- Localizar a fonte real dos 1128 leads e versionar como `crm-frontend/public/data/leads.json` ou importar para Supabase com script auditavel.
- Completar os modulos ausentes do SDR ou ajustar empacotamento para preservar imports originais.
- Definir uma API CRM backend propria como camada unica entre frontend, SDR e provedores externos.
- Validar renderizacao dos 1128 via `/crm/deals` e `/crm/pipeline` apos a fonte existir.

### P1 importante

- Criar endpoint `GET /crm/leads` com fallback controlado para arquivo local e/ou Supabase.
- Criar endpoint `GET /crm/status` verificando CRM DB, SDR, WhatsApp gateway, Pipedrive e API4Com.
- Substituir caminhos absolutos por env vars documentadas.
- Adicionar testes pequenos para normalizacao de leads e fallback local.
- Adicionar metricas operacionais para WhatsApp, email, ligacoes e warm leads.

### P2 melhorias

- Code split no frontend.
- Corrigir encoding dos textos.
- Atualizar Browserslist.
- Rodar `npm audit` e corrigir vulnerabilidades com avaliacao de risco.

## 10. Arquitetura segura proposta

### Camadas

- CRM Frontend: somente UI, sem chaves sensiveis.
- API CRM propria: autentica usuario, aplica RBAC, agrega dados locais/Supabase/SDR.
- SDR Backend: permanece dono da logica de cadencia, inbound, warm leads e Pipedrive.
- Provedores: WhatsApp/Baileys, Email, API4Com, Pipedrive e Gemini acessados somente pelo backend.

### Endpoints ideais

- `GET /crm/status`: status agregado da API, DB, SDR, WhatsApp, Email, API4Com, Pipedrive.
- `GET /crm/metrics`: funil, mensagens, ligacoes, emails, warm leads, erros.
- `GET /crm/leads`: leads normalizados com paginacao/filtros.
- `GET /crm/deals`: deals CRM/Pipedrive normalizados.
- `GET /crm/whatsapp/status`: sessao, numero conectado, uptime, ultimo erro.
- `GET /crm/whatsapp/qr`: QR atual, TTL e status.
- `POST /crm/whatsapp/send`: envio com auditoria, rate limit, opt-out e blocklist.
- `POST /crm/bot/test`: playground com payload controlado, sem escrever no Pipedrive por default.
- `POST /crm/settings/api-keys`: salva segredos no backend/secret store, retorna sempre mascarado.

### Contact center

Viavel, mas precisa backend. Requisitos:

- QRCode WhatsApp: expor estado do Baileys via API, nunca direto do browser.
- API4Com: integrar no backend para chamadas, eventos e metricas.
- Metricas de ligacao: guardar chamadas, duracao, status, recording URL se permitido.
- Metricas WhatsApp: enviado, entregue, lido, respondido, opt-out, erro.
- Metricas email: enviado, aberto, clique, resposta, bounce, unsubscribe.
- Playground bot: modo dry-run default, fixtures e trilha de auditoria.
- Gemini API key: salvar em secret store ou env backend; frontend ve apenas `****abcd`.
- Configs seguras: RBAC, auditoria, rotacao de chaves, validacao de origem e rate limiting.

## 11. Patches aplicados

- `crm-frontend/src/modules/crm/services/localLeads.ts`: novo loader local seguro para `/data/leads.json`.
- `crm-frontend/src/modules/crm/services/api.ts`: logs de carregamento e fallback local para deals/stages.
- `crm-frontend/src/modules/crm/layouts/CrmLayout.tsx`: modo local sem redirecionar para auth em localhost ou `VITE_CRM_LOCAL_MODE=true`.
- `crm-frontend/src/modules/crm/pages/DealsListPage.tsx`: log `[LEADS_RENDER_TOTAL]`.
- `crm-frontend/src/modules/crm/pages/PipelinePage.tsx`: log `[LEADS_RENDER_TOTAL]`.
- `docs/MAND_CENTRAL_LEADS_AUDIT.csv`: planilha com 657 leads encontrados.

Nenhuma logica critica do SDR foi removida ou alterada.
