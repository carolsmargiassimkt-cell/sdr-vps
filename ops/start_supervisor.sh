#!/usr/bin/env bash
set -euo pipefail
echo "[LEGACY_SUPERVISOR_DISABLED] Supervisor antigo nao deve ser iniciado."
echo "Use: pm2 startOrRestart /root/sdr-vps/ecosystem.vps.config.js --only api,handler,whatsapp"
exit 0
