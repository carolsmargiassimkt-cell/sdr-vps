#!/usr/bin/env bash
set -euo pipefail

export PM2_HOME=/root/.pm2
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

if ! /root/sdr-vps/venv/bin/python3 /root/sdr-vps/ops/freeze_automation.py can-start --service whatsapp >/dev/null 2>&1; then
  echo "[START_ABORTED_FREEZE] service=whatsapp"
  exit 0
fi

if ! /root/sdr-vps/venv/bin/python3 /root/sdr-vps/ops/freeze_automation.py can-start --service sdr-bot >/dev/null 2>&1; then
  echo "[START_ABORTED_FREEZE] service=sdr-bot"
  exit 0
fi

/usr/local/bin/pm2 startOrRestart /root/sdr-vps/ecosystem.vps.config.js --only api
/usr/local/bin/pm2 startOrRestart /root/sdr-vps/ecosystem.vps.config.js --only handler
/usr/local/bin/pm2 startOrRestart /root/sdr-vps/ecosystem.vps.config.js --only whatsapp
