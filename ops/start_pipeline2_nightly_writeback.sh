#!/usr/bin/env bash
set -euo pipefail

export PM2_HOME=/root/.pm2
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

cd /root/sdr-vps

if ! /root/sdr-vps/venv/bin/python3 /root/sdr-vps/ops/freeze_automation.py can-start --service enrichment >/dev/null 2>&1; then
  echo "[START_ABORTED_FREEZE] service=enrichment"
  exit 0
fi

/root/sdr-vps/venv/bin/python3 /root/sdr-vps/ops/wait_for_crm_and_start_pm2.py \
  --pm2-app super-minas-writeback \
  --pm2-config /root/sdr-vps/ecosystem.vps.config.js \
  --pm2-bin /usr/local/bin/pm2 \
  --poll-sec 300 \
  --deadline-hour 8 \
  --timezone America/Sao_Paulo

/root/sdr-vps/venv/bin/python3 /root/sdr-vps/ops/wait_for_crm_and_start_pm2.py \
  --pm2-app pipeline2-nightly-writeback \
  --pm2-config /root/sdr-vps/ecosystem.vps.config.js \
  --pm2-bin /usr/local/bin/pm2 \
  --poll-sec 300 \
  --deadline-hour 8 \
  --timezone America/Sao_Paulo
