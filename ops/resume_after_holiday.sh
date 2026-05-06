#!/usr/bin/env bash
set -euo pipefail

export PM2_HOME=/root/.pm2
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

cd /root/sdr-vps

/root/sdr-vps/venv/bin/python3 /root/sdr-vps/ops/freeze_automation.py disable --reason scheduled_resume_2026_04_22
/root/sdr-vps/venv/bin/python3 /root/sdr-vps/ops/quarantine_inbound_queue.py
/bin/bash /root/sdr-vps/ops/start_sdr_bot.sh
/bin/bash /root/sdr-vps/ops/start_pipeline2_nightly_writeback.sh
