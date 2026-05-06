#!/usr/bin/env bash
set -e
cd /root/sdr-vps
if [ -z "$1" ]; then
  echo "Uso: ./ops/rollback.sh COMMIT_ID"
  git log --oneline -5
  exit 1
fi
git reset --hard "$1"
python3 -m py_compile inbox_handler.py supervisor.py logic/whatsapp_pitch_engine.py crm/pipedrive_client.py
pm2 restart handler whatsapp api
pm2 stop supervisor
pm2 save
pm2 list
