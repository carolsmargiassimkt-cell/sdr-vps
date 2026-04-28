#!/usr/bin/env bash
cd /root/sdr-vps || exit 1
git status --short
python3 -m py_compile inbox_handler.py supervisor.py logic/whatsapp_pitch_engine.py crm/pipedrive_client.py || exit 1
echo "OK_DEPLOY_CHECK"
