#!/usr/bin/env bash
cd /root/sdr-vps || exit 1
python3 -m py_compile inbox_handler.py supervisor.py logic/whatsapp_pitch_engine.py crm/pipedrive_client.py || exit 1
pm2 start ecosystem.vps.config.js --only api,handler,whatsapp
pm2 stop supervisor
pm2 save
pm2 list
