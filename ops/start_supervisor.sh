#!/usr/bin/env bash
cd /root/sdr-vps || exit 1
python3 -m py_compile supervisor.py || exit 1
pm2 start ecosystem.vps.config.js --only supervisor
pm2 save
pm2 logs supervisor --lines 80
