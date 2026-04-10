@echo off
cd /d C:\Users\Asus\legacy\bot_sdr_ai
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\Users\Asus\legacy\bot_sdr_ai\kill_port_8001.ps1"
C:\Users\Asus\Python311\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8001
