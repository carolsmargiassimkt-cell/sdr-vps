@echo off
cd /d C:\Users\Asus\legacy\bot_sdr_ai

echo ===== INICIANDO STACK SDR =====
C:\Users\Asus\Python311\python.exe -u supervisor.py --all

pause
