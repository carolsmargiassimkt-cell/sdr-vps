@echo off
chcp 65001 > nul
cd /d C:\Users\Asus\legacy\bot_sdr_ai

if not exist auth_info_baileys mkdir auth_info_baileys
if not exist logs mkdir logs

echo ===== INICIANDO WHATSAPP + EMAIL =====
set PYTHONIOENCODING=utf-8
C:\Users\Asus\Python311\python.exe -u supervisor_whatsapp_email.py

pause
