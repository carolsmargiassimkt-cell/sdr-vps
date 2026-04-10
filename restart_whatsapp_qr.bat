@echo off
chcp 65001 > nul
cd /d C:\Users\Asus\legacy\bot_sdr_ai

echo ===== REINICIANDO WHATSAPP / QR =====
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":3000" ^| findstr "LISTENING"') do if not "%%a"=="0" (
  echo Encerrando processo da porta 3000 PID %%a
  taskkill /PID %%a /F
)

timeout /t 3 /nobreak > nul

if exist baileys.lock del /f /q baileys.lock

echo Subindo WhatsApp + Email com QR no PowerShell...
C:\Users\Asus\Python311\python.exe -u supervisor_whatsapp_email.py

pause
