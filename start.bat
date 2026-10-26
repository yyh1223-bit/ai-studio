@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo [AI 数字工作室] 正在启动...
python -m uvicorn app:app --host 127.0.0.1 --port 8600
pause