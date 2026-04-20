@echo off
setlocal

cd /d "%~dp0"

python audit_runner.py

echo.
echo Reporte generado en:
echo reports\system_audit_report.md
echo.

pause
