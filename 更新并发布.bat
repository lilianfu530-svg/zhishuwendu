@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
set "PYTHONIOENCODING=utf-8"
set "PYTHONUTF8=1"

set "HTTP_PROXY="
set "HTTPS_PROXY="
set "ALL_PROXY="
set "NO_PROXY="

echo ================================
echo Index temperature update
echo ================================

if not exist ".venv\Scripts\python.exe" (
    echo Python environment is missing: .venv\Scripts\python.exe
    goto failed
)

echo [1/2] Updating data and HTML
".venv\Scripts\python.exe" "scripts\main.py"
if errorlevel 1 goto failed

echo [2/2] Publishing to GitHub
".venv\Scripts\python.exe" "scripts\publish_update.py"
if errorlevel 3 goto failed
if errorlevel 2 goto pending
if errorlevel 1 (
    echo Git push failed. Local data and HTML remain available.
    echo %date% %time% Git push failed>>"logs\update.log"
    goto failed
)
echo %date% %time% Git push succeeded>>"logs\update.log"
echo GitHub is up to date. GitHub Pages will publish automatically:
echo https://lilianfu530-svg.github.io/zhishuwendu/
if /I not "%~1"=="--scheduled" pause
exit /b 0

:pending
echo Page published; source data is pending. The scheduled task will retry.
echo %date% %time% Published with pending source data>>"logs\update.log"
if /I not "%~1"=="--scheduled" pause
exit /b 2

:failed
echo Update or publishing failed. Check logs\update.log and the error above.
if /I not "%~1"=="--scheduled" pause
exit /b 1
