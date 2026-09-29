@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

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
git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 goto failed
git add -- index.html
if errorlevel 1 goto failed
git diff --cached --quiet -- index.html
if errorlevel 1 (
    git commit -m "Auto update index data"
    if errorlevel 1 goto failed
) else (
    echo No page changes; no new commit.
)
git push origin main
if errorlevel 1 (
    echo Git push failed. Local data and HTML remain available.
    echo %date% %time% Git push failed>>"logs\update.log"
    goto failed
)
echo %date% %time% Git push succeeded>>"logs\update.log"
echo GitHub is up to date. A connected Cloudflare Pages project will deploy automatically.
pause
exit /b 0

:failed
echo Update or publishing failed. Check logs\update.log and the error above.
pause
exit /b 1
