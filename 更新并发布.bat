@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

echo ================================
echo 指数温度计自动更新
echo ================================

if not exist ".venv\Scripts\python.exe" (
    echo 未找到项目 Python 环境：.venv\Scripts\python.exe
    goto failed
)

echo [1/2] 更新数据、指标和 HTML
".venv\Scripts\python.exe" "scripts\main.py"
if errorlevel 1 goto failed

echo [2/2] 上传 GitHub
git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
    echo 尚未配置 Git 仓库，已完成本地更新。
    goto failed
)
git add -- index.html
if errorlevel 1 goto failed
git diff --cached --quiet -- index.html
if errorlevel 1 (
    git commit -m "Auto update index data"
    if errorlevel 1 goto failed
) else (
    echo 页面无变化，无需新增提交。
)
git push origin main
if errorlevel 1 (
    echo Git Push 失败，本地数据和页面已保留。
    goto failed
)
echo 已推送 GitHub，Cloudflare Pages 将自动部署。
pause
exit /b 0

:failed
echo 更新或发布失败，请查看 logs\update.log 和上方错误。
pause
exit /b 1
