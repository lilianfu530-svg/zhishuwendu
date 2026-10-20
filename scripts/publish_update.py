from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STATUS = ROOT / "logs" / "daily_update_status.json"
PUBLISHED = ROOT / "logs" / "publish_status.json"


def git(*arguments: str, remote: bool = False) -> str:
    command = ([sys.executable, str(ROOT / "proxy_env.py"), "-c", "http.lowSpeedLimit=1",
                "-c", "http.lowSpeedTime=60"] if remote else ["git"]) + list(arguments)
    environment = os.environ.copy()
    environment.update(GIT_TERMINAL_PROMPT="0", GCM_INTERACTIVE="Never")
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True,
                            encoding="utf-8", errors="replace", timeout=120, check=False, env=environment)
    if result.returncode:
        raise RuntimeError(f"git {arguments[0]} 失败，退出码 {result.returncode}")
    return result.stdout.strip()


def main() -> int:
    status = json.loads(STATUS.read_text(encoding="utf-8"))
    if status["state"] not in {"complete", "partial"}:
        raise RuntimeError("本次数据更新尚未完成，拒绝发布")
    page = ROOT / "index.html"
    page_sha256 = hashlib.sha256(page.read_bytes()).hexdigest()
    if page_sha256 != status["page_sha256"]:
        raise RuntimeError("页面与本次更新记录不一致，拒绝发布")
    if git("branch", "--show-current") != "main":
        raise RuntimeError("当前分支不是 main，拒绝自动提交")
    if git("hash-object", "index.html") != git("rev-parse", "HEAD:index.html"):
        # 只提交本次页面，保留其他文件的工作区及暂存区状态。
        git("add", "--", "index.html")
        git("commit", "--only", "-m", f"Auto update index data for {status['target_date']}", "--", "index.html")
    commit = git("rev-parse", "HEAD")
    if git("rev-parse", "HEAD:index.html") != git("hash-object", "index.html"):
        raise RuntimeError("提交中的页面与本地页面不一致")
    for attempt in range(1, 4):
        try:
            git("push", "origin", "main", remote=True)
            remote_head = git("ls-remote", "--exit-code", "origin", "refs/heads/main", remote=True).split()[0]
            if remote_head != commit:
                raise RuntimeError("远端 main 尚未指向本次提交")
            break
        except (RuntimeError, subprocess.TimeoutExpired):
            if attempt == 3:
                raise
            print(f"Git 发布或远端校验失败，第 {attempt}/3 次，稍后重试", flush=True)
            time.sleep(5 * attempt)
    published = {"target_date": status["target_date"], "commit": commit,
                 "page_sha256": page_sha256, "data_state": status["state"],
                 "verified_at": datetime.now().isoformat(timespec="seconds"),
                 "issues": status["issues"]}
    temporary = PUBLISHED.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(published, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(PUBLISHED)
    print(f"已核对 GitHub main：{commit}，数据状态 {status['state']}", flush=True)
    # 页面先发布；仍有主源延迟时向计划任务返回失败，让调度器继续补跑。
    return 0 if status["state"] == "complete" else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"发布失败：{type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(1) from exc
