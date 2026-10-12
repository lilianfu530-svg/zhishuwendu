"""让项目进程使用 Windows 系统代理设置。"""

import os
import subprocess
import sys
from urllib.request import getproxies


def follow_windows_system_proxy() -> None:
    # 移除启动进程继承的代理地址，交由 Windows 系统代理设置决定。
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
                 "http_proxy", "https_proxy", "all_proxy", "no_proxy"):
        os.environ.pop(name, None)


def run_git_with_windows_system_proxy(arguments: list[str]) -> int:
    # Git 不会自动读取 Windows 的用户代理设置；运行时转换为本进程的代理变量。
    follow_windows_system_proxy()
    proxies = getproxies()
    for scheme in ("http", "https"):
        address = proxies.get(scheme)
        if address:
            os.environ[f"{scheme.upper()}_PROXY"] = address
    return subprocess.run(["git", *arguments], check=False).returncode


if __name__ == "__main__":
    raise SystemExit(run_git_with_windows_system_proxy(sys.argv[1:]))
