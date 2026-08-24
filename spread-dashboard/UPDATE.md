# 不再 SSH 部署：自动更新

这个版本支持 GitHub 自动部署：服务器上的 `supervisor.py` 每隔 `UPDATE_CHECK_SECONDS` 秒检查 GitHub `main` 分支。

当你让我增加功能并把新代码提交到仓库后：

1. GitHub 出现新 commit
2. 服务器自动检测到新 commit
3. 自动 `git fetch` + `git reset --hard origin/main`
4. 自动安装新的 Python 依赖
5. 自动重启 Web 程序
6. 原来的网页地址保持不变

因此以后不需要再 SSH 服务器，也不需要重新 `git clone`。

## 第一次启用

在服务器上只需要做一次：

```bash
cd /你的路径/-/spread-dashboard
source .venv/bin/activate
python supervisor.py
```

生产环境建议用 systemd 让 `supervisor.py` 常驻运行。以后只需要让我修改 GitHub 仓库代码即可。

## 重要

服务器必须能够通过 HTTPS 访问 GitHub，并且仓库的 Git remote 必须已经配置好认证（私有仓库推荐 SSH deploy key / machine user）。不要把 GitHub Token 写进网页或代码。

`.env` 不会被 Git reset 覆盖，因为它被 `.gitignore` 排除。
