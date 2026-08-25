import os
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BRANCH = os.getenv("UPDATE_BRANCH", "main")
CHECK_SECONDS = int(os.getenv("UPDATE_CHECK_SECONDS", "15"))
FLAG = ROOT / ".update-now"
PORT = int(os.getenv("PORT", "8080"))


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def remote_head():
    r = git("ls-remote", "origin", f"refs/heads/{BRANCH}")
    if r.returncode != 0 or not r.stdout.strip():
        return None
    return r.stdout.split()[0]


def local_head():
    r = git("rev-parse", "HEAD")
    return r.stdout.strip() if r.returncode == 0 else None


def port_ready(timeout=12):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", PORT), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.25)
    return False


def update():
    """Fetch a requested version, validate it, and roll back on startup failure."""
    old = local_head()
    if not old:
        print("[updater] cannot determine current commit", flush=True)
        return False

    print("[updater] updating from GitHub...", flush=True)
    r = git("fetch", "origin", BRANCH)
    if r.returncode != 0:
        print(r.stdout, flush=True)
        return False

    r = git("reset", "--hard", f"origin/{BRANCH}")
    if r.returncode != 0:
        print(r.stdout, flush=True)
        git("reset", "--hard", old)
        return False

    check = subprocess.run([sys.executable, "-m", "py_compile", "app.py", "market_engine_v2.py", "alpha_ws_patch.py", "cex_bulk_patch.py", "supervisor.py"], cwd=ROOT)
    if check.returncode != 0:
        print("[updater] syntax check failed; rolling back", flush=True)
        git("reset", "--hard", old)
        return False

    subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], cwd=ROOT, check=False)
    try:
        FLAG.unlink()
    except FileNotFoundError:
        pass
    return True


def start_app():
    return subprocess.Popen([sys.executable, "app.py"], cwd=ROOT)


def restart(child):
    child.terminate()
    try:
        child.wait(timeout=10)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=3)
    new_child = start_app()
    if port_ready(12):
        return new_child, True

    # New process did not bind the service port. Put the previous commit back
    # and start it again so a bad update cannot take the dashboard offline.
    print("[updater] new version failed health check; rolling back", flush=True)
    new_child.terminate()
    try:
        new_child.wait(timeout=5)
    except subprocess.TimeoutExpired:
        new_child.kill()
    old = getattr(restart, "rollback_sha", None)
    if old:
        git("reset", "--hard", old)
    restored = start_app()
    return restored, port_ready(12)


def main():
    child = start_app()
    print(f"[updater] running; automatic GitHub deployment is disabled; checking status every {CHECK_SECONDS}s", flush=True)
    last_good = local_head()
    try:
        while True:
            time.sleep(CHECK_SECONDS)
            if child.poll() is not None:
                child = start_app()
                continue

            # GitHub is only queried for status. A deployment is performed only
            # after the web UI creates .update-now with the correct update token.
            forced = FLAG.exists()
            if not forced:
                continue

            last_good = local_head() or last_good
            if not update():
                continue

            restart.rollback_sha = last_good
            child, ok = restart(child)
            if not ok:
                print("[updater] rollback/start failed; service manager should restart the process", flush=True)
    except KeyboardInterrupt:
        child.terminate()
        child.wait(timeout=10)


if __name__ == "__main__":
    main()
