import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BRANCH = os.getenv("UPDATE_BRANCH", "main")
CHECK_SECONDS = int(os.getenv("UPDATE_CHECK_SECONDS", "15"))
FLAG = ROOT / ".update-now"


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


def update():
    print("[updater] updating from GitHub...", flush=True)
    r = git("fetch", "origin", BRANCH)
    if r.returncode != 0:
        print(r.stdout, flush=True)
        return False
    r = git("reset", "--hard", f"origin/{BRANCH}")
    if r.returncode != 0:
        print(r.stdout, flush=True)
        return False
    subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], cwd=ROOT, check=False)
    try: FLAG.unlink()
    except FileNotFoundError: pass
    return True


def start_app():
    return subprocess.Popen([sys.executable, "app.py"], cwd=ROOT)


def restart(child):
    child.terminate()
    try: child.wait(timeout=10)
    except subprocess.TimeoutExpired: child.kill()
    return start_app()


def main():
    child = start_app()
    print(f"[updater] running; checking GitHub every {CHECK_SECONDS}s", flush=True)
    try:
        while True:
            time.sleep(CHECK_SECONDS)
            if child.poll() is not None:
                child = start_app()
                continue
            forced = FLAG.exists()
            rh = remote_head()
            lh = local_head()
            if forced or (rh and lh and rh != lh):
                if update():
                    child = restart(child)
    except KeyboardInterrupt:
        child.terminate()
        child.wait(timeout=10)


if __name__ == "__main__":
    main()
