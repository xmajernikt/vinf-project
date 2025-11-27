# proxy_manager.py
import subprocess
import shutil
import time
import os
import signal

class ProxyManager:

    def __init__(self, addon_path: str, port: int = 8080, mitmdump_path: str = "mitmdump", extra_args: list = None):
        self.addon_path = addon_path
        self.port = port
        self.mitmdump_path = mitmdump_path
        self.proc = None
        self.extra_args = extra_args or []

        if not shutil.which(self.mitmdump_path):
            raise FileNotFoundError(f"mitmdump not found in PATH as '{self.mitmdump_path}'. Install mitmproxy or give full path.")

    def start(self, timeout: float = 5.0):

        if self.proc:
            return

        cmd = [self.mitmdump_path, "-s", self.addon_path, "-p", str(self.port)]
        cmd += ["--set", "web.open_browser=false"]
        cmd += self.extra_args
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        start_time = time.time()
        while True:
            if self.proc.poll() is not None:
                out, err = self.proc.communicate(timeout=0.1)
                raise RuntimeError(f"mitmdump exited immediately. stdout: {out}\nstderr: {err}")

            if time.time() - start_time > timeout:
                break
            time.sleep(0.1)

        return True

    def stop(self):
        if not self.proc:
            return

        try:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=3)
        except Exception:
            try:
                self.proc.kill()
                self.proc.wait(timeout=1)
            except Exception:
                pass
        finally:
            self.proc = None
