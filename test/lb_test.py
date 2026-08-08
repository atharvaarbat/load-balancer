#!/usr/bin/env python3
"""Self-contained integration test harness for the load balancer.

Builds the backend image, (re)creates the three backend containers, builds
and starts the load balancer binary, waits for everything to report
healthy, then runs the test suite. The load balancer process is stopped at
the end; the containers are left running so you can keep poking at the
system manually afterward.

Prerequisite: nothing else should already be bound to :8080 (stop any
`go run ./cmd/server` you have running manually first).
"""

import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

BACKEND_PORTS = [9001, 9002, 9003]
LB_URL = "http://localhost:8080"
SERVER_BIN = "test/bin/server.exe"
LB_LOG = "test/lb_output.log"


def run(cmd, env=None, **kwargs):
    print(f"$ {' '.join(cmd)}")
    full_env = {**os.environ, **env} if env else None
    return subprocess.run(cmd, cwd=ROOT, check=True, env=full_env, **kwargs)


def build_backend_image():
    run(
        ["go", "build", "-o", "cmd/backend/bin/backend", "./cmd/backend"],
        env={"GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "0"},
    )
    run(["docker", "build", "-t", "lb-backend", "cmd/backend"])


def recreate_containers():
    subprocess.run(
        ["docker", "rm", "-f", "backend1", "backend2", "backend3"],
        cwd=ROOT, capture_output=True,
    )
    for i, port in enumerate(BACKEND_PORTS, start=1):
        run([
            "docker", "run", "-d", "--name", f"backend{i}",
            "-p", f"{port}:{port}", "-e", f"PORT={port}", "lb-backend",
        ])


def build_server():
    (ROOT / "test" / "bin").mkdir(parents=True, exist_ok=True)
    run(["go", "build", "-o", SERVER_BIN, "./cmd/server"])


def wait_for(url, timeout=10):
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError) as e:
            last_err = e
        time.sleep(0.3)
    raise TimeoutError(f"{url} did not become healthy in time ({last_err})")


def start_lb():
    logfile = open(ROOT / LB_LOG, "w")
    proc = subprocess.Popen(
        [str(ROOT / SERVER_BIN)], cwd=ROOT, stdout=logfile, stderr=subprocess.STDOUT,
    )
    wait_for(f"{LB_URL}/health")
    return proc


def stop_lb(proc):
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def setup():
    print("== building backend image ==")
    build_backend_image()

    print("== starting backend containers ==")
    recreate_containers()
    for port in BACKEND_PORTS:
        wait_for(f"http://localhost:{port}/health")

    print("== building + starting load balancer ==")
    build_server()
    proc = start_lb()

    print("everything is up.\n")
    return proc


def smoke_test():
    with urllib.request.urlopen(LB_URL) as resp:
        body = resp.read().decode()
        assert resp.status == 200, f"expected 200, got {resp.status}"
        assert "Hello from backend" in body, f"unexpected body: {body}"
    print("PASS: smoke test")


def main():
    proc = setup()
    try:
        smoke_test()
    finally:
        print("\n== stopping load balancer ==")
        stop_lb(proc)


if __name__ == "__main__":
    main()
