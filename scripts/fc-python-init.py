#!/usr/bin/python3
import base64
import os
import shlex
import subprocess
import time


def console_print(message):
    print(message, flush=True)


def attach_console():
    fd = os.open("/dev/console", os.O_RDWR)
    for target in (0, 1, 2):
        os.dup2(fd, target)
    if fd > 2:
        os.close(fd)


def best_effort_mount(target, fstype, source):
    os.makedirs(target, exist_ok=True)
    subprocess.run(
        ["/usr/bin/mount", "-t", fstype, source, target],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def cmdline_values():
    with open("/proc/cmdline", encoding="utf-8") as cmdline:
        values = {}
        for item in shlex.split(cmdline.read()):
            if "=" in item:
                key, value = item.split("=", 1)
                values[key] = value
        return values


def main():
    attach_console()
    best_effort_mount("/proc", "proc", "proc")
    best_effort_mount("/sys", "sysfs", "sysfs")

    values = cmdline_values()
    start_ns = int(values.get("fc_start_ns", "0"))
    if start_ns > 0:
        startup_ms = (time.time_ns() - start_ns) // 1_000_000
        console_print(f"vm startup_ms={startup_ms}")
    else:
        console_print("vm startup_ms=unknown")

    runtime_start_ns = time.time_ns()
    encoded = values.get("fc_py_b64", "")
    if encoded:
        command = base64.b64decode(encoded).decode("utf-8")
        result = subprocess.run(["/usr/bin/python3", "-c", command], check=False)
    else:
        console_print("python repl ready")
        result = subprocess.run(["/usr/bin/python3", "-i"], check=False)

    runtime_ms = (time.time_ns() - runtime_start_ns) // 1_000_000
    console_print(f"vm runtime_ms={runtime_ms}")
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
