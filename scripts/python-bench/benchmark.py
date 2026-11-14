#!/usr/bin/env python3
"""Isolated KVM/NATS benchmark; never uses deployment configuration."""
import argparse
import http.client
import json
import os
from pathlib import Path
import socket
import shutil
import subprocess
import threading
import time
import uuid


def command(args, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, check=True, **kwargs).stdout.strip()


class DockerConnection(http.client.HTTPConnection):
    def __init__(self):
        super().__init__('localhost', timeout=5)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect('/var/run/docker.sock')


def api(path):
    conn = DockerConnection()
    try:
        conn.request('GET', path)
        response = conn.getresponse()
        data = response.read()
        if response.status != 200:
            raise RuntimeError(f'Docker {path}: {response.status}')
        return json.loads(data)
    finally:
        conn.close()


def monitor(cid, path, stop, phase):
    with path.open('w') as f:
        while not stop.is_set():
            started = time.monotonic()
            try:
                data = api(f'/containers/{cid}/stats?stream=false&one-shot=true')
                mem = data.get('memory_stats', {})
                f.write(json.dumps(dict(at=time.time(), phase=phase[0], memory=mem,
                                       cpu=data.get('cpu_stats', {}))) + '\n')
                f.flush()
            except Exception as error:
                f.write(json.dumps(dict(at=time.time(), error=str(error))) + '\n')
            stop.wait(max(0, .05 - (time.monotonic() - started)))


def cgroup(name):
    script = 'for f in memory.current memory.peak memory.max memory.swap.current memory.swap.peak memory.swap.max memory.events memory.stat memory.pressure; do echo FILE:$f; cat /sys/fs/cgroup/$f; done'
    return command(['docker', 'exec', name, '/bin/sh', '-c', script], timeout=10)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--image', required=True)
    p.add_argument('--runtime', type=Path, required=True)
    p.add_argument('--client', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--workload', choices=['small', 'cpu', 'io', 'plot', 'blas', 'blas1', 'audit', 'backend', 'state'], default='cpu')
    p.add_argument('--repeats', type=int, default=5)
    p.add_argument('--memory-mib', type=int, default=1024)
    p.add_argument('--guest-memory-mib', type=int, default=128)
    p.add_argument('--keep-snapshots', action='store_true')
    p.add_argument('--import-profile', choices=['', 'numpy', 'plot-agg', 'plot-cairo'], default='')
    p.add_argument('--renderer', choices=['agg', 'cairo'], default='agg')
    p.add_argument('--plot-types', nargs='+', choices=['line', 'scatter', 'dense'], default=['line'])
    p.add_argument('--charts', type=int, default=1)
    p.add_argument('--png-compression', type=int, default=6)
    args = p.parse_args()
    if args.repeats < 1 or args.memory_mib < 1 or args.guest_memory_mib < 1:
        p.error('counts and memory must be positive')
    if args.charts < 1 or not 0 <= args.png_compression <= 9:
        p.error('charts must be positive and PNG compression must be 0 through 9')
    if (args.import_profile == 'plot-agg' and args.renderer != 'agg') or (args.import_profile == 'plot-cairo' and args.renderer != 'cairo'):
        p.error('renderer conflicts with warm profile')
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    state = out / 'snapshots'
    state.mkdir()
    tag = 'python-bench-' + uuid.uuid4().hex[:12]
    broker, name = tag + '-nats', tag + '-runtime'
    created = []
    stop = threading.Event()
    phase = ['startup']
    thread = None
    meta = dict(vars(args), runtime=str(args.runtime.resolve()), client=str(args.client.resolve()),
                output=str(out), started=time.time(), workers=2, parallel=2, guest_swap_mib=256,
                workspace_mib=64, broker_memory_mib=512, combined_memory_swap_mib=args.memory_mib + 256)
    meta['image'] = args.image
    meta['fixture_transport'] = 'workspace_file'
    try:
        meta['image_id'] = command(['docker', 'image', 'inspect', args.image, '--format', '{{.Id}}'])
        command(['docker', 'network', 'create', tag])
        command(['docker', 'run', '-d', '--name', broker, '--network', tag, '--memory', '512m', '--memory-swap', '512m', '-e', 'GOMEMLIMIT=384MiB', '--pids-limit', '128', '-p', '127.0.0.1::4222', 'nats:2', '-js', '-sd', '/data'])
        created.append(broker)
        port = api(f'/containers/{broker}/json')['NetworkSettings']['Ports']['4222/tcp'][0]['HostPort']
        bucket = 'PY_BENCH_' + uuid.uuid4().hex
        cid = command(['docker', 'run', '-d', '--name', name, '--network', tag, '--memory', f'{args.memory_mib}m', '--memory-swap', f'{args.memory_mib + 256}m', '--pids-limit', '256', '--device', '/dev/kvm', '--security-opt', 'seccomp=unconfined', '--mount', f'type=bind,src={args.runtime.resolve()},dst=/bench/runtime,readonly', '--mount', f'type=bind,src={state},dst=/opt/nats-sandbox-runtime/firecracker-assets/python-snapshot', '--entrypoint', '/bench/runtime', args.image, 'runtime', 'python', '--url', f'nats://{broker}:4222', '--bucket', bucket, '--workers', '2', '--memory-mib', str(args.guest_memory_mib), '--swap-mib', '256', '--workspace-mib', '64', '--exec-timeout', '30s', '--hide-firecracker-log', '--artifact-ttl', '0s', '--artifact-cleanup-interval', '0s'])
        created.append(name)
        thread = threading.Thread(target=monitor, args=(cid, out / 'samples.jsonl', stop, phase), daemon=True)
        thread.start()
        for _ in range(100):
            if 'ready: service=' in command(['docker', 'logs', name], timeout=5):
                break
            if not api(f'/containers/{name}/json')['State']['Running']:
                raise RuntimeError('runtime stopped before registration')
            time.sleep(.1)
        else:
            raise RuntimeError('runtime readiness timeout')
        (out / 'host-memory-before.txt').write_text(command(['docker', 'exec', name, '/bin/cat', '/proc/meminfo', '/proc/pressure/memory']))
        (out / 'cgroup-start.txt').write_text(cgroup(name))
        base = [str(args.client.resolve()), '--url', 'nats://127.0.0.1:' + port, '--bucket', bucket, '--parallel', '2', '--import-profile', args.import_profile, '--renderer', args.renderer, '--charts', str(args.charts), '--png-compression', str(args.png_compression)]
        phases = [('warmup', ['--phase', 'warmup', '--repeats', '1'])]
        plot_types = args.plot_types if args.workload == 'plot' else ['line']
        for kind in plot_types:
            label = 'measured-' + kind if len(plot_types) > 1 else 'measured'
            phases.append((label, ['--workload', args.workload, '--repeats', str(args.repeats), '--plot-type', kind, '--save-dir', str(out / 'images' / kind)]))
        for phase_name, flags in phases:
            phase[0] = phase_name
            result = subprocess.run(base + flags, text=True, capture_output=True, timeout=360)
            (out / (phase_name + '.jsonl')).write_text(result.stdout)
            (out / (phase_name + '.stderr')).write_text(result.stderr)
            meta[phase_name + '_exit'] = result.returncode
            if result.returncode:
                raise RuntimeError(phase_name + ' failed; see saved client and runtime logs')
            (out / ('cgroup-' + phase_name + '.txt')).write_text(cgroup(name))
        meta['state'] = api(f'/containers/{name}/json')['State']
        meta['success'] = True
    except Exception as error:
        meta.update(success=False, error=str(error))
    finally:
        if name in created:
            try:
                (out / 'cgroup-final.txt').write_text(cgroup(name))
                (out / 'host-memory-after.txt').write_text(command(['docker', 'exec', name, '/bin/cat', '/proc/meminfo', '/proc/pressure/memory']))
            except Exception as error:
                meta['success'] = False
                meta['final_stats_error'] = str(error)
        stop.set()
        if thread:
            thread.join(timeout=10)
        for container in reversed(created):
            try:
                subprocess.run(['docker', 'stop', '-t', '40', container], capture_output=True, timeout=50)
                logs = subprocess.run(['docker', 'logs', container], text=True, capture_output=True, timeout=10)
                (out / ('runtime.log' if container == name else 'broker.log')).write_text(logs.stdout + logs.stderr)
            except Exception as error:
                meta['success'] = False
                meta.setdefault('cleanup_errors', []).append(str(error))
            finally:
                removed = subprocess.run(['docker', 'rm', '-f', container], capture_output=True, timeout=30)
                if removed.returncode:
                    meta['success'] = False
                    meta.setdefault('cleanup_errors', []).append(removed.stderr.decode(errors='replace'))
        subprocess.run(['docker', 'network', 'rm', tag], capture_output=True)
        meta['snapshot_files'] = []
        try:
            # The runtime created these fresh bind-mounted files as container root.
            # Return ownership after shutdown; never follow exported symlinks.
            command(['docker', 'run', '--rm', '--network', 'none', '--user', '0', '--mount', f'type=bind,src={state},dst=/snapshots', '--entrypoint', '/bin/chown', args.image, '-R', '-h', f'{os.getuid()}:{os.getgid()}', '/snapshots'], timeout=30)
            for path in sorted(state.rglob('*')):
                if path.is_file():
                    stat = path.stat()
                    entry = dict(path=str(path.relative_to(state)), bytes=stat.st_size, allocated_bytes=stat.st_blocks * 512)
                    if path.name == 'version':
                        entry['content'] = path.read_text()
                    meta['snapshot_files'].append(entry)
            if meta.get('success') and not args.keep_snapshots:
                shutil.rmtree(state)
        except Exception as error:
            meta['success'] = False
            meta.setdefault('cleanup_errors', []).append(str(error))
        meta['finished'] = time.time()
        (out / 'metadata.json').write_text(json.dumps(meta, indent=2))
    print(json.dumps(dict(output=str(out), success=meta['success'], error=meta.get('error'))), flush=True)
    return 0 if meta['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
