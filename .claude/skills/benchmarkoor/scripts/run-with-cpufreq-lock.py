#!/usr/bin/python3 -I

import fcntl
import hashlib
import json
import os
import re
import secrets
import signal
import stat
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone


TOOLCHAIN_ROOT = "/usr/local/libexec/benchmarkoor"
TOOLCHAIN_DIR = os.path.dirname(os.path.realpath(__file__))
TOOLCHAIN_ID = os.path.basename(TOOLCHAIN_DIR)
GUARD_HELPER = os.path.join(TOOLCHAIN_DIR, "restore-cpufreq-state")
BENCHMARKOOR_BINARY = os.path.join(TOOLCHAIN_DIR, "benchmarkoor")
TOOLCHAIN_MANIFEST = os.path.join(TOOLCHAIN_DIR, "toolchain.sha256")
DOCKER_BINARY = "/usr/bin/docker"
DOCKER_HOST = "unix:///var/run/docker.sock"
DOCKER_QUIET_SECONDS = 30
PYTHON_BINARY = "/usr/bin/python3"
LOCK_FILE = "/run/lock/benchmarkoor/cpufreq.lock"
WORKLOAD_RECORD = "/run/lock/benchmarkoor/cpufreq-workload"
STAGING_HOLD_FILE = "/run/lock/benchmarkoor/staging-exit-hold"
CGROUP_ROOT = "/sys/fs/cgroup"
TERMINATION_SIGNALS = (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)
GATED_EXEC_CODE = """\
import os
import signal
import sys

gate = int(sys.argv[1])
try:
    authorization = os.read(gate, 1)
finally:
    os.close(gate)
if authorization != b"1":
    os._exit(125)
signal.pthread_sigmask(
    signal.SIG_UNBLOCK,
    (signal.SIGHUP, signal.SIGINT, signal.SIGTERM),
)
os.execve(sys.argv[2], sys.argv[2:], dict(os.environ))
"""
TOOLCHAIN_FILES = (
    "restore-cpufreq-state",
    "run-with-cpufreq-lock",
    "benchmarkoor",
)

benchmark_pidfd = None
launch_gate_fd = None
termination_signal = None
signal_forward_failed = False


def report(message):
    print(message, file=sys.stderr, flush=True)


def forward_signal(signum, _frame=None):
    global launch_gate_fd, termination_signal, signal_forward_failed

    if termination_signal is None:
        termination_signal = signum
    gate_descriptor = launch_gate_fd
    launch_gate_fd = None
    if gate_descriptor is not None:
        try:
            os.close(gate_descriptor)
        except OSError as error:
            report(f"failed to close the Benchmarkoor launch gate: {error}")
            signal_forward_failed = True
    if benchmark_pidfd is None:
        return
    try:
        signal.pidfd_send_signal(benchmark_pidfd, signum)
    except ProcessLookupError:
        pass
    except OSError as error:
        report(f"failed to forward signal {signum}: {error}")
        signal_forward_failed = True


def call_guard(*arguments):
    subprocess.run(
        [GUARD_HELPER, *arguments],
        check=True,
        close_fds=True,
    )


def require_root_file(path, mode):
    metadata = os.lstat(path)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != mode
        or metadata.st_nlink != 1
        or os.path.realpath(path) != path
    ):
        raise RuntimeError(f"invalid installed toolchain file: {path}")


def close_launch_gate():
    global launch_gate_fd

    descriptor = launch_gate_fd
    launch_gate_fd = None
    if descriptor is not None:
        os.close(descriptor)


def verify_toolchain():
    if (
        not re.fullmatch(r"[0-9a-f]{64}", TOOLCHAIN_ID)
        or os.path.dirname(TOOLCHAIN_DIR) != TOOLCHAIN_ROOT
        or os.path.realpath(TOOLCHAIN_ROOT) != TOOLCHAIN_ROOT
    ):
        raise RuntimeError("the launcher is not in a digest-scoped toolchain")

    for directory, mode in (
        (TOOLCHAIN_ROOT, 0o755),
        (TOOLCHAIN_DIR, 0o555),
    ):
        metadata = os.lstat(directory)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != mode
            or os.path.realpath(directory) != directory
        ):
            raise RuntimeError(f"invalid installed toolchain directory: {directory}")

    for filename in TOOLCHAIN_FILES:
        require_root_file(os.path.join(TOOLCHAIN_DIR, filename), 0o555)
    require_root_file(TOOLCHAIN_MANIFEST, 0o444)

    with open(TOOLCHAIN_MANIFEST, "rb") as manifest_file:
        manifest = manifest_file.read()
    if hashlib.sha256(manifest).hexdigest() != TOOLCHAIN_ID:
        raise RuntimeError("the toolchain manifest does not match its directory")

    try:
        lines = manifest.decode("ascii").splitlines()
    except UnicodeDecodeError as error:
        raise RuntimeError("the toolchain manifest is not ASCII") from error
    if len(lines) != len(TOOLCHAIN_FILES):
        raise RuntimeError("the toolchain manifest has unexpected entries")

    for line, filename in zip(lines, TOOLCHAIN_FILES):
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9-]+)", line)
        if match is None or match.group(2) != filename:
            raise RuntimeError("the toolchain manifest is malformed")
        digest = hashlib.sha256()
        with open(os.path.join(TOOLCHAIN_DIR, filename), "rb") as installed:
            while chunk := installed.read(1024 * 1024):
                digest.update(chunk)
        if digest.hexdigest() != match.group(1):
            raise RuntimeError(f"toolchain checksum failed: {filename}")

    require_root_file(DOCKER_BINARY, 0o755)


def managed_containers():
    result = subprocess.run(
        [
            DOCKER_BINARY,
            "--host",
            DOCKER_HOST,
            "ps",
            "-aq",
            "--no-trunc",
            "--filter",
            "label=benchmarkoor.managed-by=benchmarkoor",
        ],
        check=True,
        close_fds=True,
        env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin"},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.stderr:
        raise RuntimeError("Docker container scan produced diagnostics")
    container_ids = result.stdout.splitlines()
    if len(container_ids) != len(set(container_ids)) or any(
        re.fullmatch(r"[0-9a-f]{64}", container_id) is None
        for container_id in container_ids
    ):
        raise RuntimeError("Docker returned invalid Benchmarkoor container IDs")
    return sorted(container_ids)


def require_managed_containers_quiet(docker_baseline):
    observed_since = datetime.now(timezone.utc)
    remaining_containers = new_managed_containers(docker_baseline)
    if remaining_containers:
        raise RuntimeError(
            "Benchmarkoor-created Docker containers remain: "
            f"{','.join(remaining_containers)}"
        )

    quiet_started = time.monotonic()
    observed_until = datetime.now(timezone.utc) + timedelta(
        seconds=DOCKER_QUIET_SECONDS
    )
    result = subprocess.run(
        [
            DOCKER_BINARY,
            "--host",
            DOCKER_HOST,
            "events",
            "--since",
            observed_since.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "--until",
            observed_until.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "--filter",
            "type=container",
            "--filter",
            "label=benchmarkoor.managed-by=benchmarkoor",
            "--format",
            "{{.ID}} {{.Action}}",
        ],
        check=True,
        close_fds=True,
        env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin"},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=DOCKER_QUIET_SECONDS + 15,
    )
    if time.monotonic() - quiet_started < DOCKER_QUIET_SECONDS:
        raise RuntimeError(
            "Docker event scan ended before the required quiet window elapsed"
        )

    remaining_containers = new_managed_containers(docker_baseline)
    if remaining_containers:
        raise RuntimeError(
            "Benchmarkoor-created Docker containers remain: "
            f"{','.join(remaining_containers)}"
        )
    if result.stderr:
        raise RuntimeError("Docker event scan produced diagnostics")
    if result.stdout:
        raise RuntimeError(
            "Benchmarkoor Docker activity occurred during the required quiet window"
        )


def acquire_lock():
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    descriptor = os.open(LOCK_FILE, flags)
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode):
        os.close(descriptor)
        raise RuntimeError("CPU lock is not a regular file")
    if (
        metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_nlink != 1
    ):
        os.close(descriptor)
        raise RuntimeError("CPU lock ownership, mode, or link count is invalid")
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(descriptor)
        raise RuntimeError("CPU controls are owned by another benchmark task")
    os.set_inheritable(descriptor, False)
    return descriptor


def command_status(returncode):
    if returncode < 0:
        return 128 - returncode
    return returncode


def cgroup_parent():
    with open("/proc/self/cgroup", encoding="ascii") as cgroup_file:
        entries = [line.rstrip("\n") for line in cgroup_file]
    if len(entries) != 1 or not entries[0].startswith("0::/"):
        raise RuntimeError("a unified cgroup v2 hierarchy is required")
    root = os.path.realpath(CGROUP_ROOT)
    if root != CGROUP_ROOT or not os.path.isfile(f"{root}/cgroup.controllers"):
        raise RuntimeError("the trusted cgroup v2 root is unavailable")
    parent = os.path.realpath(
        os.path.join(root, entries[0][3:].lstrip("/"))
    )
    if os.path.commonpath((root, parent)) != root:
        raise RuntimeError("the launcher cgroup escapes the trusted hierarchy")
    if not os.path.isfile(f"{parent}/cgroup.procs"):
        raise RuntimeError("the launcher cgroup is unavailable")
    return parent


def create_workload_cgroup(docker_baseline):
    parent = cgroup_parent()
    name = f"benchmarkoor-cpufreq-{os.getpid()}-{secrets.token_hex(16)}"
    path = os.path.join(parent, name)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW
    record_descriptor = os.open(WORKLOAD_RECORD, flags, 0o600)
    try:
        metadata = os.fstat(record_descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
        ):
            raise RuntimeError("workload record ownership or mode is invalid")
        record = json.dumps(
            {
                "version": 2,
                "toolchain_id": TOOLCHAIN_ID,
                "cgroup": path,
                "docker_baseline": docker_baseline,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii") + b"\n"
        if os.write(record_descriptor, record) != len(record):
            raise RuntimeError("short write while creating the workload record")
        os.fsync(record_descriptor)
    except Exception:
        os.close(record_descriptor)
        os.unlink(WORKLOAD_RECORD)
        raise
    os.close(record_descriptor)
    try:
        os.mkdir(path, 0o755)
        if not os.path.isfile(f"{path}/cgroup.procs") or not os.path.isfile(
            f"{path}/cgroup.events"
        ):
            raise RuntimeError("failed to create the workload cgroup")
    except Exception:
        if os.path.isdir(path):
            os.rmdir(path)
        os.unlink(WORKLOAD_RECORD)
        raise
    return path


def join_workload_cgroup(path):
    with open(f"{path}/cgroup.procs", "w", encoding="ascii") as procs_file:
        procs_file.write("0\n")


def cgroup_is_populated(path):
    with open(f"{path}/cgroup.events", encoding="ascii") as events_file:
        events = dict(line.split() for line in events_file)
    if events.get("populated") not in ("0", "1"):
        raise RuntimeError("invalid workload cgroup state")
    return events["populated"] == "1"


def kill_workload_cgroup(path):
    with open(f"{path}/cgroup.kill", "w", encoding="ascii") as kill_file:
        kill_file.write("1\n")


def remove_workload_gate(path, docker_baseline):
    if cgroup_is_populated(path):
        raise RuntimeError(f"Benchmarkoor workload cgroup is still populated: {path}")
    os.rmdir(path)
    require_managed_containers_quiet(docker_baseline)
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    record_descriptor = os.open(WORKLOAD_RECORD, flags)
    try:
        metadata = os.fstat(record_descriptor)
        record = os.read(record_descriptor, 1024 * 1024).decode("ascii")
        expected_record = json.dumps(
            {
                "version": 2,
                "toolchain_id": TOOLCHAIN_ID,
                "cgroup": path,
                "docker_baseline": docker_baseline,
            },
            separators=(",", ":"),
            sort_keys=True,
        ) + "\n"
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
            or record != expected_record
        ):
            raise RuntimeError("workload record changed unexpectedly")
        identity = (metadata.st_dev, metadata.st_ino)
    finally:
        os.close(record_descriptor)
    current = os.stat(WORKLOAD_RECORD, follow_symlinks=False)
    if (current.st_dev, current.st_ino) != identity:
        raise RuntimeError("workload record identity changed unexpectedly")
    os.unlink(WORKLOAD_RECORD)


def create_staging_hold():
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW
    descriptor = os.open(STAGING_HOLD_FILE, flags, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
        ):
            raise RuntimeError("staging exit hold ownership or mode is invalid")
        record = json.dumps(
            {
                "version": 1,
                "toolchain_id": TOOLCHAIN_ID,
                "sudo_uid": int(os.environ["SUDO_UID"]),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii") + b"\n"
        if os.write(descriptor, record) != len(record):
            raise RuntimeError("short write while creating the staging exit hold")
        os.fsync(descriptor)
        identity = (metadata.st_dev, metadata.st_ino)
    except Exception:
        os.close(descriptor)
        os.unlink(STAGING_HOLD_FILE)
        raise
    os.close(descriptor)
    return identity


def verify_staging_hold(identity):
    metadata = os.stat(STAGING_HOLD_FILE, follow_symlinks=False)
    if (
        (metadata.st_dev, metadata.st_ino) != identity
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_nlink != 1
    ):
        raise RuntimeError("staging exit hold changed unexpectedly")


def wait_for_staging_release(identity):
    verify_staging_hold(identity)
    report(
        "Benchmarkoor exited cleanly; staging exit hold is ready for release: "
        f"{STAGING_HOLD_FILE}"
    )
    while termination_signal is None:
        try:
            verify_staging_hold(identity)
        except FileNotFoundError:
            return
        time.sleep(0.25)
    raise RuntimeError("termination requested while waiting for staging release")


def cleanup_staging_hold(identity):
    if identity is None:
        return
    try:
        metadata = os.stat(STAGING_HOLD_FILE, follow_symlinks=False)
    except FileNotFoundError:
        return
    if (metadata.st_dev, metadata.st_ino) != identity:
        report("staging exit hold identity changed; refusing to remove it")
        return
    try:
        os.unlink(STAGING_HOLD_FILE)
    except OSError as error:
        report(f"failed to remove staging exit hold: {error}")


def new_managed_containers(docker_baseline):
    return sorted(set(managed_containers()) - set(docker_baseline))


def recover():
    try:
        call_guard("recover")
    except (OSError, subprocess.CalledProcessError) as error:
        report(f"CPU recovery remains armed; no later guarded run can start: {error}")
        return False
    return True


def reserve_pidfd():
    if not callable(getattr(os, "pidfd_open", None)) or not callable(
        getattr(signal, "pidfd_send_signal", None)
    ):
        raise RuntimeError("Python does not provide the required pidfd operations")

    descriptor = None
    try:
        descriptor = os.pidfd_open(os.getpid(), 0)
        os.set_inheritable(descriptor, False)
        signal.pidfd_send_signal(descriptor, 0)
    except OSError as error:
        if descriptor is not None:
            os.close(descriptor)
        raise RuntimeError(f"pidfd runtime probe failed: {error}") from error
    return descriptor


def stop_and_wait_child(process):
    try:
        process.terminate()
    except OSError as error:
        report(f"failed to terminate child: {error}")
    try:
        process.wait(timeout=30)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        process.kill()
    except OSError as error:
        report(f"failed to kill child: {error}")
    process.wait()


def require_sudo_caller():
    if os.geteuid() != 0:
        raise RuntimeError("run the installed guarded launcher through sudo")
    sudo_uid = os.environ.get("SUDO_UID", "")
    if not sudo_uid.isdecimal() or int(sudo_uid) == 0:
        raise RuntimeError("a non-root sudo caller is required")


def build_benchmark_environment():
    environment = {
        "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME": "/root",
        "USER": "root",
        "LOGNAME": "root",
        "BENCHMARKOOR_BUILDER_CLEANUP_ON_START": "false",
        "BENCHMARKOOR_RUNNER_CLEANUP_ON_START": "false",
        "BENCHMARKOOR_RUNNER_CONTAINER_RUNTIME": "docker",
        "BENCHMARKOOR_RUNNER_CPU_SYSFS_PATH": "/sys/devices/system/cpu",
        "DOCKER_HOST": DOCKER_HOST,
    }
    github_token = os.environ.get("BENCHMARKOOR_RUNNER_GITHUB_TOKEN")
    if github_token:
        environment["BENCHMARKOOR_RUNNER_GITHUB_TOKEN"] = github_token
    return environment


def main():
    global benchmark_pidfd, launch_gate_fd

    try:
        require_sudo_caller()
        verify_toolchain()
    except RuntimeError as error:
        report(str(error))
        return 2

    if len(sys.argv) == 2 and sys.argv[1] == "--check-github-token":
        if not os.environ.get("BENCHMARKOOR_RUNNER_GITHUB_TOKEN"):
            report("the GitHub token did not cross the sudo boundary")
            return 2
        return 0

    arguments = sys.argv[2:]
    hold_after_exit = bool(arguments and arguments[0] == "--hold-after-exit")
    if hold_after_exit:
        arguments = arguments[1:]
    if not arguments or arguments[0] != "run":
        report(
            f"usage: {sys.argv[0]} CPU_IDS|none "
            "[--hold-after-exit] run [ARG ...]"
        )
        return 2

    for signum in TERMINATION_SIGNALS:
        signal.signal(signum, forward_signal)

    cpu_ids = sys.argv[1]
    command = [BENCHMARKOOR_BINARY, *arguments]
    benchmark_environment = build_benchmark_environment()
    guard_armed = False
    lock_descriptor = None
    benchmark = None
    gate_read_fd = None
    pidfd_reserve = None
    workload_cgroup = None
    docker_baseline = None
    workload_started = False
    staging_hold_identity = None

    if termination_signal is not None:
        return 128 + termination_signal

    try:
        pidfd_reserve = reserve_pidfd()
    except RuntimeError as error:
        report(str(error))
        return 2
    if termination_signal is not None:
        os.close(pidfd_reserve)
        pidfd_reserve = None
        return 128 + termination_signal

    try:
        call_guard("arm", cpu_ids)
        guard_armed = True

        if termination_signal is not None:
            return 128 + termination_signal if recover() else 125

        lock_descriptor = acquire_lock()
        call_guard("verify-armed")

        if termination_signal is not None:
            os.close(lock_descriptor)
            lock_descriptor = None
            return 128 + termination_signal if recover() else 125

        docker_baseline = managed_containers()
        workload_cgroup = create_workload_cgroup(docker_baseline)
        if hold_after_exit:
            staging_hold_identity = create_staging_hold()

        previous_signal_mask = signal.pthread_sigmask(
            signal.SIG_BLOCK, TERMINATION_SIGNALS
        )
        try:
            if set(previous_signal_mask).intersection(TERMINATION_SIGNALS):
                raise RuntimeError(
                    "launcher termination signals must not be blocked by its caller"
                )
            pending_signals = set(signal.sigpending()).intersection(
                TERMINATION_SIGNALS
            )
            if termination_signal is not None or pending_signals:
                raise RuntimeError("termination requested before Benchmarkoor start")

            gate_read_fd, launch_gate_fd = os.pipe2(os.O_CLOEXEC)
            gated_command = [
                PYTHON_BINARY,
                "-I",
                "-c",
                GATED_EXEC_CODE,
                str(gate_read_fd),
                *command,
            ]
            benchmark = subprocess.Popen(
                gated_command,
                close_fds=True,
                env=benchmark_environment,
                pass_fds=(gate_read_fd,),
                preexec_fn=lambda: join_workload_cgroup(workload_cgroup),
                start_new_session=True,
            )
            os.close(gate_read_fd)
            gate_read_fd = None
            os.close(pidfd_reserve)
            pidfd_reserve = None
            try:
                benchmark_pidfd = os.pidfd_open(benchmark.pid, 0)
            except OSError:
                try:
                    kill_workload_cgroup(workload_cgroup)
                except OSError as cgroup_error:
                    report(
                        f"failed to terminate workload cgroup: {cgroup_error}"
                    )
                close_launch_gate()
                stop_and_wait_child(benchmark)
                raise
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_signal_mask)

        if termination_signal is not None:
            close_launch_gate()
            stop_and_wait_child(benchmark)
            raise RuntimeError("termination requested before Benchmarkoor start")

        gate_descriptor = launch_gate_fd
        if gate_descriptor is None:
            stop_and_wait_child(benchmark)
            raise RuntimeError("Benchmarkoor launch authorization was cancelled")
        try:
            if os.write(gate_descriptor, b"1") != 1:
                raise RuntimeError("failed to authorize Benchmarkoor launch")
        except (OSError, RuntimeError):
            close_launch_gate()
            stop_and_wait_child(benchmark)
            raise
        close_launch_gate()
        workload_started = True

        returncode = benchmark.wait()
        benchmark_pidfd_to_close = benchmark_pidfd
        benchmark_pidfd = None
        os.close(benchmark_pidfd_to_close)

        if returncode != 0 or termination_signal is not None or signal_forward_failed:
            report(
                "Benchmarkoor did not finish cleanly; CPU recovery remains armed"
            )
            return 125
        if cgroup_is_populated(workload_cgroup):
            report(
                "Benchmarkoor cgroup still has live processes; "
                "CPU recovery remains armed"
            )
            return 125
        if hold_after_exit:
            wait_for_staging_release(staging_hold_identity)
            staging_hold_identity = None

        remove_workload_gate(workload_cgroup, docker_baseline)
        workload_cgroup = None
        os.close(lock_descriptor)
        lock_descriptor = None
        if not recover():
            return 125
        guard_armed = False
        if termination_signal is not None:
            return 128 + termination_signal

        return command_status(returncode)
    except (
        OSError,
        RuntimeError,
        subprocess.SubprocessError,
    ) as error:
        report(str(error))
        if workload_started:
            report("Benchmarkoor may not have cleaned up; CPU recovery remains armed")
            return 125
        if workload_cgroup is not None:
            try:
                remove_workload_gate(workload_cgroup, docker_baseline)
                workload_cgroup = None
            except (
                OSError,
                RuntimeError,
                subprocess.SubprocessError,
            ) as cleanup_error:
                report(f"workload cleanup remains required: {cleanup_error}")
                return 125
        if guard_armed:
            if lock_descriptor is not None:
                os.close(lock_descriptor)
                lock_descriptor = None
            if not recover():
                return 125
        return 125
    finally:
        if benchmark_pidfd is not None:
            os.close(benchmark_pidfd)
            benchmark_pidfd = None
        if lock_descriptor is not None:
            os.close(lock_descriptor)
        if pidfd_reserve is not None:
            os.close(pidfd_reserve)
        if gate_read_fd is not None:
            os.close(gate_read_fd)
        try:
            close_launch_gate()
        except OSError as error:
            report(f"failed to close the Benchmarkoor launch gate: {error}")
        cleanup_staging_hold(staging_hold_identity)


if __name__ == "__main__":
    sys.exit(main())
