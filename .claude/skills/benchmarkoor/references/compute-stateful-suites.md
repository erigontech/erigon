# Compute and stateful suites

Use this reference when locating, selecting, filtering, running, or validating a Benchmarkoor
compute or stateful suite.

## Contents

- Discover the current files
- Distinguish the suites
- Validate the selection
- Locate, download, and enumerate fixtures
- Run the selected suite
- Validate the result
- Recover from interruption
- Pre-populated snapshot workflow

## Discover the current files

Search the checked-out `benchmarkoor-tests` revision instead of assuming a context version,
dataset, or devnet path from an earlier run:

```bash
CONTEXT_NAME=${CONTEXT_NAME:?set the exact requested context directory name}
SUITE_KIND=${SUITE_KIND:?set to compute or stateful}
case "$CONTEXT_NAME" in ''|*[!A-Za-z0-9._-]*) exit 2 ;; esac
case "$SUITE_KIND" in compute|stateful) ;; *) exit 2 ;; esac
DOCKER=(env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin \
  /usr/bin/docker --host unix:///var/run/docker.sock)
ROOT_DOCKER=(sudo -n env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin \
  /usr/bin/docker --host unix:///var/run/docker.sock)

CONTEXTS_ROOT="$TESTS_DIR/configs/contexts"
mapfile -t SOURCE_CANDIDATES < <(
  while IFS= read -r candidate; do
    test "$(basename -- "$(dirname -- "$candidate")")" = "$CONTEXT_NAME" || continue
    printf '%s\n' "$candidate"
  done < <(
    rg --files "$CONTEXTS_ROOT" \
      -g "test-source.${SUITE_KIND}.runner.yaml" \
      -g "test-source.${SUITE_KIND}.runner.yml"
  ) | sort
)
test "${#SOURCE_CANDIDATES[@]}" -gt 0 || exit 1
printf '%s\n' "${SOURCE_CANDIDATES[@]}"

SUITE_RUNNER_CONFIG=${SUITE_RUNNER_CONFIG:?select one candidate after inspecting it}
SUITE_RUNNER_CONFIG=$(realpath -e -- "$SUITE_RUNNER_CONFIG")
SOURCE_MATCH=false
for candidate in "${SOURCE_CANDIDATES[@]}"; do
  if test "$(realpath -e -- "$candidate")" = "$SUITE_RUNNER_CONFIG"; then
    SOURCE_MATCH=true
    break
  fi
done
test "$SOURCE_MATCH" = true || exit 1

CONTEXT_DIR=$(dirname -- "$SUITE_RUNNER_CONFIG")
test "$(basename -- "$CONTEXT_DIR")" = "$CONTEXT_NAME" || exit 1
CONTEXT_GLOBAL_CONFIG="$CONTEXT_DIR/global.yaml"
CLIENTS_CONFIG="$CONTEXT_DIR/clients.yaml"

test -f "$CONTEXT_GLOBAL_CONFIG" || exit 1
test -f "$CLIENTS_CONFIG" || exit 1

yq -r '.runner.instances[] | [.id, .client, .image] | @tsv' "$CLIENTS_CONFIG"
INSTANCE_ID=${INSTANCE_ID:?select one listed Erigon instance ID}
test "$(INSTANCE_ID="$INSTANCE_ID" yq -r \
  '[.runner.instances[] | select(.id == strenv(INSTANCE_ID) and .client == "erigon")] | length' \
  "$CLIENTS_CONFIG")" -eq 1 || exit 1
```

Multiple candidates can share the same context name under different dataset trees. Inspect each
candidate's suite name, labels, fixture source, rollback strategy, context global, and clients file;
do not select by shortest path. A sibling such as `${CONTEXT_NAME}-full` is a different context and
must be selected only when requested. The context name is independent of running all versus a
filtered subset. Files ending in `.builder.yaml` build fixture archives; use `.runner.yaml` to run
downloaded fixtures locally. A new context requires a new `CONTEXT_NAME`, not an edit to this skill.

## Distinguish the suites

| Property | Compute | Stateful |
|---|---|---|
| Runner file | `test-source.compute.runner.yaml` | `test-source.stateful.runner.yaml` |
| `test-type` label | `compute` | `stateful` |
| Fixture provenance | selected compute build/source | selected stateful build/source |
| Expected rollback | `none` | `container-recreate` |
| Typical test path | `benchmark/compute/...` | `benchmark/stateful/bloatnet/...` |
| Datadir lifetime | one disposable overlay for the run | fresh container and overlay per fixture |

## Validate the selection

Inspect the resolved runner file before starting:

```bash
yq '.runner.benchmark.tests' "$SUITE_RUNNER_CONFIG"
yq -r '.runner.client.config.rollback_strategy // ""' "$SUITE_RUNNER_CONFIG"
git -C "$TESTS_DIR" rev-parse HEAD
git -C "$TESTS_DIR" status --short --branch
```

Require the suite name, `test-type`, fixture source map, and rollback strategy to describe the
requested suite. Do not classify a future source from substrings in its URL. The shared
`blockchain_tests_stateful_engine` fixtures subdirectory also does not distinguish compute from
stateful. If the fixture archive is cached, the run log may omit a download line; retain the
selected YAML blob and resolved source as provenance.

For a subset, put `runner.benchmark.tests.filter` in the last local override. EEST names can carry
a leading `tests/` while filtering even though persisted result keys omit it, so anchor local
regular expressions with `^(tests/)?benchmark/...`, not only `^benchmark/...`. A filter changes the
derived suite hash. Record the parent runner config and full-suite hash alongside the filtered run,
and reject a smoke run that resolves to zero tests.

## Locate, download, and enumerate fixtures

For EEST suites, the pinned runner YAML is the authoritative download specification:

```yaml
runner:
  benchmark:
    tests:
      source:
        eest_fixtures:
          fixtures_url: https://host/path/to/fixtures.tar.gz
          fixtures_subdir: path/inside/extracted/archive
```

Copy the exact `fixtures_url` and `fixtures_subdir` from the selected runner or authoritative
hosted run; do not guess a release from the suite name. A standalone `fixtures_url` is either a
direct `.tar.gz`/release asset or a GitHub Actions artifact URL. Benchmarkoor downloads and
extracts it automatically. GitHub Actions artifact URLs require the configured GitHub token;
public release assets normally do not. If the source instead uses `github_release`, an artifact
name, or a local source, preserve that mode rather than converting it silently.

Define the privileged invocation after inspecting the selected source. Do not rely on the
invoking user's `gh` login being available under root, and do not use broad `sudo -E`. For a source
that needs GitHub authentication, obtain the token without printing it and preserve only
`BENCHMARKOOR_RUNNER_GITHUB_TOKEN` across the privileged boundary:

Use `github-token` for a GitHub Actions artifact or another authenticated GitHub source; use `none`
only for a source that is demonstrably public or local.

Have a host administrator copy the candidate toolchain into a root-only directory. The checkout is
mutable input, so audit the staged copy, not the checkout files. Do not include these staging or
installation commands in a benchmark command alias or sudo rule:

```bash
set -euo pipefail
ensure_root_directory() {
  local directory=${1:?set directory}
  local expected_mode=${2:?set mode}
  local parent
  local parent_mode

  parent=$(dirname -- "$directory")
  test "$(sudo realpath -e -- "$parent")" = "$parent"
  test "$(sudo stat -Lc '%u:%g' -- "$parent")" = 0:0
  parent_mode=$(sudo stat -Lc '%a' -- "$parent")
  case "$parent_mode" in ''|*[!0-7]*) exit 2 ;; esac
  test "$((8#$parent_mode & 0022))" -eq 0
  if sudo test -e "$directory" || sudo test -L "$directory"; then
    sudo test ! -L "$directory"
    sudo test -d "$directory"
    test "$(sudo realpath -e -- "$directory")" = "$directory"
    test "$(sudo stat -Lc '%u:%g:%a' -- "$directory")" = \
      "0:0:$expected_mode"
  else
    sudo mkdir -- "$directory"
    sudo chown root:root "$directory"
    sudo chmod "$expected_mode" "$directory"
    test "$(sudo stat -Lc '%u:%g:%a' -- "$directory")" = \
      "0:0:$expected_mode"
  fi
}

BENCHMARKOOR_SKILL_DIR=${BENCHMARKOOR_SKILL_DIR:?set the directory containing SKILL.md}
BENCHMARKOOR_SOURCE_BIN=${BENCHMARKOOR_SOURCE_BIN:?set the built benchmarkoor binary}
CPU_GUARD_SOURCE=$(realpath -e -- \
  "$BENCHMARKOOR_SKILL_DIR/scripts/restore-cpufreq-state.sh")
CPU_GUARD_RUNNER_SOURCE=$(realpath -e -- \
  "$BENCHMARKOOR_SKILL_DIR/scripts/run-with-cpufreq-lock.py")
BENCHMARKOOR_SOURCE_BIN=$(realpath -e -- "$BENCHMARKOOR_SOURCE_BIN")
ensure_root_directory /var/lib/benchmarkoor 755
ensure_root_directory /var/lib/benchmarkoor/admin 700
ensure_root_directory /usr/local/libexec 755
ensure_root_directory /usr/local/libexec/benchmarkoor 755
CANDIDATE_DIR=$(sudo mktemp -d \
  /var/lib/benchmarkoor/admin/toolchain-candidate.XXXXXXXXXX)
test "$(sudo stat -Lc '%u:%g:%a' -- "$CANDIDATE_DIR")" = 0:0:700
sudo install -o root -g root -m 0700 \
  "$CPU_GUARD_SOURCE" "$CANDIDATE_DIR/restore-cpufreq-state"
sudo install -o root -g root -m 0700 \
  "$CPU_GUARD_RUNNER_SOURCE" "$CANDIDATE_DIR/run-with-cpufreq-lock"
sudo install -o root -g root -m 0700 \
  "$BENCHMARKOOR_SOURCE_BIN" "$CANDIDATE_DIR/benchmarkoor"
sudo /bin/sh -c '
  set -eu
  cd "$1" || exit 1
  umask 077
  /usr/bin/sha256sum restore-cpufreq-state run-with-cpufreq-lock benchmarkoor \
    > toolchain.sha256
' benchmarkoor-admin "$CANDIDATE_DIR"
```

The administrator must audit and syntax-check that root-owned candidate. Record `CANDIDATE_DIR` in
the administrator's private task notes because a later shell cannot reconstruct its random name.
Only after approving those exact bytes, derive their toolchain ID and install them without replacing
an existing path:

```bash
set -euo pipefail
CANDIDATE_DIR=${CANDIDATE_DIR:?set the audited root-only candidate directory}
CANDIDATE_DIR=$(sudo realpath -e -- "$CANDIDATE_DIR")
case "$CANDIDATE_DIR" in
  /var/lib/benchmarkoor/admin/toolchain-candidate.*) ;;
  *) exit 2 ;;
esac
CPU_LOCK_FILE=/run/lock/benchmarkoor/cpufreq.lock
CPU_GUARD_RECORD=/run/lock/benchmarkoor/cpufreq-recovery.json
CPU_WORKLOAD_RECORD=/run/lock/benchmarkoor/cpufreq-workload
for candidate in restore-cpufreq-state run-with-cpufreq-lock benchmarkoor; do
  test "$(sudo stat -Lc '%u:%g:%a:%h' -- "$CANDIDATE_DIR/$candidate")" = \
    0:0:700:1
done
test "$(sudo stat -Lc '%u:%g:%a:%h' -- "$CANDIDATE_DIR/toolchain.sha256")" = \
  0:0:600:1
test "$(sudo awk '{print $2}' "$CANDIDATE_DIR/toolchain.sha256" | paste -sd, -)" = \
  restore-cpufreq-state,run-with-cpufreq-lock,benchmarkoor
sudo /bin/bash -n "$CANDIDATE_DIR/restore-cpufreq-state"
sudo /usr/bin/python3 -I -c \
  'import pathlib,sys; p=pathlib.Path(sys.argv[1]); compile(p.read_bytes(), str(p), "exec")' \
  "$CANDIDATE_DIR/run-with-cpufreq-lock"
sudo "$CANDIDATE_DIR/benchmarkoor" version
sudo /bin/sh -c 'cd "$1" && /usr/bin/sha256sum --check --strict toolchain.sha256' \
  benchmarkoor-admin "$CANDIDATE_DIR"
TOOLCHAIN_ID=$(sudo sha256sum "$CANDIDATE_DIR/toolchain.sha256" | awk '{print $1}')
[[ "$TOOLCHAIN_ID" =~ ^[0-9a-f]{64}$ ]] || exit 1
TOOLCHAIN_DIR="/usr/local/libexec/benchmarkoor/$TOOLCHAIN_ID"

test "$(sudo /usr/bin/realpath -e -- /run/lock)" = /run/lock
test "$(sudo /usr/bin/stat -Lc '%u:%g' -- /run/lock)" = 0:0
lock_parent_mode=$(sudo /usr/bin/stat -Lc '%a' -- /run/lock)
case "$lock_parent_mode" in ''|*[!0-7]*) exit 2 ;; esac
if test "$((8#$lock_parent_mode & 0022))" -ne 0; then
  test "$((8#$lock_parent_mode & 01000))" -ne 0
fi
sudo /bin/mkdir -m 0755 -- /run/lock/benchmarkoor 2>/dev/null || true
test "$(sudo /usr/bin/stat -c '%f:%u:%g:%a' -- /run/lock/benchmarkoor)" = \
  41ed:0:0:755
test "$(sudo /usr/bin/realpath -e -- /run/lock/benchmarkoor)" = \
  /run/lock/benchmarkoor
if test -e "$CPU_LOCK_FILE" || test -L "$CPU_LOCK_FILE"; then
  test ! -L "$CPU_LOCK_FILE"
  test "$(stat -Lc '%u:%g:%h' -- "$CPU_LOCK_FILE")" = 0:0:1
  lock_mode=$(stat -Lc '%a' -- "$CPU_LOCK_FILE")
  case "$lock_mode" in 600|644) ;; *) exit 1 ;; esac
fi

sudo /bin/bash -s -- \
  "$CANDIDATE_DIR" "$TOOLCHAIN_ID" "$CPU_LOCK_FILE" \
  "$CPU_GUARD_RECORD" "$CPU_WORKLOAD_RECORD" <<'BENCHMARKOOR_ADMIN'
set -euo pipefail
candidate_dir=${1:?set candidate directory}
toolchain_id=${2:?set toolchain ID}
cpu_lock_file=${3:?set CPU lock file}
cpu_guard_record=${4:?set CPU recovery record}
cpu_workload_record=${5:?set CPU workload record}
toolchain_root=/usr/local/libexec/benchmarkoor
toolchain_dir="$toolchain_root/$toolchain_id"
install_tmp=

if test ! -e "$cpu_lock_file" && test ! -L "$cpu_lock_file"; then
  if (umask 077; set -o noclobber; : > "$cpu_lock_file") 2>/dev/null; then
    :
  else
    test -e "$cpu_lock_file" || test -L "$cpu_lock_file"
  fi
fi
test ! -L "$cpu_lock_file"
exec {cpu_lock_fd}<>"$cpu_lock_file"
/usr/bin/flock --exclusive "$cpu_lock_fd"
lock_fd_path="/proc/$$/fd/$cpu_lock_fd"
test -f "$cpu_lock_file"
test ! -L "$cpu_lock_file"
test -f "$lock_fd_path"
test "$(stat -Lc '%d:%i' -- "$lock_fd_path")" = \
  "$(stat -Lc '%d:%i' -- "$cpu_lock_file")"
test "$(stat -Lc '%u:%g:%h' -- "$lock_fd_path")" = 0:0:1
lock_mode=$(stat -Lc '%a' -- "$lock_fd_path")
case "$lock_mode" in
  600) ;;
  644)
    test ! -e "$cpu_guard_record"
    test ! -L "$cpu_guard_record"
    test ! -e "$cpu_workload_record"
    test ! -L "$cpu_workload_record"
    /bin/chmod 0600 "$lock_fd_path"
    ;;
  *) exit 1 ;;
esac
test "$(stat -Lc '%u:%g:%a:%h' -- "$lock_fd_path")" = 0:0:600:1
test "$(stat -Lc '%d:%i' -- "$lock_fd_path")" = \
  "$(stat -Lc '%d:%i' -- "$cpu_lock_file")"

cleanup_install_tmp() {
  if test -z "$install_tmp"; then
    return
  fi
  case "$install_tmp" in
    "$toolchain_root"/.toolchain-install."$toolchain_id".*) ;;
    *) return 2 ;;
  esac
  if test ! -e "$install_tmp" && test ! -L "$install_tmp"; then
    return
  fi
  test ! -L "$install_tmp"
  test -d "$install_tmp"
  chmod 0700 "$install_tmp"
  for installed_file in \
    restore-cpufreq-state run-with-cpufreq-lock benchmarkoor toolchain.sha256; do
    if test -e "$install_tmp/$installed_file" || \
      test -L "$install_tmp/$installed_file"; then
      test ! -d "$install_tmp/$installed_file"
      unlink -- "$install_tmp/$installed_file"
    fi
  done
  rmdir -- "$install_tmp"
}
trap cleanup_install_tmp EXIT
test ! -e "$cpu_guard_record"
test ! -L "$cpu_guard_record"
test ! -e "$cpu_workload_record"
test ! -L "$cpu_workload_record"
test "$(sha256sum "$candidate_dir/toolchain.sha256" | awk '{print $1}')" = \
  "$toolchain_id"

if test -e "$toolchain_dir" || test -L "$toolchain_dir"; then
  test ! -L "$toolchain_dir"
  test "$(realpath -e -- "$toolchain_dir")" = "$toolchain_dir"
  test "$(stat -Lc '%u:%g:%a' -- "$toolchain_dir")" = 0:0:555
  cmp --silent "$candidate_dir/toolchain.sha256" "$toolchain_dir/toolchain.sha256"
  for installed_file in restore-cpufreq-state run-with-cpufreq-lock benchmarkoor; do
    cmp --silent "$candidate_dir/$installed_file" "$toolchain_dir/$installed_file"
  done
else
  install_tmp=$(mktemp -d \
    "$toolchain_root/.toolchain-install.$toolchain_id.XXXXXXXXXX")
  test "$(stat -Lc '%u:%g:%a' -- "$install_tmp")" = 0:0:700
  for installed_file in restore-cpufreq-state run-with-cpufreq-lock benchmarkoor; do
    install -o root -g root -m 0555 \
      "$candidate_dir/$installed_file" "$install_tmp/$installed_file"
  done
  install -o root -g root -m 0444 \
    "$candidate_dir/toolchain.sha256" "$install_tmp/toolchain.sha256"
  for installed_file in restore-cpufreq-state run-with-cpufreq-lock benchmarkoor; do
    test "$(stat -Lc '%u:%g:%a:%h' -- "$install_tmp/$installed_file")" = \
      0:0:555:1
  done
  test "$(stat -Lc '%u:%g:%a:%h' -- "$install_tmp/toolchain.sha256")" = \
    0:0:444:1
  (cd "$install_tmp" && sha256sum --check --strict toolchain.sha256)
  chmod 0555 "$install_tmp"
  mv -T --no-clobber -- "$install_tmp" "$toolchain_dir"
  test ! -e "$install_tmp"
  test ! -L "$install_tmp"
  install_tmp=
fi

test "$(stat -Lc '%u:%g:%a' -- "$toolchain_dir")" = 0:0:555
for installed_file in restore-cpufreq-state run-with-cpufreq-lock benchmarkoor; do
  test "$(stat -Lc '%u:%g:%a:%h' -- "$toolchain_dir/$installed_file")" = \
    0:0:555:1
done
test "$(stat -Lc '%u:%g:%a:%h' -- "$toolchain_dir/toolchain.sha256")" = \
  0:0:444:1
(cd "$toolchain_dir" && sha256sum --check --strict toolchain.sha256)
trap - EXIT
BENCHMARKOOR_ADMIN
```

The manifest digest names the final directory. The root shell creates an absent lock with shell
no-clobber semantics, opens it without truncation, locks that descriptor, and proves the descriptor
and protected pathname still identify the same inode before doing any work. A concurrent launcher
can therefore only contend on that same inode. Installation populates and validates a temporary
sibling while holding the host-wide CPU lock, then publishes it with one rename. An interruption
can leave an identifiable temporary sibling but never a partial final path; the EXIT trap removes
the temporary sibling on ordinary failures. An existing digest directory is compared, never
overwritten. A different reviewed revision gets a different directory, so staging, measurement,
and recovery can keep using the exact bundle recorded for their task even while another revision
is installed alongside it. Retain the `TOOLCHAIN_ID` in task notes outside disposable paths.

```bash
set -euo pipefail
case "$-" in
  *x*) printf 'disable shell xtrace before loading credentials\n' >&2; exit 1 ;;
esac
FIXTURE_AUTH_MODE=${FIXTURE_AUTH_MODE:?set to none or github-token}
CPU_IDS=${CPU_IDS:?set the task-wide tuned CPU union as a comma list, or none}
TOOLCHAIN_ID=${TOOLCHAIN_ID:?set the administrator-approved toolchain digest}
[[ "$TOOLCHAIN_ID" =~ ^[0-9a-f]{64}$ ]] || exit 2
BENCHMARKOOR_SUDO=(sudo -n)
CPU_LOCK_DIR=/run/lock/benchmarkoor
CPU_LOCK_FILE="$CPU_LOCK_DIR/cpufreq.lock"
STAGING_EXIT_HOLD="$CPU_LOCK_DIR/staging-exit-hold"
TOOLCHAIN_DIR="/usr/local/libexec/benchmarkoor/$TOOLCHAIN_ID"
CPU_GUARD_HELPER="$TOOLCHAIN_DIR/restore-cpufreq-state"
CPU_GUARD_RUNNER="$TOOLCHAIN_DIR/run-with-cpufreq-lock"
GUARDED_BENCHMARKOOR="$TOOLCHAIN_DIR/benchmarkoor"
TOOLCHAIN_MANIFEST="$TOOLCHAIN_DIR/toolchain.sha256"
for privileged_dir in \
  /bin /usr /usr/bin /usr/local /usr/local/libexec \
  /usr/local/libexec/benchmarkoor; do
  test "$(stat -Lc '%u:%g' -- "$privileged_dir")" = 0:0 || exit 1
  privileged_mode=$(stat -Lc '%a' -- "$privileged_dir")
  case "$privileged_mode" in ''|*[!0-7]*) exit 2 ;; esac
  test "$((8#$privileged_mode & 0022))" -eq 0 || exit 1
done
test "$(realpath -e -- "$TOOLCHAIN_DIR")" = "$TOOLCHAIN_DIR" || exit 1
test "$(stat -Lc '%u:%g:%a' -- "$TOOLCHAIN_DIR")" = 0:0:555 || exit 1
test "$(realpath -e -- "$CPU_GUARD_HELPER")" = "$CPU_GUARD_HELPER" || exit 1
test "$(stat -Lc '%u:%g:%a:%h' -- "$CPU_GUARD_HELPER")" = 0:0:555:1 || exit 1
test "$(realpath -e -- "$CPU_GUARD_RUNNER")" = "$CPU_GUARD_RUNNER" || exit 1
test "$(stat -Lc '%u:%g:%a:%h' -- "$CPU_GUARD_RUNNER")" = 0:0:555:1 || exit 1
test "$(realpath -e -- "$GUARDED_BENCHMARKOOR")" = "$GUARDED_BENCHMARKOOR" || exit 1
test "$(stat -Lc '%u:%g:%a:%h' -- "$GUARDED_BENCHMARKOOR")" = 0:0:555:1 || exit 1
test "$(stat -Lc '%u:%g:%a:%h' -- /bin/bash)" = 0:0:755:1 || exit 1
test "$(stat -Lc '%u:%g:%a:%h' -- /usr/bin/python3)" = 0:0:755:1 || exit 1
test "$(stat -Lc '%u:%g:%a:%h' -- /usr/bin/docker)" = 0:0:755:1 || exit 1
test "$(realpath -e -- "$TOOLCHAIN_MANIFEST")" = "$TOOLCHAIN_MANIFEST" || exit 1
test "$(stat -Lc '%u:%g:%a:%h' -- "$TOOLCHAIN_MANIFEST")" = 0:0:444:1 || exit 1
test "$(sha256sum "$TOOLCHAIN_MANIFEST" | awk '{print $1}')" = \
  "$TOOLCHAIN_ID" || exit 1
test "$(awk '{print $2}' "$TOOLCHAIN_MANIFEST" | paste -sd, -)" = \
  restore-cpufreq-state,run-with-cpufreq-lock,benchmarkoor || exit 1
(cd "$TOOLCHAIN_DIR" && \
  /usr/bin/sha256sum --check --strict toolchain.sha256 >/dev/null) || exit 1
/usr/bin/python3 -c \
  'import os, signal; assert hasattr(os, "pidfd_open"); assert hasattr(signal, "pidfd_send_signal")' || exit 1
test "$(findmnt -n -T /sys/fs/cgroup -o FSTYPE)" = cgroup2 || exit 1
if test -e "$CPU_LOCK_FILE" || test -L "$CPU_LOCK_FILE"; then
  test ! -L "$CPU_LOCK_FILE" || exit 1
  test "$(stat -Lc '%u:%g:%a:%h' -- "$CPU_LOCK_FILE")" = 0:0:600:1 || exit 1
fi
test ! -e "$STAGING_EXIT_HOLD"
test ! -L "$STAGING_EXIT_HOLD"
case "$FIXTURE_AUTH_MODE" in
  none) ;;
  github-token)
    if test -z "${BENCHMARKOOR_RUNNER_GITHUB_TOKEN:-}"; then
      BENCHMARKOOR_RUNNER_GITHUB_TOKEN=$(gh auth token) || exit 1
    fi
    test -n "$BENCHMARKOOR_RUNNER_GITHUB_TOKEN" || exit 1
    export BENCHMARKOOR_RUNNER_GITHUB_TOKEN
    BENCHMARKOOR_SUDO+=(
      --preserve-env=BENCHMARKOOR_RUNNER_GITHUB_TOKEN
    )
    "${BENCHMARKOOR_SUDO[@]}" \
      "$CPU_GUARD_RUNNER" --check-github-token || exit 1
    ;;
  *) exit 2 ;;
esac
BENCHMARKOOR_RUN=(
  "${BENCHMARKOOR_SUDO[@]}"
  "$CPU_GUARD_RUNNER"
  "$CPU_IDS"
)
```

Use the same `BENCHMARKOOR_RUN` array and digest-scoped toolchain for staging, smoke, measured runs,
and recovery. The invoking user can edit either checkout, so privileged execution must never use a
mutable copy. Do not delete an installed bundle while any task notes, recovery record, or workload
record names it.

The launcher gives Benchmarkoor a fixed minimal environment. It drops every inherited
`BENCHMARKOOR_*` and Docker connection override, then adds only the forced local settings and the
optional GitHub token that crossed the explicit sudo boundary. Put required fork and path variables
in the ordered YAML stack; do not rely on ambient shell variables to complete or override a run.

Benchmarkoor `run` configs control root container commands and host paths. Therefore an account that
can call this launcher with caller-selected arguments is root-equivalent. Do not grant this launcher
through sudo to an account that is not already authorized for unrestricted root administration; an
exact-path sudo rule is command hygiene, not a privilege boundary. The launcher still accepts only
Benchmarkoor's `run` subcommand and its token probe, but that restriction does not make arbitrary
configs safe for an untrusted caller. Its token probe only confirms that a nonempty credential
crossed the same sudo boundary.

Before constructing `BENCHMARKOOR_RUN`, set `CPU_IDS` once to the union of numeric CPU IDs whose
frequency policy any staging, smoke, or measured command in this task can change. A command that
omits the performance-limit config does not narrow that task-wide union. For explicit `cpuset`
values, combine their unique IDs across all commands. If any command uses `cpuset_count`,
Benchmarkoor can select a new random subset while preparing each container, so the possible union
is every online CPU. The same all-online rule applies when any command configures CPU tuning without
a cpuset. Derive that conservative list without assuming a contiguous CPU range:

```bash
CPU_IDS=$(lscpu -p=CPU,ONLINE |
  awk -F, '$1 !~ /^#/ && $2 == "Y" {print $1}' |
  sort -n -u | paste -sd, -)
test -n "$CPU_IDS"
```

Use `none` only after proving that no command in the task has a CPU-frequency, governor, or turbo
setting. If the planned config stack changes, recompute the union and reconstruct the array before
launching another guarded command; never narrow it after staging. The runner still serializes an
untuned task with CPU-tuned tasks. Require the effective
`runner.cpu_sysfs_path` to be `/sys/devices/system/cpu`, the fixed trusted sysfs root enforced by the
helper.

The runner first creates `/run/lock/benchmarkoor/cpufreq-recovery.json` as a root-owned mode-0600
record of the live controls, then holds a root-owned mode-0600 host-wide lock while Benchmarkoor
runs. Before starting the binary, it records the toolchain digest, a unique child cgroup, and the
complete pre-run set of Benchmarkoor-labelled Docker container IDs in the root-owned mode-0600
`/run/lock/benchmarkoor/cpufreq-workload` gate. The child joins that cgroup before `exec`, so every
ordinary descendant is included from its first instruction. Docker moves client processes into
daemon-managed cgroups, so after the child cgroup empties the launcher observes an uninterrupted
30-second quiet window on the fixed local Docker daemon and queries labelled containers both before
and after it. Any matching Docker event, scan diagnostic, or container absent from the baseline
retains the gate. The runner releases the lock only to run its digest-matched recovery helper, which
restores and verifies the record before removing it. If the runner is killed or recovery fails, the
records remain; every later guarded run rejects them before starting. An unavailable lock or
outstanding recovery/workload record is a hard stop; do not bypass or delete either one. Before the
first run, also require the operator to confirm that no external tuning service or legacy benchmark
ignores this lock and controls the target CPUs.

The digest-scoped launcher runs inside sudo and starts the Benchmarkoor binary from its own bundle
as its direct child in a separate session. Before arming any guard, it opens a pidfd for itself,
sends signal 0 through it to probe the running kernel, and retains that descriptor as capacity for
the child pidfd. It first starts a dormant child behind a one-byte exec gate with termination signals
blocked. That child joins the workload cgroup and its own session but cannot exec Benchmarkoor until
the parent has opened its pidfd, restored signal delivery, and confirmed that cancellation did not
close the gate. The child unblocks termination signals only after authorization and then execs the
exact bundled binary, so an earlier cancellation never starts Benchmarkoor. The launcher closes the
CPU-lock file descriptor in the child and forwards `SIGHUP`, `SIGINT`, and `SIGTERM` through the
pidfd before waiting for cleanup.
It does not mistake the outer sudo monitor for Benchmarkoor. If that monitor exits while its
privileged child survives, the launcher continues holding the lock and recovery gate. Session
isolation prevents a foreground process-group signal from reaching Benchmarkoor both directly and
through the launcher, while the cgroup's `populated` state covers descendants that create another
process group. Require `/usr/bin/python3` to provide `os.pidfd_open` and
`signal.pidfd_send_signal`; the launcher's runtime probe is authoritative. Do not replace this with
numeric-PID signaling. If the reserved child-pidfd handoff nevertheless fails after the dormant
child starts, the launcher closes the exec gate, terminates and waits for that still-unreaped child,
and returns with recovery armed without authorizing Benchmarkoor. The launcher restores
automatically only after Benchmarkoor exits zero, its cgroup is empty, the Docker quiet-window check
passes, and Docker has no new Benchmarkoor-labelled container relative to the recorded baseline. A
nonzero exit, failed signal forward, unconfirmed child exit, populated cgroup, Docker scan or event
failure, or remaining container leaves recovery armed for task-scoped inspection. A cancellation
during post-run safety cleanup does not skip cleanup, but the launcher returns its signal-derived
status instead of reporting success.

The privileged launcher replaces both cleanup-on-start environment variables with `false`, forces
`runner.container_runtime: docker`, fixes `runner.cpu_sysfs_path` to the helper's trusted sysfs
root, and fixes Docker to the local root daemon before starting Benchmarkoor. This keeps its Docker
gate and CPU-control assumptions aligned with the effective run. Do not preserve the token for
commands that cannot download an artifact. If
sudo policy rejects the named environment preservation, stop and use a root-readable mode-0600
override containing `runner.github_token`, loaded last and only for the exact task. Keep that
override outside repositories, cache, results, and handoff artifacts; verify the exact Benchmarkoor
revision does not serialize the token, and remove the override after the process exits. Keep shell
tracing disabled while the token is present, unset the environment variable after the final
authenticated run, and never print the token or place it in a checked-in config.

Set `global.directories.cachedir` to a task-owned path outside every pristine/lower tree. It
defaults to `~/.cache/benchmarkoor`; standalone URLs are extracted below
`$CACHE_DIR/eest-url/<url-hash>/fixtures`. The hash directory is an internal detail, so discover it
from the run output instead of hard-coding it. A cache hit can suppress the download log; provenance
must still come from the pinned YAML and persisted suite summary.

After source preparation or the required smoke run, set `RESULTS_DIR` to that command's effective
`runner.benchmark.results_dir`. Select `RUN_DIR` only from its `runs` child; never infer the results
root from `$TESTS_DIR` or from an unrelated run. Then enumerate both the full archive inventory and
the fixtures selected by the current filter:

```bash
RESULTS_DIR=${RESULTS_DIR:?set to the effective runner.benchmark.results_dir}
RESULTS_DIR=$(realpath -e -- "$RESULTS_DIR")
RUNS_DIR=$(realpath -e -- "$RESULTS_DIR/runs")
RUN_DIR=${RUN_DIR:?set to a smoke or measured run below RESULTS_DIR/runs}
RUN_DIR=$(realpath -e -- "$RUN_DIR")
test "$(dirname -- "$RUN_DIR")" = "$RUNS_DIR"
SUITE_HASH=$(jq -r .suite_hash "$RUN_DIR/config.json")
SUITE_DIR="$RESULTS_DIR/suites/$SUITE_HASH"
SUITE_SUMMARY="$SUITE_DIR/summary.json"
EEST_INDEX="$SUITE_DIR/.eest-meta/index.json"

test -f "$SUITE_SUMMARY"
test -f "$EEST_INDEX"
jq '{source, filter, resolved_tests: (.tests | length), eest_metadata}' "$SUITE_SUMMARY"
jq '{archive_test_count: .test_count, indexed_cases: (.test_cases | length)}' "$EEST_INDEX"
jq -r '.test_cases[].id' "$EEST_INDEX"
```

`.eest-meta/index.json` is copied from the extracted artifact and lists the source archive's full
fixture inventory even when the smoke or measured suite is filtered. `summary.json.tests` lists
only the fixtures selected for that suite hash.

To run every fixture in the selected source, assemble a measured config stack in which no loaded
file sets `runner.benchmark.tests.filter`; specifically, do not reuse the smoke/subset override.
Require the persisted summary filter to be empty and the selected count to equal the archive count:

```bash
test "$(jq -r '.filter // ""' "$SUITE_SUMMARY")" = ""
test "$(jq -r '.tests | length' "$SUITE_SUMMARY")" -eq \
  "$(jq -r '.test_count' "$EEST_INDEX")"
```

For a subset, retain the explicit filter and compare the resolved names against the intended IDs.
Never present a filtered count or suite hash as the full artifact.

## Run the selected suite

Check the read-only lower, image, free space, and residual state first:

```bash
findmnt -n -o SOURCE,OPTIONS "$LOWER_DIR"
findmnt -rn -t overlay | rg "$OVERLAY_TMP|benchmarkoor-overlay" || true
"${DOCKER[@]}" image inspect "$LOCAL_IMAGE_ID"
"${DOCKER[@]}" ps --format '{{.Names}} {{.Image}} {{.Status}}'
df -h "$OVERLAY_TMP"
```

Require `ro` in the lower mount options and no unexplained overlay or client container. For a
State Actor run, resolve these files from the checked-out revision, preserve this config order, and
keep the local override last:

```bash
RESOURCE_LIMIT_CONFIG=${RESOURCE_LIMIT_CONFIG:?set the requested resource-limit config}
DATADIR_GLOBAL_CONFIG=${DATADIR_GLOBAL_CONFIG:?set the State Actor datadir global config}
DATADIR_RUNNER_CONFIG=${DATADIR_RUNNER_CONFIG:?set the State Actor datadir runner config}
LOCAL_RUN_OVERRIDE=${LOCAL_RUN_OVERRIDE:?set the complete local override}
GLOBAL_CONFIG=$(realpath -e -- "$TESTS_DIR/configs/global.yaml")
RESOURCE_LIMIT_CONFIG=$(realpath -e -- "$RESOURCE_LIMIT_CONFIG")
DATADIR_GLOBAL_CONFIG=$(realpath -e -- "$DATADIR_GLOBAL_CONFIG")
DATADIR_RUNNER_CONFIG=$(realpath -e -- "$DATADIR_RUNNER_CONFIG")
LOCAL_RUN_OVERRIDE=$(realpath -e -- "$LOCAL_RUN_OVERRIDE")
declare -p BENCHMARKOOR_SUDO >/dev/null 2>&1 || exit 1
declare -p BENCHMARKOOR_RUN >/dev/null 2>&1 || exit 1

cd "$TESTS_DIR" || exit 1
"${BENCHMARKOOR_RUN[@]}" run \
  --config "$GLOBAL_CONFIG" \
  --config "$RESOURCE_LIMIT_CONFIG" \
  --config "$DATADIR_GLOBAL_CONFIG" \
  --config "$DATADIR_RUNNER_CONFIG" \
  --config "$CONTEXT_GLOBAL_CONFIG" \
  --config "$SUITE_RUNNER_CONFIG" \
  --config "$CLIENTS_CONFIG" \
  --config "$LOCAL_RUN_OVERRIDE" \
  --limit-instance-client=erigon \
  --limit-instance-id="$INSTANCE_ID"
```

For stateful, confirm the Benchmarkoor revision eagerly removes each previous container and
OverlayFS datadir before preparing the next. During a smoke run, require at most one live matching
overlay mount. Compute does not recreate per fixture, but its single upper layer is still disposable
and must never be the pristine directory.

## Validate the result

After exit, validate the persisted result rather than trusting the input variables:

```bash
jq '{suite_hash,status,test_counts,instance:{
  id:.instance.id,
  rollback_strategy:.instance.rollback_strategy,
  resource_limits:.instance.resource_limits,
  datadir:.instance.datadir
}}' "$RUN_DIR/config.json"

jq -r '.tests | keys[0:5][]' "$RUN_DIR/result.json"
rg 'Downloading fixtures tarball|Suite output created|Running tests with' \
  "$RUN_DIR/benchmarkoor.log"
```

Require compute result keys and rollback behavior to match compute, or stateful keys and rollback
behavior to match stateful. Confirm the resolved test count is nonzero, pass/fail totals are final,
the datadir method is `overlayfs`, the lower remained read-only, and no overlay mounts or benchmark
containers remain.

## Recover from interruption

This recovery applies only when no deliberately retained staging overlay exists. In the final local
override, set and verify `runner.cleanup_on_start: false`; the inherited true value performs forced,
host-wide cleanup before the task starts. Before launch, create a task record and capture the exact
benchmarkoor process at start through a pidfd or child-process handle that remains stable after PID
reuse. Also record its numeric PID together with the Linux boot ID, start-time ticks from
`/proc/<pid>/stat`, and device/inode identity of `/proc/<pid>/exe` for diagnosis. As task containers
and disposable overlays are created, append each exact container ID, mount target, and directory
below the canonical `OVERLAY_TMP`.

On interruption, use only a retained pidfd or child-process handle whose signaling API guarantees
that it still refers to the launched process. If no stable handle was retained, do not send a signal
and stop for operator input; `/proc/<pid>` fields can prove that a process was once the target but
cannot close the exit-and-PID-reuse race before `kill(2)`. If the stable handle reports that the
process exited, do not fall back to its numeric PID. Otherwise send `SIGINT` through that handle and
wait on the same handle for normal cleanup. If it cannot finish, stop only the recorded task
containers so their log streams drain, then wait through the handle again. Every later instruction
in this reference to signal or stop a recorded process requires this stable handle; a checked bare
numeric PID is never sufficient. Require every task process and open handle to be gone before
unmounting anything.

Resolve each recorded mount again and prove that its target and `upperdir`/`workdir` belong to this
task's `OVERLAY_TMP`. Unmount only those exact targets, remove only their verified task-owned
directories, and remove only the recorded stopped containers. If any ownership or path mapping is
uncertain, stop and ask the user. Keep the protected lower mounted until its final integrity checks
pass. Never run `benchmarkoor cleanup --force` on a shared host: it is not scoped by the config
stack and can remove resources belonging to other runs.

Require every concurrent Benchmarkoor task on the host to use this guarded protocol and common
lock. The helper opens only the fixed lock, fixed root-owned recovery/workload records, the cgroup
named by the root-created workload record, the fixed local Docker endpoint, and controls that
resolve below the trusted sysfs mount. It rejects records created by another toolchain digest.
Require no `benchmarkoor-cpufreq-*.json` file to exist in the private task `CACHE_DIR` at launch.

The guarded runner invokes automatic recovery only after a clean zero exit with an empty
Benchmarkoor cgroup, a complete Docker quiet window, and no Docker container added after its recorded
baseline. If it reports exit 125, is killed, or leaves any root-owned gate, do not start another run.
Exit 125 can also follow a startup failure for which automatic recovery already succeeded, so first
test every fixed record.
After the exact Benchmarkoor process, its cgroup, every container reported by the workload gate,
and inherited lock holders have exited, use the exact digest-matched helper to verify the workload
and Docker domains before restoring CPU controls:

```bash
set -euo pipefail
TOOLCHAIN_ID=${TOOLCHAIN_ID:?restore with the digest recorded for this task}
[[ "$TOOLCHAIN_ID" =~ ^[0-9a-f]{64}$ ]] || exit 2
CPU_GUARD_HELPER="/usr/local/libexec/benchmarkoor/$TOOLCHAIN_ID/restore-cpufreq-state"
CPU_GUARD_RECORD=/run/lock/benchmarkoor/cpufreq-recovery.json
CPU_WORKLOAD_RECORD=/run/lock/benchmarkoor/cpufreq-workload
STAGING_EXIT_HOLD=/run/lock/benchmarkoor/staging-exit-hold
test "$(realpath -e -- "$CPU_GUARD_HELPER")" = "$CPU_GUARD_HELPER" || exit 1
if test -e "$CPU_WORKLOAD_RECORD" || test -L "$CPU_WORKLOAD_RECORD"; then
  sudo -n "$CPU_GUARD_HELPER" clear-workload
fi
if test -e "$CPU_GUARD_RECORD" || test -L "$CPU_GUARD_RECORD"; then
  sudo -n "$CPU_GUARD_HELPER" recover
fi
test ! -e "$CPU_WORKLOAD_RECORD"
test ! -L "$CPU_WORKLOAD_RECORD"
test ! -e "$CPU_GUARD_RECORD"
test ! -L "$CPU_GUARD_RECORD"
test ! -e "$STAGING_EXIT_HOLD"
test ! -L "$STAGING_EXIT_HOLD"
```

`clear-workload` uses only the fixed root-owned record and refuses a populated cgroup. It removes
the empty workload cgroup before observing the fixed Docker daemon, so a winding-down descendant
cannot submit another request. It takes an initial labelled-container snapshot, observes 30 seconds
of labelled Docker events, and then takes a second snapshot. Any event, diagnostic, timeout, or
current Benchmarkoor-labelled Docker ID absent from the recorded pre-run baseline retains the gate;
retry only after inspecting the exact task resources and allowing another complete quiet window. If
the cgroup or its transient parent has disappeared, the helper still validates the fixed cgroup v2
root and performs the same two scans and event window before clearing the stale gate. For an
interrupted staging command, it also validates and removes that toolchain's fixed staging hold only
after the workload and Docker gates pass.
Inspect, stop, and remove only the exact reported containers, then repeat `clear-workload`. The
helper rejects a
record from another boot, sudo user, or toolchain digest, attempts every independent control after a
write failure, chooses a feasible order for frequency bounds, verifies every write, re-verifies the
complete recovered state to detect shared-policy conflicts, and removes the guard record only after
complete success. It does not trust or open a caller-selected state, lock, cgroup, Docker endpoint,
or sysfs path.

After guard recovery succeeds, inspect only the private task cache. Current Benchmarkoor may leave
one redundant state file after its own cleanup failure. More than one is ambiguous and must stop;
for exactly one, prove it is a direct child of `CACHE_DIR`, then unlink it as the invoking user:

```bash
CACHE_DIR=$(realpath -e -- "$CACHE_DIR")
mapfile -t TASK_CPU_STATE_FILES < <(
  find "$CACHE_DIR" -maxdepth 1 -type f \
    -name 'benchmarkoor-cpufreq-*.json' -print | sort
)
test "${#TASK_CPU_STATE_FILES[@]}" -le 1 || exit 1
if test "${#TASK_CPU_STATE_FILES[@]}" -eq 1; then
  CPU_STATE_FILE=$(realpath -e -- "${TASK_CPU_STATE_FILES[0]}")
  test "$(dirname -- "$CPU_STATE_FILE")" = "$CACHE_DIR" || exit 1
  unlink -- "$CPU_STATE_FILE"
fi
```

Do not resume benchmarking, validate the run as successful, or tear down the task record while the
fixed recovery record remains, its helper cannot verify every setting, or a Benchmarkoor state file
remains.

## Pre-populated snapshot workflow

Use this variant when the suite starts from an existing bloated, pruned, or otherwise pre-populated
datadir instead of State Actor. Resolve the current authoritative configuration at task time from
the checked-out repositories and, when supplied, the hosted run. Record its benchmarkoor and
benchmarkoor-tests commits, image digest and embedded client commit, ordered configs, genesis, fork
override, arguments, fixture source and digest, filter, rollback strategy, resource limits, base
head, and pre-run end head in the run notes. Do not embed a hosted run URL, suite hash, fixture URL,
or test count in this skill: those values are revision-specific and must be rediscovered. Keep any
hosted API token in a protected secret source; never print it or include it in repositories,
results, logs, or handoff artifacts.

An old hosted run may use `schelk`. Match its dataset, fixtures, client, and limits while replacing
only the storage mechanism with OverlayFS. Check historical arguments against the current Erigon
source and image help so removed flags do not survive in the local override.

### Select the dataset context

Use the source, context, and clients selected by the discovery section. Resolve the matching
pre-populated datadir global from the authoritative ordered config stack; do not substitute the
State Actor datadir configs or choose a datadir merely because its directory name looks familiar:

```bash
DATADIRS_ROOT="$TESTS_DIR/configs/datadirs"
mapfile -t DATADIR_GLOBAL_CANDIDATES < <(
  rg --files "$DATADIRS_ROOT" | rg '/global\.ya?ml$' | sort
)
printf '%s\n' "${DATADIR_GLOBAL_CANDIDATES[@]}"

DATASET_DATADIR_GLOBAL=${DATASET_DATADIR_GLOBAL:?set from the authoritative config stack}
DATASET_DATADIR_GLOBAL=$(realpath -e -- "$DATASET_DATADIR_GLOBAL")
DATADIR_MATCH=false
for candidate in "${DATADIR_GLOBAL_CANDIDATES[@]}"; do
  if test "$(realpath -e -- "$candidate")" = "$DATASET_DATADIR_GLOBAL"; then
    DATADIR_MATCH=true
    break
  fi
done
test "$DATADIR_MATCH" = true || exit 1

DATASET_CONTEXT_GLOBAL="$CONTEXT_GLOBAL_CONFIG"
DATASET_CLIENTS_CONFIG="$CLIENTS_CONFIG"
DATASET_SOURCE_CONFIG="$SUITE_RUNNER_CONFIG"

test -f "$DATASET_DATADIR_GLOBAL" || exit 1
test -f "$DATASET_CONTEXT_GLOBAL" || exit 1
test -f "$DATASET_CLIENTS_CONFIG" || exit 1
test -f "$DATASET_SOURCE_CONFIG" || exit 1

SOURCE_CONFIG_YAML=$(yq '.' "$DATASET_SOURCE_CONFIG") || exit 1
SELECTED_INSTANCE_YAML=$(INSTANCE_ID="$INSTANCE_ID" yq \
  '.runner.instances[] | select(.id == strenv(INSTANCE_ID) and .client == "erigon")' \
  "$DATASET_CLIENTS_CONFIG") || exit 1
printf '%s\n%s\n' "$SOURCE_CONFIG_YAML" "$SELECTED_INSTANCE_YAML" |
  awk '{
    braced = $0
    while (match(braced, /\$\{[A-Za-z_][A-Za-z0-9_]*\}/)) {
      name = substr(braced, RSTART + 2, RLENGTH - 3)
      print name
      braced = substr(braced, RSTART + RLENGTH)
    }
    bare = $0
    while (match(bare, /\$[A-Za-z_][A-Za-z0-9_]*/)) {
      name = substr(bare, RSTART + 1, RLENGTH - 1)
      print name
      bare = substr(bare, RSTART + RLENGTH)
    }
  }' |
  sort -u
```

This emits required `${VAR}` and `$VAR` references as normalized names. It intentionally omits
`${VAR:-default}` because Benchmarkoor can resolve that form without a definition. Require every
listed variable used by the selected source and instance to be defined by an earlier global config
in both staging and measured stacks. Resolve the selected fork activation from those globals and
client arguments, whatever the fork or variable name is. After launch, require the persisted
Erigon command to contain the resolved fork override exactly; an empty, inherited from a different
context, or otherwise different value is a hard failure.

### Protect and identify the original

Require the operator-designated pristine datadir and a task-owned runtime root, then canonicalize
them. The runtime root may have any name and may be on another filesystem, but it must not contain
or be contained by the pristine directory:

```bash
PRISTINE_DIR=${PRISTINE_DIR:?operator must designate the pristine datadir}
RUN_ROOT=${RUN_ROOT:?set a task-owned runtime root}
PRISTINE_DIR=$(realpath -e -- "$PRISTINE_DIR")
RUN_ROOT=$(realpath -m -- "$RUN_ROOT")

case "$RUN_ROOT/" in "$PRISTINE_DIR/"*) exit 1 ;; esac
case "$PRISTINE_DIR/" in "$RUN_ROOT/"*) exit 1 ;; esac

ORIGINAL_LOWER="$RUN_ROOT/lower-ro"
ADVANCED_LOWER="$RUN_ROOT/advanced-lower-ro"
OVERLAY_TMP="$RUN_ROOT/overlay-runtime"
LOWER_DIR="$ORIGINAL_LOWER"
```

Before binding, run the main skill's descendant-mount gate and stop if any mount target is below
`PRISTINE_DIR`; do not let a plain bind hide separately mounted state. Then apply the main skill's
read-only bind, probe-overlay canary, sizes, full-tree metadata fingerprint, and
dataset-appropriate critical hashes before starting a client. Set `INTEGRITY_ROOT` to
`ORIGINAL_LOWER` after its read-only remount, and use that path for every traversal, `du`, content
hash, and before/after fingerprint. Never collect integrity data through `PRISTINE_DIR`: reads on a
writable `relatime` mount can mutate atime. Retain the resulting baseline fingerprint, sizes, path
count, and critical hashes in task notes outside the disposable `RUN_ROOT`; interrupted-staging
recovery must be able to compare a newly recreated guard with this same baseline. Require `ro` as a
distinct mount option first, then require an expected-failure write probe through the protected
bind without ever writing through or inspecting the original path:

```bash
INTEGRITY_ROOT="$ORIGINAL_LOWER"
READONLY_CANARY=.benchmarkoor-readonly-canary
test ! -e "$INTEGRITY_ROOT/$READONLY_CANARY"
test ! -L "$INTEGRITY_ROOT/$READONLY_CANARY"
! sudo touch "$INTEGRITY_ROOT/$READONLY_CANARY"
test ! -e "$INTEGRITY_ROOT/$READONLY_CANARY"
test ! -L "$INTEGRITY_ROOT/$READONLY_CANARY"
```

For an Erigon pre-populated datadir, include stable sidecars and lock files plus a full hash of
`chaindata/mdbx.dat` when feasible; the State Actor filenames in the main skill need not exist.
Keep every upper, work, cache, result, and probe path outside the pristine and lower trees.

The bind protects accesses through `ORIGINAL_LOWER`; the raw pristine path is still writable on the
host. Before starting, require no process has an open file below `PRISTINE_DIR`, no container in
any lifecycle state uses it as a mount source, and no alternate bind exposes it to a workload.
Inspect host handles, mount tables, and all container definitions rather than checking names or
only running containers. A stopped container can later restart with its old writable bind. For the
required Docker runtime, reject a mount source equal to, below, or above the pristine path because
an ancestor mount also exposes the snapshot:

```bash
HOST_INSPECT=(sudo -n)
assert_no_container_path_exposure() {
  local protected_path=${1:?set protected path}
  local protected_label=${2:-protected path}
  local DOCKER_CONTAINER_IDS container_id container_mounts container_mount
  local mount_type mount_source mount_name volume_config volume_driver
  local volume_mountpoint volume_type volume_device volume_options
  local candidate_record candidate_kind candidate_path path_exposed
  local -a mount_candidates

  protected_path=$("${HOST_INSPECT[@]}" realpath -e -- "$protected_path") || return 1
DOCKER_CONTAINER_IDS=$("${ROOT_DOCKER[@]}" ps -aq) || exit 1
while IFS= read -r container_id; do
  test -n "$container_id" || continue
  container_mounts=$("${ROOT_DOCKER[@]}" inspect "$container_id" |
    jq -ce '[.[0].Mounts[]? | {
      type: (.Type // ""), name: (.Name // ""), source: (.Source // ""),
      destination: (.Destination // ""), rw: .RW
    }]') || exit 1
  while IFS= read -r container_mount; do
    mount_type=$(jq -r '.type' <<<"$container_mount") || exit 1
    mount_source=$(jq -r '.source' <<<"$container_mount") || exit 1
    mount_name=$(jq -r '.name' <<<"$container_mount") || exit 1
    mount_candidates=()
    test -z "$mount_source" || mount_candidates+=("source:$mount_source")

    if test "$mount_type" = volume; then
      test -n "$mount_name" || exit 1
      volume_config=$("${ROOT_DOCKER[@]}" volume inspect "$mount_name" |
        jq -ce '.[0] | {
          driver: (.Driver // ""), mountpoint: (.Mountpoint // ""),
          type: (.Options.type // ""),
          device: (.Options.device // ""),
          options: (.Options.o // "")
        }') || exit 1
      volume_driver=$(jq -r '.driver' <<<"$volume_config") || exit 1
      volume_mountpoint=$(jq -r '.mountpoint' <<<"$volume_config") || exit 1
      volume_type=$(jq -r '.type' <<<"$volume_config") || exit 1
      volume_device=$(jq -r '.device' <<<"$volume_config") || exit 1
      volume_options=$(jq -r '.options' <<<"$volume_config") || exit 1
      test -z "$volume_mountpoint" || \
        mount_candidates+=("volume-mountpoint:$volume_mountpoint")
      if test "$volume_driver" = local; then
        if test -n "$volume_type" || test -n "$volume_device" || \
          test -n "$volume_options"; then
          test "$volume_type" = none || {
            printf 'rejecting opaque local-volume filesystem type %s for container %s\n' \
              "$volume_type" "$container_id" >&2
            exit 1
          }
          case ",$volume_options," in
            *,bind,*|*,rbind,*) ;;
            *)
              printf 'rejecting non-bind local-volume options for container %s\n' \
                "$container_id" >&2
              exit 1
              ;;
          esac
          case "$volume_device" in
            /*) mount_candidates+=("volume-bind-device:$volume_device") ;;
            *)
              printf 'cannot resolve local-volume bind device %s for container %s\n' \
                "$volume_device" "$container_id" >&2
              exit 1
              ;;
          esac
        fi
      else
        printf 'inspect non-local volume driver %s for container %s before proceeding\n' \
          "$volume_driver" "$container_id" >&2
        exit 1
      fi
    fi

    for candidate_record in "${mount_candidates[@]}"; do
      candidate_kind=${candidate_record%%:*}
      candidate_path=${candidate_record#*:}
      candidate_path=$("${HOST_INSPECT[@]}" realpath -e -- "$candidate_path") || exit 1
      path_exposed=false
      if test "$candidate_path" = /; then
        path_exposed=true
      else
        case "$candidate_path/" in "$protected_path/"*) path_exposed=true ;; esac
        case "$protected_path/" in "$candidate_path/"*) path_exposed=true ;; esac
      fi
      if test "$path_exposed" = true; then
        printf 'container %s exposes %s through %s %s\n' \
          "$container_id" "$protected_label" "$candidate_kind" \
          "$candidate_path" >&2
        exit 1
      fi
    done
  done < <(jq -c '.[]' <<<"$container_mounts")
done <<<"$DOCKER_CONTAINER_IDS"
}

assert_no_container_path_exposure "$PRISTINE_DIR" 'pristine state' || exit 1
```

Only a plain local volume or an explicit absolute `type=none,o=bind|rbind` device is accepted.
Reject local-driver block-device, NFS, CIFS, tmpfs, and other filesystem mounts: comparing their
device strings with a datadir path cannot prove that they do not expose the pristine filesystem.

Repeat the equivalent all-state inspection for every other installed runtime and for each rootful
or rootless runtime namespace that an operator could restart. Do not stop, remove, or rewrite an
unrelated container; coordinate with its owner or choose another snapshot. Perform any required
tree traversal through `ORIGINAL_LOWER`. During staging and measurement, keep a recursive
write-event watcher on `ORIGINAL_LOWER` and periodically repeat the handle, mount-table, and
all-state container-definition checks. The function is deliberately path-parametric: after the
staging container has been removed, call it again for `RUN_ROOT`, and repeat the equivalent check
for other runtimes and pre-existing mount namespaces. A writable bind whose source is `RUN_ROOT`,
the staging tree, or any ancestor is an alias of the staged upper/work trees; reject it even when
its destination has a different name.

With `method: overlayfs`, the container must mount a task-owned `merged` directory, not either
protected lower directly. Verify the container's datadir source and the corresponding host overlay
as one chain. Use `ORIGINAL_LOWER` as `expected_lower` for compute and during stateful staging. Use
the `MEASURED_LOWER` selected below for stateful smoke and measured runs: `ADVANCED_LOWER` after
staging, or `ORIGINAL_LOWER` when staging is skipped. Replace `/data` only when the effective client
config uses another datadir target. Benchmarkoor's privileged process normally creates its
temporary overlay directory as root-owned mode 0700, so inspect it through the same non-interactive
privilege boundary; never `chmod` or `chown` a retained overlay merely to inspect it:

```bash
OVERLAY_INSPECT=(sudo -n)

verify_task_overlay() {
  local container_id=${1:?set container ID}
  local expected_lower=${2:?set expected read-only lower}
  local container_datadir=${3:-/data}
  local -a data_sources
  local merged merged_base mount_target overlay_options
  local lowerdir upperdir workdir lower_options
  local resolved_mount_target resolved_merged resolved_lowerdir resolved_expected_lower
  local resolved_overlay_tmp resolved_upperdir resolved_workdir

  mapfile -t data_sources < <(
    "${ROOT_DOCKER[@]}" inspect "$container_id" |
      jq -r --arg target "$container_datadir" \
        '.[0].Mounts[] | select(.Destination == $target) | .Source'
  )
  test "${#data_sources[@]}" -eq 1 || return 1
  merged=${data_sources[0]}
  case "$merged" in
    "$OVERLAY_TMP"/benchmarkoor-overlay-*/merged) ;;
    *) return 1 ;;
  esac

  test "$("${OVERLAY_INSPECT[@]}" findmnt -n -T "$merged" -o FSTYPE)" = overlay || return 1
  mount_target=$("${OVERLAY_INSPECT[@]}" findmnt -n -T "$merged" -o TARGET) || return 1
  resolved_mount_target=$("${OVERLAY_INSPECT[@]}" realpath -e -- "$mount_target") || return 1
  resolved_merged=$("${OVERLAY_INSPECT[@]}" realpath -e -- "$merged") || return 1
  test "$resolved_mount_target" = "$resolved_merged" || return 1
  resolved_overlay_tmp=$("${OVERLAY_INSPECT[@]}" realpath -e -- "$OVERLAY_TMP") || return 1
  case "$resolved_merged" in
    "$resolved_overlay_tmp"/benchmarkoor-overlay-*/merged) ;;
    *) return 1 ;;
  esac

  overlay_options=$("${OVERLAY_INSPECT[@]}" findmnt -n -T "$merged" -o OPTIONS) || return 1
  lowerdir=${overlay_options#*lowerdir=}
  test "$lowerdir" != "$overlay_options" || return 1
  lowerdir=${lowerdir%%,*}
  upperdir=${overlay_options#*upperdir=}
  test "$upperdir" != "$overlay_options" || return 1
  upperdir=${upperdir%%,*}
  workdir=${overlay_options#*workdir=}
  test "$workdir" != "$overlay_options" || return 1
  workdir=${workdir%%,*}
  resolved_lowerdir=$("${OVERLAY_INSPECT[@]}" realpath -e -- "$lowerdir") || return 1
  resolved_expected_lower=$("${OVERLAY_INSPECT[@]}" realpath -e -- "$expected_lower") || return 1
  test "$resolved_lowerdir" = "$resolved_expected_lower" || return 1
  resolved_upperdir=$("${OVERLAY_INSPECT[@]}" realpath -e -- "$upperdir") || return 1
  resolved_workdir=$("${OVERLAY_INSPECT[@]}" realpath -e -- "$workdir") || return 1
  merged_base=$(dirname -- "$resolved_merged")
  test "$resolved_upperdir" = "$merged_base/upper" || return 1
  test "$resolved_workdir" = "$merged_base/work" || return 1

  lower_options=$("${OVERLAY_INSPECT[@]}" findmnt -n -T "$expected_lower" -o OPTIONS) || return 1
  case ",$lower_options," in
    *,ro,*) ;;
    *) return 1 ;;
  esac

  printf '%s\n' "$resolved_merged" "$resolved_upperdir" "$resolved_workdir"
}

verify_staging_aux_mounts() {
  local container_id=${1:?set container ID}
  local expected_merged=${2:?set verified merged path}
  local container_datadir=${3:-/data}
  local inspect_json mounts_json mount_json mount_type mount_source mount_destination
  local resolved_expected_merged resolved_source source_parent
  local resolved_cache_dir aux_dir=
  local datadir_mounts=0
  local -a aux_sources=()

  case "$INSTANCE_ID" in ''|*[!A-Za-z0-9._-]*) return 1 ;; esac
  resolved_expected_merged=$("${OVERLAY_INSPECT[@]}" realpath -e -- \
    "$expected_merged") || return 1
  resolved_cache_dir=$("${OVERLAY_INSPECT[@]}" realpath -e -- "$CACHE_DIR") || return 1
  inspect_json=$("${ROOT_DOCKER[@]}" inspect "$container_id") || return 1
  mounts_json=$(jq -ce '.[0].Mounts // []' <<<"$inspect_json") || return 1

  while IFS= read -r mount_json; do
    mount_type=$(jq -er '.Type | strings' <<<"$mount_json") || return 1
    mount_source=$(jq -er '.Source | strings' <<<"$mount_json") || return 1
    mount_destination=$(jq -er '.Destination | strings' <<<"$mount_json") || return 1
    test -n "$mount_source" || return 1
    test -n "$mount_destination" || return 1

    if test "$mount_destination" = "$container_datadir"; then
      test "$mount_type" = bind || return 1
      resolved_source=$("${OVERLAY_INSPECT[@]}" realpath -e -- "$mount_source") || return 1
      test "$resolved_source" = "$resolved_expected_merged" || return 1
      datadir_mounts=$((datadir_mounts + 1))
      continue
    fi
    case "$mount_destination" in
      "$container_datadir"/*) return 1 ;;
    esac
    test "$mount_type" = bind || continue

    "${OVERLAY_INSPECT[@]}" test ! -L "$mount_source" || return 1
    "${OVERLAY_INSPECT[@]}" test -f "$mount_source" || return 1
    resolved_source=$("${OVERLAY_INSPECT[@]}" realpath -e -- "$mount_source") || return 1
    source_parent=$(dirname -- "$resolved_source")
    if test -z "$aux_dir"; then
      aux_dir=$source_parent
    else
      test "$source_parent" = "$aux_dir" || return 1
    fi
    test "$("${OVERLAY_INSPECT[@]}" stat -Lc '%u:%g:%h' -- "$resolved_source")" = \
      0:0:1 || return 1
    aux_sources+=("$resolved_source")
  done < <(jq -c '.[]' <<<"$mounts_json")

  test "$datadir_mounts" -eq 1 || return 1
  test "${#aux_sources[@]}" -gt 0 || return 1
  test "${#aux_sources[@]}" -eq \
    "$(printf '%s\n' "${aux_sources[@]}" | sort -u | wc -l)" || return 1
  test "$(dirname -- "$aux_dir")" = "$resolved_cache_dir" || return 1
  case "$aux_dir" in
    "$resolved_cache_dir"/benchmarkoor-"$INSTANCE_ID"-*) ;;
    *) return 1 ;;
  esac
  "${OVERLAY_INSPECT[@]}" test ! -L "$aux_dir" || return 1
  "${OVERLAY_INSPECT[@]}" test -d "$aux_dir" || return 1
  test "$("${OVERLAY_INSPECT[@]}" realpath -e -- "$aux_dir")" = "$aux_dir" || return 1
  test "$("${OVERLAY_INSPECT[@]}" stat -Lc '%u:%g:%a' -- "$aux_dir")" = \
    0:0:700 || return 1

  printf '%s\n' "$aux_dir"
  printf '%s\n' "${aux_sources[@]}" | sort
}
```

The verifier prints the canonical merged, upper, and work paths on separate lines. Require exactly
one datadir source from Docker inspection, capture all three paths, and repeat this verification
whenever benchmarkoor recreates the client. Neither protected lower should appear directly in a
task container's mount sources. Use privileged inspection for every later `realpath`, `findmnt`,
`du`, open-handle scan, and canary operation that addresses a Benchmarkoor-created overlay
directory. On any unexplained access, mount chain, or write event, stop the exact task-owned process
and ask the user; do not stop or alter the unrelated accessor. Replace `OVERLAY_INSPECT` with an
approved allowlisted helper when the host does not grant direct non-interactive sudo for these
commands. `verify_staging_aux_mounts` is only for the retained stateful staging container. It
records the root-created directory that holds staging-only bind sources such as copied genesis and
JWT files, plus every exact file source, before Docker removes the container metadata. Capture
volume mounts separately: client images can declare inherited anonymous volumes that an ordinary
`docker rm` leaves behind.

Download sidecars can be stale after bloating. Resolve the authoritative raw-base block, hash, and
state root from the selected source and effective reference configuration. If that source has
`pre_runs`, also resolve its authoritative post-pre-run end block, hash, and state root. Do not
infer either tuple from a filename or a sidecar alone.

Boot the exact client only through a disposable OverlayFS over `ORIGINAL_LOWER`, query
`eth_getBlockByNumber("latest")`, and record its block, hash, and state root. Stop it gracefully and
remove its exact container and overlay. Normalize block-number representation and hash case, then
compare all three fields. Set `SNAPSHOT_STATE=post-prerun` when the live tuple matches the pinned
post-pre-run tuple; otherwise set `SNAPSHOT_STATE=raw-base` when it matches the pinned raw tuple. If
the source has no `pre_runs`, only `raw-base` is valid. Reject an empty tuple, a partial match, and
every other head. Record the classification and both authoritative tuples in the run notes.

### Replace the remote datadir config

Do not load a hosted `schelk` runner file and override only `method`: Viper retains sibling
`schelk_options`, yielding an invalid mixed config. Load the dataset's global/genesis file and
supply a complete local map after the selected test source and clients. Test-source files can set
`runner.container_runtime` and `data-disk-type`; if loaded later, those values override Docker and
OverlayFS settings from an earlier local map:

```yaml
global:
  env:
    ERIGON_SNAPSHOT_DIR: /absolute/path/to/read-only-lower
  directories:
    cachedir: /absolute/path/to/task-owned-cache

runner:
  container_runtime: docker
  cleanup_on_start: false
  live_reporting:
    enabled: false
  directories:
    tmp_datadir: /absolute/path/to/overlay-runtime
  benchmark:
    results_dir: /absolute/path/to/results
    results_owner: "<invoking-uid>:<invoking-gid>"
    tests:
      metadata:
        labels:
          data-disk-type: overlayfs
          snapshot-kind: pre-populated
  client:
    config:
      genesis:
        erigon: ${ERIGON_GENESIS}
    datadirs:
      erigon:
        source_dir: ${ERIGON_SNAPSHOT_DIR}
        method: overlayfs
```

Replace the owner placeholder with the exact UID:GID of the account that performs validation. This
keeps results readable after the sanitized privileged launcher exits.

Copy the complete selected instance from `DATASET_CLIENTS_CONFIG`; instance lists replace rather
than merge. Keep `cleanup_on_start: false` while a deliberate staging overlay is mounted because
broad orphan cleanup can destroy that baseline.

Record the exact absolute `results_dir` used by each command. Keep every durable results root
outside the disposable `RUN_ROOT`, verify that neither path contains the other, and retain the
results after runtime cleanup. Before using the shared validation or fixture-enumeration recipes,
assign that path to the shell `RESULTS_DIR`. If staging, smoke, and measured commands use different
results roots, reset `RESULTS_DIR` for each corresponding run; the recipes intentionally never fall
back to `$TESTS_DIR/results`.

### Run compute with one protected overlay

For compute, require the complete selected source to declare `test-type: compute` and
`rollback_strategy: none`. Preserve its `pre_runs` map when one exists. Load the dataset context and
complete compute source rather than either State Actor source or the stateful source:

```text
compute: configs/global.yaml
         -> resource-limit config
         -> $DATASET_DATADIR_GLOBAL
         -> $DATASET_CONTEXT_GLOBAL
         -> $DATASET_SOURCE_CONFIG (complete compute source, including pre_runs)
         -> $DATASET_CLIENTS_CONFIG
         -> complete local Docker/OverlayFS config using ORIGINAL_LOWER
         -> complete local compute instance and optional exact-filter override
```

Omit the hosted datadir runner config and reassert Docker, the complete OverlayFS datadir map, and
`data-disk-type: overlayfs` after the source and clients. Do not use
`--debug.stop-after-prerun`, a no-`pre_runs` source copy, or `ADVANCED_LOWER` for this path. When the
protected original is classified as `raw-base`, benchmarkoor replays the pre-run once into the
compute upper before measuring fixtures. When it is classified as `post-prerun`, require
benchmarkoor to skip replay; never apply the pre-run a second time. No other `SNAPSHOT_STATE` is
valid.

The same compute container and OverlayFS layer then remain active for every selected fixture, as
required by `rollback_strategy: none`. Require exactly one task-owned overlay during the run and
verify it with `verify_task_overlay "$CONTAINER_ID" "$ORIGINAL_LOWER"`. Give every smoke and
measured command a fresh upper; never reuse or promote a smoke upper. With `raw-base`, each fresh
command replays the pre-run into its own upper; with `post-prerun`, each command must skip it. No
command may write to `ORIGINAL_LOWER` or the pristine path.

After the smoke, require the persisted rollback strategy to be `none`, the suite label to be
`test-type: compute`, and the datadir method to be `overlayfs`. The persisted `start_block` is the
head observed before executor-level pre-runs: it must equal the raw tuple when replay was needed, or
the post-pre-run tuple when benchmarkoor skipped replay. In the raw case, require a successful
`Pre-run steps completed` record before the first fixture and no failed pre-run step. In the
post-pre-run case, require the logs and persisted start head to prove replay did not run. A filtered
smoke proves configuration and isolation, not the fixture count or state/cache history of an
unfiltered compute run.

### Stage a stateful pre-run once

For stateful, require `rollback_strategy: container-recreate`. Choose the measured lower from the
verified snapshot classification:

- If the source has no `pre_runs`, require `SNAPSHOT_STATE=raw-base`, skip staging, and use
  `ORIGINAL_LOWER` with the selected source.
- If the source has `pre_runs` and `SNAPSHOT_STATE=post-prerun`, skip staging, use
  `ORIGINAL_LOWER`, and use a complete source copy with only `pre_runs` removed.
- If the source has `pre_runs` and `SNAPSHOT_STATE=raw-base`, stage exactly once as described below,
  then use `ADVANCED_LOWER` with that same no-`pre_runs` source copy.

Reject every other combination. A plain stateful run against a raw lower would otherwise restore
that lower and replay the bundle for every fixture. Only the third case builds an immutable
advanced baseline:

For that third case, create and pin the future advanced-lower mountpoint before starting the long
staging command. A fresh task must not already contain this path. Requiring it to be a canonical
direct child of `RUN_ROOT`, root-owned, and a private self-bind prevents the invoking user from
replacing it with a symlink while staging runs. Record `ADVANCED_LOWER_PIN_ID`; this empty guard is
a separate mount below the later staged bind and must be removed during final or interrupted
cleanup:

```bash
set -euo pipefail

RUN_ROOT=$(realpath -e -- "$RUN_ROOT") || exit 1
EXPECTED_ADVANCED_LOWER="$RUN_ROOT/advanced-lower-ro"
test "$(realpath -m -- "$ADVANCED_LOWER")" = \
  "$EXPECTED_ADVANCED_LOWER" || exit 1
test "$(dirname -- "$EXPECTED_ADVANCED_LOWER")" = "$RUN_ROOT" || exit 1
if sudo -n test -e "$ADVANCED_LOWER" || sudo -n test -L "$ADVANCED_LOWER"; then
  printf 'fresh advanced-lower mountpoint already exists: %s\n' \
    "$ADVANCED_LOWER" >&2
  exit 1
fi
sudo -n install -d -o root -g root -m 0700 -- "$ADVANCED_LOWER" || exit 1
sudo -n test ! -L "$ADVANCED_LOWER" || exit 1
ADVANCED_LOWER=$(sudo -n realpath -e -- "$ADVANCED_LOWER") || exit 1
test "$ADVANCED_LOWER" = "$EXPECTED_ADVANCED_LOWER" || exit 1
test "$(dirname -- "$ADVANCED_LOWER")" = "$RUN_ROOT" || exit 1
test "$(sudo -n stat -Lc '%u:%g:%a' -- "$ADVANCED_LOWER")" = \
  0:0:700 || exit 1
sudo -n mount --bind "$ADVANCED_LOWER" "$ADVANCED_LOWER" || exit 1
sudo -n mount --make-private "$ADVANCED_LOWER" || exit 1
test "$(sudo -n realpath -e -- \
  "$(findmnt -n -M "$ADVANCED_LOWER" -o TARGET)")" = \
  "$ADVANCED_LOWER" || exit 1
test "$(findmnt -n -M "$ADVANCED_LOWER" -o PROPAGATION)" = \
  private || exit 1
ADVANCED_LOWER_PIN_ID=$(findmnt -n -M "$ADVANCED_LOWER" -o ID) || exit 1
case "$ADVANCED_LOWER_PIN_ID" in ''|*[!0-9]*) exit 1 ;; esac
```

1. Run the exact stateful source against `ORIGINAL_LOWER` with
   `"${BENCHMARKOOR_RUN[@]}" --hold-after-exit run ... --debug.stop-after-prerun`. Omit the CPU and
   memory performance-limit config during this unmeasured command. The wrapper creates
   `STAGING_EXIT_HOLD` before starting Benchmarkoor. The debug flag deliberately retains the staged
   datadir and container state for inspection; it does not make this a measured run. Keep a
   controlling shell or exact process handle and capture the wrapper's combined output.
2. Follow the staging log until both `Pre-run steps completed` and the
   `--stop-after-prerun set` record appear. Do not wait for the benchmarkoor process yet. Require
   successful replay to the expected end block/hash/root, then resolve the exact retained container
   and logged data mount. Before removing the container, capture the verifier outputs in the same
   controlling shell:

   ```bash
   STAGING_OVERLAY_PATHS_TEXT=$(verify_task_overlay \
     "$CONTAINER_ID" "$ORIGINAL_LOWER") || exit 1
   mapfile -t STAGING_OVERLAY_PATHS <<<"$STAGING_OVERLAY_PATHS_TEXT"
   test "${#STAGING_OVERLAY_PATHS[@]}" -eq 3 || exit 1
   STAGING_MERGED=${STAGING_OVERLAY_PATHS[0]}
   STAGING_UPPER=${STAGING_OVERLAY_PATHS[1]}
   STAGING_WORK=${STAGING_OVERLAY_PATHS[2]}

   STAGING_AUX_PATHS_TEXT=$(verify_staging_aux_mounts \
     "$CONTAINER_ID" "$STAGING_MERGED") || exit 1
   mapfile -t STAGING_AUX_PATHS <<<"$STAGING_AUX_PATHS_TEXT"
   test "${#STAGING_AUX_PATHS[@]}" -ge 2 || exit 1
   STAGING_AUX_DIR=${STAGING_AUX_PATHS[0]}
   STAGING_AUX_SOURCES=("${STAGING_AUX_PATHS[@]:1}")

   STAGING_VOLUME_NAMES_JSON=$("${ROOT_DOCKER[@]}" inspect "$CONTAINER_ID" |
     jq -ce '
       [.[0].Mounts[]? | select(.Type == "volume") | .Name] as $names |
       if all($names[];
            type == "string" and test("^[A-Za-z0-9][A-Za-z0-9_.-]*$")) and
          (($names | length) == ($names | unique | length))
       then $names
       else error("invalid or duplicate staging volume names")
       end
     ') || exit 1
   mapfile -t STAGING_CONTAINER_VOLUMES < <(
     jq -r '.[]' <<<"$STAGING_VOLUME_NAMES_JSON"
   )
   for staging_volume in "${STAGING_CONTAINER_VOLUMES[@]}"; do
     "${ROOT_DOCKER[@]}" volume inspect "$staging_volume" >/dev/null || exit 1
   done
   ```

   Require the logged data mount to resolve to `STAGING_MERGED`, using
   `"${OVERLAY_INSPECT[@]}" realpath -e` for both paths. Retain the three overlay paths, auxiliary
   directory, exact auxiliary sources, and exact volume names after container removal. From the
   logged run directory, require every persisted fork override and client argument to equal the
   values resolved from the selected context.
3. Stop that exact container gracefully and wait until it has exited. This closes container stdio
   so Benchmarkoor's following log stream can reach EOF. Inspect and record its stopped state, run
   `sync`, and require a clean Erigon shutdown with no timeout, SIGKILL, or OOM kill. Wait until the
   wrapper emits `Benchmarkoor exited cleanly; staging exit hold is ready for release`; require
   `STAGING_EXIT_HOLD` to remain a root-owned, mode-0600, single-link regular file. This marker is
   the handshake proving that the child exited and the wrapper is paused before its residual
   container gate. Verify the hold with
   `test "$(sudo -n /usr/bin/stat -Lc '%u:%g:%a:%h' -- "$STAGING_EXIT_HOLD")" = 0:0:600:1`.
   Then remove only that stopped container, including its inherited anonymous volumes, without
   unmounting its retained OverlayFS mount:

   ```bash
   "${ROOT_DOCKER[@]}" rm --volumes "$CONTAINER_ID" || exit 1
   CURRENT_CONTAINERS_TEXT=$("${ROOT_DOCKER[@]}" ps -aq --no-trunc) || exit 1
   if grep -Fqx -- "$CONTAINER_ID" <<<"$CURRENT_CONTAINERS_TEXT"; then
     exit 1
   fi
   CURRENT_VOLUMES_TEXT=$("${ROOT_DOCKER[@]}" volume ls -q) || exit 1
   for staging_volume in "${STAGING_CONTAINER_VOLUMES[@]}"; do
     if grep -Fqx -- "$staging_volume" <<<"$CURRENT_VOLUMES_TEXT"; then
       printf 'staging container volume remains: %s\n' "$staging_volume" >&2
       exit 1
     fi
   done
   ```

   `--volumes` removes anonymous volumes associated with the exact container. Treat any recorded
   volume that remains as a hard cleanup failure; do not run a broad volume-prune command. Only
   after the container and recorded volumes are absent, release the wrapper with
   `sudo -n /usr/bin/unlink -- "$STAGING_EXIT_HOLD"`. Never remove the hold before the ready marker.
4. Only after releasing the hold, wait for the exact guarded-launcher process. Preserve and require
   its successful exit status and final logs. Revalidate the captured `STAGING_MERGED`,
   `STAGING_UPPER`, and `STAGING_WORK` relationship, then require no process, cwd, mapping, or open
   descriptor anywhere below the complete staging base or auxiliary directory. Remove only the
   exact recorded auxiliary files and their now-empty directory. Leave the disposable merged
   overlay mounted. If any shutdown, process-exit, handle, path, or cleanup gate fails, discard the
   stage instead of promoting potentially inconsistent MDBX state.
5. Remount the staging overlay at `STAGING_MERGED` read-only.
6. Bind `STAGING_MERGED` to `ADVANCED_LOWER` and remount that bind read-only. Do this before adding
   the backing-tree guard so the bind resolves to the overlay mount rather than the covered
   directory beneath it.
7. Self-bind `STAGING_UPPER` and `STAGING_WORK` separately and remount both binds read-only. This
   protects the backing trees without covering the nested `STAGING_MERGED` mount. Require all four
   exact targets to have a distinct `ro` option, require both data views to be OverlayFS, and prove
   writes through every alias fail. Before applying these guards, rerun the all-state exposure
   function for `RUN_ROOT`, repeat the equivalent source/volume scan for every other installed
   runtime, and inspect every pre-existing mount namespace for writable binds sourced from
   `RUN_ROOT`, `STAGING_BASE`, or one of their ancestors. A differently named destination is still
   an alias. Stop if any such view exists; a private mount added now cannot retroactively protect
   it:

   ```bash
   set -euo pipefail

   declare -p STAGING_AUX_SOURCES >/dev/null 2>&1 || exit 1
   test "${#STAGING_AUX_SOURCES[@]}" -gt 0 || exit 1
   STAGING_MERGED=$("${OVERLAY_INSPECT[@]}" realpath -e -- \
     "${STAGING_MERGED:?capture the verified staging merged path}") || exit 1
   STAGING_UPPER=$("${OVERLAY_INSPECT[@]}" realpath -e -- \
     "${STAGING_UPPER:?capture the verified staging upper path}") || exit 1
   STAGING_WORK=$("${OVERLAY_INSPECT[@]}" realpath -e -- \
     "${STAGING_WORK:?capture the verified staging work path}") || exit 1
   STAGING_AUX_DIR=$("${OVERLAY_INSPECT[@]}" realpath -e -- \
     "${STAGING_AUX_DIR:?capture the verified staging auxiliary directory}") || exit 1
   case "$STAGING_MERGED" in
     "$OVERLAY_TMP"/benchmarkoor-overlay-*/merged) ;;
     *) exit 1 ;;
   esac
   STAGING_BASE=$(dirname -- "$STAGING_MERGED")
   test "$STAGING_UPPER" = "$STAGING_BASE/upper" || exit 1
   test "$STAGING_WORK" = "$STAGING_BASE/work" || exit 1
   RESOLVED_CACHE_DIR=$("${OVERLAY_INSPECT[@]}" realpath -e -- "$CACHE_DIR") || exit 1
   test "$(dirname -- "$STAGING_AUX_DIR")" = "$RESOLVED_CACHE_DIR" || exit 1
   case "$STAGING_AUX_DIR" in
     "$RESOLVED_CACHE_DIR"/benchmarkoor-"$INSTANCE_ID"-*) ;;
     *) exit 1 ;;
   esac
   "${OVERLAY_INSPECT[@]}" test ! -L "$STAGING_AUX_DIR" || exit 1
   "${OVERLAY_INSPECT[@]}" test -d "$STAGING_AUX_DIR" || exit 1
   test "$("${OVERLAY_INSPECT[@]}" stat -Lc '%u:%g:%a' -- \
     "$STAGING_AUX_DIR")" = 0:0:700 || exit 1

   assert_no_open_handles() {
     local scan_root=${1:?set the exact scan root}
     local handle_errors
     local handle_report
     local lsof_status

     handle_report=$(mktemp "$RUN_ROOT/staging-open-handles.XXXXXXXXXX") || return 1
     if ! handle_errors=$(mktemp "$RUN_ROOT/staging-handle-errors.XXXXXXXXXX"); then
       unlink -- "$handle_report"
       return 1
     fi
     if "${OVERLAY_INSPECT[@]}" /usr/bin/lsof -nP +D "$scan_root" \
       >"$handle_report" 2>"$handle_errors"; then
       printf 'open handles remain below %s; inspect %s and %s\n' \
         "$scan_root" "$handle_report" "$handle_errors" >&2
       return 1
     else
       lsof_status=$?
     fi
     if test "$lsof_status" -ne 1 || test -s "$handle_report" || \
       test -s "$handle_errors"; then
       printf 'staging handle scan failed or was incomplete; inspect %s and %s\n' \
         "$handle_report" "$handle_errors" >&2
       return 1
     fi
     unlink -- "$handle_report"
     unlink -- "$handle_errors"
   }

   remove_staging_aux_files() {
     local index source
     local -a current_sources=()
     local -a recorded_sources=()

     mapfile -d '' -t current_sources < <(
       "${OVERLAY_INSPECT[@]}" find "$STAGING_AUX_DIR" -xdev \
         -mindepth 1 -maxdepth 1 -print0 | sort -z
     )
     mapfile -d '' -t recorded_sources < <(
       printf '%s\0' "${STAGING_AUX_SOURCES[@]}" | sort -z
     )
     test "${#current_sources[@]}" -eq "${#recorded_sources[@]}" || return 1
     for index in "${!current_sources[@]}"; do
       test "${current_sources[$index]}" = "${recorded_sources[$index]}" || return 1
       source=${current_sources[$index]}
       test "$(dirname -- "$source")" = "$STAGING_AUX_DIR" || return 1
       "${OVERLAY_INSPECT[@]}" test ! -L "$source" || return 1
       "${OVERLAY_INSPECT[@]}" test -f "$source" || return 1
       test "$("${OVERLAY_INSPECT[@]}" realpath -e -- "$source")" = \
         "$source" || return 1
       test "$("${OVERLAY_INSPECT[@]}" stat -Lc '%u:%g:%h' -- "$source")" = \
         0:0:1 || return 1
     done
     for source in "${current_sources[@]}"; do
       "${OVERLAY_INSPECT[@]}" unlink -- "$source" || return 1
     done
     "${OVERLAY_INSPECT[@]}" rmdir -- "$STAGING_AUX_DIR" || return 1
     "${OVERLAY_INSPECT[@]}" test ! -e "$STAGING_AUX_DIR" || return 1
     "${OVERLAY_INSPECT[@]}" test ! -L "$STAGING_AUX_DIR" || return 1
   }

   assert_no_open_handles "$STAGING_BASE" || exit 1
   assert_no_open_handles "$STAGING_AUX_DIR" || exit 1
   remove_staging_aux_files || exit 1
   assert_no_container_path_exposure \
     "$RUN_ROOT" 'staging runtime tree' || exit 1
   sudo -n mount -o remount,ro "$STAGING_MERGED" || exit 1
   sudo -n test ! -L "$ADVANCED_LOWER" || exit 1
   test "$(sudo -n realpath -e -- "$ADVANCED_LOWER")" = \
     "$RUN_ROOT/advanced-lower-ro" || exit 1
   test "$(dirname -- "$ADVANCED_LOWER")" = "$RUN_ROOT" || exit 1
   test "$(findmnt -n -M "$ADVANCED_LOWER" -o ID)" = \
     "$ADVANCED_LOWER_PIN_ID" || exit 1
   sudo -n mount --bind "$STAGING_MERGED" "$ADVANCED_LOWER" || exit 1
   sudo -n mount -o remount,bind,ro "$ADVANCED_LOWER" || exit 1
   sudo -n mount --bind "$STAGING_UPPER" "$STAGING_UPPER" || exit 1
   sudo -n mount -o remount,bind,ro "$STAGING_UPPER" || exit 1
   sudo -n mount --bind "$STAGING_WORK" "$STAGING_WORK" || exit 1
   sudo -n mount -o remount,bind,ro "$STAGING_WORK" || exit 1
   assert_no_open_handles "$STAGING_BASE" || exit 1

   for guarded_path in \
     "$STAGING_MERGED" "$ADVANCED_LOWER" "$STAGING_UPPER" "$STAGING_WORK"; do
     mount_target=$("${OVERLAY_INSPECT[@]}" findmnt -n -T "$guarded_path" -o TARGET) || exit 1
     mount_options=$("${OVERLAY_INSPECT[@]}" findmnt -n -T "$guarded_path" -o OPTIONS) || exit 1
     test "$("${OVERLAY_INSPECT[@]}" realpath -e -- "$mount_target")" = \
       "$("${OVERLAY_INSPECT[@]}" realpath -e -- "$guarded_path")" || exit 1
     case ",$mount_options," in *,ro,*) ;; *) exit 1 ;; esac
   done
   test "$("${OVERLAY_INSPECT[@]}" findmnt -n -T "$STAGING_MERGED" -o FSTYPE)" = \
     overlay || exit 1
   test "$("${OVERLAY_INSPECT[@]}" findmnt -n -T "$ADVANCED_LOWER" -o FSTYPE)" = \
     overlay || exit 1

   STAGING_READONLY_CANARY=.benchmarkoor-staging-readonly-canary
   for guarded_path in \
     "$STAGING_MERGED" "$ADVANCED_LOWER" "$STAGING_UPPER" "$STAGING_WORK"; do
     "${OVERLAY_INSPECT[@]}" test ! -e \
       "$guarded_path/$STAGING_READONLY_CANARY" || exit 1
     "${OVERLAY_INSPECT[@]}" test ! -L \
       "$guarded_path/$STAGING_READONLY_CANARY" || exit 1
     if "${OVERLAY_INSPECT[@]}" touch "$guarded_path/$STAGING_READONLY_CANARY"; then
       exit 1
     fi
     "${OVERLAY_INSPECT[@]}" test ! -e \
       "$guarded_path/$STAGING_READONLY_CANARY" || exit 1
     "${OVERLAY_INSPECT[@]}" test ! -L \
       "$guarded_path/$STAGING_READONLY_CANARY" || exit 1
   done
   ```

8. Canary-test a short-lived OverlayFS over `ADVANCED_LOWER`. Before unmounting the probe, require
   the canary to appear only in its upper and to be absent through `ADVANCED_LOWER`,
   `ORIGINAL_LOWER`, and the read-only staging merged view. Never inspect the raw `PRISTINE_DIR` for
   this check; all lower verification stays behind a read-only mount.

After creating `ADVANCED_LOWER`, never address the protected staging tree or merged alias from a
client or benchmark command; use their paths only for read-only measurement and exact cleanup.

Create a field-for-field copy of the selected test-source map and remove only `pre_runs`. Preserve
its name, labels, complete fixture source, rollback strategy, runner settings, and every other
source field in that copy. Do not try to delete the inherited key with a partial later map.
Deliberately replace its environment-specific runtime and storage-label values with the final local
Docker/OverlayFS config described below. During measured runs, use this complete local source and
the lower selected above, and restore the resource-limit config.

Do not reuse context, source, client, or datadir variables from a previous State Actor task.
Replace both State Actor datadir configs with `DATASET_DATADIR_GLOBAL`, use
`DATASET_CONTEXT_GLOBAL`, and load `DATASET_CLIENTS_CONFIG`. Omit the hosted datadir runner config.
Use `DATASET_SOURCE_CONFIG` for staging only in the raw-base case. When the selected source has
`pre_runs`, save its complete no-pre-runs copy at a task-owned path as
`DATASET_NO_PRERUN_CONFIG`; canonicalize that variable to the new file below `RUN_ROOT`, and never
edit the checked-in source. After the dataset context, source, and clients files, load the complete
local Docker/OverlayFS config, then the local instance and optional exact-filter overrides. The
final local config must reassert `container_runtime: docker`, the complete OverlayFS datadir map,
and `data-disk-type: overlayfs`.

The staging stack exists only for a `raw-base` snapshot with `pre_runs`. The measured stack uses
`MEASURED_LOWER=ADVANCED_LOWER` after successful staging and
`MEASURED_LOWER=ORIGINAL_LOWER` when staging was skipped:

```text
staging: configs/global.yaml
         -> $DATASET_DATADIR_GLOBAL
         -> $DATASET_CONTEXT_GLOBAL
         -> $DATASET_SOURCE_CONFIG (complete stateful source with pre_runs)
         -> $DATASET_CLIENTS_CONFIG
         -> complete local Docker/OverlayFS config using ORIGINAL_LOWER
         -> complete local staging instance

measured: configs/global.yaml
          -> resource-limit config
          -> $DATASET_DATADIR_GLOBAL
          -> $DATASET_CONTEXT_GLOBAL
          -> $DATASET_NO_PRERUN_CONFIG
          -> $DATASET_CLIENTS_CONFIG
          -> complete local Docker/OverlayFS config using MEASURED_LOWER
          -> complete local measured instance and optional exact-filter override
```

When the selected source never had `pre_runs`, keep that complete source in the measured stack in
place of `DATASET_NO_PRERUN_CONFIG`; do not create an unnecessary partial replacement.

Use `--debug.stop-after-prerun` only on the staging command. Use the same
`--limit-instance-client` and `--limit-instance-id` on both. Let the smoke create its persisted
`config.json`, require its instance runtime to be `docker` and datadir method to be `overlayfs`,
require the persisted suite summary's `data-disk-type` to be `overlayfs`, and confirm its nonzero
resolved tests before starting the requested measured run. If the requested subset has at most ten
fixtures, it may itself serve as the smoke when it uses this identical final stack and passes every
smoke gate. For an all-fixtures run, remove the smoke filter and pass the archive-count equality
gate above.

### Monitor and clean up

Compute has one disposable per-run upper over `ORIGINAL_LOWER`; it persists and may grow throughout
the suite. Budget at least the larger of the original lower's allocated and apparent sizes plus the
emergency floor. Apply the device-ID and separate-results-filesystem gate shown below with that
original-lower bound in place of `COPYUP_BOUND_BYTES`. Monitor the single upper plus free space at
both `OVERLAY_TMP` and `RESULTS_DIR`, sampling only once when their device IDs match, and stop if
another unexplained task overlay appears. After a compute run, require its container, overlay mount,
upper directory, and benchmarkoor process to be gone; then compare the pristine integrity records
through `ORIGINAL_LOWER` before unmounting that guard. Unmount the private task-root self-bind only
after `ORIGINAL_LOWER`, then require no mount target equal to or below the verified task root before
removing it. Retain the separately owned results directory and its validated artifacts.

The staged stateful design has one fixed read-only overlay, read-only guards over its upper and work
trees, and a read-only advanced bind, plus at most one disposable per-test overlay. Count unique
upper directories or explicitly exclude the fixed paths; a raw mountpoint count includes multiple
views of the fixed baseline. Require the benchmarkoor revision to remove the prior per-test
container and mount before creating the next one. Stop immediately if two per-test uppers persist.

After staging, measure the fixed upper and the complete read-only advanced baseline, then re-check
the writable filesystem. The per-test upper may have to copy any file visible through
`ADVANCED_LOWER`, including files created or enlarged during staging, so the original pristine size
is not a sufficient bound:

```bash
EMERGENCY_FLOOR_BYTES=${EMERGENCY_FLOOR_BYTES:?set the predeclared free-space floor}
declare -p OVERLAY_INSPECT >/dev/null 2>&1 || exit 1
STAGING_MERGED=$("${OVERLAY_INSPECT[@]}" realpath -e -- \
  "${STAGING_MERGED:?retain the verified staging merged path}") || exit 1
STAGING_UPPER=$("${OVERLAY_INSPECT[@]}" realpath -e -- \
  "${STAGING_UPPER:?retain the verified staging upper path}") || exit 1
test "$STAGING_UPPER" = "$(dirname -- "$STAGING_MERGED")/upper" || exit 1

"${OVERLAY_INSPECT[@]}" du -sx --block-size=1 "$STAGING_UPPER"
"${OVERLAY_INSPECT[@]}" du -sx --apparent-size --block-size=1 "$STAGING_UPPER"
ADVANCED_ALLOCATED_BYTES=$("${OVERLAY_INSPECT[@]}" du -sx --block-size=1 \
  "$ADVANCED_LOWER" | awk '{print $1}')
ADVANCED_APPARENT_BYTES=$("${OVERLAY_INSPECT[@]}" du -sx --apparent-size --block-size=1 \
  "$ADVANCED_LOWER" | awk '{print $1}')
OVERLAY_AVAILABLE_BYTES=$("${OVERLAY_INSPECT[@]}" df -B1 --output=avail \
  "$OVERLAY_TMP" | awk 'NR == 2 {print $1}')
RESULTS_DIR=${RESULTS_DIR:?set to the effective runner.benchmark.results_dir}
RESULTS_DIR=$(realpath -e -- "$RESULTS_DIR")
OVERLAY_FILESYSTEM=$("${OVERLAY_INSPECT[@]}" stat -Lc '%d' -- "$OVERLAY_TMP")
RESULTS_FILESYSTEM=$("${OVERLAY_INSPECT[@]}" stat -Lc '%d' -- "$RESULTS_DIR")

for byte_count in "$EMERGENCY_FLOOR_BYTES" "$ADVANCED_ALLOCATED_BYTES" \
  "$ADVANCED_APPARENT_BYTES" "$OVERLAY_AVAILABLE_BYTES" \
  "$OVERLAY_FILESYSTEM" "$RESULTS_FILESYSTEM"; do
  case "$byte_count" in
    ''|*[!0-9]*) exit 2 ;;
  esac
done
if test "$ADVANCED_APPARENT_BYTES" -gt "$ADVANCED_ALLOCATED_BYTES"; then
  COPYUP_BOUND_BYTES=$ADVANCED_APPARENT_BYTES
else
  COPYUP_BOUND_BYTES=$ADVANCED_ALLOCATED_BYTES
fi
REQUIRED_BYTES=$((COPYUP_BOUND_BYTES + EMERGENCY_FLOOR_BYTES))
test "$REQUIRED_BYTES" -ge "$COPYUP_BOUND_BYTES" || exit 2
test "$OVERLAY_AVAILABLE_BYTES" -ge "$REQUIRED_BYTES" || exit 1

if test "$RESULTS_FILESYSTEM" = "$OVERLAY_FILESYSTEM"; then
  RESULTS_AVAILABLE_BYTES=$OVERLAY_AVAILABLE_BYTES
  RESULTS_CAPACITY=shared
else
  RESULTS_AVAILABLE_BYTES=$("${OVERLAY_INSPECT[@]}" df -B1 --output=avail \
    "$RESULTS_DIR" | awk 'NR == 2 {print $1}')
  case "$RESULTS_AVAILABLE_BYTES" in
    ''|*[!0-9]*) exit 2 ;;
  esac
  test "$RESULTS_AVAILABLE_BYTES" -ge "$EMERGENCY_FLOOR_BYTES" || exit 1
  RESULTS_CAPACITY=separate
fi
printf 'advanced allocated=%s apparent=%s copyup-bound=%s overlay-available=%s required=%s results-available=%s results-capacity=%s\n' \
  "$ADVANCED_ALLOCATED_BYTES" "$ADVANCED_APPARENT_BYTES" \
  "$COPYUP_BOUND_BYTES" "$OVERLAY_AVAILABLE_BYTES" "$REQUIRED_BYTES" \
  "$RESULTS_AVAILABLE_BYTES" "$RESULTS_CAPACITY"
```

Measure this gate after the fixed staging upper exists, so `OVERLAY_AVAILABLE_BYTES` already
accounts for it. The emergency floor must cover anticipated result and log growth as well as cache
and operating headroom. When results use a separate filesystem, require that floor independently
there; when both paths have the same device ID, the combined overlay requirement already reserves
it and the recipe deliberately does not count it twice. During measured runs, sample free space at
both `OVERLAY_TMP` and `RESULTS_DIR`, deduplicated by their device IDs, and monitor the aggregate
size of both the fixed staging upper and the current per-test upper. Accounting for only the
per-test layer understates peak consumption.

After stateful result validation, require zero per-test mounts, directories, containers, and
benchmarkoor processes. Compare pristine fingerprints, sizes, path count, and critical hashes while
`ORIGINAL_LOWER` is still mounted, and require every recorded canary name to be absent through that
protected view. For a staged run, then unmount `ADVANCED_LOWER`, the read-only self-binds on
`STAGING_UPPER` and `STAGING_WORK`, and the exact staging overlay, in that order, before removing the
verified staging tree. The first `ADVANCED_LOWER` unmount reveals its empty pin; require the visible
mount ID to equal `ADVANCED_LOWER_PIN_ID`, then unmount that exact pin and remove its empty
root-owned directory. For an unstaged run, require that no staging mount or pin exists. Immediately
before unmounting `ORIGINAL_LOWER`, repeat its `ro` assertion and canary-absence checks, then unmount
it last and remove only its now-empty root-owned mountpoint. After that, unmount the exact private
task-root self-bind, require no mount target equal to or below the verified task root, and inspect
only mount tables and task-owned resource paths; never traverse or hash the raw `PRISTINE_DIR` as a
final check.

For an interrupted unstaged stateful run, use the task-scoped recovery above and retain the
`ORIGINAL_LOWER` guard until its integrity checks pass. For an interrupted measured run over a
staged baseline, signal benchmarkoor only through the retained stable handle and stop its exact
client container, wait for both to exit, and require no open handle before unmounting anything.
Then resolve and remove only that run's disposable per-test mount and directory. Preserve the fixed
staging overlay, both backing-tree guards, and `ADVANCED_LOWER`; re-prove every baseline view
read-only and repeat the smoke gates before resuming. Complete the task-scoped CPU-state recovery
above before another measured command when the resource limits changed CPU tuning.

If staging itself is interrupted, shutdown was not clean, or the mapping of a mount to its upper is
uncertain, stop every exact task-owned container first so its log stream can drain, then wait for or
stop its exact benchmarkoor process. Prove each retained mount belongs to this task before cleanup;
if any target cannot be resolved exactly, stop and ask the user instead of unmounting or discarding
it. After all processes have exited and ownership is proven, unmount the staged bind on
`ADVANCED_LOWER` if present, the `STAGING_UPPER` and `STAGING_WORK` self-binds if present, and the
exact staging overlay. Once the empty advanced-lower pin is visible, require its recorded mount ID,
unmount it, and remove only that empty mountpoint. Unmount `ORIGINAL_LOWER` last. Then unmount the
exact private task-root self-bind after removing the now-empty root-owned original-lower
mountpoint. Require zero mount targets equal to or below the verified task root plus zero open
handles before discarding only that root; never guess with a broad mount, process, or cleanup
target.

The discarded root contained `ORIGINAL_LOWER`, so it cannot be reused. Select and create a new
task-owned `RUN_ROOT`, recompute all derived paths, and repeat the full
[Protect and identify the original](#protect-and-identify-the-original) procedure from the
canonical operator-designated `PRISTINE_DIR`: path-overlap and descendant-mount gates, read-only
bind/remount, `ro` assertion, rejected write probe, probe-overlay canary, host-access checks, sizes,
path count, full fingerprint, and critical hashes. Compare the new integrity values with the
baseline retained outside the discarded root. Stop if any value differs or any gate cannot be
reproduced. Re-run the live-head classification, and only then stage again from the newly created
`ORIGINAL_LOWER`.

Do not preserve historical run IDs, suite hashes, fixture URLs, test counts, block tuples, context
names, or host paths as defaults in this reference. Discover them from the selected revision and
effective run configuration each time, verify them, and include them only in that run's handoff.
