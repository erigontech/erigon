#!/bin/bash

set -euo pipefail

PATH=/usr/sbin:/usr/bin:/sbin:/bin
LC_ALL=C
export PATH LC_ALL

readonly cpu_lock_dir=/run/lock/benchmarkoor
readonly cpu_lock_file=$cpu_lock_dir/cpufreq.lock
readonly recovery_file=$cpu_lock_dir/cpufreq-recovery.json
readonly workload_file=$cpu_lock_dir/cpufreq-workload
readonly staging_hold_file=$cpu_lock_dir/staging-exit-hold
readonly sysfs_cpu_root=/sys/devices/system/cpu
readonly toolchain_root=/usr/local/libexec/benchmarkoor
toolchain_script=$(realpath -e -- "$0")
toolchain_dir=$(dirname -- "$toolchain_script")
toolchain_id=$(basename -- "$toolchain_dir")
readonly toolchain_script toolchain_dir toolchain_id
readonly toolchain_manifest=$toolchain_dir/toolchain.sha256
readonly docker_binary=/usr/bin/docker
readonly docker_host=unix:///var/run/docker.sock
readonly docker_quiet_seconds=30
readonly python_binary=/usr/bin/python3
recovery_tmp=

usage() {
  printf 'usage: %s arm CPU_IDS|none | verify-armed | clear-workload | recover\n' \
    "$0" >&2
  exit 2
}

require_sudo_root() {
  test "$(id -u)" -eq 0 || {
    printf 'run this helper through sudo\n' >&2
    exit 2
  }
  case "${SUDO_UID:-}" in
    ''|0|*[!0-9]*)
      printf 'a non-root sudo caller is required\n' >&2
      exit 2
      ;;
  esac
}

validate_toolchain() {
  local directory
  local mode
  local manifest_id
  local manifest_names
  local installed_file

  [[ "$toolchain_id" =~ ^[0-9a-f]{64}$ ]]
  test "$(dirname -- "$toolchain_dir")" = "$toolchain_root"
  for directory in "$toolchain_root" "$toolchain_dir"; do
    test "$(realpath -e -- "$directory")" = "$directory"
    test "$(stat -Lc '%u:%g' -- "$directory")" = 0:0
    mode=$(stat -Lc '%a' -- "$directory")
    case "$mode:$directory" in
      755:"$toolchain_root"|555:"$toolchain_dir") ;;
      *) exit 2 ;;
    esac
  done
  for installed_file in \
    "$toolchain_dir/restore-cpufreq-state" \
    "$toolchain_dir/run-with-cpufreq-lock" \
    "$toolchain_dir/benchmarkoor"; do
    test "$(realpath -e -- "$installed_file")" = "$installed_file"
    test "$(stat -Lc '%u:%g:%a:%h' -- "$installed_file")" = 0:0:555:1
  done
  test "$(realpath -e -- "$toolchain_manifest")" = "$toolchain_manifest"
  test "$(stat -Lc '%u:%g:%a:%h' -- "$toolchain_manifest")" = 0:0:444:1
  manifest_id=$(sha256sum "$toolchain_manifest" | awk '{print $1}')
  test "$manifest_id" = "$toolchain_id"
  test "$(wc -l <"$toolchain_manifest")" -eq 3
  manifest_names=$(awk '{print $2}' "$toolchain_manifest" | paste -sd, -)
  test "$manifest_names" = \
    'restore-cpufreq-state,run-with-cpufreq-lock,benchmarkoor'
  (
    cd "$toolchain_dir"
    sha256sum --check --strict toolchain.sha256 >/dev/null
  )
  test "$(realpath -e -- "$docker_binary")" = "$docker_binary"
  test "$(stat -Lc '%u:%g:%a:%h' -- "$docker_binary")" = 0:0:755:1
  test "$(stat -Lc '%u:%g:%a:%h' -- "$python_binary")" = 0:0:755:1
}

ensure_lock_dir() {
  local lock_parent=/run/lock
  local lock_parent_mode
  local lock_dir_metadata

  test "$(realpath -e -- "$lock_parent")" = "$lock_parent"
  test "$(stat -Lc '%u:%g' -- "$lock_parent")" = 0:0
  lock_parent_mode=$(stat -Lc '%a' -- "$lock_parent")
  case "$lock_parent_mode" in ''|*[!0-7]*) exit 2 ;; esac
  if test "$((8#$lock_parent_mode & 0022))" -ne 0; then
    test "$((8#$lock_parent_mode & 01000))" -ne 0
  fi
  umask 022
  mkdir -m 0755 -- "$cpu_lock_dir" 2>/dev/null || true
  lock_dir_metadata=$(stat -c '%f:%u:%g:%a' -- "$cpu_lock_dir")
  test "$lock_dir_metadata" = 41ed:0:0:755
  test "$(realpath -e -- "$cpu_lock_dir")" = "$cpu_lock_dir"
}

validate_fixed_paths() {
  test "$(realpath -e -- "$cpu_lock_dir")" = "$cpu_lock_dir"
  test "$(stat -Lc '%u:%g:%a' -- "$cpu_lock_dir")" = 0:0:755
  test "$(realpath -e -- "$sysfs_cpu_root")" = "$sysfs_cpu_root"
  test "$(findmnt -n -T "$sysfs_cpu_root" -o FSTYPE)" = sysfs

  if test -e "$cpu_lock_file" || test -L "$cpu_lock_file"; then
    test -f "$cpu_lock_file"
    test ! -L "$cpu_lock_file"
    test "$(stat -Lc '%u:%g:%a:%h' -- "$cpu_lock_file")" = 0:0:600:1
  fi
  if test -e "$workload_file" || test -L "$workload_file"; then
    test -f "$workload_file"
    test ! -L "$workload_file"
    test "$(stat -Lc '%u:%g:%a:%h' -- "$workload_file")" = 0:0:600:1
  fi
  if test -e "$staging_hold_file" || test -L "$staging_hold_file"; then
    test -f "$staging_hold_file"
    test ! -L "$staging_hold_file"
    test "$(stat -Lc '%u:%g:%a:%h' -- "$staging_hold_file")" = 0:0:600:1
  fi
  umask 077
  exec {cpu_lock_fd}<>"$cpu_lock_file"
  test -f "$cpu_lock_file"
  test ! -L "$cpu_lock_file"
  test "$(stat -Lc '%u:%g:%a:%h' -- "$cpu_lock_file")" = 0:0:600:1
}

managed_container_snapshot() {
  local container_id
  local containers_text
  local docker_stderr
  local -a container_ids=()

  docker_stderr=$(mktemp "$cpu_lock_dir/.docker-scan-errors.XXXXXX") || return 1
  if test "$(stat -Lc '%u:%g:%a:%h' -- "$docker_stderr")" != 0:0:600:1; then
    unlink -- "$docker_stderr" || true
    return 1
  fi
  if ! containers_text=$(env -i PATH="$PATH" \
    "$docker_binary" --host "$docker_host" ps -aq --no-trunc \
      --filter label=benchmarkoor.managed-by=benchmarkoor \
      2>"$docker_stderr"); then
    printf 'Docker container scan failed; retained the workload gate\n' >&2
    unlink -- "$docker_stderr" || true
    return 1
  fi
  if test -s "$docker_stderr"; then
    printf 'Docker container scan produced diagnostics; retained the workload gate\n' >&2
    unlink -- "$docker_stderr" || true
    return 1
  fi
  if ! unlink -- "$docker_stderr"; then
    printf 'could not remove the Docker scan diagnostic file\n' >&2
    return 1
  fi
  if test -z "$containers_text"; then
    return
  fi
  mapfile -t container_ids <<<"$containers_text"
  for container_id in "${container_ids[@]}"; do
    [[ "$container_id" =~ ^[0-9a-f]{64}$ ]] || return 1
  done
  test "${#container_ids[@]}" -eq \
    "$(printf '%s\n' "${container_ids[@]}" | sort -u | wc -l)" || return 1
  printf '%s\n' "${container_ids[@]}" | sort || return 1
}

monotonic_nanoseconds() {
  "$python_binary" -I -c 'import time; print(time.monotonic_ns())'
}

lock_controls() {
  if ! flock --exclusive --nonblock "$cpu_lock_fd"; then
    printf 'CPU controls are owned by another benchmark task\n' >&2
    exit 1
  fi
}

resolve_control() {
  local requested=${1:?set CPU control}
  local resolved

  resolved=$(realpath -e -- "$requested") || return 1
  case "$resolved" in
    "$sysfs_cpu_root"/*) ;;
    *)
      printf 'CPU control escapes trusted sysfs: %s\n' "$requested" >&2
      return 1
      ;;
  esac
  if test "$(findmnt -n -T "$resolved" -o FSTYPE)" != sysfs; then
    printf 'CPU control is not on sysfs: %s\n' "$requested" >&2
    return 1
  fi
  test -f "$resolved" || return 1
  printf '%s\n' "$resolved"
}

read_control() {
  local target
  local actual

  target=$(resolve_control "$1") || return 1
  actual=$(tr -d '[:space:]' <"$target") || return 1
  printf '%s\n' "$actual"
}

write_and_verify() {
  local target
  local expected=${2-}
  local actual

  target=$(resolve_control "$1") || {
    printf 'invalid CPU control: %s\n' "$1" >&2
    return 1
  }
  if ! printf '%s' "$expected" >"$target"; then
    printf 'failed to write CPU control: %s\n' "$target" >&2
    return 1
  fi
  if ! actual=$(tr -d '[:space:]' <"$target"); then
    printf 'failed to read CPU control: %s\n' "$target" >&2
    return 1
  fi
  if test "$actual" != "$expected"; then
    printf 'CPU control verification failed: %s\n' "$target" >&2
    return 1
  fi
}

verify_value() {
  local target
  local expected=${2-}
  local actual

  target=$(resolve_control "$1") || {
    printf 'invalid CPU control: %s\n' "$1" >&2
    return 1
  }
  if ! actual=$(tr -d '[:space:]' <"$target"); then
    printf 'failed to read CPU control: %s\n' "$target" >&2
    return 1
  fi
  if test "$actual" != "$expected"; then
    printf 'CPU control final verification failed: %s\n' "$target" >&2
    return 1
  fi
}

read_uint() {
  local actual

  actual=$(read_control "$1") || return 1
  case "$actual" in
    ''|*[!0-9]*)
      printf 'CPU control is not an unsigned integer: %s\n' "$1" >&2
      return 1
      ;;
  esac
  printf '%s\n' "$actual"
}

load_recovery_record() {
  local state_identity

  test -f "$recovery_file"
  test ! -L "$recovery_file"
  test "$(stat -Lc '%u:%g:%a:%h' -- "$recovery_file")" = 0:0:600:1
  state_identity=$(stat -Lc '%d:%i' -- "$recovery_file")
  state_json=$(jq -cse '
    def uint:
      type == "number" and . >= 0 and floor == . and . <= 9007199254740991;
    if length == 1 and
      (.[0] |
        type == "object" and
        .version == 2 and
        (.toolchain_id | type == "string" and test("^[0-9a-f]{64}$")) and
        (.boot_id | type == "string" and test("^[0-9a-f-]+$")) and
        (.sudo_uid | uint) and .sudo_uid > 0 and
        (.cpus | type == "object") and
        all(.cpus | to_entries[];
          (.key | test("^(0|[1-9][0-9]*)$")) and
          ((.key | tonumber) <= 1048575) and
          (.value | type == "object") and
          (.value.governor | type == "string") and
          (.value.governor | test("^[A-Za-z0-9_-]+$")) and
          (.value.scaling_max_khz | uint) and .value.scaling_max_khz > 0 and
          (.value.scaling_min_khz | uint) and .value.scaling_min_khz > 0 and
          .value.scaling_min_khz <= .value.scaling_max_khz
        ) and
        (.turbo_boost == null or
          ((.turbo_boost | type) == "object" and
            (.turbo_boost.type == "intel" or .turbo_boost.type == "amd") and
            (.turbo_boost.value == 0 or .turbo_boost.value == 1))))
    then .[0]
    else error("invalid Benchmarkoor CPU recovery record")
    end
  ' "$recovery_file")
  test "$(jq -r '.boot_id' <<<"$state_json")" = \
    "$(tr -d '[:space:]' </proc/sys/kernel/random/boot_id)"
  test "$(jq -r '.sudo_uid' <<<"$state_json")" = "$SUDO_UID"
  test "$(jq -r '.toolchain_id' <<<"$state_json")" = "$toolchain_id"
  test "$(stat -Lc '%d:%i' -- "$recovery_file")" = "$state_identity"
  recovery_identity=$state_identity
}

capture_turbo() {
  local value

  if test -e "$sysfs_cpu_root/intel_pstate/no_turbo"; then
    value=$(read_uint "$sysfs_cpu_root/intel_pstate/no_turbo") || return 1
    case "$value" in 0|1) ;; *) return 1 ;; esac
    jq -cn --argjson value "$value" '{type: "intel", value: $value}' || return 1
    return
  fi
  if test -e "$sysfs_cpu_root/cpufreq/boost"; then
    value=$(read_uint "$sysfs_cpu_root/cpufreq/boost") || return 1
    case "$value" in 0|1) ;; *) return 1 ;; esac
    jq -cn --argjson value "$value" '{type: "amd", value: $value}' || return 1
    return
  fi
  printf 'null\n'
}

arm_guard() {
  local cpu_list=${1:?set CPU IDs or none}
  local cpus_json={}
  local turbo_json
  local boot_id
  local cpu_id
  local governor
  local scaling_max
  local scaling_min
  local -a cpu_ids=()

  lock_controls
  if test -e "$recovery_file" || test -L "$recovery_file"; then
    printf 'CPU recovery is still required: %s\n' "$recovery_file" >&2
    exit 1
  fi
  if test -e "$workload_file" || test -L "$workload_file"; then
    printf 'Benchmarkoor workload inspection is still required: %s\n' \
      "$workload_file" >&2
    exit 1
  fi
  if test -e "$staging_hold_file" || test -L "$staging_hold_file"; then
    printf 'staging exit hold cleanup is still required: %s\n' \
      "$staging_hold_file" >&2
    exit 1
  fi

  if test "$cpu_list" != none; then
    case "$cpu_list" in
      ''|*[!0-9,]*|,*|*,|*,,*) usage ;;
    esac
    IFS=, read -r -a cpu_ids <<<"$cpu_list"
    mapfile -t cpu_ids < <(printf '%s\n' "${cpu_ids[@]}" | sort -n -u)
    test "${#cpu_ids[@]}" -gt 0
  fi

  for cpu_id in "${cpu_ids[@]}"; do
    [[ "$cpu_id" =~ ^(0|[1-9][0-9]*)$ ]] || usage
    test "$cpu_id" -le 1048575
    governor=$(read_control \
      "$sysfs_cpu_root/cpu$cpu_id/cpufreq/scaling_governor")
    case "$governor" in ''|*[!A-Za-z0-9_-]*) exit 2 ;; esac
    scaling_max=$(read_uint \
      "$sysfs_cpu_root/cpu$cpu_id/cpufreq/scaling_max_freq")
    scaling_min=$(read_uint \
      "$sysfs_cpu_root/cpu$cpu_id/cpufreq/scaling_min_freq")
    test "$scaling_max" -gt 0
    test "$scaling_min" -gt 0
    test "$scaling_min" -le "$scaling_max"
    cpus_json=$(jq -c \
      --arg cpu_id "$cpu_id" \
      --arg governor "$governor" \
      --argjson scaling_max "$scaling_max" \
      --argjson scaling_min "$scaling_min" \
      '. + {($cpu_id): {
        governor: $governor,
        scaling_max_khz: $scaling_max,
        scaling_min_khz: $scaling_min
      }}' <<<"$cpus_json")
  done

  turbo_json=$(capture_turbo)
  boot_id=$(tr -d '[:space:]' </proc/sys/kernel/random/boot_id)
  recovery_tmp=$(mktemp "$cpu_lock_dir/.cpufreq-recovery.XXXXXX")
  trap 'test -z "$recovery_tmp" || test ! -e "$recovery_tmp" || unlink -- "$recovery_tmp"' EXIT
  jq -cn \
    --arg toolchain_id "$toolchain_id" \
    --arg boot_id "$boot_id" \
    --argjson sudo_uid "$SUDO_UID" \
    --argjson cpus "$cpus_json" \
    --argjson turbo_boost "$turbo_json" \
    '{
      version: 2,
      toolchain_id: $toolchain_id,
      boot_id: $boot_id,
      sudo_uid: $sudo_uid,
      cpus: $cpus,
      turbo_boost: $turbo_boost
    }' >"$recovery_tmp"
  chmod 0600 "$recovery_tmp"
  test "$(stat -Lc '%u:%g:%a:%h' -- "$recovery_tmp")" = 0:0:600:1
  mv -- "$recovery_tmp" "$recovery_file"
  trap - EXIT
  recovery_tmp=
  test "$(stat -Lc '%u:%g:%a:%h' -- "$recovery_file")" = 0:0:600:1
}

verify_armed() {
  load_recovery_record
  if flock --exclusive --nonblock "$cpu_lock_fd"; then
    flock --unlock "$cpu_lock_fd"
    printf 'the guarded Benchmarkoor launcher does not hold the CPU lock\n' >&2
    exit 1
  fi
}

clear_workload() {
  local current_containers_text
  local docker_event_report
  local docker_event_since
  local docker_event_until
  local quiet_elapsed_nanoseconds
  local quiet_finished_nanoseconds
  local quiet_required_nanoseconds
  local quiet_started_nanoseconds
  local timestamp
  local workload_json
  local workload_identity
  local workload_cgroup
  local workload_parent
  local workload_name
  local staging_hold_identity
  local staging_hold_json
  local populated
  local -a baseline_containers
  local -a current_containers
  local -a remaining_containers

  lock_controls
  test -f "$workload_file"
  test ! -L "$workload_file"
  test "$(stat -Lc '%u:%g:%a:%h' -- "$workload_file")" = 0:0:600:1
  workload_identity=$(stat -Lc '%d:%i' -- "$workload_file")
  workload_json=$(jq -cse '
    if length == 1 and
      (.[0] |
        type == "object" and
        (keys | sort) ==
          ["cgroup", "docker_baseline", "toolchain_id", "version"] and
        .version == 2 and
        (.toolchain_id | type == "string" and test("^[0-9a-f]{64}$")) and
        (.cgroup | type == "string") and
        (.docker_baseline | type == "array") and
        all(.docker_baseline[];
          type == "string" and test("^[0-9a-f]{64}$")) and
        (.docker_baseline == (.docker_baseline | sort | unique)))
    then .[0]
    else error("invalid Benchmarkoor workload record")
    end
  ' "$workload_file")
  test "$(jq -r '.toolchain_id' <<<"$workload_json")" = "$toolchain_id"
  workload_cgroup=$(jq -r '.cgroup' <<<"$workload_json")
  mapfile -t baseline_containers < <(
    jq -r '.docker_baseline[]' <<<"$workload_json"
  )
  case "$workload_cgroup" in
    /sys/fs/cgroup/*) ;;
    *) exit 2 ;;
  esac
  case "$workload_cgroup" in
    */|*//*|*/./*|*/../*|*/.|*/..) exit 2 ;;
  esac
  workload_name=$(basename -- "$workload_cgroup")
  [[ "$workload_name" =~ ^benchmarkoor-cpufreq-(0|[1-9][0-9]*)-[0-9a-f]{32}$ ]]
  test "$(realpath -e -- /sys/fs/cgroup)" = /sys/fs/cgroup
  test "$(findmnt -n -T /sys/fs/cgroup -o FSTYPE)" = cgroup2

  if test -e "$workload_cgroup" || test -L "$workload_cgroup"; then
    workload_parent=$(dirname -- "$workload_cgroup")
    workload_parent=$(realpath -e -- "$workload_parent")
    test "$workload_cgroup" = "$workload_parent/$workload_name"
    test "$(findmnt -n -T "$workload_parent" -o FSTYPE)" = cgroup2
    test -d "$workload_cgroup"
    test ! -L "$workload_cgroup"
    test "$(realpath -e -- "$workload_cgroup")" = "$workload_cgroup"
    test "$(grep -c '^populated ' "$workload_cgroup/cgroup.events")" -eq 1
    populated=$(awk '$1 == "populated" {print $2}' \
      "$workload_cgroup/cgroup.events")
    test "$populated" = 0 || {
      printf 'Benchmarkoor workload cgroup is still populated: %s\n' \
        "$workload_cgroup" >&2
      exit 1
    }
    rmdir -- "$workload_cgroup"
  fi

  docker_event_since=$(date -u +'%Y-%m-%dT%H:%M:%S.%NZ')
  current_containers_text=$(managed_container_snapshot)
  current_containers=()
  if test -n "$current_containers_text"; then
    mapfile -t current_containers <<<"$current_containers_text"
  fi
  mapfile -t remaining_containers < <(
    comm -13 \
      <(printf '%s\n' "${baseline_containers[@]}" | sed '/^$/d') \
      <(printf '%s\n' "${current_containers[@]}" | sed '/^$/d')
  )
  if test "${#remaining_containers[@]}" -ne 0; then
    printf 'remove the recorded Benchmarkoor Docker containers first:\n' >&2
    printf '  %s\n' "${remaining_containers[@]}" >&2
    exit 1
  fi

  quiet_started_nanoseconds=$(monotonic_nanoseconds)
  docker_event_until=$(date -u --date="+$docker_quiet_seconds seconds" \
    +'%Y-%m-%dT%H:%M:%S.%NZ')
  if ! docker_event_report=$(env -i PATH="$PATH" \
    "$docker_binary" --host "$docker_host" events \
      --since "$docker_event_since" \
      --until "$docker_event_until" \
      --filter type=container \
      --filter label=benchmarkoor.managed-by=benchmarkoor \
      --format '{{.ID}} {{.Action}}' 2>&1); then
    printf 'Docker event scan failed; retained the workload gate\n' >&2
    exit 1
  fi
  quiet_finished_nanoseconds=$(monotonic_nanoseconds)
  for timestamp in \
    "$quiet_started_nanoseconds" "$quiet_finished_nanoseconds"; do
    case "$timestamp" in ''|*[!0-9]*) exit 2 ;; esac
  done
  quiet_elapsed_nanoseconds=$((
    quiet_finished_nanoseconds - quiet_started_nanoseconds
  ))
  quiet_required_nanoseconds=$((docker_quiet_seconds * 1000000000))
  if test "$quiet_elapsed_nanoseconds" -lt "$quiet_required_nanoseconds"; then
    printf 'Docker event scan ended before the required quiet window elapsed\n' \
      >&2
    exit 1
  fi

  current_containers_text=$(managed_container_snapshot)
  current_containers=()
  if test -n "$current_containers_text"; then
    mapfile -t current_containers <<<"$current_containers_text"
  fi
  mapfile -t remaining_containers < <(
    comm -13 \
      <(printf '%s\n' "${baseline_containers[@]}" | sed '/^$/d') \
      <(printf '%s\n' "${current_containers[@]}" | sed '/^$/d')
  )
  if test "${#remaining_containers[@]}" -ne 0; then
    printf 'remove the recorded Benchmarkoor Docker containers first:\n' >&2
    printf '  %s\n' "${remaining_containers[@]}" >&2
    exit 1
  fi
  if test -n "$docker_event_report"; then
    printf 'Benchmarkoor Docker activity occurred during the required quiet window\n' \
      >&2
    exit 1
  fi

  if test -e "$staging_hold_file" || test -L "$staging_hold_file"; then
    test -f "$staging_hold_file"
    test ! -L "$staging_hold_file"
    test "$(stat -Lc '%u:%g:%a:%h' -- "$staging_hold_file")" = 0:0:600:1
    staging_hold_identity=$(stat -Lc '%d:%i' -- "$staging_hold_file")
    staging_hold_json=$(jq -cse '
      if length == 1 and
        (.[0] |
          type == "object" and
          (keys | sort) == ["sudo_uid", "toolchain_id", "version"] and
          .version == 1 and
          (.toolchain_id | type == "string" and test("^[0-9a-f]{64}$")) and
          (.sudo_uid | type == "number" and . > 0 and floor == .))
      then .[0]
      else error("invalid Benchmarkoor staging exit hold")
      end
    ' "$staging_hold_file")
    test "$(jq -r '.toolchain_id' <<<"$staging_hold_json")" = "$toolchain_id"
    test "$(jq -r '.sudo_uid' <<<"$staging_hold_json")" = "$SUDO_UID"
    test "$(stat -Lc '%d:%i' -- "$staging_hold_file")" = \
      "$staging_hold_identity"
    unlink -- "$staging_hold_file"
  fi

  test "$(stat -Lc '%d:%i' -- "$workload_file")" = "$workload_identity"
  unlink -- "$workload_file"
  printf 'cleared empty Benchmarkoor workload and Docker-container gate\n'
}

failures=0
attempt_write() {
  if ! write_and_verify "$1" "$2"; then
    failures=$((failures + 1))
  fi
}

attempt_restore() {
  local current

  if ! current=$(read_control "$1"); then
    failures=$((failures + 1))
  elif test "$current" != "$2"; then
    attempt_write "$1" "$2"
  fi
}

attempt_verify() {
  if ! verify_value "$1" "$2"; then
    failures=$((failures + 1))
  fi
}

restore_limits() {
  local cpu_dir=${1:?set CPU directory}
  local desired_max=${2:?set desired maximum}
  local desired_min=${3:?set desired minimum}
  local current_min
  local current_max

  if ! current_min=$(read_uint "$cpu_dir/scaling_min_freq"); then
    failures=$((failures + 1))
    return
  fi
  if ! current_max=$(read_uint "$cpu_dir/scaling_max_freq"); then
    failures=$((failures + 1))
    return
  fi
  if test "$current_min" -eq "$desired_min" && \
    test "$current_max" -eq "$desired_max"; then
    return
  fi
  if test "$current_min" -gt "$desired_max"; then
    test "$current_min" -eq "$desired_min" || \
      attempt_write "$cpu_dir/scaling_min_freq" "$desired_min"
    test "$current_max" -eq "$desired_max" || \
      attempt_write "$cpu_dir/scaling_max_freq" "$desired_max"
  else
    test "$current_max" -eq "$desired_max" || \
      attempt_write "$cpu_dir/scaling_max_freq" "$desired_max"
    test "$current_min" -eq "$desired_min" || \
      attempt_write "$cpu_dir/scaling_min_freq" "$desired_min"
  fi
}

recover_guard() {
  local turbo_type
  local turbo_value
  local expected_cpu_count
  local cpu_record
  local cpu_id
  local governor
  local scaling_max
  local scaling_min
  local cpu_dir
  local -a cpu_records

  lock_controls
  if test -e "$workload_file" || test -L "$workload_file"; then
    printf 'clear the Benchmarkoor workload gate before CPU recovery: %s\n' \
      "$workload_file" >&2
    exit 1
  fi
  load_recovery_record

  turbo_type=$(jq -r '.turbo_boost.type // ""' <<<"$state_json")
  turbo_value=$(jq -r '.turbo_boost.value // ""' <<<"$state_json")
  case "$turbo_type" in
    '') ;;
    intel) attempt_restore "$sysfs_cpu_root/intel_pstate/no_turbo" "$turbo_value" ;;
    amd) attempt_restore "$sysfs_cpu_root/cpufreq/boost" "$turbo_value" ;;
    *) exit 2 ;;
  esac

  mapfile -t cpu_records < <(
    jq -c '.cpus | to_entries | sort_by(.key | tonumber)[]' <<<"$state_json"
  )
  expected_cpu_count=$(jq -r '.cpus | length' <<<"$state_json")
  test "${#cpu_records[@]}" -eq "$expected_cpu_count"

  for cpu_record in "${cpu_records[@]}"; do
    cpu_id=$(jq -r '.key' <<<"$cpu_record")
    governor=$(jq -r '.value.governor' <<<"$cpu_record")
    scaling_max=$(jq -r '.value.scaling_max_khz' <<<"$cpu_record")
    scaling_min=$(jq -r '.value.scaling_min_khz' <<<"$cpu_record")
    cpu_dir="$sysfs_cpu_root/cpu$cpu_id/cpufreq"
    attempt_restore "$cpu_dir/scaling_governor" "$governor"
    restore_limits "$cpu_dir" "$scaling_max" "$scaling_min"
  done

  case "$turbo_type" in
    '') ;;
    intel) attempt_verify "$sysfs_cpu_root/intel_pstate/no_turbo" "$turbo_value" ;;
    amd) attempt_verify "$sysfs_cpu_root/cpufreq/boost" "$turbo_value" ;;
  esac

  for cpu_record in "${cpu_records[@]}"; do
    cpu_id=$(jq -r '.key' <<<"$cpu_record")
    governor=$(jq -r '.value.governor' <<<"$cpu_record")
    scaling_max=$(jq -r '.value.scaling_max_khz' <<<"$cpu_record")
    scaling_min=$(jq -r '.value.scaling_min_khz' <<<"$cpu_record")
    cpu_dir="$sysfs_cpu_root/cpu$cpu_id/cpufreq"
    attempt_verify "$cpu_dir/scaling_governor" "$governor"
    attempt_verify "$cpu_dir/scaling_max_freq" "$scaling_max"
    attempt_verify "$cpu_dir/scaling_min_freq" "$scaling_min"
  done

  if test "$failures" -ne 0; then
    printf 'CPU frequency restoration failed for %s controls; retained %s\n' \
      "$failures" "$recovery_file" >&2
    exit 1
  fi

  test ! -L "$recovery_file"
  test "$(stat -Lc '%d:%i' -- "$recovery_file")" = "$recovery_identity"
  unlink -- "$recovery_file"
  printf 'restored and verified CPU frequency state for %s CPUs\n' \
    "${#cpu_records[@]}"
}

require_sudo_root
validate_toolchain
ensure_lock_dir
validate_fixed_paths

case "${1:-}" in
  arm)
    test "$#" -eq 2 || usage
    arm_guard "$2"
    ;;
  verify-armed)
    test "$#" -eq 1 || usage
    verify_armed
    ;;
  clear-workload)
    test "$#" -eq 1 || usage
    clear_workload
    ;;
  recover)
    test "$#" -eq 1 || usage
    recover_guard
    ;;
  *) usage ;;
esac
