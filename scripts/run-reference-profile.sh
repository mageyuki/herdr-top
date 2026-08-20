#!/usr/bin/bash -p
set -euo pipefail

case $- in
  *p*) ;;
  *) builtin printf '%s\n' 'error: protected Bash mode is required' >&2; exit 20 ;;
esac

bootstrap_source_fixture() {
  [[ ${source_fixture_bootstrap_parsed-} != 1 ]] || return 0
  [[ ${HERDR_INCREMENT5_BOOTSTRAP_TOOLS_V1+x} != x ]] || return 20
  local controller_requested=${HERDR_INCREMENT5_CONTROLLER_REQUESTED:?}
  local controller_canonical=${HERDR_INCREMENT5_CONTROLLER_CANONICAL:?}
  local controller_sha256=${HERDR_INCREMENT5_CONTROLLER_SHA256:?}
  local runner_requested=${HERDR_INCREMENT5_RUNNER_REQUESTED:?}
  local runner_canonical=${HERDR_INCREMENT5_RUNNER_CANONICAL:?}
  local runner_sha256=${HERDR_INCREMENT5_RUNNER_SHA256:?}
  local manifest=${HERDR_INCREMENT5_BOOTSTRAP_TOOLS_SOURCE_FIXTURE_V1:?}
  local requested canonical digest extra role variable
  local -a requested_paths=() canonical_paths=() digests=()
  local -ar roles=(
    env id mkdir mktemp mv pidstat prlimit readlink rmdir setsid
    sha256sum sleep stat taskset time unlink
  )

  [[ $controller_requested == /* && $controller_canonical == /* ]] || return 20
  [[ $runner_requested == /* && $runner_canonical == /* ]] || return 20
  [[ $controller_sha256 =~ ^[0-9a-f]{64}$ ]] || return 20
  [[ $runner_sha256 =~ ^[0-9a-f]{64}$ ]] || return 20
  [[ ${BASH_SOURCE[0]} == "$runner_canonical" ]] || return 20
  [[ ${runner_script-} == "$runner_canonical" ]] || return 20
  while IFS=$'\t' read -r requested canonical digest extra; do
    [[ -n $requested && -n $canonical && -n $digest && -z $extra ]] || return 20
    [[ $requested == /* && $canonical == /* ]] || return 20
    [[ $requested != *$'\n'* && $requested != *$'\r'* ]] || return 20
    [[ $canonical != *$'\n'* && $canonical != *$'\r'* ]] || return 20
    [[ $digest =~ ^[0-9a-f]{64}$ ]] || return 20
    requested_paths+=("$requested")
    canonical_paths+=("$canonical")
    digests+=("$digest")
  done < <(builtin printf '%s' "$manifest")
  [[ ${#requested_paths[@]} -eq ${#roles[@]} ]] || return 20

  for ((index=0; index<${#roles[@]}; index++)); do
    role=${roles[$index]}
    variable="source_${role}_requested"
    builtin printf -v "$variable" '%s' "${requested_paths[$index]}" || return 20
    readonly "$variable" || return 20
    variable="source_${role}_executable"
    builtin printf -v "$variable" '%s' "${canonical_paths[$index]}" || return 20
    readonly "$variable" || return 20
    variable="source_${role}_sha256"
    builtin printf -v "$variable" '%s' "${digests[$index]}" || return 20
    readonly "$variable" || return 20
  done

  [[ ${BASH-} == /* && -f $BASH && -x $BASH ]] || return 20
  source_bash_executable=$BASH
  readonly source_bash_executable || return 20

  source_fixture_bootstrap_parsed=1
  readonly source_fixture_bootstrap_parsed || return 20
}

revalidate_source_fixture_bootstrap() {
  [[ ${source_fixture_bootstrap_parsed-} == 1 ]] || return 20
  local -ar roles=(
    env id mkdir mktemp mv pidstat prlimit readlink rmdir setsid
    sha256sum sleep stat taskset time unlink
  )
  local role requested_variable canonical_variable digest_variable
  local requested canonical digest actual digest_line actual_digest
  for role in "${roles[@]}"; do
    requested_variable="source_${role}_requested"
    canonical_variable="source_${role}_executable"
    digest_variable="source_${role}_sha256"
    requested=${!requested_variable}
    canonical=${!canonical_variable}
    digest=${!digest_variable}
    actual="$("$source_readlink_executable" -e -- "$requested")" || return 20
    [[ $actual == "$canonical" ]] || return 20
    digest_line="$("$source_sha256sum_executable" -- "$canonical")" || return 20
    actual_digest=${digest_line%% *}
    [[ $actual_digest == "$digest" ]] || return 20
  done
  actual="$("$source_readlink_executable" -e -- "$HERDR_INCREMENT5_CONTROLLER_REQUESTED")" || return 20
  [[ $actual == "$HERDR_INCREMENT5_CONTROLLER_CANONICAL" ]] || return 20
  digest_line="$("$source_sha256sum_executable" -- "$HERDR_INCREMENT5_CONTROLLER_CANONICAL")" || return 20
  actual_digest=${digest_line%% *}
  [[ $actual_digest == "$HERDR_INCREMENT5_CONTROLLER_SHA256" ]] || return 20
  actual="$("$source_readlink_executable" -e -- "$HERDR_INCREMENT5_RUNNER_REQUESTED")" || return 20
  [[ $actual == "$HERDR_INCREMENT5_RUNNER_CANONICAL" ]] || return 20
  digest_line="$("$source_sha256sum_executable" -- "$HERDR_INCREMENT5_RUNNER_CANONICAL")" || return 20
  actual_digest=${digest_line%% *}
  [[ $actual_digest == "$HERDR_INCREMENT5_RUNNER_SHA256" ]] || return 20
  return 0
}

contain_attempt_id() {
  runner_attempt_id="${HERDR_INCREMENT5_ATTEMPT_ID:?missing attempt ID}"
  export -n runner_attempt_id
  unset HERDR_INCREMENT5_ATTEMPT_ID
  readonly runner_attempt_id || return 20
  [[ $runner_attempt_id =~ ^[0-9]{8}$ && $runner_attempt_id != 00000000 ]] || return 20
}

validate_output_containment() {
  [[ $# -ge 2 ]] || return 20
  local output=$1
  shift
  local protected
  [[ $output == /* ]] || return 20
  for protected in "$@"; do
    [[ $protected == /* ]] || return 20
    if [[ $protected == / || $output == "$protected" || $output == "$protected/"* ]]; then
      builtin printf '%s\n' \
        'error: --output-dir must be outside the repository and all linked worktrees' >&2
      return 20
    fi
  done
  return 0
}

guard_fixture_output_node() {
  [[ $# -eq 1 ]] || return 20
  local output=$1
  if [[ -L $output ]]; then
    builtin printf '%s\n' 'error: fixture output path is a symbolic link' >&2
    return 20
  fi
  if [[ -p $output ]]; then
    builtin printf '%s\n' 'error: fixture output path is a FIFO' >&2
    return 20
  fi
}

validate_fixture_output_path() {
  [[ $# -eq 1 ]] || return 20
  local output=$1
  [[ $output == /* && ${output##*/} != result-v1.json ]] || return 20
  guard_fixture_output_node "$output" || return 20
  [[ ! -e $output ]] || return 20
}

runtime_socket_path_has_shape() {
  [[ $# -eq 1 ]] || return 20
  local socket_path=$1
  [[ $socket_path == /tmp/herdr-i5.????????/*.sock && ${#socket_path} -le 107 ]]
}

publish_runner_test_outcome() {
  [[ $# -eq 3 ]] || return 20
  local output=$1
  local status=$2
  local reaped=$3
  local temporary
  validate_fixture_output_path "$output" || return 20
  case "$status" in 0|10|20) ;; *) return 20 ;; esac
  case "$reaped" in true|false) ;; *) return 20 ;; esac
  temporary="${output}.tmp.${BASHPID}"
  validate_fixture_output_path "$temporary" || return 20
  builtin printf \
    '{"schema_version":1,"non_authoritative":true,"exit_code":%s,"all_process_groups_reaped":%s}\n' \
    "$status" "$reaped" >"$temporary" || return 20
  "$source_mv_executable" -T -- "$temporary" "$output"
}

publish_trial_status() {
  [[ $# -eq 2 || $# -eq 3 ]] || return 20
  local herdr_i5_injected_output=$1
  local herdr_i5_injected_status=$2
  local herdr_i5_injected_mv_executable=${3-${source_mv_executable-}}
  local herdr_i5_injected_token herdr_i5_injected_temporary
  [[ $herdr_i5_injected_mv_executable == /* ]] || return 20
  case "$herdr_i5_injected_status" in
    0) herdr_i5_injected_token=ok:0 ;;
    ''|*[!0-9]*) return 20 ;;
    *) [[ $herdr_i5_injected_status -ge 1 && $herdr_i5_injected_status -le 255 ]] || return 20
       [[ $herdr_i5_injected_status == "${herdr_i5_injected_status#0}" ]] || return 20
       herdr_i5_injected_token="failed:$herdr_i5_injected_status" ;;
  esac
  validate_fixture_output_path "$herdr_i5_injected_output" || return 20
  herdr_i5_injected_temporary="${herdr_i5_injected_output}.tmp.${BASHPID}"
  validate_fixture_output_path "$herdr_i5_injected_temporary" || return 20
  builtin printf '%s\n' "$herdr_i5_injected_token" >"$herdr_i5_injected_temporary" || return 20
  "$herdr_i5_injected_mv_executable" -T -- \
    "$herdr_i5_injected_temporary" "$herdr_i5_injected_output"
}

publish_outer_runtime_state() {
  [[ $# -eq 5 ]] || return 20
  local herdr_i5_injected_output=$1
  local herdr_i5_injected_mv_executable=$2
  local herdr_i5_injected_measured=$3
  local herdr_i5_injected_observer=$4
  local herdr_i5_injected_socket_identity=$5
  local herdr_i5_injected_temporary="${herdr_i5_injected_output}.tmp.${BASHPID}"
  [[ $herdr_i5_injected_output == /* && $herdr_i5_injected_mv_executable == /* ]] || return 20
  [[ $herdr_i5_injected_measured == - || $herdr_i5_injected_measured =~ ^[1-9][0-9]*$ ]] || return 20
  [[ $herdr_i5_injected_observer == - || $herdr_i5_injected_observer =~ ^[1-9][0-9]*$ ]] || return 20
  [[ $herdr_i5_injected_socket_identity == - || $herdr_i5_injected_socket_identity == *:*:*:*:* ]] || return 20
  guard_fixture_output_node "$herdr_i5_injected_output" || return 20
  validate_fixture_output_path "$herdr_i5_injected_temporary" || return 20
  builtin printf '%s %s %s\n' "$herdr_i5_injected_measured" \
    "$herdr_i5_injected_observer" "$herdr_i5_injected_socket_identity" \
    >"$herdr_i5_injected_temporary" || return 20
  if [[ ${herdr_i5_interrupt_group_publication-} == true ]]; then
    builtin kill -TERM "$BASHPID" || return 20
  fi
  "$herdr_i5_injected_mv_executable" -T -- \
    "$herdr_i5_injected_temporary" "$herdr_i5_injected_output"
}

select_process_status() {
  [[ $# -eq 2 ]] || return 20
  local herdr_i5_injected_measured_status=$1
  local herdr_i5_injected_observer_status=$2
  [[ $herdr_i5_injected_measured_status =~ ^(0|[1-9][0-9]{0,2})$ && $herdr_i5_injected_measured_status -le 255 ]] || return 20
  [[ $herdr_i5_injected_observer_status =~ ^(0|[1-9][0-9]{0,2})$ && $herdr_i5_injected_observer_status -le 255 ]] || return 20
  if [[ $herdr_i5_injected_measured_status -ne 0 ]]; then
    selected_process_status=$herdr_i5_injected_measured_status
  elif [[ $herdr_i5_injected_observer_status -ne 0 ]]; then
    selected_process_status=$herdr_i5_injected_observer_status
  else
    selected_process_status=0
  fi
}

wait_process_pair() {
  [[ $# -eq 4 ]] || return 20
  local herdr_i5_injected_measured_pid=$1
  local herdr_i5_injected_observer_pid=$2
  local herdr_i5_injected_supervisor_pid=$3
  local herdr_i5_injected_supervisor_status=$4
  local herdr_i5_injected_completed_pid herdr_i5_injected_completed_status
  local herdr_i5_injected_measured_status= herdr_i5_injected_observer_status=
  local herdr_i5_injected_pending_pid herdr_i5_injected_sweep_pid
  local herdr_i5_injected_found
  local -a herdr_i5_injected_pending_pids=(
    "$herdr_i5_injected_measured_pid"
    "$herdr_i5_injected_observer_pid"
    "$herdr_i5_injected_supervisor_pid"
  )
  local -a herdr_i5_injected_sweep_pids=(
    "$herdr_i5_injected_supervisor_pid"
    "$herdr_i5_injected_measured_pid"
    "$herdr_i5_injected_observer_pid"
  )
  local -a herdr_i5_injected_next_pending_pids=()
  [[ $herdr_i5_injected_measured_pid =~ ^[1-9][0-9]*$ && $herdr_i5_injected_observer_pid =~ ^[1-9][0-9]*$ ]] || return 20
  [[ $herdr_i5_injected_supervisor_pid =~ ^[1-9][0-9]*$ ]] || return 20
  [[ $herdr_i5_injected_supervisor_status =~ ^[1-9][0-9]{0,2}$ && $herdr_i5_injected_supervisor_status -le 255 ]] || return 20
  [[ $herdr_i5_injected_measured_pid != "$herdr_i5_injected_observer_pid" && $herdr_i5_injected_measured_pid != "$herdr_i5_injected_supervisor_pid" ]] || return 20
  [[ $herdr_i5_injected_observer_pid != "$herdr_i5_injected_supervisor_pid" ]] || return 20
  # Bash <= 5.2 wait -n ignores children that terminated before the call. Sweep
  # the supervisor first to preserve its tie-break, then block only if all
  # pending children were alive at the sweep.
  supervisor_completed_first=false
  while [[ -z $herdr_i5_injected_measured_status || -z $herdr_i5_injected_observer_status ]]; do
    herdr_i5_injected_completed_pid=
    for herdr_i5_injected_sweep_pid in "${herdr_i5_injected_sweep_pids[@]}"; do
      herdr_i5_injected_found=false
      for herdr_i5_injected_pending_pid in "${herdr_i5_injected_pending_pids[@]}"; do
        if [[ $herdr_i5_injected_pending_pid == "$herdr_i5_injected_sweep_pid" ]]; then
          herdr_i5_injected_found=true
          break
        fi
      done
      if [[ $herdr_i5_injected_found == true ]] \
        && ! builtin kill -0 "$herdr_i5_injected_sweep_pid" 2>/dev/null; then
        set +e
        wait "$herdr_i5_injected_sweep_pid"
        herdr_i5_injected_completed_status=$?
        set -e
        herdr_i5_injected_completed_pid=$herdr_i5_injected_sweep_pid
        break
      fi
    done
    if [[ -z $herdr_i5_injected_completed_pid ]]; then
      set +e
      wait -n -p herdr_i5_injected_completed_pid \
        "${herdr_i5_injected_pending_pids[@]}"
      herdr_i5_injected_completed_status=$?
      set -e
    fi
    [[ $herdr_i5_injected_completed_pid =~ ^[1-9][0-9]*$ ]] || return 20
    herdr_i5_injected_found=false
    herdr_i5_injected_next_pending_pids=()
    for herdr_i5_injected_pending_pid in "${herdr_i5_injected_pending_pids[@]}"; do
      if [[ $herdr_i5_injected_pending_pid == "$herdr_i5_injected_completed_pid" ]]; then
        herdr_i5_injected_found=true
      else
        herdr_i5_injected_next_pending_pids+=("$herdr_i5_injected_pending_pid")
      fi
    done
    [[ $herdr_i5_injected_found == true ]] || return 20
    herdr_i5_injected_pending_pids=("${herdr_i5_injected_next_pending_pids[@]}")
    case "$herdr_i5_injected_completed_pid" in
      "$herdr_i5_injected_measured_pid")
        herdr_i5_injected_measured_status=$herdr_i5_injected_completed_status
        ;;
      "$herdr_i5_injected_observer_pid")
        herdr_i5_injected_observer_status=$herdr_i5_injected_completed_status
        ;;
      "$herdr_i5_injected_supervisor_pid")
        supervisor_completed_first=true
        selected_process_status=$herdr_i5_injected_supervisor_status
        return 0
        ;;
      *) return 20 ;;
    esac
  done
  select_process_status \
    "$herdr_i5_injected_measured_status" "$herdr_i5_injected_observer_status" || return 20
}

wait_orchestration_process() {
  [[ $# -eq 3 ]] || return 20
  local herdr_i5_injected_orchestration_pid=$1
  local herdr_i5_injected_supervisor_pid=$2
  local herdr_i5_injected_sleep_executable=$3
  local herdr_i5_injected_dummy_pid herdr_i5_injected_attempt
  [[ $herdr_i5_injected_orchestration_pid =~ ^[1-9][0-9]*$ ]] || return 20
  [[ $herdr_i5_injected_supervisor_pid =~ ^[1-9][0-9]*$ ]] || return 20
  ( exit 0 ) &
  herdr_i5_injected_dummy_pid=$!
  wait_process_pair "$herdr_i5_injected_orchestration_pid" \
    "$herdr_i5_injected_dummy_pid" "$herdr_i5_injected_supervisor_pid" 124 || return 20
  waited_orchestration_status=$selected_process_status
  if [[ $supervisor_completed_first == true ]]; then
    builtin kill -TERM "$herdr_i5_injected_orchestration_pid" 2>/dev/null || true
    for ((herdr_i5_injected_attempt=0; herdr_i5_injected_attempt<100; herdr_i5_injected_attempt++)); do
      builtin kill -0 "$herdr_i5_injected_orchestration_pid" 2>/dev/null || break
      "$herdr_i5_injected_sleep_executable" 0.01 || return 20
    done
    if builtin kill -0 "$herdr_i5_injected_orchestration_pid" 2>/dev/null; then
      builtin kill -KILL "$herdr_i5_injected_orchestration_pid" 2>/dev/null || return 20
    fi
    wait "$herdr_i5_injected_orchestration_pid" 2>/dev/null || true
  else
    builtin kill "$herdr_i5_injected_supervisor_pid" 2>/dev/null || true
    wait "$herdr_i5_injected_supervisor_pid" 2>/dev/null || true
  fi
}

install_orchestration_signal_traps() {
  local trap_marker_body
  trap_marker_body='if [[ ${HERDR_PERF_RUNNER_TEST_TRAP_MARKER+x} == x && $HERDR_PERF_RUNNER_TEST_TRAP_MARKER == /* && ${HERDR_PERF_RUNNER_TEST_TRAP_MARKER##*/} != result-v1.json ]]; then
    if [[ -L $HERDR_PERF_RUNNER_TEST_TRAP_MARKER ]]; then
      builtin printf "%s\n" "error: fixture output path is a symbolic link" >&2
    elif [[ -p $HERDR_PERF_RUNNER_TEST_TRAP_MARKER ]]; then
      builtin printf "%s\n" "error: fixture output path is a FIFO" >&2
    elif [[ ! -e $HERDR_PERF_RUNNER_TEST_TRAP_MARKER ]]; then
      : 2>/dev/null >"$HERDR_PERF_RUNNER_TEST_TRAP_MARKER" || :
    fi
  fi'
  trap "$trap_marker_body; exit 130" INT
  trap "$trap_marker_body; exit 143" TERM HUP
  trap "$trap_marker_body; exit 124" USR1
}

run_orchestration_signal_probe() {
  [[ $# -eq 2 ]] || return 20
  local signal=$1
  local ready=$2
  local signal_target signal_sender signal_sender_status
  case "$signal" in INT|TERM|HUP|USR1) ;; *) return 20 ;; esac
  validate_fixture_output_path "$ready" || return 20
  install_orchestration_signal_traps || return 20
  signal_target=$BASHPID
  (
    local ready_target readiness_deadline=$((SECONDS + 300))
    while :; do
      if [[ -f $ready && ! -L $ready ]] &&
        IFS= builtin read -r ready_target <"$ready" &&
        [[ $ready_target == "$signal_target" ]]; then
        break
      fi
      if (( SECONDS >= readiness_deadline )); then
        builtin kill -KILL "$signal_target" 2>/dev/null || true
        exit 20
      fi
      "$source_sleep_executable" 0.01 || {
        builtin kill -KILL "$signal_target" 2>/dev/null || true
        exit 20
      }
    done
    builtin kill -"$signal" "$signal_target"
  ) &
  signal_sender=$!
  if ! builtin printf '%s\n' "$signal_target" >"$ready"; then
    builtin kill -KILL "$signal_sender" 2>/dev/null || true
    wait "$signal_sender" 2>/dev/null || true
    return 20
  fi
  set +e
  wait "$signal_sender"
  signal_sender_status=$?
  set -e
  [[ $signal_sender_status -eq 0 ]] || return 20
  return 20
}

aggregate_closed_statuses() {
  local status
  aggregate_status=0
  aggregate_processed=0
  for status in "$@"; do
    case "$status" in
      0) ;;
      10) [[ $aggregate_status -eq 20 ]] || aggregate_status=10 ;;
      20) aggregate_status=20 ;;
      *) return 20 ;;
    esac
    ((aggregate_processed+=1))
    [[ $status -ne 20 ]] || break
  done
}

validate_pidstat_status_pair() {
  [[ $# -eq 3 ]] || return 20
  local mode=$1
  local trial_token=$2
  local observed_status=$3
  local trial_code
  case "$trial_token" in
    ok:0) trial_code=0 ;;
    failed:*)
      trial_code=${trial_token#failed:}
      [[ $trial_code =~ ^[1-9][0-9]{0,2}$ && $trial_code -le 255 ]] || return 20
      ;;
    *) return 20 ;;
  esac
  [[ $observed_status =~ ^(0|[1-9][0-9]{0,2})$ && $observed_status -le 255 ]] || return 20
  case "$mode" in
    propagates_child_status) [[ $observed_status -eq $trial_code ]] || return 20 ;;
    monitor_only) [[ $observed_status -eq 0 ]] || return 20 ;;
    *) return 20 ;;
  esac
}

fixture_no_sleep() {
  return 1
}

cleanup_process_groups() {
  [[ $# -eq 3 ]] || return 20
  local herdr_i5_injected_sleep_executable=$1
  local herdr_i5_injected_measured_pid=$2
  local herdr_i5_injected_observer_pid=$3
  local herdr_i5_injected_group herdr_i5_injected_child herdr_i5_injected_any_live
  local herdr_i5_injected_attempt herdr_i5_injected_cleanup_status=0
  process_groups_reaped=false
  for herdr_i5_injected_group in \
    "$herdr_i5_injected_measured_pid" "$herdr_i5_injected_observer_pid"; do
    [[ -n $herdr_i5_injected_group ]] || continue
    if ! builtin kill -TERM -- "-$herdr_i5_injected_group" 2>/dev/null; then
      ! builtin kill -0 -- "-$herdr_i5_injected_group" 2>/dev/null || \
        herdr_i5_injected_cleanup_status=20
    fi
  done
  for ((herdr_i5_injected_attempt=0; herdr_i5_injected_attempt<100; herdr_i5_injected_attempt++)); do
    herdr_i5_injected_any_live=false
    for herdr_i5_injected_group in \
      "$herdr_i5_injected_measured_pid" "$herdr_i5_injected_observer_pid"; do
      [[ -n $herdr_i5_injected_group ]] || continue
      if builtin kill -0 -- "-$herdr_i5_injected_group" 2>/dev/null; then
        herdr_i5_injected_any_live=true
      fi
    done
    [[ $herdr_i5_injected_any_live == false ]] && break
    "$herdr_i5_injected_sleep_executable" 0.01 || herdr_i5_injected_cleanup_status=20
  done
  for herdr_i5_injected_group in \
    "$herdr_i5_injected_measured_pid" "$herdr_i5_injected_observer_pid"; do
    [[ -n $herdr_i5_injected_group ]] || continue
    if ! builtin kill -KILL -- "-$herdr_i5_injected_group" 2>/dev/null; then
      ! builtin kill -0 -- "-$herdr_i5_injected_group" 2>/dev/null || \
        herdr_i5_injected_cleanup_status=20
    fi
  done
  for ((herdr_i5_injected_attempt=0; herdr_i5_injected_attempt<6000; herdr_i5_injected_attempt++)); do
    herdr_i5_injected_any_live=false
    for herdr_i5_injected_child in \
      "$herdr_i5_injected_measured_pid" "$herdr_i5_injected_observer_pid"; do
      [[ -n $herdr_i5_injected_child ]] || continue
      if builtin kill -0 "$herdr_i5_injected_child" 2>/dev/null; then
        herdr_i5_injected_any_live=true
      fi
    done
    for herdr_i5_injected_group in \
      "$herdr_i5_injected_measured_pid" "$herdr_i5_injected_observer_pid"; do
      [[ -n $herdr_i5_injected_group ]] || continue
      if builtin kill -0 -- "-$herdr_i5_injected_group" 2>/dev/null; then
        herdr_i5_injected_any_live=true
      fi
    done
    [[ $herdr_i5_injected_any_live == false ]] && break
    "$herdr_i5_injected_sleep_executable" 0.01 || herdr_i5_injected_cleanup_status=20
  done
  for herdr_i5_injected_child in \
    "$herdr_i5_injected_measured_pid" "$herdr_i5_injected_observer_pid"; do
    [[ -n $herdr_i5_injected_child ]] || continue
    if builtin kill -0 "$herdr_i5_injected_child" 2>/dev/null; then
      herdr_i5_injected_cleanup_status=20
      continue
    fi
    set +e
    wait "$herdr_i5_injected_child" 2>/dev/null
    set -e
  done
  for herdr_i5_injected_group in \
    "$herdr_i5_injected_measured_pid" "$herdr_i5_injected_observer_pid"; do
    [[ -n $herdr_i5_injected_group ]] || continue
    ! builtin kill -0 -- "-$herdr_i5_injected_group" 2>/dev/null || \
      herdr_i5_injected_cleanup_status=20
  done
  if [[ $herdr_i5_injected_cleanup_status -eq 0 ]]; then
    process_groups_reaped=true
    return 0
  fi
  return 20
}

wait_fixture_group_ready() {
  [[ $# -eq 1 ]] || return 20
  local group=$1
  local attempt
  for ((attempt=0; attempt<6000; attempt++)); do
    if builtin kill -0 -- "-$group" 2>/dev/null; then return 0; fi
    builtin kill -0 "$group" 2>/dev/null || return 20
    "$source_sleep_executable" 0.01 || return 20
  done
  return 20
}

launch_fixture_pair() {
  [[ $# -eq 4 ]] || return 20
  local groups_output=$1
  local measured_callback=$2
  local observer_callback=$3
  local require_ready=$4
  [[ $groups_output == /* && $measured_callback == /* && $observer_callback == /* ]] || return 20
  validate_fixture_output_path "$groups_output" || return 20
  [[ -x $measured_callback && -f $measured_callback ]] || return 20
  [[ -x $observer_callback && -f $observer_callback ]] || return 20
  "$source_setsid_executable" "$source_env_executable" -i \
    HOME=/home/mageyuki PATH=/usr/bin:/bin LC_ALL=C TZ=UTC \
    "$measured_callback" &
  fixture_measured_pid=$!
  "$source_setsid_executable" "$source_env_executable" -i \
    HOME=/home/mageyuki PATH=/usr/bin:/bin LC_ALL=C TZ=UTC \
    "$observer_callback" &
  fixture_observer_pid=$!
  readonly fixture_measured_pid fixture_observer_pid || return 20
  if [[ $require_ready == true ]]; then
    wait_fixture_group_ready "$fixture_measured_pid" || return 20
    wait_fixture_group_ready "$fixture_observer_pid" || return 20
  else
    [[ $require_ready == false ]] || return 20
  fi
  builtin printf '%s %s\n' "$fixture_measured_pid" "$fixture_observer_pid" >"$groups_output" || return 20
}

launch_fixture_handshake_phase() {
  [[ $# -eq 2 ]] || return 20
  local groups_output=$1
  local measured_callback=$2
  [[ $groups_output == /* && $measured_callback == /* ]] || return 20
  validate_fixture_output_path "$groups_output" || return 20
  [[ -x $measured_callback && -f $measured_callback ]] || return 20
  "$source_setsid_executable" "$source_env_executable" -i \
    HOME=/home/mageyuki PATH=/usr/bin:/bin LC_ALL=C TZ=UTC \
    "$measured_callback" &
  fixture_measured_pid=$!
  fixture_observer_pid=
  readonly fixture_measured_pid fixture_observer_pid || return 20
  wait_fixture_group_ready "$fixture_measured_pid" || return 20
  builtin printf '%s\n' "$fixture_measured_pid" >"$groups_output" || return 20
}

run_fixture_process_case() {
  [[ $# -eq 8 ]] || return 20
  local mode=$1
  local outcome=$2
  local groups_output=$3
  local trial_status_output=$4
  local measured_callback=$5
  local observer_callback=$6
  local trap_marker_path=$7
  local requested_status=$8
  local measured_status observer_status watchdog_pid cleanup_status=0
  local wait_attempt
  local cleanup_measured_pid cleanup_observer_pid
  local signal_probe_ready
  fixture_measured_pid=
  fixture_observer_pid=
  case "$mode" in
    timeout|signal-*)
      [[ $trap_marker_path != - ]] || return 20
      validate_fixture_output_path "$trap_marker_path" || return 20
      ;;
    precedence) [[ $trap_marker_path == - ]] || return 20 ;;
    *) return 20 ;;
  esac
  case "$mode" in
    signal-*-handshake)
      launch_fixture_handshake_phase "$groups_output" "$measured_callback" || return 20
      ;;
    *)
      if [[ $mode == precedence ]]; then
        launch_fixture_pair "$groups_output" "$measured_callback" "$observer_callback" false || return 20
      else
        launch_fixture_pair "$groups_output" "$measured_callback" "$observer_callback" true || return 20
      fi
      ;;
  esac
  cleanup_measured_pid=${fixture_measured_pid-}
  cleanup_observer_pid=${fixture_observer_pid-}
  case "$mode" in
    precedence)
      "$source_sleep_executable" 300 &
      watchdog_pid=$!
      if wait_process_pair \
        "$fixture_measured_pid" "$fixture_observer_pid" "$watchdog_pid" 124; then
        requested_status=$selected_process_status
        [[ $supervisor_completed_first == false ]] || requested_status=20
      else
        requested_status=20
      fi
      builtin kill "$watchdog_pid" 2>/dev/null || true
      wait "$watchdog_pid" 2>/dev/null || true
      watchdog_pid=
      ;;
    timeout)
      "$source_sleep_executable" 0.01 &
      watchdog_pid=$!
      for ((wait_attempt=0; wait_attempt<6000; wait_attempt++)); do
        builtin kill -0 "$watchdog_pid" 2>/dev/null || break
        "$source_sleep_executable" 0.01 || requested_status=20
      done
      if builtin kill -0 "$watchdog_pid" 2>/dev/null; then
        requested_status=20
      elif wait_process_pair \
        "$fixture_measured_pid" "$fixture_observer_pid" "$watchdog_pid" 124; then
        requested_status=$selected_process_status
        [[ $supervisor_completed_first == true ]] || requested_status=20
      else
        requested_status=20
      fi
      ;;
    signal-int-handshake|signal-int-after-observer)
      signal_probe_ready="${trap_marker_path}.ready"
      set +e
      ( HERDR_PERF_RUNNER_TEST_TRAP_MARKER=$trap_marker_path \
        run_orchestration_signal_probe INT "$signal_probe_ready" )
      requested_status=$?
      set -e
      ;;
    signal-term-handshake|signal-term-after-observer)
      signal_probe_ready="${trap_marker_path}.ready"
      set +e
      ( HERDR_PERF_RUNNER_TEST_TRAP_MARKER=$trap_marker_path \
        run_orchestration_signal_probe TERM "$signal_probe_ready" )
      requested_status=$?
      set -e
      ;;
    signal-hup-handshake|signal-hup-after-observer)
      signal_probe_ready="${trap_marker_path}.ready"
      set +e
      ( HERDR_PERF_RUNNER_TEST_TRAP_MARKER=$trap_marker_path \
        run_orchestration_signal_probe HUP "$signal_probe_ready" )
      requested_status=$?
      set -e
      ;;
    signal-usr1-handshake)
      signal_probe_ready="${trap_marker_path}.ready"
      set +e
      ( HERDR_PERF_RUNNER_TEST_TRAP_MARKER=$trap_marker_path \
        run_orchestration_signal_probe USR1 "$signal_probe_ready" )
      requested_status=$?
      set -e
      ;;
    *) return 20 ;;
  esac
  if [[ -n ${signal_probe_ready-} && ( -e $signal_probe_ready || -L $signal_probe_ready ) ]]; then
    if [[ -f $signal_probe_ready && ! -L $signal_probe_ready && ! -p $signal_probe_ready ]]; then
      "$source_unlink_executable" -- "$signal_probe_ready" || requested_status=20
    else
      requested_status=20
    fi
  fi
  cleanup_process_groups "$source_sleep_executable" \
    "$cleanup_measured_pid" "$cleanup_observer_pid" || cleanup_status=20
  [[ $cleanup_status -eq 0 ]] || requested_status=20
  publish_trial_status "$trial_status_output" "$requested_status" || return 20
  if [[ $requested_status -eq 0 ]]; then
    publish_runner_test_outcome "$outcome" 0 "$process_groups_reaped" || return 20
    return 0
  fi
  publish_runner_test_outcome "$outcome" 20 "$process_groups_reaped" || return 20
  return 20
}

run_orchestration_fixture() {
  bootstrap_source_fixture || return 20
  contain_attempt_id || return 20
  revalidate_source_fixture_bootstrap || return 20
  [[ ${HERDR_INCREMENT5_ATTEMPT_ID+x} != x ]] || return 20
  [[ ${HERDR_PERF_RUNNER_TEST_TRAP_MARKER+x} != x ]] || return 20
  local mode=${1-}
  shift || return 20
  case "$mode" in
    attempt-check)
      [[ $# -eq 1 || $# -eq 2 ]] || return 20
      local attempt_outcome=$1
      if [[ $# -eq 2 ]]; then
        local child_environment_output=$2
        validate_fixture_output_path "$child_environment_output" || return 20
        "$source_env_executable" -i \
          HOME=/home/mageyuki RUSTUP_HOME=/home/mageyuki/.rustup \
          CARGO_HOME=/home/mageyuki/.cargo PATH=/usr/bin:/bin LC_ALL=C TZ=UTC \
          >"$child_environment_output" || return 20
      fi
      publish_runner_test_outcome "$attempt_outcome" 0 true || return 20
      ;;
    scenario-loop)
      [[ $# -eq 2 ]] || return 20
      local scenario_status=$1
      local scenario_calls_path=$2
      case "$scenario_status" in 10|20) ;; *) return 20 ;; esac
      validate_fixture_output_path "$scenario_calls_path" || return 20
      runner_scenario=all
      validate_baseline_layout_up_front() { return 0; }
      run_single_reference_scenario() {
        local scenario=$1
        builtin printf '%s\n' "$scenario" >>"$scenario_calls_path" || return 20
        if [[ $scenario == startup ]]; then
          set -e
          return "$scenario_status"
        fi
        return 0
      }
      run_reference_scenarios
      ;;
    aggregate)
      [[ $# -ge 2 ]] || return 20
      local aggregate_outcome=$1
      shift
      local processed_output temporary
      aggregate_closed_statuses "$@" || return 20
      processed_output="${aggregate_outcome%.*}.processed"
      validate_fixture_output_path "$aggregate_outcome" || return 20
      validate_fixture_output_path "$processed_output" || return 20
      temporary="${processed_output}.tmp.${BASHPID}"
      builtin printf '%s\n' "$aggregate_processed" >"$temporary" || return 20
      "$source_mv_executable" -T -- "$temporary" "$processed_output" || return 20
      publish_runner_test_outcome "$aggregate_outcome" "$aggregate_status" true || return 20
      return "$aggregate_status"
      ;;
    sentinel)
      [[ $# -eq 4 ]] || return 20
      local sentinel_outcome=$1
      local sentinel_path=$2
      local orchestrator_status=$3
      local pidstat_status=$4
      [[ $pidstat_status =~ ^(0|[1-9][0-9]{0,2})$ && $pidstat_status -le 255 ]] || return 20
      publish_trial_status "$sentinel_path" "$orchestrator_status" || return 20
      publish_runner_test_outcome "$sentinel_outcome" 0 true || return 20
      ;;
    read-status)
      [[ $# -eq 2 ]] || return 20
      local read_status_outcome=$1
      local read_status_path=$2
      read_trial_status "$read_status_path" || return 20
      publish_runner_test_outcome "$read_status_outcome" 0 true || return 20
      ;;
    identity-revalidation)
      [[ $# -eq 3 ]] || return 20
      local revalidation_outcome=$1
      local mutation_callback=$2
      local mutation_operand=$3
      [[ $mutation_callback == /* && -f $mutation_callback && -x $mutation_callback ]] || return 20
      "$source_env_executable" -i HOME=/home/mageyuki PATH=/usr/bin:/bin LC_ALL=C TZ=UTC \
        "$mutation_callback" "$mutation_operand" || return 20
      revalidate_source_fixture_bootstrap || return 20
      publish_runner_test_outcome "$revalidation_outcome" 0 true || return 20
      ;;
    socket-shape)
      [[ $# -eq 2 ]] || return 20
      local socket_shape_outcome=$1
      local socket_shape_path=$2
      runtime_socket_path_has_shape "$socket_shape_path" || return 20
      publish_runner_test_outcome "$socket_shape_outcome" 0 true || return 20
      ;;
    fixture-output-guard)
      [[ $# -eq 2 ]] || return 20
      local fixture_guard_site=$1
      local fixture_guard_path=$2
      case "$fixture_guard_site" in
        validator)
          validate_fixture_output_path "$fixture_guard_path" || return 20
          ;;
        runner-outcome)
          publish_runner_test_outcome "$fixture_guard_path" 0 true || return 20
          ;;
        trial-status)
          publish_trial_status "$fixture_guard_path" 0 || return 20
          ;;
        trap-marker)
          local fixture_guard_status fixture_guard_ready
          fixture_guard_ready="${fixture_guard_path}.ready"
          set +e
          ( HERDR_PERF_RUNNER_TEST_TRAP_MARKER=$fixture_guard_path \
            run_orchestration_signal_probe TERM "$fixture_guard_ready" )
          fixture_guard_status=$?
          set -e
          if [[ -f $fixture_guard_ready && ! -L $fixture_guard_ready ]]; then
            "$source_unlink_executable" -- "$fixture_guard_ready" || return 20
          else
            return 20
          fi
          [[ $fixture_guard_status -eq 143 ]] || return 20
          return 20
          ;;
        *) return 20 ;;
      esac
      return 20
      ;;
    outer-identity-window)
      [[ $# -eq 1 ]] || return 20
      local identity_window_capture=$1
      validate_fixture_output_path "$identity_window_capture" || return 20
      bind_source_trial_tools || return 20
      herdr_i5_identity_window_capture=$identity_window_capture
      prepare_runtime_dir fx i0001 || return 20
      return 20
      ;;
    outer-group-publication)
      [[ $# -eq 2 ]] || return 20
      local group_publication_directory_capture=$1
      local group_publication_state_capture=$2
      validate_fixture_output_path "$group_publication_directory_capture" || return 20
      validate_fixture_output_path "$group_publication_state_capture" || return 20
      bind_source_trial_tools || return 20
      prepare_runtime_dir fx g0001 || return 20
      builtin printf '%s\n' "$active_runtime_dir" \
        >"$group_publication_directory_capture" || return 20
      publish_outer_runtime_state "$active_runtime_state" "$auth_mv_executable" \
        - - - || return 20
      herdr_i5_group_publication_capture=$group_publication_state_capture
      herdr_i5_interrupt_group_publication=true
      publish_outer_runtime_state "$active_runtime_state" "$auth_mv_executable" \
        999999 999998 - || return 20
      return 20
      ;;
    publisher-temp-cleanup)
      [[ $# -eq 1 ]] || return 20
      local publisher_temp_directory_capture=$1
      validate_fixture_output_path "$publisher_temp_directory_capture" || return 20
      bind_source_trial_tools || return 20
      prepare_runtime_dir fx p0001 || return 20
      builtin printf '%s\n' "$active_runtime_dir" \
        >"$publisher_temp_directory_capture" || return 20
      : >"${active_runtime_state}.tmp.${BASHPID}" || return 20
      builtin kill -TERM "$BASHPID" || return 20
      return 20
      ;;
    orchestration-deadline)
      [[ $# -eq 1 ]] || return 20
      local orchestration_deadline_outcome=$1
      local orchestration_deadline_pid orchestration_deadline_supervisor
      "$source_sleep_executable" 300 &
      orchestration_deadline_pid=$!
      "$source_sleep_executable" 0.01 &
      orchestration_deadline_supervisor=$!
      wait_orchestration_process "$orchestration_deadline_pid" \
        "$orchestration_deadline_supervisor" "$source_sleep_executable" || return 20
      [[ $waited_orchestration_status -eq 124 ]] || return 20
      publish_runner_test_outcome "$orchestration_deadline_outcome" 0 true || return 20
      ;;
    scratch-root)
      [[ $# -eq 3 ]] || return 20
      local scratch_outcome=$1
      local scratch_trial_root=$2
      local scratch_capture=$3
      validate_fixture_output_path "$scratch_outcome" || return 20
      validate_fixture_output_path "$scratch_capture" || return 20
      prepare_trial_scratch_root "$scratch_trial_root" "$source_mkdir_executable" || return 20
      builtin printf '%s\n' "$trial_scratch_root" >"$scratch_capture" || return 20
      publish_runner_test_outcome "$scratch_outcome" 0 true || return 20
      ;;
    outer-runtime-signal)
      [[ $# -eq 2 ]] || return 20
      local runtime_capture=$1
      local runtime_callback=$2
      local runtime_attempt runtime_group
      validate_fixture_output_path "$runtime_capture" || return 20
      [[ $runtime_callback == /* && -f $runtime_callback && -x $runtime_callback ]] || return 20
      bind_source_trial_tools || return 20
      prepare_runtime_dir t t0001 || return 20
      "$source_setsid_executable" "$source_env_executable" -i \
        HERDR_FIXTURE_SOCKET="$active_runtime_socket" \
        "$runtime_callback" fixture_outer_runtime_live_child --exact --ignored \
          --test-threads=1 &
      active_measured_pid=$!
      for ((runtime_attempt=0; runtime_attempt<6000; runtime_attempt++)); do
        [[ -S $active_runtime_socket ]] && break
        builtin kill -0 "$active_measured_pid" 2>/dev/null || return 20
        "$source_sleep_executable" 0.01 || return 20
      done
      [[ -S $active_runtime_socket && ! -L $active_runtime_socket ]] || return 20
      active_socket_identity="$("$auth_stat_executable" --format='%d:%i:%u:%f:%F' -- "$active_runtime_socket")" || return 20
      runtime_group=$active_measured_pid
      publish_outer_runtime_state "$active_runtime_state" "$auth_mv_executable" \
        "$runtime_group" - "$active_socket_identity" || return 20
      builtin printf '%s\n%s\n%s\n' \
        "$active_runtime_dir" "$active_runtime_socket" "$runtime_group" \
        >"$runtime_capture" || return 20
      active_measured_pid=
      active_socket_identity=
      builtin kill -TERM "$BASHPID"
      return 20
      ;;
    nested-trial)
      [[ $# -eq 7 ]] || return 20
      local nested_outcome=$1
      local nested_trial_root=$2
      local nested_runtime_capture=$3
      local nested_test_binary=$4
      local nested_scenario=$5
      local nested_deadline=$6
      local nested_handshake_attempt_limit=$7
      local nested_status
      validate_fixture_output_path "$nested_outcome" || return 20
      validate_fixture_output_path "$nested_runtime_capture" || return 20
      [[ $nested_trial_root == /* && ! -e $nested_trial_root && ! -L $nested_trial_root ]] || return 20
      [[ $nested_test_binary == /* && -f $nested_test_binary && -x $nested_test_binary ]] || return 20
      case "$nested_scenario" in target|idle) ;; *) return 20 ;; esac
      [[ $nested_deadline =~ ^[1-9][0-9]*$ ]] || return 20
      [[ $nested_handshake_attempt_limit =~ ^[1-9][0-9]*$ ]] || return 20
      bind_source_trial_tools || return 20
      test_binary=$nested_test_binary
      pidstat_child_status_mode=propagates_child_status
      prepare_trial_scratch_root "$nested_trial_root" "$auth_mkdir_executable" || return 20
      prepare_runtime_dir fx n001 || return 20
      builtin printf '%s\n%s\n' "$active_runtime_dir" "$active_runtime_socket" \
        >"$nested_runtime_capture" || return 20
      run_trial_process_tree \
        "$nested_trial_root/gnu-time.txt" \
        "$nested_trial_root/stdout" "$nested_trial_root/stderr" \
        "$nested_trial_root/harness.json" "$nested_trial_root/observer-handshake" \
        "$active_runtime_socket" "$nested_trial_root/observer-control.json" \
        "$nested_trial_root/process-tree.json" "$nested_trial_root/observer-stdout" \
        "$nested_trial_root/observer-stderr" "$nested_trial_root/pidstat.json" \
        "$nested_trial_root/pidstat-stderr" "$nested_trial_root/trial-status" \
        "$trial_scratch_root" "$active_runtime_dir" "$nested_scenario" baseline \
        0123456789abcdef0123456789abcdef01234567 - "$nested_deadline" \
        "$nested_handshake_attempt_limit" || return 20
      safe_outer_runtime_state_cleanup || return 20
      safe_outer_runtime_cleanup || return 20
      clear_outer_runtime_traps || return 20
      nested_status=0
      [[ $last_trial_code -eq 0 ]] || nested_status=20
      publish_runner_test_outcome "$nested_outcome" "$nested_status" true || return 20
      return "$nested_status"
      ;;
    cleanup-failure)
      [[ $# -eq 2 ]] || return 20
      local cleanup_outcome=$1
      local cleanup_status_path=$2
      "$source_setsid_executable" "$source_bash_executable" -p -c \
        'trap "" HUP TERM; "$1" 300 </dev/null >/dev/null 2>&1' \
        herdr-i5-cleanup-failure "$source_sleep_executable" &
      local cleanup_fixture_pid=$!
      wait_fixture_group_ready "$cleanup_fixture_pid" || return 20
      cleanup_process_groups fixture_no_sleep "$cleanup_fixture_pid" '' && return 20
      publish_trial_status "$cleanup_status_path" 20 || return 20
      publish_runner_test_outcome "$cleanup_outcome" 20 "$process_groups_reaped" || return 20
      return 20
      ;;
    cleanup-missed-group)
      [[ $# -eq 3 ]] || return 20
      local missed_group_outcome=$1
      local missed_group_status_path=$2
      local missed_group_ready=$3
      local missed_group_pid missed_group_cleanup_status
      validate_fixture_output_path "$missed_group_outcome" || return 20
      validate_fixture_output_path "$missed_group_status_path" || return 20
      validate_fixture_output_path "$missed_group_ready" || return 20
      "$source_sleep_executable" 300 &
      missed_group_pid=$!
      builtin printf '%s\n' "$missed_group_pid" >"$missed_group_ready" || return 20
      if cleanup_process_groups fixture_no_sleep "$missed_group_pid" ''; then
        missed_group_cleanup_status=0
      else
        missed_group_cleanup_status=$?
      fi
      builtin kill -KILL "$missed_group_pid" 2>/dev/null || true
      wait "$missed_group_pid" 2>/dev/null || true
      [[ $missed_group_cleanup_status -eq 20 ]] || return 20
      publish_trial_status "$missed_group_status_path" 20 || return 20
      publish_runner_test_outcome \
        "$missed_group_outcome" 20 "$process_groups_reaped" || return 20
      return 20
      ;;
    baseline-set)
      [[ $# -eq 3 ]] || return 20
      local baseline_outcome=$1
      local baseline_root=$2
      local baseline_validator=$3
      local baseline_status
      [[ $baseline_validator == /* && -f $baseline_validator && -x $baseline_validator ]] || return 20
      set +e
      "$source_env_executable" -i \
        HERDR_PERF_VALIDATE_BASELINE_RESULTS_ROOT="$baseline_root" \
        "$baseline_validator" validate_reference_baseline_set --exact --ignored --test-threads=1
      baseline_status=$?
      set -e
      [[ $baseline_status -eq 0 ]] || return 20
      publish_runner_test_outcome "$baseline_outcome" 0 true || return 20
      ;;
    pidstat-calibration)
      [[ $# -eq 6 ]] || return 20
      local pidstat_outcome=$1
      local pidstat_mode_output=$2
      local zero_output=$3
      local failing_output=$4
      local sentinel_status=$5
      local observed_pidstat=$6
      local temporary
      validate_fixture_output_path "$pidstat_outcome" || return 20
      validate_fixture_output_path "$pidstat_mode_output" || return 20
      validate_fixture_output_path "$zero_output" || return 20
      validate_fixture_output_path "$failing_output" || return 20
      calibrate_pidstat_mode "$zero_output" "$failing_output" source || return 20
      recalibrate_pidstat_mode "$zero_output" "$failing_output" source || return 20
      if [[ $sentinel_status -eq 0 ]]; then
        validate_pidstat_status_pair "$pidstat_child_status_mode" ok:0 "$observed_pidstat" || return 20
      else
        validate_pidstat_status_pair "$pidstat_child_status_mode" \
          "failed:$sentinel_status" "$observed_pidstat" || return 20
      fi
      temporary="${pidstat_mode_output}.tmp.${BASHPID}"
      builtin printf '%s\n' "$pidstat_child_status_mode" >"$temporary" || return 20
      "$source_mv_executable" -T -- "$temporary" "$pidstat_mode_output" || return 20
      publish_runner_test_outcome "$pidstat_outcome" 0 true || return 20
      ;;
    timeout|signal-int-handshake|signal-term-handshake|signal-hup-handshake|signal-usr1-handshake|\
      signal-int-after-observer|signal-term-after-observer|signal-hup-after-observer)
      [[ $# -eq 6 ]] || return 20
      run_fixture_process_case "$mode" "$1" "$2" "$3" "$4" "$5" "$6" 0 || return 20
      ;;
    precedence)
      [[ $# -eq 5 ]] || return 20
      run_fixture_process_case "$mode" "$1" "$2" "$3" "$4" "$5" - 0 || return 20
      ;;
    *) return 20 ;;
  esac
}

run_output_containment_fixture() {
  bootstrap_source_fixture || return 20
  contain_attempt_id || return 20
  revalidate_source_fixture_bootstrap || return 20
  [[ $# -ge 3 ]] || return 20
  local repository_root=$1
  local root_count=$2
  shift 2
  [[ $root_count =~ ^(0|[1-9][0-9]*)$ ]] || return 20
  [[ $# -eq $((root_count + 1)) ]] || return 20
  local -a roots=()
  local index
  for ((index=0; index<root_count; index++)); do
    roots+=("$1")
    shift
  done
  local output_dir=$1
  validate_output_containment "$output_dir" "$repository_root" "${roots[@]}" || return 20
}

bootstrap_authoritative_manifest() {
  [[ ${authoritative_bootstrap_parsed-} != 1 ]] || return 0
  [[ ${HERDR_INCREMENT5_BOOTSTRAP_TOOLS_SOURCE_FIXTURE_V1+x} != x ]] || return 20
  local controller_requested=${HERDR_INCREMENT5_CONTROLLER_REQUESTED:?}
  local controller_canonical=${HERDR_INCREMENT5_CONTROLLER_CANONICAL:?}
  local controller_sha256=${HERDR_INCREMENT5_CONTROLLER_SHA256:?}
  local runner_requested=${HERDR_INCREMENT5_RUNNER_REQUESTED:?}
  local runner_canonical=${HERDR_INCREMENT5_RUNNER_CANONICAL:?}
  local runner_sha256=${HERDR_INCREMENT5_RUNNER_SHA256:?}
  local manifest=${HERDR_INCREMENT5_BOOTSTRAP_TOOLS_V1:?}
  local requested canonical digest extra role variable
  local -a requested_paths=() canonical_paths=() digests=()
  local -ar roles=(
    rustup awk bash env findmnt git id jq lsblk lscpu mkdir mktemp mv
    pidstat prlimit readlink rg rmdir setsid sha256sum sleep stat taskset
    time uname unlink
  )
  local -ar expected_requested=(
    /home/mageyuki/.cargo/bin/rustup
    /usr/bin/awk /usr/bin/bash /usr/bin/env /usr/bin/findmnt /usr/bin/git
    /usr/bin/id /usr/bin/jq /usr/bin/lsblk /usr/bin/lscpu /usr/bin/mkdir
    /usr/bin/mktemp /usr/bin/mv /usr/bin/pidstat /usr/bin/prlimit
    /usr/bin/readlink /usr/bin/rg /usr/bin/rmdir /usr/bin/setsid
    /usr/bin/sha256sum /usr/bin/sleep /usr/bin/stat /usr/bin/taskset
    /usr/bin/time /usr/bin/uname /usr/bin/unlink
  )

  [[ $controller_requested == /* && $controller_canonical == /* ]] || return 20
  [[ $runner_requested == /* && $runner_canonical == /* ]] || return 20
  [[ $controller_sha256 =~ ^[0-9a-f]{64}$ ]] || return 20
  [[ $runner_sha256 =~ ^[0-9a-f]{64}$ ]] || return 20
  [[ ${BASH_SOURCE[0]} == "$runner_canonical" ]] || return 20
  while IFS=$'\t' read -r requested canonical digest extra; do
    [[ -n $requested && -n $canonical && -n $digest && -z $extra ]] || return 20
    [[ $requested == /* && $canonical == /* ]] || return 20
    [[ $requested != *$'\n'* && $requested != *$'\r'* ]] || return 20
    [[ $canonical != *$'\n'* && $canonical != *$'\r'* ]] || return 20
    [[ $digest =~ ^[0-9a-f]{64}$ ]] || return 20
    requested_paths+=("$requested")
    canonical_paths+=("$canonical")
    digests+=("$digest")
  done < <(builtin printf '%s' "$manifest")
  [[ ${#requested_paths[@]} -eq ${#roles[@]} ]] || return 20
  for ((index=0; index<${#roles[@]}; index++)); do
    [[ ${requested_paths[$index]} == "${expected_requested[$index]}" ]] || return 20
    role=${roles[$index]}
    variable="auth_${role}_requested"
    builtin printf -v "$variable" '%s' "${requested_paths[$index]}" || return 20
    readonly "$variable" || return 20
    variable="auth_${role}_executable"
    builtin printf -v "$variable" '%s' "${canonical_paths[$index]}" || return 20
    readonly "$variable" || return 20
    variable="auth_${role}_sha256"
    builtin printf -v "$variable" '%s' "${digests[$index]}" || return 20
    readonly "$variable" || return 20
  done
  authoritative_bootstrap_parsed=1
  readonly authoritative_bootstrap_parsed || return 20
}

revalidate_authoritative_bootstrap() {
  [[ ${authoritative_bootstrap_parsed-} == 1 ]] || return 20
  local -ar roles=(
    rustup awk bash env findmnt git id jq lsblk lscpu mkdir mktemp mv
    pidstat prlimit readlink rg rmdir setsid sha256sum sleep stat taskset
    time uname unlink
  )
  local role requested_variable canonical_variable digest_variable
  local requested canonical digest actual digest_line actual_digest
  for role in "${roles[@]}"; do
    requested_variable="auth_${role}_requested"
    canonical_variable="auth_${role}_executable"
    digest_variable="auth_${role}_sha256"
    requested=${!requested_variable}
    canonical=${!canonical_variable}
    digest=${!digest_variable}
    actual="$("$auth_readlink_executable" -e -- "$requested")" || return 20
    [[ $actual == "$canonical" ]] || return 20
    digest_line="$("$auth_sha256sum_executable" -- "$canonical")" || return 20
    actual_digest=${digest_line%% *}
    [[ $actual_digest == "$digest" ]] || return 20
  done
  actual="$("$auth_readlink_executable" -e -- "$HERDR_INCREMENT5_CONTROLLER_REQUESTED")" || return 20
  [[ $actual == "$HERDR_INCREMENT5_CONTROLLER_CANONICAL" ]] || return 20
  digest_line="$("$auth_sha256sum_executable" -- "$HERDR_INCREMENT5_CONTROLLER_CANONICAL")" || return 20
  actual_digest=${digest_line%% *}
  [[ $actual_digest == "$HERDR_INCREMENT5_CONTROLLER_SHA256" ]] || return 20
  actual="$("$auth_readlink_executable" -e -- "$HERDR_INCREMENT5_RUNNER_REQUESTED")" || return 20
  [[ $actual == "$HERDR_INCREMENT5_RUNNER_CANONICAL" ]] || return 20
  digest_line="$("$auth_sha256sum_executable" -- "$HERDR_INCREMENT5_RUNNER_CANONICAL")" || return 20
  actual_digest=${digest_line%% *}
  [[ $actual_digest == "$HERDR_INCREMENT5_RUNNER_SHA256" ]] || return 20
  [[ $auth_bash_requested == "$auth_bash_executable" ]] || return 20
  return 0
}

parse_authoritative_arguments() {
  runner_subject_argument=
  runner_stage=
  runner_scenario=
  runner_output_argument=
  runner_baseline_argument=
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --subject)
        [[ -z $runner_subject_argument && $# -ge 2 ]] || return 20
        runner_subject_argument=$2
        shift 2
        ;;
      --stage)
        [[ -z $runner_stage && $# -ge 2 ]] || return 20
        runner_stage=$2
        shift 2
        ;;
      --scenario)
        [[ -z $runner_scenario && $# -ge 2 ]] || return 20
        runner_scenario=$2
        shift 2
        ;;
      --output-dir)
        [[ -z $runner_output_argument && $# -ge 2 ]] || return 20
        runner_output_argument=$2
        shift 2
        ;;
      --baseline-results-root)
        [[ -z $runner_baseline_argument && $# -ge 2 ]] || return 20
        runner_baseline_argument=$2
        shift 2
        ;;
      *) return 20 ;;
    esac
  done
  [[ -n $runner_subject_argument && -n $runner_stage ]] || return 20
  [[ -n $runner_scenario && -n $runner_output_argument ]] || return 20
  case "$runner_stage" in
    baseline) [[ -z $runner_baseline_argument ]] || return 20 ;;
    post-reliability|final) [[ -n $runner_baseline_argument ]] || return 20 ;;
    *) return 20 ;;
  esac
  case "$runner_scenario" in
    target|sustained|burst|startup|idle|fallback-rescan|twice-target|all) ;;
    *) return 20 ;;
  esac
  [[ $runner_output_argument == /* ]] || return 20
  readonly runner_subject_argument runner_stage runner_scenario || return 20
  readonly runner_output_argument runner_baseline_argument || return 20
}

revalidate_authoritative_executable() {
  [[ $# -eq 1 ]] || return 20
  local role=$1
  local requested_variable="auth_${role}_requested"
  local canonical_variable="auth_${role}_executable"
  local digest_variable="auth_${role}_sha256"
  local actual digest_line
  actual="$("$auth_readlink_executable" -e -- "${!requested_variable}")" || return 20
  [[ $actual == "${!canonical_variable}" ]] || return 20
  digest_line="$("$auth_sha256sum_executable" -- "${!canonical_variable}")" || return 20
  [[ ${digest_line%% *} == "${!digest_variable}" ]] || return 20
}

freeze_worktree_roots() {
  local line path
  local output
  output="$("$auth_git_executable" worktree list --porcelain)" || return 20
  worktree_roots=()
  while IFS= read -r line; do
    case "$line" in
      worktree\ *)
        path=${line#worktree }
        [[ $path == /* ]] || return 20
        path="$("$auth_readlink_executable" -e -- "$path")" || return 20
        worktree_roots+=("$path")
        ;;
    esac
  done <<<"$output"
  [[ ${#worktree_roots[@]} -ge 1 ]] || return 20
  readonly worktree_roots || return 20
}

validate_attempt_paths() {
  local output_parent output_basename canonical_parent expected_basename stage_label subject12
  output_parent=${runner_output_argument%/*}
  output_basename=${runner_output_argument##*/}
  [[ -n $output_parent && -n $output_basename ]] || return 20
  [[ -d $output_parent && ! -L $output_parent ]] || return 20
  canonical_parent="$("$auth_readlink_executable" -e -- "$output_parent")" || return 20
  runner_output_root="$canonical_parent/$output_basename"
  [[ $runner_output_argument == "$runner_output_root" ]] || return 20
  [[ ! -e $runner_output_root && ! -L $runner_output_root ]] || return 20
  case "$runner_stage" in
    baseline) stage_label=baseline ;;
    post-reliability) stage_label=post-reliability ;;
    final) stage_label=final ;;
    *) return 20 ;;
  esac
  subject12=${runner_subject:0:12}
  expected_basename="${stage_label}-${subject12}-attempt-${runner_attempt_id}"
  [[ $output_basename == "$expected_basename" ]] || return 20
  [[ $output_basename =~ ^(baseline|post-reliability|final)-[0-9a-f]{12}-attempt-[0-9]{8}$ ]] || return 20
  validate_output_containment "$runner_output_root" "$repository_root" "${worktree_roots[@]}" || return 20
  readonly runner_output_root || return 20

  if [[ $runner_stage != baseline ]]; then
    runner_baseline_root="$("$auth_readlink_executable" -e -- "$runner_baseline_argument")" || return 20
    [[ -d $runner_baseline_root && ! -L $runner_baseline_root ]] || return 20
    validate_output_containment "$runner_baseline_root" "$repository_root" "${worktree_roots[@]}" || return 20
    [[ $runner_output_root != "$runner_baseline_root" ]] || return 20
    [[ $runner_output_root != "$runner_baseline_root/"* ]] || return 20
    [[ $runner_baseline_root != "$runner_output_root/"* ]] || return 20
    readonly runner_baseline_root || return 20
  else
    runner_baseline_root=
    readonly runner_baseline_root || return 20
  fi
}

validate_cargo_configuration_absence() {
  local current candidate
  local -a candidates=()
  current=$invocation_cwd
  while :; do
    candidates+=("$current/.cargo/config" "$current/.cargo/config.toml")
    [[ $current != / ]] || break
    current=${current%/*}
    [[ -n $current ]] || current=/
  done
  for candidate in /home/mageyuki/.cargo/config /home/mageyuki/.cargo/config.toml; do
    local present=false existing
    for existing in "${candidates[@]}"; do
      [[ $existing != "$candidate" ]] || present=true
    done
    [[ $present == true ]] || candidates+=("$candidate")
  done
  for candidate in "${candidates[@]}"; do
    [[ ! -e $candidate && ! -L $candidate ]] || return 20
  done
  cargo_configuration_candidates=("${candidates[@]}")
  readonly cargo_configuration_candidates
}

pidstat_diagnostic_is_valid() {
  [[ $# -eq 2 ]] || return 20
  local path=$1
  local jq_executable=$2
  [[ -f $path && ! -L $path ]] || return 20
  [[ $jq_executable == /* && -f $jq_executable && -x $jq_executable ]] || return 20
  "$jq_executable" --exit-status '
    type == "object"
    and (.sysstat | type == "object")
    and (.sysstat.hosts | type == "array")
  ' "$path" >/dev/null 2>&1 || return 20
}

probe_pidstat_mode() {
  [[ $# -eq 3 ]] || return 20
  local zero_output=$1
  local failing_output=$2
  local inventory_prefix=$3
  local env_variable="${inventory_prefix}_env_executable"
  local pidstat_variable="${inventory_prefix}_pidstat_executable"
  local bash_variable="${inventory_prefix}_bash_executable"
  local jq_executable
  local env_executable=${!env_variable-}
  local pidstat_executable=${!pidstat_variable-}
  local bash_executable=${!bash_variable-}
  local zero_status failing_status
  [[ $env_executable == /* && $pidstat_executable == /* && $bash_executable == /* ]] || return 20
  case "$inventory_prefix" in
    auth)
      revalidate_authoritative_executable env || return 20
      revalidate_authoritative_executable pidstat || return 20
      revalidate_authoritative_executable bash || return 20
      revalidate_authoritative_executable jq || return 20
      jq_executable=$auth_jq_executable
      ;;
    source)
      revalidate_source_fixture_bootstrap || return 20
      # The attested pidstat shim is deliberately dual-interface: jq's --exit-status
      # argv dispatches it to the Rust JSON validator helper; source has no jq role.
      jq_executable=$source_pidstat_executable
      ;;
    *) return 20 ;;
  esac
  set +e
  "$env_executable" -i HOME=/home/mageyuki PATH=/usr/bin:/bin LC_ALL=C TZ=UTC \
    "$pidstat_executable" -u -r -T ALL -o JSON 1 -e \
    "$bash_executable" -p -c 'exit 0' >"$zero_output" 2>&1
  zero_status=$?
  "$env_executable" -i HOME=/home/mageyuki PATH=/usr/bin:/bin LC_ALL=C TZ=UTC \
    "$pidstat_executable" -u -r -T ALL -o JSON 1 -e \
    "$bash_executable" -p -c 'exit 23' >"$failing_output" 2>&1
  failing_status=$?
  set -e
  pidstat_diagnostic_is_valid "$zero_output" "$jq_executable" || return 20
  pidstat_diagnostic_is_valid "$failing_output" "$jq_executable" || return 20
  probed_pidstat_zero_status=$zero_status
  probed_pidstat_failing_status=$failing_status
  if [[ $zero_status -eq 0 && $failing_status -eq 23 ]]; then
    probed_pidstat_mode=propagates_child_status
  elif [[ $zero_status -eq 0 && $failing_status -eq 0 ]]; then
    probed_pidstat_mode=monitor_only
  else
    return 20
  fi
}

calibrate_pidstat_mode() {
  [[ $# -eq 3 ]] || return 20
  probe_pidstat_mode "$1" "$2" "$3" || return 20
  pidstat_child_status_mode=$probed_pidstat_mode
  pidstat_calibration_zero_status=$probed_pidstat_zero_status
  pidstat_calibration_failing_status=$probed_pidstat_failing_status
  readonly pidstat_child_status_mode || return 20
  readonly pidstat_calibration_zero_status pidstat_calibration_failing_status || return 20
}

recalibrate_pidstat_mode() {
  [[ $# -eq 3 ]] || return 20
  probe_pidstat_mode "$1" "$2" "$3" || return 20
  [[ $probed_pidstat_mode == "$pidstat_child_status_mode" ]] || return 20
  [[ $probed_pidstat_zero_status -eq $pidstat_calibration_zero_status ]] || return 20
  [[ $probed_pidstat_failing_status -eq $pidstat_calibration_failing_status ]] || return 20
}

select_measured_binary() {
  local cargo_artifact_json=$1
  local jq_status digest_line
  set +e
  measured_binary_requested="$(
    "$auth_jq_executable" --exit-status --slurp --raw-output \
      --arg manifest "$canonical_manifest_path" '
        [ .[]
          | select(
              .reason == "compiler-artifact"
              and .manifest_path == $manifest
              and .target.name == "workload_harness"
              and .target.kind == ["test"]
              and .profile.test == true
              and (.executable | type == "string")
              and (.executable | startswith("/"))
            )
          | .executable
        ]
        | if length == 1 then .[0]
          else error("expected exactly one absolute workload_harness test executable")
          end
      ' "$cargo_artifact_json"
  )"
  jq_status=$?
  set -e
  [[ $jq_status -eq 0 ]] || return 20
  test_binary="$("$auth_readlink_executable" -e -- "$measured_binary_requested")" || return 20
  [[ -f $test_binary && -x $test_binary ]] || return 20
  digest_line="$("$auth_sha256sum_executable" -- "$test_binary")" || return 20
  measured_binary_sha256=${digest_line%% *}
  [[ $measured_binary_sha256 =~ ^[0-9a-f]{64}$ ]] || return 20
  readonly measured_binary_requested test_binary measured_binary_sha256 || return 20
}

revalidate_measured_binary() {
  local actual digest_line
  actual="$("$auth_readlink_executable" -e -- "$measured_binary_requested")" || return 20
  [[ $actual == "$test_binary" ]] || return 20
  digest_line="$("$auth_sha256sum_executable" -- "$test_binary")" || return 20
  [[ ${digest_line%% *} == "$measured_binary_sha256" ]] || return 20
}

verify_measured_entrypoint_exists() {
  local list_output=$1
  local status entrypoint
  set +e
  "$auth_env_executable" -i HOME=/home/mageyuki RUSTUP_HOME=/home/mageyuki/.rustup \
    CARGO_HOME=/home/mageyuki/.cargo PATH=/usr/bin:/bin LC_ALL=C TZ=UTC \
    "$test_binary" --list --ignored >"$list_output" 2>&1
  status=$?
  set -e
  [[ $status -eq 0 ]] || return 20
  for entrypoint in reference_profile_entrypoint verify_subject_diff_is_harness_only; do
    set +e
    "$auth_rg_executable" --no-config --line-regexp \
      "${entrypoint}: test" "$list_output" >/dev/null
    status=$?
    set -e
    [[ $status -eq 0 ]] || return 20
  done
}

validate_cpu_topology() {
  local line cpu package core pair extra
  local -A observed=() measured_pairs=()
  local output
  output="$("$auth_lscpu_executable" -p=CPU,SOCKET,CORE)" || return 20
  while IFS=, read -r cpu package core extra; do
    [[ $cpu == \#* ]] && continue
    [[ -z ${extra-} ]] || return 20
    [[ $cpu =~ ^[0-9]+$ && $package =~ ^[0-9]+$ && $core =~ ^[0-9]+$ ]] || return 20
    observed[$cpu]="$package:$core"
  done <<<"$output"
  for cpu in 0 1 2 3 4 5 6 7 12 13 14 15; do
    [[ -n ${observed[$cpu]-} ]] || return 20
  done
  for cpu in 0 1 2 3; do
    pair=${observed[$cpu]}
    [[ -z ${measured_pairs[$pair]-} ]] || return 20
    measured_pairs[$pair]=1
  done
  [[ ${#measured_pairs[@]} -eq 4 ]] || return 20
}

authoritative_preflight() {
  local kernel architecture lscpu_output git_status cargo_artifact_json
  local controller_artifact_list pidstat_zero pidstat_failure
  kernel="$("$auth_uname_executable" -s)" || return 20
  architecture="$("$auth_uname_executable" -m)" || return 20
  [[ $kernel == Linux && $architecture == x86_64 ]] || return 20
  lscpu_output="$("$auth_lscpu_executable")" || return 20
  [[ $lscpu_output == *'AMD Ryzen 7 5700X'* ]] || return 20
  validate_cpu_topology || return 20

  invocation_cwd="$("$auth_readlink_executable" -e -- "$PWD")" || return 20
  readonly invocation_cwd
  repository_root="$("$auth_git_executable" rev-parse --show-toplevel)" || return 20
  repository_root="$("$auth_readlink_executable" -e -- "$repository_root")" || return 20
  readonly repository_root || return 20
  canonical_manifest_path="$repository_root/Cargo.toml"
  [[ -f $canonical_manifest_path && ! -L $canonical_manifest_path ]] || return 20
  [[ "$("$auth_readlink_executable" -e -- "$canonical_manifest_path")" == "$canonical_manifest_path" ]] || return 20
  readonly canonical_manifest_path || return 20
  freeze_worktree_roots || return 20
  runner_subject="$("$auth_git_executable" rev-parse --verify "$runner_subject_argument^{commit}")" || return 20
  [[ $runner_subject =~ ^[0-9a-f]{40}$ ]] || return 20
  preflight_head="$("$auth_git_executable" rev-parse --verify 'HEAD^{commit}')" || return 20
  [[ $preflight_head =~ ^[0-9a-f]{40}$ ]] || return 20
  [[ "$("$auth_git_executable" rev-parse HEAD)" == "$preflight_head" ]] || return 20
  case "$runner_stage" in
    baseline)
      [[ $runner_subject == 9cd98131038a53b6dd36ff53e9b89825acba70ae ]] || return 20
      ;;
    post-reliability|final)
      [[ $runner_subject == "$preflight_head" ]] || return 20
      ;;
    *) return 20 ;;
  esac
  readonly runner_subject preflight_head || return 20
  validate_attempt_paths || return 20
  validate_cargo_configuration_absence || return 20
  "$auth_git_executable" diff --quiet --exit-code || return 20
  "$auth_git_executable" diff --cached --quiet --exit-code || return 20
  if [[ $runner_stage == baseline ]]; then
    "$auth_git_executable" diff --quiet "$runner_subject" -- Cargo.lock 'src/**' \
      ':(exclude)src/herdr/controller.rs' ':(exclude)src/herdr/collector.rs' \
      ':(exclude)src/reducer.rs' ':(exclude)src/operator.rs' \
      ':(exclude)src/store/mod.rs' ':(exclude)src/tui/app.rs' || return 20
  fi

  "$auth_mkdir_executable" -- "$runner_output_root" || return 20
  [[ "$("$auth_readlink_executable" -e -- "$runner_output_root")" == "$runner_output_root" ]] || return 20
  cargo_artifact_json="$runner_output_root/cargo-artifacts.json"
  "$auth_env_executable" -i HOME=/home/mageyuki RUSTUP_HOME=/home/mageyuki/.rustup \
    CARGO_HOME=/home/mageyuki/.cargo PATH=/usr/bin:/bin LC_ALL=C TZ=UTC \
    "$auth_rustup_executable" run 1.97.1 cargo test --release --locked \
      --features workload-harness --test workload_harness --no-run \
      --message-format=json >"$cargo_artifact_json" || return 20
  select_measured_binary "$cargo_artifact_json" || return 20
  controller_artifact_list="$runner_output_root/measured-entrypoints.txt"
  verify_measured_entrypoint_exists "$controller_artifact_list" || return 20
  if [[ $runner_stage == baseline ]]; then
    "$auth_env_executable" -i HOME=/home/mageyuki RUSTUP_HOME=/home/mageyuki/.rustup \
      CARGO_HOME=/home/mageyuki/.cargo PATH=/usr/bin:/bin LC_ALL=C TZ=UTC \
      HERDR_PERF_VERIFY_INVOCATION_CWD="$invocation_cwd" \
      HERDR_PERF_VERIFY_SUBJECT="$runner_subject" \
      "$test_binary" verify_subject_diff_is_harness_only --exact --ignored \
        --nocapture --test-threads=1 || return 20
  fi
  pidstat_zero="$runner_output_root/pidstat-calibration-zero.json"
  pidstat_failure="$runner_output_root/pidstat-calibration-failure.json"
  calibrate_pidstat_mode "$pidstat_zero" "$pidstat_failure" auth || return 20
  "$auth_git_executable" diff --quiet --exit-code || return 20
  "$auth_git_executable" diff --cached --quiet --exit-code || return 20
  [[ "$("$auth_git_executable" rev-parse HEAD)" == "$preflight_head" ]] || return 20
}

scenario_properties() {
  [[ $# -eq 1 ]] || return 20
  case "$1" in
    target) mapped_scenario=target; short_scenario=t; recorded_trials=5; trial_deadline_seconds=180 ;;
    sustained) mapped_scenario=sustained; short_scenario=s; recorded_trials=5; trial_deadline_seconds=180 ;;
    burst) mapped_scenario=burst; short_scenario=b; recorded_trials=5; trial_deadline_seconds=120 ;;
    startup) mapped_scenario=startup; short_scenario=u; recorded_trials=10; trial_deadline_seconds=300 ;;
    idle) mapped_scenario=idle; short_scenario=i; recorded_trials=5; trial_deadline_seconds=90 ;;
    fallback-rescan) mapped_scenario=fallback_rescan; short_scenario=f; recorded_trials=5; trial_deadline_seconds=120 ;;
    twice-target) mapped_scenario=twice_target; short_scenario=x; recorded_trials=5; trial_deadline_seconds=180 ;;
    *) return 20 ;;
  esac
}

validate_nvme_storage() {
  [[ $# -eq 1 ]] || return 20
  local target=$1
  local major_minor sysfs node base rotational
  local -a pending=() leaves=()
  major_minor="$("$auth_findmnt_executable" -T "$target" -rno MAJ:MIN)" || return 20
  [[ $major_minor =~ ^[0-9]+:[0-9]+$ ]] || return 20
  sysfs="$("$auth_readlink_executable" -e -- "/sys/dev/block/$major_minor")" || return 20
  pending+=("$sysfs")
  while [[ ${#pending[@]} -gt 0 ]]; do
    node=${pending[0]}
    pending=("${pending[@]:1}")
    local -a slaves=("$node"/slaves/*)
    if [[ -e ${slaves[0]} ]]; then
      local slave
      for slave in "${slaves[@]}"; do
        local resolved_slave
        resolved_slave="$("$auth_readlink_executable" -e -- "$slave")" || return 20
        pending+=("$resolved_slave")
      done
      continue
    fi
    base=${node##*/}
    if [[ -f $node/partition ]]; then
      node="$("$auth_readlink_executable" -e -- "$node/..")" || return 20
      base=${node##*/}
    fi
    [[ $base =~ ^nvme[0-9]+n[0-9]+$ ]] || return 20
    IFS= read -r rotational <"$node/queue/rotational" || return 20
    [[ $rotational == 0 ]] || return 20
    leaves+=("$base")
  done
  [[ ${#leaves[@]} -ge 1 ]] || return 20
  local left right temporary
  for ((left=0; left<${#leaves[@]}; left++)); do
    for ((right=left+1; right<${#leaves[@]}; right++)); do
      if [[ ${leaves[$right]} < ${leaves[$left]} ]]; then
        temporary=${leaves[$left]}
        leaves[$left]=${leaves[$right]}
        leaves[$right]=$temporary
      fi
    done
  done
  local previous=
  storage_devices=()
  for temporary in "${leaves[@]}"; do
    [[ $temporary != "$previous" ]] || continue
    storage_devices+=("$temporary")
    previous=$temporary
  done
  [[ ${#storage_devices[@]} -ge 1 ]] || return 20
}

prepare_trial_scratch_root() {
  [[ $# -eq 2 ]] || return 20
  local trial_root=$1
  local mkdir_executable=$2
  [[ $trial_root == /* && $mkdir_executable == /* ]] || return 20
  [[ ! -e $trial_root && ! -L $trial_root ]] || return 20
  "$mkdir_executable" -- "$trial_root" || return 20
  trial_scratch_root="$trial_root/scratch"
  [[ ! -e $trial_scratch_root && ! -L $trial_scratch_root ]] || return 20
  "$mkdir_executable" -- "$trial_scratch_root" || return 20
  [[ -d $trial_scratch_root && ! -L $trial_scratch_root ]] || return 20
}

bind_source_trial_tools() {
  auth_env_executable=$source_env_executable
  auth_id_executable=$source_id_executable
  auth_mkdir_executable=$source_mkdir_executable
  auth_mktemp_executable=$source_mktemp_executable
  auth_mv_executable=$source_mv_executable
  auth_pidstat_executable=$source_pidstat_executable
  auth_prlimit_executable=$source_prlimit_executable
  auth_readlink_executable=$source_readlink_executable
  auth_rmdir_executable=$source_rmdir_executable
  auth_setsid_executable=$source_setsid_executable
  auth_sleep_executable=$source_sleep_executable
  auth_stat_executable=$source_stat_executable
  auth_taskset_executable=$source_taskset_executable
  auth_time_executable=$source_time_executable
  auth_unlink_executable=$source_unlink_executable
  auth_bash_executable=$source_bash_executable
  readonly auth_env_executable auth_id_executable auth_mkdir_executable || return 20
  readonly auth_mktemp_executable auth_mv_executable auth_pidstat_executable || return 20
  readonly auth_prlimit_executable auth_readlink_executable auth_rmdir_executable || return 20
  readonly auth_setsid_executable auth_sleep_executable auth_stat_executable || return 20
  readonly auth_taskset_executable auth_time_executable auth_unlink_executable || return 20
  readonly auth_bash_executable || return 20
}

load_outer_runtime_state() {
  local measured observer socket_identity extra descriptor
  [[ -n ${active_runtime_state-} ]] || return 0
  [[ -f $active_runtime_state && ! -L $active_runtime_state ]] || return 0
  exec {descriptor}<"$active_runtime_state" || return 20
  IFS=' ' read -r measured observer socket_identity extra <&"$descriptor" || {
    exec {descriptor}<&-
    return 20
  }
  [[ -z $extra ]] || { exec {descriptor}<&-; return 20; }
  if IFS= read -r extra <&"$descriptor" || [[ -n $extra ]]; then
    exec {descriptor}<&-
    return 20
  fi
  exec {descriptor}<&-
  [[ $measured == - || $measured =~ ^[1-9][0-9]*$ ]] || return 20
  [[ $observer == - || $observer =~ ^[1-9][0-9]*$ ]] || return 20
  [[ $socket_identity == - || $socket_identity == *:*:*:*:* ]] || return 20
  [[ $measured == - ]] || active_measured_pid=$measured
  [[ $observer == - ]] || active_observer_pid=$observer
  [[ $socket_identity == - ]] || active_socket_identity=$socket_identity
}

safe_outer_runtime_state_cleanup() {
  local temporary cleanup_status=0
  if [[ -n ${active_runtime_state-} ]]; then
    for temporary in "$active_runtime_state".tmp.*; do
      [[ -e $temporary || -L $temporary ]] || continue
      if [[ -f $temporary && ! -L $temporary && ! -p $temporary ]]; then
        "$auth_unlink_executable" -- "$temporary" || cleanup_status=20
      else
        cleanup_status=20
      fi
    done
  fi
  if [[ -n ${active_runtime_state-} && ( -e $active_runtime_state || -L $active_runtime_state ) ]]; then
    [[ -f $active_runtime_state && ! -L $active_runtime_state ]] || cleanup_status=20
    if [[ $cleanup_status -eq 0 ]]; then
      "$auth_unlink_executable" -- "$active_runtime_state" || cleanup_status=20
    fi
  fi
  active_runtime_state=
  return "$cleanup_status"
}

safe_outer_runtime_cleanup() {
  local current cleanup_status=0
  if [[ -n ${active_runtime_socket-} && ( -e $active_runtime_socket || -L $active_runtime_socket ) ]]; then
    current="$("$auth_stat_executable" --format='%d:%i:%u:%f:%F' -- "$active_runtime_socket")" || cleanup_status=20
    if [[ -n ${active_socket_identity-} && $current == "$active_socket_identity" ]]; then
      "$auth_unlink_executable" -- "$active_runtime_socket" || cleanup_status=20
    else
      cleanup_status=20
    fi
  fi
  if [[ -n ${active_runtime_dir-} && ( -e $active_runtime_dir || -L $active_runtime_dir ) ]]; then
    current="$("$auth_stat_executable" --format='%d:%i:%u:%f:%F' -- "$active_runtime_dir")" || cleanup_status=20
    if [[ -n ${active_runtime_dir_identity-} && $current == "$active_runtime_dir_identity" ]]; then
      "$auth_rmdir_executable" -- "$active_runtime_dir" || cleanup_status=20
    else
      cleanup_status=20
    fi
  fi
  active_runtime_socket=
  active_socket_identity=
  active_runtime_dir=
  active_runtime_dir_identity=
  return "$cleanup_status"
}

outer_runtime_cleanup_trap() {
  local status=$?
  trap - EXIT INT TERM HUP
  load_outer_runtime_state || status=20
  if [[ -n ${herdr_i5_group_publication_capture-} ]]; then
    builtin printf '%s %s %s\n' "${active_measured_pid:--}" \
      "${active_observer_pid:--}" "${active_socket_identity:--}" \
      >"$herdr_i5_group_publication_capture" || status=20
  fi
  if [[ -n ${active_measured_pid-} || -n ${active_observer_pid-} ]]; then
    cleanup_process_groups "$auth_sleep_executable" \
      "${active_measured_pid-}" "${active_observer_pid-}" || status=20
  fi
  if [[ -n ${active_orchestration_pid-} ]]; then
    wait_orchestration_process "$active_orchestration_pid" \
      "$active_orchestration_supervisor_pid" "$auth_sleep_executable" || status=20
  elif [[ -n ${active_orchestration_supervisor_pid-} ]]; then
    builtin kill "$active_orchestration_supervisor_pid" 2>/dev/null || true
    wait "$active_orchestration_supervisor_pid" 2>/dev/null || true
  fi
  safe_outer_runtime_state_cleanup || status=20
  safe_outer_runtime_cleanup || status=20
  exit "$status"
}

install_outer_runtime_traps() {
  trap outer_runtime_cleanup_trap EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM HUP
}

clear_outer_runtime_traps() {
  trap - EXIT INT TERM HUP
}

read_trial_status() {
  [[ $# -eq 1 ]] || return 20
  local path=$1
  local token extra descriptor
  [[ -f $path && ! -L $path ]] || return 20
  exec {descriptor}<"$path" || return 20
  IFS= read -r token <&"$descriptor" || { exec {descriptor}<&-; return 20; }
  if IFS= read -r extra <&"$descriptor"; then
    exec {descriptor}<&-
    return 20
  fi
  if [[ -n $extra ]]; then
    exec {descriptor}<&-
    return 20
  fi
  exec {descriptor}<&-
  case "$token" in
    ok:0) last_trial_code=0 ;;
    failed:*)
      last_trial_code=${token#failed:}
      [[ $last_trial_code =~ ^[1-9][0-9]{0,2}$ && $last_trial_code -le 255 ]] || return 20
      ;;
    *) return 20 ;;
  esac
  last_trial_token=$token
}

run_trial_process_tree() {
  [[ $# -eq 21 ]] || return 20
  local time_output=$1
  local child_stdout=$2
  local child_stderr=$3
  local harness_output=$4
  local observer_handshake=$5
  local observer_control_socket=$6
  local observer_control_output=$7
  local process_tree_output=$8
  local observer_stdout=$9
  local observer_stderr=${10}
  local pidstat_output=${11}
  local pidstat_stderr=${12}
  local trial_status_output=${13}
  local trial_raw_root=${14}
  local trial_runtime_dir=${15}
  local scenario=${16}
  local stage=${17}
  local subject=${18}
  local baseline_results_root=${19}
  local deadline=${20}
  local handshake_attempt_limit=${21}
  local pidstat_status outer_deadline_seconds shared_orchestration_functions
  [[ $handshake_attempt_limit =~ ^[1-9][0-9]*$ ]] || return 20
  runtime_socket_path_has_shape "$observer_control_socket" || return 20
  outer_deadline_seconds=$((deadline + 10))

  shared_orchestration_functions="$(
    declare -f guard_fixture_output_node validate_fixture_output_path publish_trial_status \
      publish_outer_runtime_state cleanup_process_groups select_process_status wait_process_pair \
      install_orchestration_signal_traps
  )" || return 20

  set +e
  ( "$auth_sleep_executable" "$outer_deadline_seconds" ) &
  active_orchestration_supervisor_pid=$!
  "$auth_env_executable" -i HOME=/home/mageyuki RUSTUP_HOME=/home/mageyuki/.rustup \
    CARGO_HOME=/home/mageyuki/.cargo PATH=/usr/bin:/bin LC_ALL=C TZ=UTC \
    "$auth_taskset_executable" -c 4-7,12-15 \
    "$auth_pidstat_executable" -u -r -T ALL -o JSON 1 -e \
    "$auth_bash_executable" -p -c "$shared_orchestration_functions"$'\n''
      set -euo pipefail
      [[ $# -eq 31 ]] || exit 20
      time_output=$1
      test_binary=$2
      child_stdout=$3
      child_stderr=$4
      scenario=$5
      subject=$6
      harness_output=$7
      stage=$8
      baseline_results_root_arg=$9
      observer_handshake=${10}
      observer_control_socket=${11}
      observer_control_output=${12}
      process_tree_output=${13}
      observer_stdout=${14}
      observer_stderr=${15}
      trial_deadline_seconds=${16}
      trial_scratch_root=${17}
      trial_runtime_dir=${18}
      trial_status_output=${19}
      stat_executable=${20}
      unlink_executable=${21}
      rmdir_executable=${22}
      sleep_executable=${23}
      setsid_executable=${24}
      env_executable=${25}
      taskset_executable=${26}
      prlimit_executable=${27}
      time_executable=${28}
      id_executable=${29}
      mv_executable=${30}
      handshake_attempt_limit=${31}
      readonly time_output test_binary child_stdout child_stderr scenario subject
      readonly harness_output stage baseline_results_root_arg observer_handshake
      readonly observer_control_socket observer_control_output process_tree_output
      readonly observer_stdout observer_stderr trial_deadline_seconds trial_scratch_root
      readonly trial_runtime_dir trial_status_output stat_executable unlink_executable rmdir_executable
      readonly sleep_executable setsid_executable env_executable taskset_executable
      readonly prlimit_executable time_executable id_executable mv_executable
      readonly handshake_attempt_limit
      [[ $handshake_attempt_limit =~ ^[1-9][0-9]*$ ]] || exit 20
      safe_cleanup_runtime_socket() {
        local current
        if [[ ! -e $observer_control_socket && ! -L $observer_control_socket ]]; then return 0; fi
        [[ -n $frozen_socket_identity ]] || return 1
        current="$("$stat_executable" --format="%d:%i:%u:%f:%F" -- "$observer_control_socket")" || return 1
        [[ $current == "$frozen_socket_identity" ]] || return 1
        "$unlink_executable" -- "$observer_control_socket"
      }
      safe_cleanup_runtime_dir() {
        local current
        if [[ ! -e $trial_runtime_dir && ! -L $trial_runtime_dir ]]; then return 0; fi
        [[ -n $frozen_runtime_dir_identity ]] || return 1
        current="$("$stat_executable" --format="%d:%i:%u:%f:%F" -- "$trial_runtime_dir")" || return 1
        [[ $current == "$frozen_runtime_dir_identity" ]] || return 1
        "$rmdir_executable" -- "$trial_runtime_dir"
      }
      outer_runtime_state="$trial_runtime_dir/.outer-state"
      safe_cleanup_outer_runtime_state() {
        if [[ ! -e $outer_runtime_state && ! -L $outer_runtime_state ]]; then return 0; fi
        [[ -f $outer_runtime_state && ! -L $outer_runtime_state ]] || return 1
        "$unlink_executable" -- "$outer_runtime_state"
      }
      measured_wrapper_pid=
      observer_pid=
      watchdog_pid=
      frozen_runtime_dir_identity="$("$stat_executable" --format="%d:%i:%u:%f:%F" -- "$trial_runtime_dir")"
      frozen_socket_identity=
      cleanup_trial() {
        trial_status=$?
        trap - EXIT INT TERM HUP USR1
        if [[ -n ${watchdog_pid:-} ]]; then
          builtin kill "$watchdog_pid" 2>/dev/null || true
          wait "$watchdog_pid" 2>/dev/null || true
        fi
        cleanup_process_groups "$sleep_executable" \
          "${measured_wrapper_pid:-}" "${observer_pid:-}" || trial_status=20
        safe_cleanup_outer_runtime_state || trial_status=20
        safe_cleanup_runtime_socket || trial_status=20
        safe_cleanup_runtime_dir || trial_status=20
        publish_trial_status "$trial_status_output" "$trial_status" "$mv_executable" || trial_status=20
        exit "$trial_status"
      }
      trap cleanup_trial EXIT
      install_orchestration_signal_traps || exit 20
      # Launch the watchdog before either worker so wait_process_pair preserves
      # supervisor priority in both its pre-wait sweep and blocking wait ties.
      (
        trap - EXIT INT TERM HUP USR1
        "$sleep_executable" "$trial_deadline_seconds"
      ) &
      watchdog_pid=$!
      measured_environment=(
        HOME=/home/mageyuki RUSTUP_HOME=/home/mageyuki/.rustup
        CARGO_HOME=/home/mageyuki/.cargo PATH=/usr/bin:/bin LC_ALL=C TZ=UTC
        HERDR_PERF_SCENARIO="$scenario" HERDR_PERF_SUBJECT="$subject"
        HERDR_PERF_OUTPUT="$harness_output" HERDR_PERF_STAGE="$stage"
        HERDR_PERF_OBSERVER_HANDSHAKE="$observer_handshake"
        HERDR_PERF_OBSERVER_CONTROL_SOCKET="$observer_control_socket"
        HERDR_PERF_SCRATCH_ROOT="$trial_scratch_root"
      )
      if [[ $baseline_results_root_arg != - ]]; then
        measured_environment+=(HERDR_PERF_BASELINE_RESULTS_ROOT="$baseline_results_root_arg")
      fi
      "$setsid_executable" "$env_executable" -i "${measured_environment[@]}" \
        "$taskset_executable" -c 0-3 "$prlimit_executable" --as=17179869184 \
        "$time_executable" -v -o "$time_output" \
        "$test_binary" reference_profile_entrypoint --exact --ignored --test-threads=1 \
        >"$child_stdout" 2>"$child_stderr" &
      measured_wrapper_pid=$!
      publish_outer_runtime_state \
        "$outer_runtime_state" "$mv_executable" "$measured_wrapper_pid" - - || exit 20
      for ((attempt=0; attempt<handshake_attempt_limit; attempt++)); do
        [[ -s $observer_handshake ]] && break
        builtin kill -0 "$measured_wrapper_pid" 2>/dev/null || break
        "$sleep_executable" 0.01
      done
      [[ -s $observer_handshake ]] || exit 20
      [[ -S $observer_control_socket && ! -L $observer_control_socket ]] || exit 20
      [[ "$("$stat_executable" --format=%u -- "$observer_control_socket")" == "$("$id_executable" -u)" ]] || exit 20
      frozen_socket_identity="$("$stat_executable" --format="%d:%i:%u:%f:%F" -- "$observer_control_socket")" || exit 20
      publish_outer_runtime_state "$outer_runtime_state" "$mv_executable" \
        "$measured_wrapper_pid" - "$frozen_socket_identity" || exit 20
      IFS=" " read -r observed_root_pid observed_start_ticks trial_origin_ns <"$observer_handshake"
      observer_environment=(
        HOME=/home/mageyuki RUSTUP_HOME=/home/mageyuki/.rustup
        CARGO_HOME=/home/mageyuki/.cargo PATH=/usr/bin:/bin LC_ALL=C TZ=UTC
        HERDR_PERF_SCENARIO="$scenario"
        HERDR_PERF_OBSERVED_ROOT_PID="$observed_root_pid"
        HERDR_PERF_OBSERVED_ROOT_START_TICKS="$observed_start_ticks"
        HERDR_PERF_TRIAL_ORIGIN_NS="$trial_origin_ns"
        HERDR_PERF_OBSERVER_CONTROL_SOCKET="$observer_control_socket"
        HERDR_PERF_OBSERVER_CONTROL_OUTPUT="$observer_control_output"
        HERDR_PERF_PROCESS_TREE_OUTPUT="$process_tree_output"
      )
      "$setsid_executable" "$env_executable" -i "${observer_environment[@]}" \
        "$test_binary" reference_profile_process_tree_observer --exact --ignored \
          --test-threads=1 >"$observer_stdout" 2>"$observer_stderr" &
      observer_pid=$!
      publish_outer_runtime_state "$outer_runtime_state" "$mv_executable" \
        "$measured_wrapper_pid" "$observer_pid" "$frozen_socket_identity" || exit 20
      wait_process_pair \
        "$measured_wrapper_pid" "$observer_pid" "$watchdog_pid" 124 || exit 20
      if [[ $supervisor_completed_first == true ]]; then
        exit "$selected_process_status"
      fi
      measured_wrapper_pid=
      observer_pid=
      builtin kill "$watchdog_pid" 2>/dev/null || true
      wait "$watchdog_pid" 2>/dev/null || true
      watchdog_pid=
      exit "$selected_process_status"
    ' herdr-i5-orchestrator "$time_output" "$test_binary" "$child_stdout" "$child_stderr" \
      "$scenario" "$subject" "$harness_output" "$stage" "$baseline_results_root" \
      "$observer_handshake" "$observer_control_socket" "$observer_control_output" \
      "$process_tree_output" "$observer_stdout" "$observer_stderr" "$deadline" \
      "$trial_raw_root" "$trial_runtime_dir" "$trial_status_output" \
      "$auth_stat_executable" "$auth_unlink_executable" "$auth_rmdir_executable" \
      "$auth_sleep_executable" "$auth_setsid_executable" "$auth_env_executable" \
      "$auth_taskset_executable" "$auth_prlimit_executable" "$auth_time_executable" \
      "$auth_id_executable" "$auth_mv_executable" "$handshake_attempt_limit" \
      >"$pidstat_output" 2>"$pidstat_stderr" &
  active_orchestration_pid=$!
  if wait_orchestration_process "$active_orchestration_pid" \
    "$active_orchestration_supervisor_pid" "$auth_sleep_executable"; then
    pidstat_status=$waited_orchestration_status
  else
    pidstat_status=20
  fi
  active_orchestration_pid=
  active_orchestration_supervisor_pid=
  set -e
  last_pidstat_status=$pidstat_status
  read_trial_status "$trial_status_output" || return 20
  validate_pidstat_status_pair \
    "$pidstat_child_status_mode" "$last_trial_token" "$pidstat_status" || return 20
}

record_trial_control() {
  [[ $# -eq 5 ]] || return 20
  local trial_raw_root=$1
  local trial_index=$2
  local scenario=$3
  local baseline_root=$4
  local control_socket=$5
  local trial_status_output="$trial_raw_root/trial-status"
  runtime_socket_path_has_shape "$control_socket" || return 20
  local -a control_environment=(
    HOME=/home/mageyuki RUSTUP_HOME=/home/mageyuki/.rustup
    CARGO_HOME=/home/mageyuki/.cargo PATH=/usr/bin:/bin LC_ALL=C TZ=UTC
    HERDR_INCREMENT5_CONTROLLER_REQUESTED="$HERDR_INCREMENT5_CONTROLLER_REQUESTED"
    HERDR_INCREMENT5_CONTROLLER_CANONICAL="$HERDR_INCREMENT5_CONTROLLER_CANONICAL"
    HERDR_INCREMENT5_CONTROLLER_SHA256="$HERDR_INCREMENT5_CONTROLLER_SHA256"
    HERDR_INCREMENT5_RUNNER_REQUESTED="$HERDR_INCREMENT5_RUNNER_REQUESTED"
    HERDR_INCREMENT5_RUNNER_CANONICAL="$HERDR_INCREMENT5_RUNNER_CANONICAL"
    HERDR_INCREMENT5_RUNNER_SHA256="$HERDR_INCREMENT5_RUNNER_SHA256"
    HERDR_INCREMENT5_BOOTSTRAP_TOOLS_V1="$HERDR_INCREMENT5_BOOTSTRAP_TOOLS_V1"
    HERDR_PERF_CONTROL_RAW_ROOT="$trial_raw_root"
    HERDR_PERF_CONTROL_OUTPUT="$trial_raw_root/runner-control.json"
    HERDR_PERF_CONTROL_STAGE="$runner_stage"
    HERDR_PERF_CONTROL_SCENARIO="$scenario"
    HERDR_PERF_CONTROL_SUBJECT="$runner_subject"
    HERDR_PERF_CONTROL_PREFLIGHT_HEAD="$preflight_head"
    HERDR_PERF_CONTROL_TRIAL_INDEX="$trial_index"
    HERDR_PERF_CONTROL_INVOCATION_CWD="$invocation_cwd"
    HERDR_PERF_CONTROL_MEASURED_REQUESTED="$measured_binary_requested"
    HERDR_PERF_CONTROL_MEASURED_CANONICAL="$test_binary"
    HERDR_PERF_CONTROL_MEASURED_SHA256="$measured_binary_sha256"
    HERDR_PERF_CONTROL_TRIAL_STATUS_PATH="$trial_status_output"
    HERDR_PERF_CONTROL_PIDSTAT_EXIT_STATUS="$last_pidstat_status"
    HERDR_PERF_CONTROL_PIDSTAT_CHILD_STATUS_MODE="$pidstat_child_status_mode"
  )
  if [[ $runner_stage != baseline ]]; then
    control_environment+=(HERDR_PERF_CONTROL_BASELINE_RESULTS_ROOT="$baseline_root")
  fi
  "$auth_env_executable" -i "${control_environment[@]}" \
    "$test_binary" record_runner_control_evidence --exact --ignored \
      --nocapture --test-threads=1
}

create_runtime_dir_identity() {
  [[ $# -eq 1 ]] || return 20
  local herdr_i5_injected_parent_pid=$1
  local herdr_i5_injected_directory herdr_i5_injected_identity
  herdr_i5_injected_directory="$("$auth_mktemp_executable" -d /tmp/herdr-i5.XXXXXXXX)" || return 20
  herdr_i5_injected_identity="$("$auth_stat_executable" --format='%d:%i:%u:%f:%F' \
    -- "$herdr_i5_injected_directory")" || {
      "$auth_rmdir_executable" -- "$herdr_i5_injected_directory" || true
      return 20
    }
  if [[ -n ${herdr_i5_identity_window_capture-} ]]; then
    builtin printf '%s %s\n' "$herdr_i5_injected_directory" "$herdr_i5_injected_identity" \
      >"$herdr_i5_identity_window_capture" || return 20
  fi
  builtin printf '%s %s\n' "$herdr_i5_injected_directory" "$herdr_i5_injected_identity"
  if [[ -n ${herdr_i5_identity_window_capture-} ]]; then
    builtin kill -TERM "$herdr_i5_injected_parent_pid" || return 20
  fi
}

prepare_runtime_dir() {
  [[ $# -eq 2 ]] || return 20
  local scenario_code=$1
  local trial_code=$2
  local uid mode type extra parent_pid
  active_runtime_dir=
  active_runtime_dir_identity=
  install_outer_runtime_traps || return 20
  parent_pid=$BASHPID
  IFS=' ' read -r active_runtime_dir active_runtime_dir_identity extra \
    < <(create_runtime_dir_identity "$parent_pid") || return 20
  [[ -n $active_runtime_dir && -n $active_runtime_dir_identity && -z $extra ]] || return 20
  [[ -d $active_runtime_dir && ! -L $active_runtime_dir ]] || return 20
  uid="$("$auth_stat_executable" --format='%u' -- "$active_runtime_dir")" || return 20
  mode="$("$auth_stat_executable" --format='%a' -- "$active_runtime_dir")" || return 20
  type="$("$auth_stat_executable" --format='%F' -- "$active_runtime_dir")" || return 20
  [[ $uid == "$("$auth_id_executable" -u)" && $mode == 700 && $type == directory ]] || return 20
  active_runtime_dir="$("$auth_readlink_executable" -e -- "$active_runtime_dir")" || return 20
  [[ "$("$auth_stat_executable" --format='%d:%i:%u:%f:%F' -- "$active_runtime_dir")" == "$active_runtime_dir_identity" ]] || return 20
  active_runtime_socket="$active_runtime_dir/${scenario_code}-${trial_code}.sock"
  active_runtime_state="$active_runtime_dir/.outer-state"
  runtime_socket_path_has_shape "$active_runtime_socket" || return 20
  [[ ! -e $active_runtime_socket && ! -L $active_runtime_socket ]] || return 20
  [[ ! -e $active_runtime_state && ! -L $active_runtime_state ]] || return 20
  active_socket_identity=
  active_measured_pid=
  active_observer_pid=
  active_orchestration_pid=
  active_orchestration_supervisor_pid=
}

run_one_trial() {
  [[ $# -eq 5 ]] || return 20
  local scenario=$1
  local trial_root=$2
  local trial_index=$3
  local trial_code=$4
  local recorded=$5
  local baseline_arg=- control_status=0 artifact trial_control_socket
  revalidate_measured_binary || return 20
  revalidate_authoritative_bootstrap || return 20
  prepare_trial_scratch_root "$trial_root" "$auth_mkdir_executable" || return 20
  [[ "$("$auth_readlink_executable" -e -- "$trial_root")" == "$trial_root" ]] || return 20
  [[ "$("$auth_readlink_executable" -e -- "$trial_scratch_root")" == "$trial_scratch_root" ]] || return 20
  validate_nvme_storage "$trial_scratch_root" || return 20
  recalibrate_pidstat_mode \
    "$trial_scratch_root/pidstat-calibration-zero.json" \
    "$trial_scratch_root/pidstat-calibration-failure.json" auth || return 20
  prepare_runtime_dir "$short_scenario" "$trial_code" || return 20
  trial_control_socket=$active_runtime_socket
  if [[ $runner_stage != baseline ]]; then baseline_arg=$runner_baseline_root; fi
  run_trial_process_tree \
    "$trial_root/gnu-time.txt" "$trial_root/stdout" "$trial_root/stderr" \
    "$trial_root/harness.json" "$trial_root/observer-handshake" "$active_runtime_socket" \
    "$trial_root/observer-control.json" "$trial_root/process-tree.json" \
    "$trial_root/observer-stdout" "$trial_root/observer-stderr" \
    "$trial_root/pidstat.json" "$trial_root/pidstat-stderr" "$trial_root/trial-status" \
    "$trial_scratch_root" "$active_runtime_dir" "$scenario" "$runner_stage" "$runner_subject" \
    "$baseline_arg" "$trial_deadline_seconds" 500 || return 20
  if ! safe_outer_runtime_cleanup; then
    clear_outer_runtime_traps
    return 20
  fi
  clear_outer_runtime_traps
  read_trial_status "$trial_root/trial-status" || return 20
  if [[ $recorded == true ]]; then
    set +e
    record_trial_control "$trial_root" "$trial_index" "$mapped_scenario" \
      "$runner_baseline_root" "$trial_control_socket"
    control_status=$?
    set -e
    if [[ $last_trial_code -eq 0 ]]; then
      [[ $control_status -eq 0 ]] || return 20
      for artifact in harness.json runner-control.json process-tree.json observer-control.json \
        gnu-time.txt stdout stderr observer-stdout observer-stderr \
        pidstat.json pidstat-stderr observer-handshake trial-status; do
        [[ -f $trial_root/$artifact && ! -L $trial_root/$artifact ]] || return 20
        "$auth_sha256sum_executable" -- "$trial_root/$artifact" >/dev/null || return 20
      done
    fi
  fi
}

compose_and_validate_scenario() {
  [[ $# -eq 2 ]] || return 20
  local scenario_root=$1
  local trial_status_token=$2
  local baseline_arg=$runner_baseline_root
  local composer_status composer_status_token validator_status
  local -a composer_environment=(
    HOME=/home/mageyuki RUSTUP_HOME=/home/mageyuki/.rustup
    CARGO_HOME=/home/mageyuki/.cargo PATH=/usr/bin:/bin LC_ALL=C TZ=UTC
    HERDR_PERF_COMPOSE_RAW_ROOT="$scenario_root"
    HERDR_PERF_COMPOSE_OUTPUT="$scenario_root/candidate-v1.json"
    HERDR_PERF_COMPOSE_STAGE="$runner_stage"
    HERDR_PERF_COMPOSE_SCENARIO="$mapped_scenario"
    HERDR_PERF_COMPOSE_SUBJECT="$runner_subject"
    HERDR_PERF_COMPOSE_PREFLIGHT_HEAD="$preflight_head"
  )
  if [[ $runner_stage != baseline ]]; then
    composer_environment+=(HERDR_PERF_COMPOSE_BASELINE_RESULTS_ROOT="$baseline_arg")
  fi
  set +e
  "$auth_env_executable" -i "${composer_environment[@]}" \
    "$test_binary" compose_reference_outcome_from_raw --exact --ignored \
      --nocapture --test-threads=1
  composer_status=$?
  set -e
  case "$composer_status" in
    0|10|20) composer_status_token=$composer_status ;;
    *) composer_status_token="unexpected:$composer_status" ;;
  esac
  local -a validator_environment=(
    HOME=/home/mageyuki RUSTUP_HOME=/home/mageyuki/.rustup
    CARGO_HOME=/home/mageyuki/.cargo PATH=/usr/bin:/bin LC_ALL=C TZ=UTC
    HERDR_PERF_VALIDATE_RAW_ROOT="$scenario_root"
    HERDR_PERF_VALIDATE_CANDIDATE="$scenario_root/candidate-v1.json"
    HERDR_PERF_VALIDATE_OUTPUT="$scenario_root/result-v1.json"
    HERDR_PERF_VALIDATE_STAGE="$runner_stage"
    HERDR_PERF_VALIDATE_SCENARIO="$mapped_scenario"
    HERDR_PERF_VALIDATE_SUBJECT="$runner_subject"
    HERDR_PERF_VALIDATE_PREFLIGHT_HEAD="$preflight_head"
    HERDR_PERF_VALIDATE_COMPOSER_STATUS="$composer_status_token"
    HERDR_PERF_VALIDATE_TRIAL_STATUS="$trial_status_token"
  )
  if [[ $runner_stage != baseline ]]; then
    validator_environment+=(HERDR_PERF_VALIDATE_BASELINE_RESULTS_ROOT="$baseline_arg")
  fi
  "$auth_git_executable" diff --quiet --exit-code || return 20
  "$auth_git_executable" diff --cached --quiet --exit-code || return 20
  [[ "$("$auth_git_executable" rev-parse HEAD)" == "$preflight_head" ]] || return 20
  set +e
  "$auth_env_executable" -i "${validator_environment[@]}" \
    "$test_binary" validate_reference_outcome --exact --ignored \
      --nocapture --test-threads=1
  validator_status=$?
  set -e
  case "$validator_status" in 0|10|20) ;; *) return 20 ;; esac
  case "$trial_status_token" in
    all-ok) ;;
    failed:trial-*:*) [[ $validator_status -eq 20 ]] || return 20 ;;
    *) return 20 ;;
  esac
  case "$composer_status_token" in
    unexpected:*) [[ $validator_status -eq 20 ]] || return 20 ;;
    *) [[ $validator_status -eq $composer_status ]] || return 20 ;;
  esac
  return "$validator_status"
}

run_single_reference_scenario() {
  [[ $# -eq 1 ]] || return 20
  local scenario=$1
  local scenario_root warmup_root trial_root trial_index trial_code
  local trial_status_token=all-ok
  scenario_properties "$scenario" || return 20
  scenario_root="$runner_output_root/$mapped_scenario"
  "$auth_mkdir_executable" -- "$scenario_root" || return 20
  warmup_root="$scenario_root/warm-up-0001"
  run_one_trial "$scenario" "$warmup_root" 0 w0001 false || return 20
  [[ $last_trial_code -eq 0 ]] || return 20
  for ((trial_index=1; trial_index<=recorded_trials; trial_index++)); do
    builtin printf -v trial_code 't%04d' "$trial_index"
    builtin printf -v trial_root '%s/trial-%04d' "$scenario_root" "$trial_index"
    run_one_trial "$scenario" "$trial_root" "$trial_index" "$trial_code" true || return 20
    if [[ $last_trial_code -ne 0 ]]; then
      trial_status_token="failed:trial-${trial_index}:${last_trial_code}"
      break
    fi
  done
  compose_and_validate_scenario "$scenario_root" "$trial_status_token"
}

validate_baseline_layout_up_front() {
  [[ $runner_stage == baseline ]] && return 0
  local scenario mapped
  for scenario in target sustained burst startup idle fallback-rescan twice-target; do
    scenario_properties "$scenario" || return 20
    mapped=$mapped_scenario
    [[ -f $runner_baseline_root/$mapped/result-v1.json ]] || return 20
    [[ ! -L $runner_baseline_root/$mapped/result-v1.json ]] || return 20
    [[ -d $runner_baseline_root/$mapped/trial-0001 ]] || return 20
  done
  "$auth_env_executable" -i \
    HERDR_PERF_VALIDATE_BASELINE_RESULTS_ROOT="$runner_baseline_root" \
    "$test_binary" validate_reference_baseline_set --exact --ignored --test-threads=1 \
    || return 20
}

run_reference_scenarios() {
  validate_baseline_layout_up_front || return 20
  local scenario status
  local -a scenarios statuses=()
  if [[ $runner_scenario == all ]]; then
    scenarios=(target sustained burst startup idle fallback-rescan twice-target)
  else
    scenarios=("$runner_scenario")
  fi
  for scenario in "${scenarios[@]}"; do
    if run_single_reference_scenario "$scenario"; then
      status=0
    else
      status=$?
    fi
    statuses+=("$status")
    aggregate_closed_statuses "${statuses[@]}" || return 20
    [[ $status -ne 20 ]] || return 20
  done
  return "$aggregate_status"
}

main() {
  bootstrap_authoritative_manifest || return 20
  parse_authoritative_arguments "$@" || return 20
  contain_attempt_id || return 20
  revalidate_authoritative_bootstrap || return 20
  authoritative_preflight || return 20
  run_reference_scenarios
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
  if [[ ${HERDR_PERF_RUNNER_TEST_LIBRARY_ONLY-} == 1 ]]; then
    builtin printf '%s\n' 'error: library-only mode cannot execute main' >&2
    exit 20
  fi
  main "$@"
fi
