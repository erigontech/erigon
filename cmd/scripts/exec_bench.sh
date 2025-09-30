#!/bin/bash

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Function to gracefully kill a process
kill_process() {
    local pid="$1"
    local process_name="${2:-process}"
    
    if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
        return 0  # Process doesn't exist, nothing to do
    fi
    
    log_info "Stopping $process_name (PID: $pid)"
    
    # First try graceful termination
    kill -TERM "$pid" 2>/dev/null || true
    
    # Wait up to 5 seconds for graceful shutdown
    local wait_time=0
    while [[ $wait_time -lt 5 ]] && kill -0 "$pid" 2>/dev/null; do
        sleep 1
        ((wait_time++))
    done
    
    # Force kill if still running
    if kill -0 "$pid" 2>/dev/null; then
        log_warn "$process_name didn't terminate gracefully, force killing..."
        kill -KILL "$pid" 2>/dev/null || true
    fi
    
    # Wait for process to fully terminate
    wait "$pid" 2>/dev/null || true
}

# Function to cleanup background processes on exit
cleanup() {
    local exit_code=$?
    log_info "Cleaning up..."
    
    # Kill any remaining erigon processes we started
    if [[ -n "${ERIGON_PID1:-}" ]]; then
        kill_process "$ERIGON_PID1" "erigon process 1"
    fi
    
    if [[ -n "${ERIGON_PID2:-}" ]]; then
        kill_process "$ERIGON_PID2" "erigon process 2"
    fi
    
    exit $exit_code
}

# Set trap for cleanup
trap cleanup EXIT INT TERM

# Function to validate command line arguments
validate_args() {
    if [[ $# -ne 2 ]]; then
        log_error "Usage: $0 <config.yml> <source_datadir>"
        exit 1
    fi
    
    CONFIG_FILE="$1"
    SOURCE_DATADIR="$2"
    
    if [[ ! -f "$CONFIG_FILE" ]]; then
        log_error "Configuration file not found: $CONFIG_FILE"
        exit 1
    fi
    
    if [[ ! -d "$SOURCE_DATADIR" ]]; then
        log_error "Source datadir not found: $SOURCE_DATADIR"
        exit 1
    fi
    
    # Check if yq is installed
    if ! command -v yq &> /dev/null; then
        log_error "yq is required but not installed. Install with: brew install yq (macOS) or snap install yq (Linux)"
        exit 1
    fi
}

# Function to parse YAML configuration
parse_config() {
    log_info "Parsing configuration from $CONFIG_FILE"
    
    DATADIR1=$(yq eval '.datadir1' "$CONFIG_FILE")
    DATADIR2=$(yq eval '.datadir2' "$CONFIG_FILE")
    ERIGON_CMD1=$(yq eval '.erigon_cmd1' "$CONFIG_FILE")
    ERIGON_CMD2=$(yq eval '.erigon_cmd2' "$CONFIG_FILE")
    NO_PRUNE=$(yq eval '.no_prune // false' "$CONFIG_FILE")
    NO_MERGE=$(yq eval '.no_merge // false' "$CONFIG_FILE")
    DISCARD_COMMITMENT=$(yq eval '.discard_commitment // false' "$CONFIG_FILE")
    
    # Validate required fields
    if [[ "$DATADIR1" == "null" ]] || [[ -z "$DATADIR1" ]]; then
        log_error "datadir1 is not specified in config"
        exit 1
    fi
    
    if [[ "$DATADIR2" == "null" ]] || [[ -z "$DATADIR2" ]]; then
        log_error "datadir2 is not specified in config"
        exit 1
    fi
    
    if [[ "$ERIGON_CMD1" == "null" ]] || [[ -z "$ERIGON_CMD1" ]]; then
        log_error "erigon_cmd1 is not specified in config"
        exit 1
    fi
    
    if [[ "$ERIGON_CMD2" == "null" ]] || [[ -z "$ERIGON_CMD2" ]]; then
        log_error "erigon_cmd2 is not specified in config"
        exit 1
    fi
    
    log_info "Configuration loaded:"
    log_info "  datadir1: $DATADIR1"
    log_info "  datadir2: $DATADIR2"
    log_info "  erigon_cmd1: $ERIGON_CMD1"
    log_info "  erigon_cmd2: $ERIGON_CMD2"
    log_info "  no_prune: $NO_PRUNE"
    log_info "  no_merge: $NO_MERGE"
    log_info "  discard_commitment: $DISCARD_COMMITMENT"
}

# Function to validate that commands use correct datadirs
validate_commands() {
    log_info "Validating command datadir usage..."
    
    # Check if erigon_cmd1 contains --datadir with datadir1
    if ! echo "$ERIGON_CMD1" | grep -q -- "--datadir"; then
        log_error "erigon_cmd1 does not contain --datadir flag"
        exit 1
    fi
    
    if ! echo "$ERIGON_CMD1" | grep -q -- "--datadir[[:space:]]*[\"']?${DATADIR1}[\"']?"; then
        if ! echo "$ERIGON_CMD1" | grep -q -- "--datadir=${DATADIR1}"; then
            log_error "erigon_cmd1 does not use datadir1 ($DATADIR1) in --datadir flag"
            log_error "Command: $ERIGON_CMD1"
            exit 1
        fi
    fi
    
    # Check if erigon_cmd2 contains --datadir with datadir2
    if ! echo "$ERIGON_CMD2" | grep -q -- "--datadir"; then
        log_error "erigon_cmd2 does not contain --datadir flag"
        exit 1
    fi
    
    if ! echo "$ERIGON_CMD2" | grep -q -- "--datadir[[:space:]]*[\"']?${DATADIR2}[\"']?"; then
        if ! echo "$ERIGON_CMD2" | grep -q -- "--datadir=${DATADIR2}"; then
            log_error "erigon_cmd2 does not use datadir2 ($DATADIR2) in --datadir flag"
            log_error "Command: $ERIGON_CMD2"
            exit 1
        fi
    fi
    
    log_info "Command validation passed"
}

# Function to check if mirror script exists
check_mirror_script() {
    MIRROR_SCRIPT="./cmd/scripts/mirror-datadir.sh"
    
    if [[ ! -f "$MIRROR_SCRIPT" ]]; then
        log_error "Mirror script not found: $MIRROR_SCRIPT"
        exit 1
    fi
    
    if [[ ! -x "$MIRROR_SCRIPT" ]]; then
        log_warn "Mirror script is not executable, attempting to make it executable"
        chmod +x "$MIRROR_SCRIPT"
    fi
}

# Function to check if datadir is empty or needs initial sync
check_datadir_needs_sync() {
    local datadir="$1"
    
    # Check if directory exists but is empty or has no chaindata
    if [[ ! -d "$datadir/chaindata" ]] || [[ -z "$(ls -A "$datadir/chaindata" 2>/dev/null)" ]]; then
        return 0  # Needs sync
    fi
    
    # Check if there's actual blockchain data (look for common Erigon DB files)
    if [[ ! -f "$datadir/chaindata/mdbx.dat" ]] && [[ ! -f "$datadir/chaindata/data.mdb" ]]; then
        return 0  # Needs sync
    fi
    
    return 1  # Has data, no sync needed
}

# Function to perform initial sync
perform_initial_sync() {
    log_info "Source datadir appears to be empty or missing chaindata"
    log_info "Performing initial sync to populate source datadir: $SOURCE_DATADIR"
    
    # Create the sync command by replacing datadir1 with SOURCE_DATADIR in erigon_cmd1
    local sync_cmd="${ERIGON_CMD1//$DATADIR1/$SOURCE_DATADIR}"
    
    # Apply environment variables if configured
    # For initial sync, always add ERIGON_STOP_BEFORE_STAGE=Execution
    local env_prefix="ERIGON_STOP_BEFORE_STAGE=Execution "
    
    if [[ "$NO_PRUNE" == "true" ]]; then
        env_prefix="${env_prefix}ERIGON_NO_PRUNE=true "
    fi
    if [[ "$NO_MERGE" == "true" ]]; then
        env_prefix="${env_prefix}ERIGON_NO_MERGE=true "
    fi
    if [[ "$DISCARD_COMMITMENT" == "true" ]]; then
        env_prefix="${env_prefix}ERIGON_DISCARD_COMMITMENT=true "
    fi
    
    sync_cmd="${env_prefix}${sync_cmd}"
    
    log_info "Sync command: $sync_cmd"
    log_info "Waiting for sync to reach Execution stage..."
    log_info "Will automatically stop when 'STOP_BEFORE_STAGE env flag forced to stop app' appears in logs"
    
    # Create log file for initial sync
    local sync_log="initial_sync_$(date +%Y%m%d_%H%M%S).log"
    
    # Start sync process
    eval "$sync_cmd" > "$sync_log" 2>&1 &
    local sync_pid=$!
    
    log_info "Sync process started with PID: $sync_pid"
    log_info "Output being captured to: $sync_log"
    
    # Monitor sync progress and watch for stop message
    local elapsed=0
    local stop_pattern="STOP_BEFORE_STAGE env flag forced to stop app"
    
    while true; do
        # Check if process is still running
        if ! kill -0 $sync_pid 2>/dev/null; then
            log_info "Sync process completed naturally"
            break
        fi
        
        # Check for the stop pattern in the log
        if [[ -f "$sync_log" ]] && grep -q "$stop_pattern" "$sync_log"; then
            log_info "Detected STOP_BEFORE_STAGE completion message"
            kill_process $sync_pid "sync process"
            break
        fi
        
        # Show progress every 30 seconds
        if [[ $((elapsed % 30)) -eq 0 ]] && [[ $elapsed -gt 0 ]]; then
            log_info "  ... syncing (${elapsed}s elapsed)"
            
            # Show last line of log to give sense of progress
            if [[ -f "$sync_log" ]]; then
                local last_line=$(tail -n 1 "$sync_log" 2>/dev/null | head -c 100)
                if [[ -n "$last_line" ]]; then
                    log_info "  Last log: ${last_line}..."
                fi
            fi
        fi
        
        # Safety timeout after 1 hour
        if [[ $elapsed -gt 3600 ]]; then
            log_warn "Sync running for over 1 hour, stopping for safety"
            kill_process $sync_pid "sync process"
            break
        fi
        
        sleep 1
        ((elapsed++))
    done
    
    wait $sync_pid 2>/dev/null || true
    
    log_info "Initial sync completed after ${elapsed} seconds"
    log_info "Sync log saved to: $sync_log"
    
    # Show if we found the stop message
    if grep -q "$stop_pattern" "$sync_log"; then
        log_info "Sync stopped at Execution stage as expected"
    fi
    
    # Verify we now have data
    if check_datadir_needs_sync "$SOURCE_DATADIR"; then
        log_error "Initial sync appears to have failed - datadir is still empty"
        log_error "Check $sync_log for errors"
        exit 1
    fi
    
    log_info "Source datadir now contains data, proceeding with benchmarks"
}

# Function to mirror datadir
mirror_datadir() {
    local source="$1"
    local destination="$2"
    
    log_info "Mirroring datadir from $source to $destination"
    
    # Check if destination already exists
    if [[ -d "$destination" ]]; then
        log_warn "Destination $destination already exists. Removing it first..."
        rm -rf "$destination"
    fi
    
    # Execute mirror script
    if ! "$MIRROR_SCRIPT" "$source" "$destination"; then
        log_error "Failed to mirror datadir from $source to $destination"
        exit 1
    fi
    
    log_info "Successfully mirrored datadir to $destination"
}

# Function to execute and benchmark erigon command
execute_benchmark() {
    local cmd="$1"
    local datadir="$2"
    local run_number="$3"
    local timeout_seconds=30
    
    # Prepend environment variables based on configuration
    local env_prefix=""
    if [[ "$NO_PRUNE" == "true" ]]; then
        env_prefix="${env_prefix}ERIGON_NO_PRUNE=true "
        log_info "ERIGON_NO_PRUNE is enabled"
    fi
    
    if [[ "$NO_MERGE" == "true" ]]; then
        env_prefix="${env_prefix}ERIGON_NO_MERGE=true "
        log_info "ERIGON_NO_MERGE is enabled"
    fi
    
    if [[ "$DISCARD_COMMITMENT" == "true" ]]; then
        env_prefix="${env_prefix}ERIGON_DISCARD_COMMITMENT=true "
        log_info "ERIGON_DISCARD_COMMITMENT is enabled"
    fi
    
    # Combine environment variables with command
    if [[ -n "$env_prefix" ]]; then
        cmd="${env_prefix}${cmd}"
    fi
    
    log_info "Starting benchmark run $run_number for $datadir"
    log_info "Command: $cmd"
    log_info "Will run for ${timeout_seconds} seconds"
    
    # Create a log file for this run
    local log_file="benchmark_run${run_number}_$(date +%Y%m%d_%H%M%S).log"
    
    # Start the command in background and capture its PID
    eval "$cmd" > "$log_file" 2>&1 &
    local pid=$!
    
    if [[ $run_number -eq 1 ]]; then
        ERIGON_PID1=$pid
    else
        ERIGON_PID2=$pid
    fi
    
    log_info "Process started with PID: $pid"
    log_info "Output being captured to: $log_file"
    
    # Monitor the process
    local elapsed=0
    while [[ $elapsed -lt $timeout_seconds ]]; do
        if ! kill -0 $pid 2>/dev/null; then
            log_warn "Process $pid exited before timeout (after ${elapsed}s)"
            break
        fi
        
        # Show progress every 5 seconds
        if [[ $((elapsed % 5)) -eq 0 ]] && [[ $elapsed -gt 0 ]]; then
            log_info "  ... running (${elapsed}/${timeout_seconds}s)"
        fi
        
        sleep 1
        ((elapsed++))
    done
    
    # Kill the process if it's still running
    if kill -0 $pid 2>/dev/null; then
        kill_process $pid "benchmark process"
    fi
    
    # Clear the PID variable
    if [[ $run_number -eq 1 ]]; then
        ERIGON_PID1=""
    else
        ERIGON_PID2=""
    fi
    
    log_info "Benchmark run $run_number completed"
    log_info "Log saved to: $log_file"
    
    # Show last few lines of log
    if [[ -f "$log_file" ]]; then
        log_info "Last 5 lines of output:"
        tail -n 5 "$log_file" | while IFS= read -r line; do
            echo "    $line"
        done
    fi
}

# Main execution
main() {
    log_info "Starting execution benchmarking script"
    
    # Validate arguments
    validate_args "$@"
    
    # Parse configuration
    parse_config
    
    # Validate commands use correct datadirs
    validate_commands
    
    # Check mirror script exists
    check_mirror_script
    
    # Check if source datadir needs initial sync
    if check_datadir_needs_sync "$SOURCE_DATADIR"; then
        perform_initial_sync
    else
        log_info "Source datadir contains data, skipping initial sync"
    fi
    
    # Start benchmarking
    log_info "="
    log_info "Beginning benchmark execution"
    log_info "="
    
    # Run 1: datadir1 with erigon_cmd1
    log_info "--- Run 1: Setting up datadir1 ---"
    mirror_datadir "$SOURCE_DATADIR" "$DATADIR1"
    execute_benchmark "$ERIGON_CMD1" "$DATADIR1" 1
    
    log_info ""
    log_info "--- Run 2: Setting up datadir2 ---"
    mirror_datadir "$SOURCE_DATADIR" "$DATADIR2"
    execute_benchmark "$ERIGON_CMD2" "$DATADIR2" 2
    
    # Summary
    log_info "="
    log_info "Benchmark execution completed successfully"
    log_info "="
    log_info "Results:"
    log_info "  Run 1: $DATADIR1 - Check benchmark_run1_*.log"
    log_info "  Run 2: $DATADIR2 - Check benchmark_run2_*.log"
    
    if [[ "$NO_PRUNE" == "true" ]]; then
        log_info "  ERIGON_NO_PRUNE environment variable was set for both runs"
    fi
    
    if [[ "$NO_MERGE" == "true" ]]; then
        log_info "  ERIGON_NO_MERGE environment variable was set for both runs"
    fi
    
    if [[ "$DISCARD_COMMITMENT" == "true" ]]; then
        log_info "  ERIGON_DISCARD_COMMITMENT environment variable was set for both runs"
    fi
}

# Run main function
main "$@"