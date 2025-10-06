#!/bin/bash

# benchmarking script to compare execution performance of two erigon runs
# Looks at README.md to see instructions for running


# some details:
# sample.yml: specifies erigon/stage_exec start commands and source dir
# after it does mirror datadir, it'll reset_state (to remove state data from db)
# then remove last state snapshot
# figure out what's the start execution block, and execute 3000 blocks from there (or upto the tip)

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

LOG_LOCATION="./bench-logs"
mkdir -p "$LOG_LOCATION"

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

# Function to check required tools
check_required_tools() {
    log_info "Checking required tools..."
    
    local missing_tools=()
    
    # Check if yq is installed
    if ! command -v yq &> /dev/null; then
        missing_tools+=("yq")
    fi
    
    if [[ ${#missing_tools[@]} -gt 0 ]]; then
        log_error "The following required tools are not installed: ${missing_tools[*]}"
        log_error "Installation instructions:"
        for tool in "${missing_tools[@]}"; do
            case "$tool" in
                yq)
                    log_error "  yq: brew install yq (macOS) or sudo apt install yq (Linux)"
                    ;;
            esac
        done
        exit 1
    fi
    
    log_info "All required tools are installed"
}

# Function to validate command line arguments
validate_args() {
    SKIP_MIRROR=false
    
    # Parse command line options
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-mirror)
                SKIP_MIRROR=true
                shift
                ;;
            *)
                CONFIG_FILE="$1"
                shift
                ;;
        esac
    done
    
    if [[ -z "${CONFIG_FILE:-}" ]]; then
        log_error "Usage: $0 [--skip-mirror] <config.yml>"
        log_error "  --skip-mirror: Skip mirroring datadirs (use existing ones)"
        exit 1
    fi
    
    if [[ ! -f "$CONFIG_FILE" ]]; then
        log_error "Configuration file not found: $CONFIG_FILE"
        exit 1
    fi
}

# Function to extract datadir from erigon command
extract_datadir() {
    local cmd="$1"
    local cmd_name="$2"  # For error messages
    local datadir=""
    
    # Try to extract --datadir value using different patterns
    # Pattern 1: --datadir=/path/to/dir (with equals sign)
    if [[ "$cmd" =~ --datadir=([^[:space:]]+) ]]; then
        datadir="${BASH_REMATCH[1]}"
    # Pattern 2: --datadir /path/to/dir (with space)
    elif [[ "$cmd" =~ --datadir[[:space:]]+([^[:space:]]+) ]]; then
        datadir="${BASH_REMATCH[1]}"
    # Pattern 3: --datadir "/path/to/dir" (quoted with space)
    elif [[ "$cmd" =~ --datadir[[:space:]]+\"([^\"]+)\" ]]; then
        datadir="${BASH_REMATCH[1]}"
    # Pattern 4: --datadir='/path/to/dir' (single quoted with equals)
    elif [[ "$cmd" =~ --datadir=\'([^\']+)\' ]]; then
        datadir="${BASH_REMATCH[1]}"
    fi
    
    # Remove any quotes that might still be present
    datadir="${datadir%\"}"
    datadir="${datadir#\"}"
    datadir="${datadir%\'}"
    datadir="${datadir#\'}"
    
    # Validate that we could extract datadir
    if [[ -z "$datadir" ]]; then
        log_error "Could not extract datadir from $cmd_name"
        log_error "Command: $cmd"
        log_error "Make sure the command contains --datadir flag"
        exit 1
    fi
    
    echo "$datadir"
}

clear_caches() {
    sync  # Flush file system buffers (works on both)
    
    if [[ "$(uname)" == "Darwin" ]]; then
        # macOS
        echo "Detected macOS - using purge"
        sudo purge
    elif [[ "$(uname)" == "Linux" ]]; then
        # Linux
        echo "Detected Linux - dropping caches"
        sudo sysctl vm.drop_caches=3
        # Or alternatively:
        # echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    else
        echo "Unsupported operating system: $(uname)"
        return 1
    fi
    
    echo "Cache clearing complete"
}

strip_quotes() {
    local str="$1"
    str="${str%\"}"
    str="${str#\"}"
    str="${str%\'}"
    str="${str#\'}"
    echo "$str"
}

# Function to parse YAML configuration
parse_config() {
    log_info "Parsing configuration from $CONFIG_FILE"
    
    SOURCE_DATADIR=$(yq '.source_datadir' "$CONFIG_FILE")
    ERIGON_CMD1=$(yq '.erigon_cmd1' "$CONFIG_FILE")
    ERIGON_CMD2=$(yq '.erigon_cmd2' "$CONFIG_FILE")
    CHAIN=$(yq '.chain' "$CONFIG_FILE")

    SOURCE_DATADIR=$(strip_quotes "$SOURCE_DATADIR")
    
    # Validate required fields
    if [[ "$SOURCE_DATADIR" == "null" ]] || [[ -z "$SOURCE_DATADIR" ]]; then
        log_error "source_datadir is not specified in config"
        exit 1
    fi
    
    if [[ ! -d "$SOURCE_DATADIR" ]]; then
        log_error "Source datadir not found: $SOURCE_DATADIR"
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
    
    # Extract datadirs from commands
    DATADIR1=$(extract_datadir "$ERIGON_CMD1" "erigon_cmd1")
    DATADIR2=$(extract_datadir "$ERIGON_CMD2" "erigon_cmd2")
    
    # Ensure datadirs are different
    if [[ "$DATADIR1" == "$DATADIR2" ]]; then
        log_error "Both commands use the same datadir: $DATADIR1"
        log_error "Commands must use different datadirs for benchmarking"
        exit 1
    fi
    
    log_info "Configuration loaded:"
    log_info "  source_datadir: $SOURCE_DATADIR"
    log_info "  erigon_cmd1: $ERIGON_CMD1"
    log_info "  extracted datadir1: $DATADIR1"
    log_info "  erigon_cmd2: $ERIGON_CMD2"
    log_info "  extracted datadir2: $DATADIR2"
}

# Function to mirror datadir
mirror_datadir() {
    local source="$1"
    local destination="$2"

    if [[ "$SKIP_MIRROR" == true ]]; then
        return
    fi 
    
    log_info "Mirroring datadir from $source to $destination"
    local MIRROR_SCRIPT="./cmd/scripts/mirror-datadir.sh"
    if ! "$MIRROR_SCRIPT" "$source" "$destination" > /dev/null; then
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
    
    log_info "Starting benchmark run $run_number for $datadir"

    local logfile="$LOG_LOCATION/output.txt"
    ./build/bin/integration reset_state --datadir "$datadir" --chain $CHAIN > $logfile
    if [[ "$SKIP_MIRROR" != true ]]; then
        echo "1" | ./build/bin/erigon snapshots rm-state  --datadir "$datadir" --latest
    fi

    ./build/bin/integration print_stages --datadir "$datadir" --chain $CHAIN>"$logfile"

    BLOCK_AT=$(cat $logfile|awk '/OtterSync/ {print $2}'|tail -1)
    STATE_AT_TXNUM=$(cat $logfile|awk '/accounts/ {print $3}'|tail -1)
    local logfile2="$LOG_LOCATION/output2.txt"
    ./build/bin/erigon seg txnum --datadir "$datadir"  --txnum $STATE_AT_TXNUM > "$logfile2" 2>&1
    STATE_AT=$(cat $logfile2|grep out|awk -F'block=' '{print $2}')
    STATE_TO=$((STATE_AT + 3000))

    EXEC_TO=$((BLOCK_AT < STATE_TO ? BLOCK_AT : STATE_TO))

    cmd="$(strip_quotes "$cmd") --block $EXEC_TO"
    log_info "Executing Command: $cmd"

    # Create a log file for this run
    local log_file="$LOG_LOCATION/benchmark_run${run_number}_$(date +%Y%m%d_%H%M%S).log"
    
    clear_caches
    # Start the command in background and capture its PID
    timeout --preserve-status -k 3600 -s SIGKILL 3600 bash -c "$cmd" 2>&1 | tee "$log_file"
    if [[ $? -ne 0 ]]; then
        log_error "Benchmark run $run_number failed"
        exit 1
    fi
    
    log_info "Benchmark run $run_number completed"
    log_info "Log saved to: $log_file"
}

build_erigon() {
    log_info "Building erigon and integration..."
    make erigon integration
    if [[ $? -ne 0 ]]; then
        log_error "Failed to build erigon and integration"
        exit 1
    fi
    log_info "Build completed successfully"
}

# Main execution
main() {
    log_info "Starting execution benchmarking script"
    
    # Check required tools first
    check_required_tools
    
    # Validate arguments
    validate_args "$@"
    
    # Parse configuration
    parse_config

    build_erigon
    
    # Start benchmarking
    log_info "========================================="
    log_info "Beginning benchmark execution"
    log_info "========================================="
    
    # Run 1: datadir1 with erigon_cmd1
    log_info "--- Run 1 ---"
    mirror_datadir "$SOURCE_DATADIR" "$DATADIR1"
    execute_benchmark "$ERIGON_CMD1" "$DATADIR1" 1
    
    log_info ""
    log_info "--- Run 2 ---"
    mirror_datadir "$SOURCE_DATADIR" "$DATADIR2"
    execute_benchmark "$ERIGON_CMD2" "$DATADIR2" 2
    
    # Summary
    log_info "========================================="
    log_info "Benchmark execution completed successfully"
    log_info "========================================="
    log_info "Results:"
    log_info "  Run 1: $DATADIR1 - Check benchmark_run1_*.log"
    log_info "  Run 2: $DATADIR2 - Check benchmark_run2_*.log"
}

# Run main function
main "$@"