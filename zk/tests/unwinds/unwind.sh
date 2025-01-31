#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status
set -o pipefail  # Capture errors in pipelines

# Variables
dataPath="./datadir"
datastreamPath="zk/tests/unwinds/datastream"
datastreamZipFileName="./datastream-net8-upto-11318-101.zip"
firstStop=11203
secondStop=11315
unwindBatch=70
datastreamPort=6900
logFile="script.log"

# Redirect stdout to log file and keep stderr to console
exec > >(tee -i "$logFile")  # Redirect stdout to log file
exec 2> >(tee -a "$logFile" >&2)  # Redirect stderr to log file and keep it on console

# Cleanup function
cleanup() {
    echo "[$(date)] Cleaning up..."
    if [[ -n "$dspid" ]]; then
        echo "[$(date)] Killing process with PID $dspid on port $datastreamPort"
        kill -9 "$dspid" || echo "[$(date)] No process to kill on port $datastreamPort"
    fi

    echo "[$(date)] Cleaning data directories"
    #rm -rf "$dataPath"

    echo "[$(date)] Total execution time: $SECONDS seconds"
}
trap cleanup EXIT

# Kill existing datastream server if running
echo "[$(date)] Checking for existing datastream server on port $datastreamPort..."
set +e
dspid=$(lsof -i :$datastreamPort | awk 'NR==2 {print $2}')
set -e

if [[ -n "$dspid" ]]; then
    echo "[$(date)] Killing existing datastream server with PID $dspid on port $datastreamPort"
    kill -9 "$dspid" || echo "[$(date)] Failed to kill process $dspid"
fi

# Extract datastream data
echo "[$(date)] Extracting datastream data..."
pushd "$datastreamPath" || { echo "Failed to navigate to $datastreamPath"; exit 1; }
    tar -xzf "$datastreamZipFileName" || { echo "Failed to extract $datastreamZipFileName"; exit 1; }
popd

# Remove existing data directory
echo "[$(date)] Removing existing data directory..."
rm -rf "$dataPath"

# Start datastream server
echo "[$(date)] Starting datastream server..."
go run ./zk/debug_tools/datastream-host --file="$(pwd)/zk/tests/unwinds/datastream/hermez-dynamic-integration8-datastream/data-stream.bin" &
dspid=$!
echo "[$(date)] Datastream server PID: $dspid"

# Wait for datastream server to be available
echo "[$(date)] Waiting for datastream server to become available on port $datastreamPort..."
while ! bash -c "</dev/tcp/localhost/$datastreamPort" 2>/dev/null; do
    sleep 1
done
echo "[$(date)] Datastream server is now available."

# Function to dump data
dump_data() {
    local stop=$1
    local label=$2
    echo "[$(date)] Dumping data - $label ($stop)"
    go run ./cmd/hack --action=dumpAll --chaindata="$dataPath/rpc-datadir/chaindata" --output="$dataPath/$stop" || { echo "Failed to dump data for $label"; exit 1; }
}

# Run Erigon to first stop
echo "[$(date)] Running Erigon to BlockHeight: $firstStop"
./build/bin/cdk-erigon \
    --datadir="$dataPath/rpc-datadir" \
    --config="zk/tests/unwinds/config/dynamic-integration8.yaml" \
    --debug.limit="$firstStop"

dump_data "$firstStop" "sync to first stop"

# Run Erigon to second stop
echo "[$(date)] Running Erigon to BlockHeight: $secondStop"
./build/bin/cdk-erigon \
    --datadir="$dataPath/rpc-datadir" \
    --config="zk/tests/unwinds/config/dynamic-integration8.yaml" \
    --debug.limit="$secondStop"

dump_data "$secondStop" "sync to second stop"

# Unwind to first stop
echo "[$(date)] Unwinding to batch: $unwindBatch"
go run ./cmd/integration state_stages_zkevm \
    --datadir="$dataPath/rpc-datadir" \
    --config="zk/tests/unwinds/config/dynamic-integration8.yaml" \
    --chain=dynamic-integration \
    --unwind-batch-no="$unwindBatch" || { echo "Failed to unwind"; exit 1; }

dump_data "${firstStop}-unwound" "after unwind"

# Files expected to differ after unwind
different_files=(
    "Code.txt"
    "HashedCodeHash.txt"
    "hermez_l1Sequences.txt"
    "hermez_l1Verifications.txt"
    "HermezSmt.txt"
    "PlainCodeHash.txt"
    "SyncStage.txt"
    "BadHeaderNumber.txt"
    "CallToIndex.txt"
    "bad_tx_hashes_lookup.txt"
)

is_in_array() {
    local element="$1"
    for elem in "${different_files[@]}"; do
        if [[ "$elem" == "$element" ]]; then
            return 0
        fi
    done
    return 1
}

# Function to compare dumps and print only 'UNEXPECTED' diffs with more context
compare_dumps() {
    local original_dir=$1
    local comparison_dir=$2
    local label=$3
    local expected_diffs=("${!4}")

    echo "[$(date)] Comparing dumps between $original_dir and $comparison_dir..."

    for file in "$original_dir"/*; do
        filename=$(basename "$file")
        file_comparison="$comparison_dir/$filename"

        if [[ ! -f "$file_comparison" ]]; then
            echo "[$(date)] File $filename missing in $comparison_dir." >&2
            exit 1
        fi

        if cmp -s "$file" "$file_comparison"; then
            # No difference; do nothing
            :
        else
            if is_in_array "$filename"; then
                # Expected difference; optionally log if needed
                :
            else
                # Unexpected difference; print to stderr with more context
                echo "[$(date)] $label - Unexpected differences in $filename" >&2
                echo "[$(date)] Dumping differences for $filename:" >&2
                diff -u "$file" "$file_comparison" >&2 || true
                exit 1
            fi
        fi
    done
}

# Compare first stop dumps
compare_dumps "$dataPath/${firstStop}" "$dataPath/${firstStop}-unwound" "Unwind Check" different_files[@]

# Resync to second stop
echo "[$(date)] Resyncing Erigon to second stop: $secondStop"
./build/bin/cdk-erigon \
    --datadir="$dataPath/rpc-datadir" \
    --config="zk/tests/unwinds/config/dynamic-integration8.yaml" \
    --debug.limit="$secondStop"

dump_data "${secondStop}-sync-again" "after resyncing to second stop"

# Define expected differences for second comparison
second_comparison_expected_diffs=(
    "BadHeaderNumber.txt"
    "bad_tx_hashes_lookup.txt"
)

# Compare second stop dumps
compare_dumps "$dataPath/${secondStop}" "$dataPath/${secondStop}-sync-again" "Sync forward again" second_comparison_expected_diffs[@]

echo "[$(date)] No error"
