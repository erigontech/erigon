#!/bin/bash

get_latest_l2_batch() {
    local latest_block
    latest_block=$(cast block latest --rpc-url "$(kurtosis port print cdk-v1 cdk-erigon-sequencer-001 rpc)" | grep "number" | awk '{print $2}')

    local latest_batch
    latest_batch=$(cast rpc zkevm_batchNumberByBlockNumber "$latest_block" --rpc-url "$(kurtosis port print cdk-v1 cdk-erigon-sequencer-001 rpc)" | sed 's/^"//;s/"$//')
    
    if [[ -z "$latest_batch" ]]; then
        echo "Error: Failed to get latest batch number" >&2
        return 1
    fi
    
    latest_batch_dec=$((latest_batch))
    
    echo "$latest_batch_dec"
}

get_latest_l1_verified_batch() {
    current_batch=$(cast logs --rpc-url "$(kurtosis port print cdk-v1 el-1-geth-lighthouse rpc)" --address 0x1Fe038B54aeBf558638CA51C91bC8cCa06609e91 --from-block 0 --json | jq -r '.[] | select(.topics[0] == "0x9c72852172521097ba7e1482e6b44b351323df0155f97f4ea18fcec28e1f5966" or .topics[0] == "0xd1ec3a1216f08b6eff72e169ceb548b782db18a6614852618d86bb19f3f9b0d3") | .topics[1]' | tail -n 1 | sed 's/^0x//')
    current_batch_dec=$((16#$current_batch))
    echo "$current_batch_dec"
}


wait_for_l1_batch() {
    local timeout=$1
    local batch_type=$2
    local start_time

    start_time=$(date +%s)

    latest_batch=$(get_latest_l2_batch)
    if [[ $? -ne 0 ]]; then
        echo "Error: Failed to get latest batch number" >&2
        return 1
    fi

    echo "Waiting for batch $latest_batch to be ${batch_type}..."
    while true; do
        current_time=$(date +%s)
        if [ $((current_time - start_time)) -ge "$timeout" ]; then
            echo "Timeout reached. Batch $latest_batch was not ${batch_type} within $timeout seconds."
            return 1
        fi

        if [ "$batch_type" = "virtual" ]; then

            current_batch=$(cast logs --rpc-url "$(kurtosis port print cdk-v1 el-1-geth-lighthouse rpc)" --address 0x1Fe038B54aeBf558638CA51C91bC8cCa06609e91 --from-block 0 --json | jq -r '.[] | select(.topics[0] == "0x3e54d0825ed78523037d00a81759237eb436ce774bd546993ee67a1b67b6e766") | .topics[1]' | tail -n 1 | sed 's/^0x//')
            current_batch=$((16#$current_batch))
        elif [ "$batch_type" = "verified" ]; then
            current_batch=$(cast rpc zkevm_verifiedBatchNumber --rpc-url "$(kurtosis port print cdk-v1 cdk-erigon-node-001 rpc)" | sed 's/^"//;s/"$//')
        else
            echo "Invalid batch type. Use 'virtual' or 'verified'."
            return 1
        fi

        if [[ -z "$current_batch" ]]; then
            echo "Error: Failed to get current batch number" >&2
            return 1
        fi

        current_batch_dec=$((current_batch))
        echo "Current ${batch_type} batch: $current_batch_dec, Latest batch: $latest_batch"
        if [ "$current_batch_dec" -ge "$latest_batch" ]; then
            echo "Batch $latest_batch has been ${batch_type}."
            return 0
        fi
        sleep 10
    done
}


wait_for_l2_block_number() {
    local block_number=$1
    local node_url=$2
    local latest_block=0
    local tries=0

    #while latest_block lower than block_number
    #if more than 5 attempts - throw error
    while [ "$latest_block" -lt "$block_number" ]; do
        latest_block=$(cast block latest --rpc-url "$node_url" | grep "number" | awk '{print $2}')
        if [[ $? -ne 0 ]]; then
            echo "Error: Failed to get latest block number" >&2
            return 1
        fi

        if [ "$tries" -ge 5 ]; then
            echo "Error: Failed to get block number $block_number" >&2
            return 1
        fi
        tries=$((tries + 1))

        echo "Current block number on $node_url: $latest_block, needed: $block_number. Waiting to try again."
        sleep 60
    done
}

stop_cdk_erigon_sequencer() {
    echo "Stopping cdk-erigon"
    kurtosis service exec cdk-v1 cdk-erigon-sequencer-001 "pkill -SIGTRAP proc-runner.sh" || true
    sleep 1
    kurtosis service exec cdk-v1 cdk-erigon-sequencer-001 "pkill -SIGINT cdk-erigon" || true
    sleep 30
}

# Set -e to exit on any command failure
set -e

stop_cdk_erigon_sequencer

echo "Copying and modifying config"
kurtosis service exec cdk-v1  cdk-erigon-sequencer-001 'cp \-r /etc/cdk-erigon/ /tmp/ && sed -i '\''s/zkevm\.executor-strict: true/zkevm.executor-strict: false/;s/zkevm\.executor-urls: zkevm-stateless-executor-001:50071/zkevm.executor-urls: ","/;$a zkevm.disable-virtual-counters: true'\'' /tmp/cdk-erigon/config.yaml'

echo "Starting cdk-erigon with modified config"
kurtosis service exec cdk-v1 cdk-erigon-sequencer-001 "nohup cdk-erigon --pprof=true --pprof.addr 0.0.0.0 --config /tmp/cdk-erigon/config.yaml --datadir /home/erigon/data/dynamic-kurtosis-sequencer > /proc/1/fd/1 2>&1 &"

# Wait for cdk-erigon to start
sleep 30

echo "Running loadtest using polycli"
/usr/local/bin/polycli loadtest --rpc-url "$(kurtosis port print cdk-v1 cdk-erigon-node-001 rpc)" --private-key "0x12d7de8621a77640c9241b2595ba78ce443d05e94090365ab3bb5e19df82c625" --verbosity 600 --requests 2000 --rate-limit 500  --mode uniswapv3 --legacy

echo "Waiting for batch virtualization"
if ! wait_for_l1_batch 600 "virtual"; then
    echo "Failed to wait for batch virtualization"
    exit 1
fi

echo "Stopping cdk node"
kurtosis service stop cdk-v1 cdk-node-001

stop_cdk_erigon_sequencer


# Good batch before counter overflow
latest_verified_batch=$(get_latest_l1_verified_batch)

# Rollback to the last good batch before the counter overflow on L1
echo "Rolling back to batch $latest_verified_batch"
cast send "0x2F50ef6b8e8Ee4E579B17619A92dE3E2ffbD8AD2" "rollbackBatches(address,uint64)" "0x1Fe038B54aeBf558638CA51C91bC8cCa06609e91" "$latest_verified_batch" --private-key "0x12d7de8621a77640c9241b2595ba78ce443d05e94090365ab3bb5e19df82c625" --rpc-url "$(kurtosis port print cdk-v1 el-1-geth-lighthouse rpc)"

echo "Using integration tool to unwind to batch $latest_verified_batch"
kurtosis service exec cdk-v1 cdk-erigon-sequencer-001 "integration state_stages_zkevm --config=/etc/cdk-erigon/config.yaml --unwind-batch-no=$latest_verified_batch --chain dynamic-kurtosis --datadir /home/erigon/data/dynamic-kurtosis-sequencer"

echo "Starting cdk-erigon with resequencing and counter enabled"
kurtosis service exec cdk-v1 cdk-erigon-sequencer-001 "timeout 300s cdk-erigon --pprof=true --pprof.addr 0.0.0.0 --config /etc/cdk-erigon/config.yaml --datadir /home/erigon/data/dynamic-kurtosis-sequencer  --zkevm.sequencer-resequence-strict=false --zkevm.sequencer-resequence=true --zkevm.sequencer-resequence-reuse-l1-info-index=true"

stop_cdk_erigon_sequencer

echo "Starting cdk-erigon with normal execution"
kurtosis service exec cdk-v1 cdk-erigon-sequencer-001 "nohup cdk-erigon --pprof=true --pprof.addr 0.0.0.0 --config /etc/cdk-erigon/config.yaml --datadir /home/erigon/data/dynamic-kurtosis-sequencer > /proc/1/fd/1 2>&1 &"

# Wait for cdk-erigon to start
sleep 30

echo "Restarting cdk node"
kurtosis service start cdk-v1 cdk-node-001

echo "Getting latest block number from sequencer"
latest_block=$(cast block latest --rpc-url "$(kurtosis port print cdk-v1 cdk-erigon-sequencer-001 rpc)" | grep "number" | awk '{print $2}')
echo "Latest block number from sequencer: $latest_block"

echo "Calculating comparison block number"
comparison_block=$((latest_block - 10))
echo "Block number to compare (10 blocks behind): $comparison_block"

echo "Waiting some time for the syncer to catch up"
sleep 30

echo "Getting block hash from sequencer"
sequencer_hash=$(cast block $comparison_block --rpc-url "$(kurtosis port print cdk-v1 cdk-erigon-sequencer-001 rpc)" | grep "hash" | awk '{print $2}')

# wait for block to be available on sync node
if ! wait_for_l2_block_number $comparison_block "$(kurtosis port print cdk-v1 cdk-erigon-node-001 rpc)"; then
    echo "Failed to wait for batch verification"
    exit 1
fi

echo "Getting block hash from node"
node_hash=$(cast block $comparison_block --rpc-url "$(kurtosis port print cdk-v1 cdk-erigon-node-001 rpc)" | grep "hash" | awk '{print $2}')

echo "Sequencer block hash: $sequencer_hash"
echo "Node block hash: $node_hash"

echo "Comparing block hashes"
if [ "$sequencer_hash" = "$node_hash" ]; then
    echo "The block hashes match for block number $comparison_block."
else
    echo "The block hashes do not match for block number $comparison_block."
    exit 1
fi

echo "Waiting for batch verification"
if ! wait_for_l1_batch 1200 "verified"; then
    echo "Failed to wait for batch verification"
    exit 1
fi

echo "All steps completed successfully"
