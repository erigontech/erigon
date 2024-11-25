#!/bin/bash

# steps:
# 1. run to where we will unwind to
# 2. dump the data
# 3. run to the final stop block
# 4. dump the data
# 5. unwind
# 6. dump the data
# 7. sync again to the final block
# 8. dump the data
# 9. compare the dumps at the unwind level and tip level

SECONDS=0

dspid=$(lsof -i :6900 | awk 'NR==2 {print $2}')
kill -9 "$dspid"

cleanup() {
    echo "killing datastream server"
    if [[ -n "$dspid" ]]; then
        echo "killing process with PID $dspid on port 6900"
        kill -9 "$dspid"
    fi

    echo "cleaning data directories"
    rm -rf "$dataPath/rpc-datadir"
    rm -rf "$dataPath/phase1-dump1"
    rm -rf "$dataPath/phase1-dump2"

    rm -rf "$dataPath/phase2-dump1"
    rm -rf "$dataPath/phase2-dump2"

    echo "Total execution time: $SECONDS seconds"
}

trap cleanup EXIT

dataPath="./datadir"
datastreamPath="zk/tests/unwinds/datastream"
datastreamZipFileName="./datastream-net8-upto-11318-101.zip"
firstStop=11203
stopBlock=11315
unwindBatch=70

pushd "$datastreamPath" || exit
    tar -xzf "$datastreamZipFileName"
popd || exit

rm -rf "$dataPath/rpc-datadir"
rm -rf "$dataPath/phase1-dump1"
rm -rf "$dataPath/phase1-dump2"

rm -rf "$dataPath/phase2-dump1"
rm -rf "$dataPath/phase2-dump2"

# rm -rf "$dataPath/phase1-diffs"
# rm -rf "$dataPath/phase2-diffs"

# run datastream server
echo -e '\nStarting datastream server \n'
go run ./zk/debug_tools/datastream-host --file="$(pwd)/zk/tests/unwinds/datastream/hermez-dynamic-integration8-datastream/data-stream.bin" &

dspid=$! # get the id of the DS process

echo "Waiting for datastream server to become available on port 6900..."
while ! bash -c "</dev/tcp/0.0.0.0/6900" 2>/dev/null; do
    sleep 1
done
echo "Datastream server is now available."

# try with 1 and check on time
echo -e '\nRun Erigon to BlockHeight: ' "${firstStop}" '\n'
./build/bin/cdk-erigon \
    --datadir="$dataPath/rpc-datadir" \
    --config="zk/tests/unwinds/config/dynamic-integration8.yaml" \
    --debug.limit=1 \
    --debug.limit="${firstStop}"

echo -e '\nDumping data \n'

# now get a dump of the datadir at this point
go run ./cmd/hack --action=dumpAll --chaindata="$dataPath/rpc-datadir/chaindata" --output="$dataPath/phase1-dump1"

echo -e '\nRun Erigon to Block Height: ' "${stopBlock}" '\n'
# now run to the final stop block
./build/bin/cdk-erigon \
    --datadir="$dataPath/rpc-datadir" \
    --config="zk/tests/unwinds/config/dynamic-integration8.yaml" \
    --debug.limit="${stopBlock}"  

echo -e '\nDumping data phase 2 \n'
# now get a dump of the datadir at this point
go run ./cmd/hack --action=dumpAll --chaindata="$dataPath/rpc-datadir/chaindata" --output="$dataPath/phase2-dump1"

# now run the unwind
echo -e '\nUnwinding to batch: ' "${unwindBatch}" '\n'  
go run ./cmd/integration state_stages_zkevm \
    --datadir="$dataPath/rpc-datadir" \
    --config="zk/tests/unwinds/config/dynamic-integration8.yaml" \
    --chain=dynamic-integration \
    --unwind-batch-no="${unwindBatch}"  

echo -e '\nDumping data after unwind \n'
# now get a dump of the datadir at this point
go run ./cmd/hack --action=dumpAll --chaindata="$dataPath/rpc-datadir/chaindata" --output="$dataPath/phase1-dump2"

# mkdir -p "$dataPath/phase1-diffs/pre"
# mkdir -p "$dataPath/phase1-diffs/post"

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
)

is_in_array() {
    local element
    for element in "${different_files[@]}"; do
        if [[ "$element" == "$filename" ]]; then
            return 0
        fi
    done
    return 1
}

# iterate over the files in the pre-dump folder
# we are going to check if unwind worked
for file in "$dataPath/phase1-dump1"/*; do
    # get the filename
    filename=$(basename "$file")  

    # diff the files and if there is a difference found copy the pre and post files into the diffs folder
    if cmp -s "$dataPath/phase1-dump1/$filename" "$dataPath/phase1-dump2/$filename"; then  
        echo "No difference found in $filename"
    else
        # this is a list of files where we expect differences.
        if is_in_array; then
            echo "Phase 1 - Expected differences in $filename"
        else
            # unwind tests failed
            echo "Phase 1 - Error unexpected differences in $filename"
            echo "Unwind failed"
            exit 1
        fi
    fi
done

# now sync again
# the data must match, if it doesn't match something is wrong, because if we unwinded returning to it should be the same.
echo -e '\nRunning erigon to the same stopBlock again \n'
./build/bin/cdk-erigon \
    --datadir="$dataPath/rpc-datadir" \
    --config="zk/tests/unwinds/config/dynamic-integration8.yaml" \
    --debug.limit="${stopBlock}"  

echo -e '\nDumping data after unwind \n'
# dump the data again into the post folder
go run ./cmd/hack --action=dumpAll --chaindata="$dataPath/rpc-datadir/chaindata" --output="$dataPath/phase2-dump2"

# mkdir -p "$dataPath/phase2-diffs/pre"
# mkdir -p "$dataPath/phase2-diffs/post"

# iterate over the files in the pre-dump folder
for file in "$dataPath/phase2-dump1"/*; do
    # get the filename
    filename=$(basename "$file")  

    # diff the files and if there is a difference found copy the pre and post files into the diffs folder
    if cmp -s "$dataPath/phase2-dump1/$filename" "$dataPath/phase2-dump2/$filename"; then  
        echo "Phase 2 No difference found in $filename"
    else
        # file where it should be different
        if [ "$filename" = "BadHeaderNumber.txt" ]; then  
            echo "Phase 2 - Expected differences in $filename"
        else
            echo "Phase 2 - Error unexpected differences in $filename"
            exit 2
        fi
    fi
done

echo "No error"
