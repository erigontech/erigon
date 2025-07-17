#!/bin/bash

RPC_URL=$1
PRIVATE_KEY="$2"
RUNDIR=$(dirname "$0")
CONTRACTS_DIR="$RUNDIR/../../debug_tools/test-contracts/contracts"

. "$RUNDIR/../utils.sh"

# ------------------------------------
# EIP-6780: https://eips.ethereum.org/EIPS/eip-6780 Do not delete the contract
# EIP-4758: https://eips.ethereum.org/EIPS/eip-4758 Call SENDALL instead
# ------------------------------------
testSendAllEIP4758EIP6780() {
    local RPC_URL=$1
    local RECIPIENT=0x0123456789abcdef0123456789abcdef01234567
    $RUNDIR/test_selfdestruct.sh --rpc-url $RPC_URL --private-key $PRIVATE_KEY --recipient $RECIPIENT --contract $CONTRACTS_DIR/selfdestruct.sol:SelfDestruct

    if [ $? -ne 0 ]; then
        echo "SENDALL test failed."
        return 1
    fi
}

# ------------------------------------
# EIP 4844: https://eips.ethereum.org/EIPS/eip-4844 Point eval precompile only (L2 does not support blobs)
# ------------------------------------
testPointEvalPrecompileEIP4844() {
    local RPC_URL=$1
    $RUNDIR/test_precompile_prague_pointeval.sh --rpc-url $RPC_URL

    if [ $? -ne 0 ]; then
        echo "Point eval precompile test failed."
        return 1
    fi
}

# ------------------------------------
# EIP 5656: https://eips.ethereum.org/EIPS/eip-5656 MCOPY
# ------------------------------------
testMCopyEIP5656() {
    local RPC_URL=$1
    CONTRACT=$(forge create $CONTRACTS_DIR/MCopy.sol:MinimalMCopy --broadcast --rpc-url $RPC_URL --private-key $PRIVATE_KEY --json | jq -r '.deployedTo')
    if [ -z "$CONTRACT" ]; then
        echo "Failed to deploy MCopy contract."
        return 1
    fi

    echo "MCopy contract deployed at: $CONTRACT"

    EXPECTED_DATA="0x01020304"
    DATA=$(cast call $CONTRACT "copy(bytes)(bytes)" $EXPECTED_DATA -r $RPC_URL)

    echo "MCOPY data returned: $DATA"

    if [ "$DATA" != $EXPECTED_DATA ]; then
        echo "MCOPY data verification failed: expected $EXPECTED_DATA, got $DATA"
        return 1
    fi

    echo "MCOPY data verification successful"
}

# ------------------------------------
# EIP 1153: https://eips.ethereum.org/EIPS/eip-1153 Transient storage
# ------------------------------------
testTransientStorageEIP1153() {
    local RPC_URL=$1
    CONTRACT=$(forge create $CONTRACTS_DIR/TransientStorage.sol:TransientStorage --broadcast --rpc-url $RPC_URL --private-key $PRIVATE_KEY --json | jq -r '.deployedTo')
    if [ -z "$CONTRACT" ]; then
        echo "Failed to deploy transient storage contract."
        return 1
    fi

    echo "Transient storage contract deployed at: $CONTRACT"

    # Here we pick 0x010203...0004 padded to 32 bytes:
    INPUT_WORD=0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20
    DATA=$(cast call $CONTRACT "writeThenReadTransient(bytes32)(bytes32)" $INPUT_WORD -r $RPC_URL)

    echo "Transient storage data returned: $DATA"

    if [ "$DATA" != $INPUT_WORD ]; then
        echo "Transient storage data verification failed: expected $INPUT_WORD, got $DATA"
        return 1
    fi

    echo "Transient storage data verification successful"
}

echo "=============== Running Dencun tests ==============="

run testSendAllEIP4758EIP6780 "$RPC_URL"
# run testPointEvalPrecompileEIP4844 "$RPC_URL" # Disabled due to L2 not supporting blobs
run testMCopyEIP5656 "$RPC_URL"
run testTransientStorageEIP1153 "$RPC_URL"

echo "=============== Dencun tests completed ==============="

