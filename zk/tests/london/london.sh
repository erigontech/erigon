#!/bin/bash

RPC_URL=$1
PRIVATE_KEY="$2"
RUNDIR=$(dirname "$0")
CONTRACTS_DIR="$RUNDIR/../../debug_tools/test-contracts/contracts"

if [[ -z "$RPC_URL" || -z "$PRIVATE_KEY" ]]; then
    echo "Usage: $0 <rpc-url> <private-key>"
    exit 1
fi

. "$RUNDIR/../utils.sh"

# ------------------------------------
# EIP 1559 Transaction Test
# ------------------------------------
testTxEIP1559() {
    local RPC_URL="$1"
    local VALUE="0.01ether"
    local TO="0x000000000000000000000000000000000000dead"  # example recipient

    # Gas settings for EIP-1559
    local MAX_FEE="50gwei"
    local PRIORITY_FEE="2gwei"
    local GAS_LIMIT="21000"

    # Attempt to send the transaction
    local OUTPUT
    OUTPUT=$(cast send "$TO" \
        --value "$VALUE" \
        --gas-limit "$GAS_LIMIT" \
        --private-key "$PRIVATE_KEY" \
        --rpc-url "$RPC_URL" --json | jq -r '.status')

    if [[ "$OUTPUT" != "0x1" ]]; then
        echo "Error: transaction failed to send"
        echo "$OUTPUT"
        return 1
    fi

    echo "Transaction successfully sent"
    return 0
}

# ------------------------------------
# EIP 3198 Base Fee Test
# ------------------------------------
testBaseFeeEIP3198() {
    local RPC_URL="$1"
    local CONTRACT_ADDR

    CONTRACT_ADDR=$(forge create \
        $CONTRACTS_DIR/BaseFee.sol:Basefee \
        --evm-version london \
        --broadcast \
        --rpc-url "$RPC_URL" \
        --private-key "$PRIVATE_KEY" \
        --json | jq -r '.deployedTo')

    if [ -z "$CONTRACT_ADDR" ]; then
        echo "Failed to deploy Basefee contract" >&2
        return 1
    fi

    echo "Deployed at: $CONTRACT_ADDR"
    echo "Calling getBasefee() on contract at $CONTRACT_ADDR"

    BASEFEE=$(cast call \
        "$CONTRACT_ADDR" \
        "getBasefee()(uint256)" \
        --rpc-url "$RPC_URL")

    echo "Basefee: $BASEFEE"

    # Check that no error occurred
    # "call" will not return real basefee, but returns 0
    # the real basefee is used in a tx only
    if [[ "$BASEFEE" != "0" ]]; then
        echo "Error: Expected basefee to be 0, got $BASEFEE"
        return 1
    fi

    echo "Basefee test passed"
}

# ------------------------------------
# EIP 3541 Reject new contract code starting with the 0xEF byte
# ------------------------------------
testEIP3541() {
    local RPC_URL="$1"
    local EF_INITCODE="0x60ef60005360016000f3"  # runtime = 0xef (should revert)
    local FE_INITCODE="0x60fe60005360016000f3"  # runtime = 0xfe (should succeed)
    local STATUS

    echo "Deploy runtime 0xef (must fail under EIP-3541)"

    STATUS=$(cast send \
        --rpc-url "$RPC_URL" \
        --private-key "$PRIVATE_KEY" \
        --gas-limit 100000 \
        --create "$EF_INITCODE" \
        --json | jq -r .status)

    if [[ $STATUS == "0x1" ]]; then
        echo "Error: Contract with runtime 0xef was deployed, but it should have been rejected." >&2
        return 1
    fi

    echo "Correctly reverted. EIP-3541 prevented 0xef runtime."

    echo "Deploy runtime 0xfe (must succeed)"

    STATUS=$(cast send \
        --rpc-url "$RPC_URL" \
        --private-key "$PRIVATE_KEY" \
        --gas-limit 100000 \
        --create "$FE_INITCODE" \
        --json | jq -r .status)

    if [[ $STATUS != "0x1" ]]; then
        echo "Error: Contract with runtime 0xfe failed to deploy." >&2
        return 1
    fi

    echo "EIP-3541 test passed"
}

# ------------------------------------
# EIP 3529 Gas Refund Reduction Test
# ------------------------------------
testEIP3529() {
    local RPC_URL="$1"
    local CONTRACT_ADDR
    local RESPONSE GAS_USED1 GAS_USED2 GAS_USED1_HEX GAS_USED2_HEX
    local REFUND
    local GAS_USED_CLEAR_BERLIN=13122 # Expected gas used for clearVal() before EIP-3529

    CONTRACT_ADDR=$(forge create \
        $CONTRACTS_DIR/Refund3529.sol:Refund \
        --evm-version london \
        --broadcast \
        --rpc-url "$RPC_URL" \
        --private-key "$PRIVATE_KEY" \
        --json | jq -r '.deployedTo')

    if [ -z "$CONTRACT_ADDR" ]; then
        echo "Failed to deploy Basefee contract" >&2
        return 1
    fi

    echo "Deployed at: $CONTRACT_ADDR"

    echo "Step 1: setVal(1) → val goes 0→1 (no refund)"
    # read GAS_USED1 STATUS < <(cast send "$CONTRACT_ADDR" \
    RESPONSE=$(cast send "$CONTRACT_ADDR" \
        "setVal(uint256)" "1" \
        --rpc-url "$RPC_URL" \
        --private-key "$PRIVATE_KEY" \
        --gas-limit 100000 \
        --json)

    STATUS=$(echo "$RESPONSE" | jq -r '.status')

    if [[ "$STATUS" != "0x1" ]]; then
        echo "Error: setVal(1) failed with status $STATUS" >&2
        return 1
    fi

    GAS_USED1_HEX=$(echo "$RESPONSE" | jq -r '.gasUsed')
    GAS_USED1=$((16#${GAS_USED1_HEX#0x}))

    echo "Gas used by setVal(1): $GAS_USED1"

    echo "Step 2: clearVal() → val goes 1→0 (should refund 4 000 under EIP-3529)"
    RESPONSE=$(cast send "$CONTRACT_ADDR" \
        "clearVal()" "" \
        --rpc-url "$RPC_URL" \
        --private-key "$PRIVATE_KEY" \
        --gas-limit 100000 \
        --json)

    STATUS=$(echo "$RESPONSE" | jq -r '.status')

    if [[ "$STATUS" != "0x1" ]]; then
        echo "Error: clearVal() failed with status $STATUS" >&2
        return 1
    fi

    GAS_USED2_HEX=$(echo "$RESPONSE" | jq -r '.gasUsed')
    GAS_USED2=$((16#${GAS_USED2_HEX#0x}))

    echo "Gas used by clearVal(): $GAS_USED2"

    # refund 15000 -> 4000
    REFUND_DIFF=$((GAS_USED_CLEAR_BERLIN - GAS_USED2))
    echo "Refund received: $REFUND_DIFF"

    # Exact refund is not 4000 as per EIP-3529, but depends on other factors
    # so I just compare it to pre-London gas used for clearVal() (measured) and
    # gas used by setVal(1)
    if [[ $REFUND_DIFF -ge 0 ]]; then
        echo "Error: Expected refund diff to be less than 0, got $REFUND_DIFF" >&2
        return 1
    fi

    if [[ $GAS_USED2 -ge $GAS_USED1 ]]; then
        echo "Error: Expected gas used by clearVal() to be less than gas used by setVal(1), got $GAS_USED2 >= $GAS_USED1" >&2
        return 1
    fi

    echo "EIP-3529 test passed"
}

echo "=============== Running London tests ==============="

run testTxEIP1559 "$RPC_URL"
run testBaseFeeEIP3198 "$RPC_URL"
run testEIP3541 "$RPC_URL"
run testEIP3529 "$RPC_URL"

echo "=============== London tests completed ==============="
