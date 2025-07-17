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
# EIP-3651 “Warm COINBASE” Test
# ------------------------------------
testEIP3651() {
    local RPC_URL="$1"
    local CONTRACT_ADDR
    local SEND_JSON
    local GAS_USED_HEX
    local GAS_USED_DEC

    # 1) Deploy the CoinbaseBalance contract
    echo "Deploying CoinbaseBalance contract…"
    CONTRACT_ADDR=$(forge create \
        "$CONTRACTS_DIR/CoinbaseBalance.sol:CoinbaseBalance" \
        --evm-version shanghai \
        --broadcast \
        --rpc-url "$RPC_URL" \
        --private-key "$PRIVATE_KEY" \
        --json | jq -r '.deployedTo')

    if [[ -z "$CONTRACT_ADDR" ]]; then
        echo "Error: Failed to deploy CoinbaseBalance contract" >&2
        return 1
    fi

    echo "Deployed at: $CONTRACT_ADDR"
    echo "Calling getCoinbaseBalance() on $CONTRACT_ADDR"

    # 2) Send a transaction that reads block.coinbase.balance
    SEND_JSON=$(cast send "$CONTRACT_ADDR" \
        "getCoinbaseBalance()(uint256)" \
        --rpc-url "$RPC_URL" \
        --private-key "$PRIVATE_KEY" \
        --gas-limit 100000 \
        --json)

    GAS_USED_HEX=$(echo "$SEND_JSON" | jq -r '.gasUsed')
    echo "Gas used by getCoinbaseBalance(): $GAS_USED_HEX"

    # 3) Convert to decimal
    GAS_USED_DEC=$((16#${GAS_USED_HEX#0x}))
    echo "Gas used (decimal): $GAS_USED_DEC"

    # 4) Compare against expected thresholds
    #    - If EIP-3651 is active, BALANCE(block.coinbase) is “warm” → cost ~21479
    #    - If not active, BALANCE(block.coinbase) is “cold” → cost ~ 23984
    if (( GAS_USED_DEC < 22000 )); then
        echo "EIP-3651 is active (warm COINBASE behavior detected)."
        return 0
    else
        echo "EIP-3651 not active (cold COINBASE cost observed)." >&2
        return 1
    fi

    echo "EIP-3651 test completed successfully."
}

# ------------------------------------
# EIP-3855 “PUSH0” Opcode Test
# ------------------------------------
testEIP3855() {
    local RPC_URL="$1"
    local CONTRACT_ADDR
    local RAW_RETURN
    local DECODED

    echo "Deploying Push0 contract…"
    local BYTECODE="0x6009600c60003960096000f35f60005260206000f3" #5f is PUSH0
    # 1) Deploy Push0
    CONTRACT_ADDR=$(cast send \
        --rpc-url "$RPC_URL" \
        --private-key "$PRIVATE_KEY" \
        --create "$BYTECODE" \
        --json | jq -r '.contractAddress')

    if [[ -z "$CONTRACT_ADDR" ]]; then
        echo "Error: Failed to deploy Push0 contract" >&2
        return 1
    fi

    echo "Deployed at: $CONTRACT_ADDR"
    echo "Getting contract code from $CONTRACT_ADDR"

    local CODE=$(cast code "$CONTRACT_ADDR" --rpc-url "$RPC_URL")
    if [[ -z "$CODE" ]]; then
        echo "Error: Failed to retrieve contract code" >&2
        return 1
    fi

    # Code must start with 0x5f (PUSH0 opcode)
    if [[ "${CODE:0:4}" != "0x5f" ]]; then
        echo "Error: Contract code does not start with PUSH0 opcode (0x5f)" >&2
        return 1
    fi

    echo "Contract code starts with PUSH0 opcode (0x5f). Proceeding with test."

    # 2) Call getZero() via eth_call to exercise PUSH0
    RAW_RETURN=$(cast call "$CONTRACT_ADDR" \
        0x \
        --rpc-url "$RPC_URL" 2>&1) || {
        echo "Reverted: EIP-3855 (PUSH0) not supported on this node" >&2
        return 1
    }

    echo "RAW return data: $RAW_RETURN"

    # 3) Decode the returned uint256
    echo "Decoding return data…"
    DECODED=$(cast abi-decode 'foo()(uint256)' "$RAW_RETURN")
    echo "Decoded uint256: $DECODED"

    # 4) Check that it equals 0x0
    if [[ "$DECODED" == "0" ]]; then
        echo "EIP-3855 (PUSH0) is active: returned 0."
        return 0
    else
        echo "Unexpected return: $DECODED (expected all zeros)" >&2
        return 1
    fi

    echo "EIP-3855 test completed successfully."
}


# ------------------------------------
# EIP-3860 “Limit and Meter Initcode” Test
# ------------------------------------
testEIP3860() {
    local RPC_URL="$1"
    local TX1_JSON TX2_JSON ERR_JSON
    local GAS1_HEX GAS2_HEX GAS1 GAS2 DIFF
    local TOO_LONG_INITCODE

    echo "A) Generate and deploy 32-byte initcode (1 word)"
    INIT32="0x$(printf '%064s' '' | tr ' ' '0')"
    TX1_JSON=$(cast send \
        --rpc-url "$RPC_URL" \
        --private-key "$PRIVATE_KEY" \
        --gas-limit 200000 \
        --create "$INIT32" \
        --json)

    GAS1_HEX=$(echo "$TX1_JSON" | jq -r '.gasUsed')
    # Convert hex‐string to decimal
    GAS1=$((16#${GAS1_HEX#0x}))
    echo "gasUsed for 32-byte initcode: $GAS1    (hex $GAS1_HEX)"


    echo "B) Generate and deploy 64-byte initcode (2 words)"
    INIT64="0x$(printf '%0128s' '' | tr ' ' '0')"
    TX2_JSON=$(cast send \
        --rpc-url "$RPC_URL" \
        --private-key "$PRIVATE_KEY" \
        --gas-limit 200000 \
        --create "$INIT64" \
        --json)

    GAS2_HEX=$(echo "$TX2_JSON" | jq -r '.gasUsed')
    GAS2=$((16#${GAS2_HEX#0x}))
    echo "gasUsed for 64-byte initcode: $GAS2    (hex $GAS2_HEX)"

    echo "C) Check that extra word costs 2 gas"
    CALLDATA_COST=$((4 * 32)) # 4 gas per extra byte of calldata (32 bytes = 1 word)
    DIFF=$((GAS2 - CALLDATA_COST - GAS1))
    EXPECTED_DIFF=2 # 2 gas per extra word
    if [[ "$DIFF" -eq "$EXPECTED_DIFF" ]]; then
        echo "Expected 2 gas difference per extra word, got $DIFF"
    else
        echo "Expected 2 gas difference, got $DIFF" >&2
        return 1
    fi


    echo "D) Attempt to deploy (49 152 + 1) bytes of initcode (should revert)"
    # Build a hex string of length 49 153 bytes: “0x” + 49 153 “00” pairs.
    # (Each byte = “00”, so N bytes → 2N hex digits after “0x”.)
    local TOO_LONG_BYTES=49153
    # Use `head`+`xxd` to generate a repeated “00” sequence of length 49153.
    TOO_LONG_INITCODE="0x$(head -c $((TOO_LONG_BYTES)) < /dev/zero | xxd -p | tr -d '\n')"
    # That string is 2×49 153 hex digits prefixed by “0x”, so 98 306 nibbles + “0x”.

    # Now try to CREATE—it must revert with “out of gas” or “invalid opcode” because initcode too big.
    if ERR_JSON=$(cast send\
        --rpc-url "$RPC_URL" \
        --private-key "$PRIVATE_KEY" \
        --gas-limit 10_000_000 \
        --create "$TOO_LONG_INITCODE" 2>&1); then
        echo "Unexpected: Creation of >49 152 bytes did not revert!" >&2
        return 1
    else
        echo "Deployment error (expected), message: $ERR_JSON"
        echo "Reverted when initcode exceeded 49 152 bytes (EIP-3860 limit)."
    fi

    return 0
}


echo "=============== Running Shaghai tests ==============="

run testEIP3651 "$RPC_URL"
run testEIP3855 "$RPC_URL"
run testEIP3860 "$RPC_URL"

echo "=============== Shanghai tests completed ==============="
