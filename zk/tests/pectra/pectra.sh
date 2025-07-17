#!/bin/bash

RPC_URL=$1
PRIVATE_KEY="$2"
RUNDIR=$(dirname "$0")
CONTRACTS_DIR="$RUNDIR/../../debug_tools/test-contracts/contracts"

. "$RUNDIR/../utils.sh"

# ------------------------------------
# EIP-7516: https://eips.ethereum.org/EIPS/eip-7516
# ------------------------------------
deployBlobBaseFeeContract() {
    # contract BlobBasefeeTest {
    #    /// @notice Read the blob base-fee via the new opcode (0x4a) and return it
    #    function getBlobBaseFee() public view returns (uint256) {
    #        return block.blobbasefee;  // in older compilers you can also use `block.blobGasPrice`
    #    }
    # }
    output=$(cast send --legacy --private-key $2 -r $1 --create "0x6080604052348015600e575f80fd5b5060ae80601a5f395ff3fe6080604052348015600e575f80fd5b50600436106026575f3560e01c80631f6d6ef714602a575b5f80fd5b60306044565b604051603b91906061565b60405180910390f35b5f4a905090565b5f819050919050565b605b81604b565b82525050565b5f60208201905060725f8301846054565b9291505056fea2646970667358221220202d9ccb8b2441bec663b2c099daac55191c017836d93daac4907f08c899670564736f6c63430008190033")
    echo "$output" | grep contractAddress | awk '{ print $2 }'
}

testBlobBaseFeeEIP7516() {
    echo "Deploying BlobBaseFee contract..."
    CONTRACT=$(deployBlobBaseFeeContract "$RPC_URL" "$PRIVATE_KEY")

    echo "Contract address: $CONTRACT"

    if [ -z "$CONTRACT" ]; then
        echo "Failed to deploy contract."
        return 1
    fi

    local RPC_URL=$1
    fee=$(cast call $CONTRACT "getBlobBaseFee()(uint256)" -r $RPC_URL)

    # For L2, the blob basefee should be 1
    if [ "$fee" -eq 1 ]; then
        echo "BlobBasefee is 1 as expected."
    else
        echo "BlobBasefee is not 1, it is $fee."
        return 1
    fi
}

# ------------------------------------
# EIP-7623: https://eips.ethereum.org/EIPS/eip-7623
# ------------------------------------
testCalldataCostEIP7623() {
    local RPC_URL=$1

    ZEROS=$(printf '00%.0s' {1..2048})
    ONES=$(printf 'ff%.0s' {1..2048})

    # Post-Pectra Gas Used (10 gas per “token” where a zero byte = 1 token, non-zero = 4 tokens):
    # 2048 zero bytes: Tokens = 2048 → 10 × 2048 = 20 480 → 21 000+20 480 = 41 480 gas
    # 2048 non-zero bytes: Tokens = 4 × 2048 = 8192 → 10 × 8192 = 81 920 → 21 000+81 920 = 102 920 gas
    zeros_cost=$(cast estimate 0x0000000000000000000000000000000000000000 0x$ZEROS -r $RPC_URL)
    ones_cost=$(cast estimate 0x0000000000000000000000000000000000000000 0x$ONES -r $RPC_URL)

    if [ "$zeros_cost" -eq 41480 ]; then
        echo "Gas cost for 2048 zero bytes is as expected: $zeros_cost"
    else
        echo "Gas cost for 2048 zero bytes is not as expected: $zeros_cost"
        return 1
    fi

    if [ "$ones_cost" -eq 102920 ]; then
        echo "Gas cost for 2048 non-zero bytes is as expected: $ones_cost"
    else
        echo "Gas cost for 2048 non-zero bytes is not as expected: $ones_cost"
        return 1
    fi
}

# ------------------------------------
# EIP7702: https://eips.ethereum.org/EIPS/eip-7702
# ------------------------------------
testSetCodeTxEIP7702() {
    local RPC_URL=$1
    local SPONSOR_WALLET=$(cast wallet new)
    local SPONSOR_PKEY=$(echo "$SPONSOR_WALLET" | grep "Private key" | awk '{print $3}')
    local SPONSOR_ADDRESS=$(echo "$SPONSOR_WALLET" | grep "Address" | awk '{print $2}')

    echo "Funding sender account..."
    STATUS=$(cast send --legacy --value 0.1ether --json --private-key $PRIVATE_KEY -r $RPC_URL $SPONSOR_ADDRESS | jq -r '.status')
    echo "Funding status: $STATUS"
    if [ "$STATUS" != "0x1" ]; then
        echo "Failed to fund sender account: $STATUS"
        return 1
    fi

    echo "Sender account funded successfully."
    $RUNDIR/test_eip7702_setcode_tx.sh --rpc-url $RPC_URL --private-key-eoa $PRIVATE_KEY --private-key-sender $SPONSOR_PKEY --contract $CONTRACTS_DIR/delegate.sol:Delegate --gas 100_000

    if [ $? -ne 0 ]; then
        echo "EIP-7702 test failed."
        return 1
    fi
}

echo "=============== Running Pectra tests ==============="

run testBlobBaseFeeEIP7516 "$RPC_URL"
run testCalldataCostEIP7623 "$RPC_URL"
run testSetCodeTxEIP7702 "$RPC_URL"

echo "=============== Pectra tests completed ==============="

