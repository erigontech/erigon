#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 \
  --rpc-url <url> \
  --private-key-eoa <hex> \
  --private-key-sender <hex> \
  --contract <path:ContractName> \
  [--constructor-args <arg1,arg2,...>] \
  [--gas <uint>] \
  [--help]

Options:
  --rpc-url             L2 RPC endpoint URL
  --private-key-eoa     Hex key for auth-list signing (0x…)
  --private-key-sender  Hex key for contract deployment & sponsor tx (0x…)
  --contract            Fully-qualified contract (e.g. "contracts/Foo.sol:Foo")
  --constructor-args    Comma-separated constructor args (optional)
  --gas                 Gas limit for SetCodeTx (default: 50000)
  -h, --help            Show this help and exit
EOF
  exit 1
}

RPC_URL="" PK_EOA="" PK_SENDER="" CONTRACT_FQN="" GAS=100000
declare -a CONSTRUCTOR_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    --rpc-url)             RPC_URL="$2"; shift 2;;
    --private-key-eoa)     PK_EOA="$2"; shift 2;;
    --private-key-sender)  PK_SENDER="$2"; shift 2;;
    --contract)            CONTRACT_FQN="$2"; shift 2;;
    --constructor-args)    IFS=',' read -r -a CONSTRUCTOR_ARGS <<<"$2"; shift 2;;
    --gas)                 GAS="$2"; shift 2;;
    -h|--help)             usage;;
    *) echo "Unknown option $1"; usage;;
  esac
done

if [[ -z "$RPC_URL" || -z "$PK_EOA" || -z "$PK_SENDER" || -z "$CONTRACT_FQN" ]]; then
  echo "Error: missing required flags"; usage
fi

# ──────────────────────────────────────────────────────────────────────────────
# 1) Deploy the contract with forge
# ──────────────────────────────────────────────────────────────────────────────
echo "Deploying $CONTRACT_FQN..."
if (( ${#CONSTRUCTOR_ARGS[@]} > 0 )); then
  JSON_OUT=$(forge create "$CONTRACT_FQN" "${CONSTRUCTOR_ARGS[@]}" \
    --rpc-url "$RPC_URL" \
    --private-key "$PK_SENDER" \
    --broadcast --json --evm-version "london")
else
  JSON_OUT=$(forge create "$CONTRACT_FQN" \
    --rpc-url "$RPC_URL" \
    --private-key "$PK_SENDER" \
    --broadcast --json --evm-version "london")
fi

TX_HASH=$(echo "$JSON_OUT" | jq -r '.txHash // .transactionHash')
echo "Deploy tx hash : $TX_HASH"

RECEIPT=$(cast receipt "$TX_HASH" --rpc-url "$RPC_URL" --json)
CONTRACT_ADDR=$(echo "$RECEIPT" | jq -r '.contractAddress')
echo "Contract at    : $CONTRACT_ADDR"

# ──────────────────────────────────────────────────────────────────────────────
# 2) Send the SetCodeTx
# ──────────────────────────────────────────────────────────────────────────────

echo "Sending set code TX..."
SET_CODE_TX=$(cast send "$CONTRACT_ADDR" --rpc-url "$RPC_URL" --private-key "$PK_SENDER" --gas-limit "$GAS" --auth "$(cast wallet sign-auth "$CONTRACT_ADDR" --private-key "$PK_EOA" --rpc-url "$RPC_URL")" --json)
SET_CODE_TX_HASH=$(echo "$SET_CODE_TX" | jq -r '.txHash // .transactionHash')
echo "SetCodeTx hash : $SET_CODE_TX_HASH"

SET_CODE_RECEIPT=$(cast receipt "$SET_CODE_TX_HASH" --rpc-url "$RPC_URL" --json)

TX_TYPE=$(echo "$SET_CODE_RECEIPT" | jq -r '.type')
if [[ "$TX_TYPE" != "0x4" ]]; then
  echo "Error: unexpected tx type ($TX_TYPE), expected 4 for EIP-7702" >&2
  exit 1
fi

STATUS=$(echo "$SET_CODE_RECEIPT" | jq -r '.status')
if [[ "$STATUS" -ne 1 ]]; then
  echo "Error: transaction failed (status=$STATUS)" >&2
  exit 1
fi

LOG_COUNT=$(echo "$SET_CODE_RECEIPT" | jq -r '.logs | length')
if [[ "$LOG_COUNT" -ne 1 ]]; then
  echo "Error: expected 1 log, got $LOG_COUNT" >&2
  exit 1
fi

# check the log’s address matches the delegate contract
LOG_ADDR=$(echo "$SET_CODE_RECEIPT" | jq -r '.logs[0].address')
LOG_ADDR_LOWER=$(echo "$LOG_ADDR" | tr '[:upper:]' '[:lower:]')
EXPECTED_LOWER=$(echo "$CONTRACT_ADDR" | tr '[:upper:]' '[:lower:]')
if [ "$LOG_ADDR_LOWER" != "$EXPECTED_LOWER" ]; then
  echo "Error: expected log address to be $CONTRACT_ADDR, got $LOG_ADDR" >&2
  exit 1
fi

echo "Address in transaction log matches contract address. Log Address: $LOG_ADDR, Contract Address: $CONTRACT_ADDR"

# ──────────────────────────────────────────────────────────────────────────────
# 3) Check EOA has been set
# ──────────────────────────────────────────────────────────────────────────────

echo "Checking EOA has been set at $CONTRACT_ADDR..."
EOA_ADDR=$(cast wallet address --private-key "$PK_EOA")
RAW_CODE=$(cast rpc eth_getCode "$EOA_ADDR" latest --rpc-url "$RPC_URL")
ONCHAIN_CODE=${RAW_CODE#\"}
ONCHAIN_CODE=${ONCHAIN_CODE%\"}

# A prefix is added to the code to indicate it is a delegated designation
# var DelegatedDesignationPrefix = []byte{0xef, 0x01, 0x00}
EXPECTED_CODE=$(printf '0xef0100%s' "${CONTRACT_ADDR#0x}")

if [[ "$ONCHAIN_CODE" != "$EXPECTED_CODE" ]]; then
  echo "Error: unexpected code at $EOA_ADDR:" >&2
  echo "  got:  $ONCHAIN_CODE" >&2
  echo "  want: $EXPECTED_CODE" >&2
  exit 1
fi

echo "Verified: EOA ($EOA_ADDR) now delegates to $CONTRACT_ADDR."

# ──────────────────────────────────────────────────────────────────────────────
# 4) Check that we can call a contract's function using the EOA
# ─────────────────────────────────────────────────────────────────────────────
echo "Calling setStored(uint256) on the contract using the EOA..."
SET_VALUE=777
SET_VALUE_STATUS=$(cast send "$EOA_ADDR" "setStored(uint256)" $SET_VALUE --rpc-url "$RPC_URL" --private-key "$PK_EOA" --gas-limit 50000 --json | jq -r '.status')

if [[ "$SET_VALUE_STATUS" -ne 1 ]]; then
  echo "Error: setStored(uint256) failed with status $SET_VALUE_RES" >&2
  exit 1
fi

echo "Successfully called setStored(uint256) on the contract using the EOA."

VALUE=$(cast call "$EOA_ADDR" "stored()(uint256)" --rpc-url "$RPC_URL")

if [[ "$VALUE" != "$SET_VALUE" ]]; then
  echo "Error: expected stored() to return $SET_VALUE, got $VALUE" >&2
  exit 1
fi
echo "Successfully called getStored() on the contract using the EOA. Value: $VALUE"

# ──────────────────────────────────────────────────────────────────────────────
# 5) Call the contract's function with event and check the log
# ─────────────────────────────────────────────────────────────────────────────
echo "Calling fallback() on the contract with event..."
EVENT_SIGNATURE=$(cast send "$EOA_ADDR" "fallback()" --rpc-url "$RPC_URL" --private-key "$PK_EOA" --gas-limit 50000 --json | jq -r '.logs[0].topics[0]')

# check that the log has the expected event signature
EXPECTED_EVENT_SIG=$(cast keccak "FallbackCalled(address,uint256,bytes)")
if [[ "$EVENT_SIGNATURE" != "$EXPECTED_EVENT_SIG" ]]; then
  echo "Error: unexpected event signature in log:" >&2
  echo "  got: $EVENT_SIGNATURE" >&2
  echo "  want: $EXPECTED_EVENT_SIG" >&2
  exit 1
fi

echo "Successfully called fallback() on the contract with event. Event signature: $EVENT_SIGNATURE"

echo "All EIP7702 tests passed successfully!"


