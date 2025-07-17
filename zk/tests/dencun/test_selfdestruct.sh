#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 \
  --rpc-url <url> \
  --private-key <hex> \
  --recipient <address> \
  --contract <path:ContractName> \
  [--constructor-args <arg1,arg2,...>]

Options:
  --rpc-url         L2 RPC endpoint URL
  --private-key     Hex-encoded private key (0x...)
  --recipient       Address to receive ETH on self-destruct
  --contract        Fully-qualified contract (e.g., "contracts/SelfDestruct.sol:SelfDestruct")
  --constructor-args Comma-separated constructor arguments (optional)
  -h, --help        Show this help message and exit
EOF
  exit 1
}

declare RPC_URL="" PRIVATE_KEY="" RECIPIENT="" CONTRACT_FQN=""
declare -a CONSTRUCTOR_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    --rpc-url)
      RPC_URL="$2"; shift 2;;
    --private-key)
      PRIVATE_KEY="$2"; shift 2;;
    --recipient)
      RECIPIENT="$2"; shift 2;;
    --contract)
      CONTRACT_FQN="$2"; shift 2;;
    --constructor-args)
      IFS=',' read -r -a CONSTRUCTOR_ARGS <<< "$2"; shift 2;;
    -h|--help)
      usage;;
    *)
      echo "Unknown option: $1" >&2; usage;;
  esac
done

if [[ -z "$RPC_URL" || -z "$PRIVATE_KEY" || -z "$RECIPIENT" || -z "$CONTRACT_FQN" ]]; then
  echo "Error: --rpc-url, --private-key, --recipient and --contract are required." >&2
  usage
fi

wait_for_receipt() {
  local tx_hash="$1"
  while true; do
    local raw
    raw=$(cast rpc eth_getTransactionReceipt "$tx_hash" --rpc-url "$RPC_URL" 2>/dev/null) || {
      echo "Error fetching receipt for $tx_hash" >&2; exit 1
    }
    if [[ "$raw" != "null" ]]; then
      echo "$raw"
      return
    fi
    sleep 1
  done
}

log_balance() {
  local label="$1" addr="$2"
  echo -n "$label: "
  cast balance "$addr" --rpc-url "$RPC_URL"
}

echo "Deploying ${CONTRACT_FQN}..."
if (( ${#CONSTRUCTOR_ARGS[@]} > 0 )); then
  JSON_OUT=$(forge create "$CONTRACT_FQN" "\${CONSTRUCTOR_ARGS[@]}" \
    --rpc-url "$RPC_URL" --private-key "$PRIVATE_KEY" --legacy --broadcast --json --evm-version "london")
else
  JSON_OUT=$(forge create "$CONTRACT_FQN" \
    --rpc-url "$RPC_URL" --private-key "$PRIVATE_KEY" --legacy --broadcast --json --evm-version "london")
fi

TX_HASH=$(echo "$JSON_OUT" | jq -r '.txHash // .transactionHash')
if [[ -z "$TX_HASH" || "$TX_HASH" == "null" ]]; then
  echo "Failed to get txHash from forge output" >&2; exit 1
fi

echo "Deploy tx: $TX_HASH"
RECEIPT=$(wait_for_receipt "$TX_HASH")
CONTRACT_ADDRESS=$(echo "$RECEIPT" | jq -r '.contractAddress')
if [[ -z "$CONTRACT_ADDRESS" || "$CONTRACT_ADDRESS" == "null" ]]; then
  echo "Failed to extract contractAddress" >&2; exit 1
fi

echo "Contract deployed at: $CONTRACT_ADDRESS"

echo -n "Checking code at contract... "
if [[ "$(cast rpc eth_getCode "$CONTRACT_ADDRESS" latest --rpc-url "$RPC_URL")" == "0x" ]]; then
  echo "NOT FOUND"; exit 1
else
  echo "OK"
fi

DEPLOYER=$(cast wallet address --private-key "$PRIVATE_KEY")
echo "--------------STARTING BALANCES--------------"
log_balance "Deployer balance" "$DEPLOYER"
log_balance "Recipient balance" "$RECIPIENT"
log_balance "Contract balance" "$CONTRACT_ADDRESS"

echo "Funding contract with 1 ETH..."
FUND_TX=$(cast send --rpc-url "$RPC_URL" --private-key "$PRIVATE_KEY" --value 1ether --legacy "$CONTRACT_ADDRESS")

echo "--------------BALANCES AFTER FUNDING--------------"
log_balance "Deployer balance" "$DEPLOYER"
log_balance "Recipient balance" "$RECIPIENT"
log_balance "Contract balance" "$CONTRACT_ADDRESS"

echo -n "Verifying contract balance is not 0 before selfdestruct... "
CONTRACT_BALANCE_BEFORE=$(cast to-dec $(cast rpc eth_getBalance --rpc-url "$RPC_URL" "$CONTRACT_ADDRESS" latest | tr -d '"'))
if [[ "$CONTRACT_BALANCE_BEFORE" -gt 0 ]]; then
  echo "OK"
else
  echo "FAILED: $CONTRACT_BALANCE_BEFORE"
  exit 1
fi
RECIPIENT_BALANCE_BEFORE=$(cast to-dec $(cast rpc eth_getBalance --rpc-url "$RPC_URL" "$RECIPIENT" latest | tr -d '"'))

echo "Calling selfdestruct(kill -> $RECIPIENT)..."
CALL_DATA=$(cast abi-encode "kill(address)" "$RECIPIENT")
GAS=$(cast estimate --rpc-url "$RPC_URL" --from "$DEPLOYER" "$CONTRACT_ADDRESS" "kill(address)" "$RECIPIENT")
SELF_TX=$(cast send --rpc-url "$RPC_URL" --private-key "$PRIVATE_KEY" --gas-limit "$GAS" --legacy "$CONTRACT_ADDRESS" "kill(address)" "$RECIPIENT")

echo "--------------BALANCES AFTER SELFDESTRUCT--------------"
log_balance "Deployer balance" "$DEPLOYER"
log_balance "Recipient balance" "$RECIPIENT"
log_balance "Contract balance" "$CONTRACT_ADDRESS"

echo -n "Verifying contract balance is 0 after selfdestruct... "
CONTRACT_BALANCE_AFTER=$(cast to-dec $(cast rpc eth_getBalance --rpc-url "$RPC_URL" "$CONTRACT_ADDRESS" latest | tr -d '"'))
if [[ "$CONTRACT_BALANCE_AFTER" -eq 0 ]]; then
  echo "OK"
else
  echo "FAILED: $CONTRACT_BALANCE_AFTER"; exit 1
fi

echo -n "Verifying recipient balance increased by correct amount... "
RECIPIENT_BALANCE_AFTER=$(cast to-dec $(cast rpc eth_getBalance --rpc-url "$RPC_URL" "$RECIPIENT" latest | tr -d '"'))
EXPECTED_INCREASE=$((RECIPIENT_BALANCE_BEFORE + CONTRACT_BALANCE_BEFORE))
if [[ "$RECIPIENT_BALANCE_AFTER" -eq "$EXPECTED_INCREASE" ]]; then
  echo "OK"
else
  echo "FAILED: expected $EXPECTED_INCREASE, got $RECIPIENT_BALANCE_AFTER"; exit 1
fi

echo -n "Verifying code not removed... "
code=$(cast rpc eth_getCode "$CONTRACT_ADDRESS" latest --rpc-url "$RPC_URL")
if [[ "$code" == "0x" ]]; then
  echo "CODE CLEARED - UNEXPECTED"; exit 1
else
  echo "CODE STILL EXISTS - EXPECTED";
fi

echo "--------------END OF TEST--------------"