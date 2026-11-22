#!/usr/bin/env bash
# Deploys a second ECDSA proof system for SIGNER_KEY, registers it under the
# signer's address as its verification key, and requires both proof systems.
set -euo pipefail
set -a
# shellcheck source=/dev/null
. /out/deployments.env
set +a
rpc=(--rpc-url "$EEZ_L1_RPC_URL")
signer=$(cast wallet address --private-key "$SIGNER_KEY")
out=$(cd /repo/contracts && forge script script/DeployECDSAProofSystem.s.sol:DeployECDSAProofSystem \
	--sig 'run(address)' "$signer" "${rpc[@]}" --private-key "$EEZ_DEPLOY_KEY" --broadcast 2>&1) \
	|| { echo "$out"; exit 1; }
ps=$(grep -oE 'ECDSA_PS=0x[0-9a-fA-F]{40}' <<< "$out" | tail -1 | cut -d= -f2)
[ -n "$ps" ] || { echo "$out"; echo "no ECDSA_PS address in the deployment output" >&2; exit 1; }
vkey=0x000000000000000000000000${signer#0x}
cast send "${rpc[@]}" --private-key "$EEZ_DEPLOY_KEY" "$EEZ_ROLLUP_MANAGER_ADDRESS" \
	'addProofSystem(address,bytes32)' "$ps" "$vkey" > /dev/null
cast send "${rpc[@]}" --private-key "$EEZ_DEPLOY_KEY" "$EEZ_ROLLUP_MANAGER_ADDRESS" 'setThreshold(uint256)' 2 > /dev/null
threshold=$(cast call "${rpc[@]}" "$EEZ_ROLLUP_MANAGER_ADDRESS" 'threshold()(uint256)')
[ "$threshold" = 2 ] || { echo "manager threshold $threshold, not 2" >&2; exit 1; }
key=$(cast call "${rpc[@]}" "$EEZ_ROLLUP_MANAGER_ADDRESS" 'verificationKey(address)(bytes32)' "$ps")
[ "$key" = "$(tr '[:upper:]' '[:lower:]' <<< "$vkey")" ] || { echo "verification key of $ps is $key, not $vkey" >&2; exit 1; }
mkdir -p /quorum
printf 'EEZ_PROOF_SYSTEM=%s\nEEZ_VKEY=%s\nEEZ_ATTESTER_ADDRESS=%s\n' "$ps" "$vkey" "$signer" | tee /quorum/quorum.env
