#!/usr/bin/env bash
# Deploys a second ECDSA proof system for SIGNER_KEY, keyed by its address as
# deploy.sh keys the first, and requires both.
set -euo pipefail
set -a
. /out/deployments.env
set +a
rpc=(--rpc-url "$EEZ_L1_RPC_URL")
signer=$(cast wallet address --private-key "$SIGNER_KEY")
out=$(cd /repo/contracts && forge script script/DeployECDSAProofSystem.s.sol:DeployECDSAProofSystem \
	--sig 'run(address)' "$signer" "${rpc[@]}" --private-key "$EEZ_DEPLOY_KEY" --broadcast 2>&1) \
	|| { echo "$out"; exit 1; }
ps=$(grep -oE 'ECDSA_PS=0x[0-9a-fA-F]{40}' <<< "$out" | tail -1 | cut -d= -f2)
[ -n "$ps" ] || { echo "$out"; exit 1; }
vkey=0x000000000000000000000000${signer#0x}
cast send "${rpc[@]}" --private-key "$EEZ_DEPLOY_KEY" "$EEZ_ROLLUP_MANAGER_ADDRESS" \
	'addProofSystem(address,bytes32)' "$ps" "$vkey" > /dev/null
cast send "${rpc[@]}" --private-key "$EEZ_DEPLOY_KEY" "$EEZ_ROLLUP_MANAGER_ADDRESS" 'setThreshold(uint256)' 2 > /dev/null
[ "$(cast call "${rpc[@]}" "$EEZ_ROLLUP_MANAGER_ADDRESS" 'threshold()(uint256)')" = 2 ]
[ "$(cast call "${rpc[@]}" "$EEZ_ROLLUP_MANAGER_ADDRESS" 'verificationKey(address)(bytes32)' "$ps")" = "${vkey,,}" ]
mkdir -p /quorum
printf 'EEZ_PROOF_SYSTEM=%s\nEEZ_VKEY=%s\nEEZ_ATTESTER_ADDRESS=%s\n' "$ps" "$vkey" "$signer" | tee /quorum/quorum.env
