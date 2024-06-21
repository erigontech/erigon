#!/bin/bash
#
# Call the LxLy bridge and do claims / deposits
# ./test.sh claim
# ./test.sh deposit
# Error code reference https://hackmd.io/WwahVBZERJKdfK3BbKxzQQ
#
# Examples
# env DRY_RUN=false ./test.sh deposit

set -e

readonly skey=${RAW_PRIVATE_KEY:-"bc6a95c870cce28fe9686fdd70a3a595769e019bb396cd725bc16ec553f07c83"}
readonly destination_net=${DESTINATION_NET:-"1"}
readonly destination_addr=${DESTINATION_ADDRESS:-"0x0bb7AA0b4FdC2D2862c088424260e99ed6299148"}
readonly ether_value=${ETHER_VALUE:-"0.0200000054"}
readonly token_addr=${TOKEN_ADDRESS:-"0x0000000000000000000000000000000000000000"}
readonly is_forced=${IS_FORCED:-"true"}
readonly bridge_addr=${BRIDGE_ADDRESS:-"0x528e26b25a34a4A5d0dbDa1d57D318153d2ED582"}
readonly meta_bytes=${META_BYTES:-"0x"}
readonly subcommand=${1:-"deposit"}

readonly rpc_url=${ETH_RPC_URL:-"https://rpc.cardona.zkevm-rpc.com"}
readonly bridge_api_url=${BRIDGE_API_URL:-"https://bridge-api-cdk-validium-cardona-03-zkevm.polygondev.tools"}

readonly dry_run=${DRY_RUN:-"true"}
readonly claim_sig="claimAsset(bytes32[32],bytes32[32],uint256,bytes32,bytes32,uint32,address,uint32,address,uint256,bytes)"
readonly bridge_sig='bridgeAsset(uint32,address,uint256,address,bool,bytes)'

readonly amount=$(cast to-wei $ether_value ether)
readonly current_addr="$(cast wallet address --private-key $skey)"
readonly rpc_network_id=$(cast call --rpc-url $rpc_url $bridge_addr 'networkID()(uint32)')


2>&1 echo "Running LxLy " $subcommand

2>&1 echo "Checking the current network id: "
2>&1 echo $rpc_network_id

2>&1 echo "The current private key has address: "
2>&1 echo $current_addr

if [[ $token_addr == "0x0000000000000000000000000000000000000000" ]]; then
    2>&1 echo "Checking the current ETH balance: "
    2>&1 cast balance -e --rpc-url $rpc_url $current_addr || exit 1
else
    2>&1 echo "Checking the current token balance for token at $token_addr: "
    2>&1 cast call --rpc-url $rpc_url $token_addr 'balanceOf(address)(uint256)' $current_addr || exit 1
fi

function deposit () {
    2>&1 echo "Attempting to deposit $amount wei to net $destination_net for token $token_addr"

    if [[ $dry_run == "true" ]]; then
        cast calldata $bridge_sig $destination_net $destination_addr $amount $token_addr $is_forced $meta_bytes || exit 1
    else
        if [[ $token_addr == "0x0000000000000000000000000000000000000000" ]]; then
            set -x
            cast send --legacy --private-key $skey --value $amount --rpc-url $rpc_url $bridge_addr $bridge_sig $destination_net $destination_addr $amount $token_addr $is_forced $meta_bytes || exit 1
            set +x
        else
            set -x
            cast send --legacy --private-key $skey --rpc-url $rpc_url $bridge_addr $bridge_sig $destination_net $destination_addr $amount $token_addr $is_forced $meta_bytes || exit 1
            set +x
        fi
    fi
}

function claim() {
    readonly bridge_deposit_file=$(mktemp)
    readonly claimable_deposit_file=$(mktemp)
    2>&1 echo "Getting full list of deposits"
    set -x
    2>&1 curl -s "$bridge_api_url/bridges/$destination_addr?limit=100&offset=0" | jq '.' | tee $bridge_deposit_file || exit 1
    set +x
    2>&1 echo "Looking for claimable deposits"
    2>&1 jq '[.deposits[] | select(.ready_for_claim == true and .claim_tx_hash == "" and .dest_net == '$destination_net')]' $bridge_deposit_file | tee $claimable_deposit_file || exit 1
    readonly claimable_count=$(jq '. | length' $claimable_deposit_file)
    if [[ $claimable_count == 0 ]]; then
        2>&1 echo "We have no claimable deposits at this time"
        exit 0
    fi
    if [[ $rpc_network_id != $destination_net ]]; then
        2>&1 echo "The bridge on the current rpc has network id $rpc_network_id but you are claiming a transaction on network $destination_net - are you sure you're using the right RPC??"
        exit 1
    fi
    2>&1 echo "We have $claimable_count claimable deposits on network $destination_net. Let's get this party started."
    readonly current_deposit=$(mktemp)
    readonly current_proof=$(mktemp)
    while read deposit_idx; do
        2>&1 echo "Starting claim for tx index: "$deposit_idx
        2>&1 echo "Deposit info:"
        2>&1 jq --arg idx $deposit_idx '.[($idx | tonumber)]' $claimable_deposit_file | tee $current_deposit || exit 1

        curr_deposit_cnt=$(jq -r '.deposit_cnt' $current_deposit)
        curr_network_id=$(jq -r '.network_id' $current_deposit)
        2>&1 echo "Proof:"
        set -x
        2>&1 curl -s "$bridge_api_url/merkle-proof?deposit_cnt=$curr_deposit_cnt&net_id=$curr_network_id" | jq '.' | tee $current_proof || exit 1
        set  +x

        in_merkle_proof="$(jq -r -c '.proof.merkle_proof' $current_proof | tr -d '"')"
        in_rollup_merkle_proof="$(jq -r -c '.proof.rollup_merkle_proof' $current_proof | tr -d '"')"
        in_global_index=$(jq -r '.global_index' $current_deposit)
        in_main_exit_root=$(jq -r '.proof.main_exit_root' $current_proof)
        in_rollup_exit_root=$(jq -r '.proof.rollup_exit_root' $current_proof)
        in_orig_net=$(jq -r '.orig_net' $current_deposit)
        in_orig_addr=$(jq -r '.orig_addr' $current_deposit)
        in_dest_net=$(jq -r '.dest_net' $current_deposit)
        in_dest_addr=$(jq -r '.dest_addr' $current_deposit)
        in_amount=$(jq -r '.amount' $current_deposit)
        in_metadata=$(jq -r '.metadata' $current_deposit)

        if [[ $dry_run == "true" ]]; then
            cast calldata $claim_sig "$in_merkle_proof" "$in_rollup_merkle_proof" $in_global_index $in_main_exit_root $in_rollup_exit_root $in_orig_net $in_orig_addr $in_dest_net $in_dest_addr $in_amount $in_metadata || exit 1
            set -x
            cast call --rpc-url $rpc_url $bridge_addr $claim_sig "$in_merkle_proof" "$in_rollup_merkle_proof" $in_global_index $in_main_exit_root $in_rollup_exit_root $in_orig_net $in_orig_addr $in_dest_net $in_dest_addr $in_amount $in_metadata || exit 1
            set +x
        else
            set -x
            cast send --legacy --rpc-url $rpc_url --private-key $skey $bridge_addr $claim_sig "$in_merkle_proof" "$in_rollup_merkle_proof" $in_global_index $in_main_exit_root $in_rollup_exit_root $in_orig_net $in_orig_addr $in_dest_net $in_dest_addr $in_amount $in_metadata || exit 1
            set +x
        fi
    done < <(seq 0 $((claimable_count - 1)) )
}

if [[ $subcommand == "claim" ]]; then
    claim
fi

if [[ $subcommand == "deposit" ]]; then
    deposit
fi

2>&1 echo "done"
