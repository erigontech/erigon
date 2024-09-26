#!/usr/bin/python3

# This is a small utility, to get centralized, but trusted historical rewards data for the beaconchain mainnet.
#
# Used when debugging corner-cases of Caplin's reward RPCs and the correctness of the returned data from Caplin.

import requests
import json
import sys
import pprint

def main(validator_index, epoch):
    resp = requests.request('GET', f'https://beaconcha.in/api/v1/validator/{validator_index}/incomedetailhistory?latest_epoch={epoch}&limit=1',
                            headers = {
                                'accept': 'application/json',
                                'content-type': 'application/json',
                            })
    resp = json.loads(resp.content)
    pprint.pprint(resp)
    if 'proposer_attestation_inclusion_reward' in resp['data'][0]['income']:
        print(f"Proposal    sum: {resp['data'][0]['income']['proposer_attestation_inclusion_reward'] + resp['data'][0]['income']['proposer_sync_inclusion_reward']}")
    print(f"Attestation sum: {resp['data'][0]['income']['attestation_head_reward'] + resp['data'][0]['income']['attestation_source_reward'] + resp['data'][0]['income']['attestation_target_reward']}")

if len(sys.argv) != 3:
    print(f'Usage: {sys.argv[0]} <validator_index> <epoch>')
else:
    main(sys.argv[1], sys.argv[2])
