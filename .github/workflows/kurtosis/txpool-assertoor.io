participants:
- el_type: erigon
  el_image: erigontech/erigon:latest
  cl_type: lighthouse
  count: 1
- el_type: erigon
  el_image: erigontech/erigon:latest
  cl_type: lodestar
  count: 1
- el_type: erigon
  el_image: erigontech/erigon:latest
  cl_type: prysm
  count: 1
network_params:
  #electra_fork_epoch: 1
  min_validator_withdrawability_delay: 1
  shard_committee_period: 1
additional_services:
- assertoor
assertoor_params:
  run_stability_check: false
  run_block_proposal_check: true
  image: ghcr.io/noku-team/assertoor:latest
  tests:
  - https://raw.githubusercontent.com/noku-team/assertoor/master/playbooks/dev/tx-pool-check.yaml
