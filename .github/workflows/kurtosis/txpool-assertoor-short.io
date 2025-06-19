participants:
- el_type: erigon
  el_image: test/erigon:current
  cl_type: lighthouse
  count: 1
- el_type: erigon
  el_image: test/erigon:current
  cl_type: lodestar
  count: 1
- el_type: erigon
  el_image: test/erigon:current
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
  image: ghcr.io/erigontech/assertoor:latest
  tests:
  - https://raw.githubusercontent.com/erigontech/assertoor/master/playbooks/dev/tx-pool-check-short.yaml
