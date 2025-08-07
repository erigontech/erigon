participants:
- el_type: erigon
  el_image: test/erigon:current
  el_extra_params:
    - "--txpool.globalqueue 2000000"
    - "--txpool.globalslots 2000000"
    - "--txpool.globalbasefeeslots 2000000"
  cl_type: lighthouse
  count: 1
- el_type: erigon
  el_image: test/erigon:current
  el_extra_params:
    - "--txpool.globalqueue 2000000"
    - "--txpool.globalslots 2000000"
    - "--txpool.globalbasefeeslots 2000000"
  cl_type: lodestar
  count: 1
- el_type: erigon
  el_image: test/erigon:current
  el_extra_params:
    - "--txpool.globalqueue 2000000"
    - "--txpool.globalslots 2000000"
    - "--txpool.globalbasefeeslots 2000000"
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
  - https://raw.githubusercontent.com/erigontech/assertoor/master/playbooks/dev/tx-pool-check.yaml
