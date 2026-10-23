participants_matrix:
  el:
    - el_type: erigon
      el_image: test/erigon:current
      el_log_level: "debug"
  cl:
    - cl_type: teku
      cl_image: consensys/teku:26.4.0
      cl_log_level: "debug"
      count: 2
    - cl_type: lighthouse
      cl_image: ethpandaops/lighthouse:stable-a376def
      cl_log_level: "debug"
      count: 2

network_params:
  preset: minimal
  num_validator_keys_per_node: 32
  electra_fork_epoch: 1
  fulu_fork_epoch: 100000
  min_validator_withdrawability_delay: 1
  shard_committee_period: 1
  churn_limit_quotient: 16
  seconds_per_slot: 6
  genesis_delay: 90

additional_services:
  - assertoor
  # - dora
snooper_enabled: false
assertoor_params:
  run_stability_check: true
  run_block_proposal_check: true
  image: ethpandaops/assertoor:v0.1.3
  tests:
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/deposit-request.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/el-triggered-consolidations-test.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/el-triggered-withdrawal.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/el-triggered-exit.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/eip7702-test.io
    # - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/eip7702-txpool-invalidation.io
ethereum_genesis_generator_params:
  image: ethpandaops/ethereum-genesis-generator:6.1.4
