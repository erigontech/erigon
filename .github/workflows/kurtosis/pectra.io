participants_matrix:
  el:
    - el_type: erigon
      el_image: test/erigon:current
      el_log_level: "debug"
  cl:
    - cl_type: teku
      cl_image: consensys/teku:25.7
    - cl_type: lighthouse
      cl_image: sigp/lighthouse:v7.0.1

network_params:
  electra_fork_epoch: 1
  min_validator_withdrawability_delay: 1
  shard_committee_period: 1
  churn_limit_quotient: 16
  seconds_per_slot: 4
  genesis_delay: 90

additional_services:
  - assertoor
  # - dora
snooper_enabled: false
assertoor_params:
  run_stability_check: true
  run_block_proposal_check: true
  tests:
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/deposit-request.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/el-triggered-consolidations-test.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/el-triggered-withdrawal.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/el-triggered-exit.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/eip7702-test.io
    # - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/eip7702-txpool-invalidation.io
