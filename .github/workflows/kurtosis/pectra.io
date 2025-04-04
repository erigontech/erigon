participants_matrix:
  el:
    - el_type: erigon
      el_image: test/erigon:current
  cl:
    - cl_type: teku
      cl_image: consensys/teku:develop
    - cl_type: lodestar
      cl_image: chainsafe/lodestar:v1.28.1

network_params:
  electra_fork_epoch: 1
  min_validator_withdrawability_delay: 1
  shard_committee_period: 1
  churn_limit_quotient: 16
  seconds_per_slot: 1
additional_services:
  - assertoor
snooper_enabled: false
assertoor_params:
  run_stability_check: false
  run_block_proposal_check: false
  tests:
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/som/kurtosis_lifecycle/.github/workflows/kurtosis/deposit-request.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/som/kurtosis_lifecycle/.github/workflows/kurtosis/el-triggered-consolidations-test.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/som/kurtosis_lifecycle/.github/workflows/kurtosis/el-triggered-withdrawal.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/som/kurtosis_lifecycle/.github/workflows/kurtosis/el-triggered-exit.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/som/kurtosis_lifecycle/.github/workflows/kurtosis/eip7702-test.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/som/kurtosis_lifecycle/.github/workflows/kurtosis/eip7702-txpool-invalidation.io
