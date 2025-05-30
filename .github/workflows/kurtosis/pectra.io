participants_matrix:
  el:
    - el_type: erigon
      el_image: test/erigon:current
      el_log_level: "debug"
    - el_type: geth
  cl:
    - cl_type: teku
      cl_image: consensys/teku:develop
    - cl_type: lighthouse
      cl_image: sigp/lighthouse:v7.0.1

network_params:
  electra_fork_epoch: 1
  min_validator_withdrawability_delay: 1
  shard_committee_period: 1
  churn_limit_quotient: 16
  seconds_per_slot: 3
  genesis_delay: 30

additional_services:
  - assertoor
  - dora
snooper_enabled: false
assertoor_params:
  tests:
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/som/sticky_code_cache/.github/workflows/kurtosis/eip7702-test.io

