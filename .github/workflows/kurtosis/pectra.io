participants_matrix:
  el:
    - el_type: erigon
      el_image: test/erigon:current
      el_log_level: "debug"
  cl:
    - cl_type: teku
      cl_image: consensys/teku:26.4.0
    - cl_type: lighthouse
      cl_image: sigp/lighthouse:v7.0.1

network_params:
  num_validator_keys_per_node: 64
  preregistered_validator_keys_mnemonic: &validator_mnemonic "giant issue aisle success illegal bike spike question tent bar rely arctic volcano long crawl hungry vocal artwork sniff fantasy very lucky have athlete"
  electra_fork_epoch: 1
  min_validator_withdrawability_delay: 1
  shard_committee_period: 1
  # Keep consolidation churn above 32 ETH after the 256 ETH activation/exit reserve.
  churn_limit_quotient: 8
  seconds_per_slot: 8
  genesis_delay: 90

additional_services:
  - assertoor
  # - dora
snooper_enabled: false
assertoor_params:
  run_stability_check: true
  run_block_proposal_check: true
  image: ethpandaops/assertoor:v0.0.17
  tests:
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/deposit-request.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/el-triggered-consolidations-test.io
      config:
        validatorMnemonic: *validator_mnemonic
      # Run the epoch-long waits together, using separate validators and wallets.
      schedule: &request_schedule
        startup: true
        skipQueue: true
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/el-triggered-withdrawal.io
      config:
        validatorMnemonic: *validator_mnemonic
      schedule: *request_schedule
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/el-triggered-exit.io
      config:
        validatorMnemonic: *validator_mnemonic
      schedule: *request_schedule
    - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/eip7702-test.io
    # - file: https://raw.githubusercontent.com/erigontech/erigon/refs/heads/main/.github/workflows/kurtosis/eip7702-txpool-invalidation.io
