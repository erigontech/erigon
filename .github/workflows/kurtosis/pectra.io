participants_matrix:
  el:
    - el_type: erigon
      el_image: test/erigon:current
  cl:
    - cl_type: teku
      cl_image: consensys/teku:develop
    - cl_type: lodestar
      cl_image: ethpandaops/lodestar:devnet-5-1c2b5ed
network_params:
  electra_fork_epoch: 1
  min_validator_withdrawability_delay: 1
  shard_committee_period: 1
  churn_limit_quotient: 16
additional_services:
  - assertoor
snooper_enabled: true
assertoor_params:
  run_stability_check: false
  run_block_proposal_check: true
  tests:
    - file: https://raw.githubusercontent.com/erigontech/erigon/9c41963505391d94fe6f51451e9f0ec1194c9b61/.github/workflows/kurtosis/el-triggered-consolidation-of-consolidations.io
    - file: https://raw.githubusercontent.com/erigontech/erigon/852de1e594923343ced91172754d75fba0223e93/.github/workflows/kurtosis/el-triggered-consolidations-test.io
    - file: https://raw.githubusercontent.com/ethpandaops/assertoor/refs/heads/master/playbooks/pectra-dev/kurtosis/el-triggered-exit.yaml
    - file: https://raw.githubusercontent.com/erigontech/erigon/3ecef2dce1f3a99f4f7fc6b6691f6450e56b8e04/.github/workflows/kurtosis/el-triggered-withdrawal.io
    - file: https://raw.githubusercontent.com/ethpandaops/assertoor/refs/heads/master/playbooks/pectra-dev/kurtosis/eip7702-test.yaml
    - file: https://raw.githubusercontent.com/ethpandaops/assertoor/refs/heads/master/playbooks/pectra-dev/kurtosis/eip7702-txpool-invalidation.yaml
