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
    - file: https://raw.githubusercontent.com/ethpandaops/assertoor/refs/heads/master/playbooks/pectra-dev/execution-spec-tests.yaml
    - file: https://raw.githubusercontent.com/ethpandaops/assertoor/refs/heads/master/playbooks/pectra-dev/kurtosis/all.yaml
    - file: https://raw.githubusercontent.com/ethpandaops/assertoor/refs/heads/master/playbooks/pectra-dev/blockhash-test-with-rpc-call.yaml
    - file: https://raw.githubusercontent.com/ethpandaops/assertoor/refs/heads/master/playbooks/pectra-dev/eip7002-all.yaml
    - file: https://raw.githubusercontent.com/ethpandaops/assertoor/blob/master/playbooks/pectra-dev/eip7251-all.yaml