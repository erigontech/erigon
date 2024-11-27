participants_matrix:
  el:
    - el_type: erigon
      el_image: test/erigon:current
  cl:
    - cl_type: lighthouse
      cl_image: ethpandaops/lighthouse:pawan-electra-alpha7-0dd215c
    - cl_type: teku
      cl_image: ethpandaops/teku:mekong
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
    - { file: "https://raw.githubusercontent.com/ethpandaops/assertoor/refs/heads/electra-support/playbooks/pectra-dev/kurtosis/all.yaml"}

