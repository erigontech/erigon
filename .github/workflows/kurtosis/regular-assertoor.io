participants_matrix:
  el:
    - el_type: erigon
      el_image: test/erigon:current
  cl:
    - cl_type: lighthouse
      cl_image: sigp/lighthouse:v6.0.0
    - cl_type: nimbus
      cl_image: statusim/nimbus-eth2:multiarch-v24.11.0
network_params:
  #electra_fork_epoch: 1
  min_validator_withdrawability_delay: 1
  shard_committee_period: 1
additional_services:
  - assertoor
assertoor_params:
  run_stability_check: false
  run_block_proposal_check: true
  tests:
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/all-opcodes-test.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/blob-transactions-test.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/dencun-opcodes-test.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/eoa-transactions-test.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/synchronized-check.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/validator-exit-test.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/block-proposal-check.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/stability-check.yaml
