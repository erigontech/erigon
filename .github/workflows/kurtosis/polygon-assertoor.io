participants_matrix:
  el:
    - el_type: erigon
      el_image: test/erigon-polygon:current
      el_extra_params:
        - --chain=bor-mainnet
  cl:
    - cl_type: lighthouse
      cl_extra_params:
        - --chain=gnosis
    - cl_type: nimbus
      cl_extra_params:
        - --network=gnosis
network_params:
  network_id: 137 # Polygon network ID
  chain_id: 137
  deposit_contract_address: "0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0" # Polygon's deposit contract
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
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/eoa-transactions-test.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/synchronized-check.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/block-proposal-check.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/stability-check.yaml
