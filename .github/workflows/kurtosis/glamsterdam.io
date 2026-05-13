participants:
  - cl_type: lighthouse
    cl_image: ethpandaops/lighthouse:bal-devnet-3
    el_type: erigon
    el_image: test/erigon:current
    el_log_level: "debug"
    el_extra_params: ["--experimental.bal"]
    supernode: true
    count: 3
global_log_level: 'debug'
network_params:
  preset: minimal
  seconds_per_slot: 6
  genesis_delay: 20
  fulu_fork_epoch: 0
  gloas_fork_epoch: 1
snooper_enabled: true
spamoor_params:
  image: ethpandaops/spamoor:master
  spammers:
    - scenario: evm-fuzz
      config: {throughput: 50, payload_seed: "0x0200", funding_gas_limit: 200000}
    - scenario: eoatx
      config: {throughput: 50, gas_limit: 200000, funding_gas_limit: 200000}
    - scenario: deploytx
      config: {throughput: 10, bytecodes: "0x6000", gas_limit: 10000000, funding_gas_limit: 200000}
    - scenario: storagerefundtx
      config: {throughput: 20, slots_per_call: 500, funding_gas_limit: 200000}
    - scenario: setcodetx
      config: {throughput: 20, funding_gas_limit: 200000}
ethereum_genesis_generator_params:
  image: ethpandaops/ethereum-genesis-generator:5.3.5
additional_services: [spamoor, assertoor]
assertoor_params:
  run_stability_check: true
  run_block_proposal_check: true
  image: ethpandaops/assertoor:qu0b-gloas-bals-v2
