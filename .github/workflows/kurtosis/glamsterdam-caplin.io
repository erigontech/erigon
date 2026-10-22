participants:
  - cl_type: caplin
    cl_image: test/erigon:current
    cl_log_level: "debug"
    cl_extra_params: ["--sentinel.quic.port=4002"]
    el_type: erigon
    el_image: test/erigon:current
    el_log_level: "debug"
    el_extra_params: ["--experimental.bal", "--miner.extradata=caplin-glamsterdam"]
    use_separate_vc: true
    vc_type: lighthouse
    vc_image: ethpandaops/lighthouse:glamsterdam-devnet-8
    count: 1
global_log_level: 'debug'
network_params:
  preset: minimal
  seconds_per_slot: 6
  genesis_delay: 20
  fulu_fork_epoch: 0
  gloas_fork_epoch: 1
spamoor_params:
  image: ethpandaops/spamoor:v1.2.4
  spammers:
    - scenario: evm-fuzz
      config: {throughput: 50, payload_seed: "0x0200", funding_gas_limit: 200000}
    - scenario: eoatx
      config: {throughput: 50, gas_limit: 200000, funding_gas_limit: 200000}
    - scenario: deploytx
      config: {throughput: 10, bytecodes: "0x6000", gas_limit: 10000000, funding_gas_limit: 200000}
    - scenario: storagerefundtx
      config: {throughput: 20, slots_per_call: 2, funding_gas_limit: 200000}
    - scenario: setcodetx
      config: {throughput: 20, funding_gas_limit: 200000}
ethereum_genesis_generator_params:
  image: ethpandaops/ethereum-genesis-generator:6.2.0
additional_services: [spamoor, assertoor]
assertoor_params:
  image: ethpandaops/assertoor:v0.1.3
  run_stability_check: false
  run_block_proposal_check: true
  tests:
    - file: https://raw.githubusercontent.com/erigontech/erigon/main/.github/workflows/kurtosis/caplin-glamsterdam-block-production.io
    - file: /tests/stability-check.yaml
