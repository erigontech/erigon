participants:
  - cl_type: lighthouse
    cl_image: ethpandaops/lighthouse:bal-devnet-3
    el_type: geth
    el_image: ethpandaops/geth:bal-devnet-3
    el_extra_params: ["--history.state=0", "--gcmode=archive", "--syncmode=full"]
    supernode: true
  - cl_type: lighthouse
    cl_image: ethpandaops/lighthouse:bal-devnet-3
    el_type: erigon
    el_image: test/erigon:current
    el_log_level: "debug"
    supernode: true
global_log_level: debug
network_params:
  preset: minimal
  seconds_per_slot: 6
  genesis_delay: 30
  fulu_fork_epoch: 0
  gloas_fork_epoch: 2
snooper_enabled: true
dora_params:
  image: ethpandaops/dora:eip7928-support
spamoor_params:
  image: ethpandaops/spamoor:master
  spammers:
    - scenario: eoatx
      config: {throughput: 15, funding_gas_limit: 2000000}
    - scenario: evm-fuzz
      config: {throughput: 8, funding_gas_limit: 2000000}
    - scenario: deploytx
      config: {throughput: 1, bytecodes: "0x61780080600b6000396000f3", gas_limit: 20000000, funding_gas_limit: 2000000}
ethereum_genesis_generator_params:
  image: ethpandaops/ethereum-genesis-generator:5.3.5
additional_services: [dora, spamoor]
