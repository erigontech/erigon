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
      config: {throughput: 15, payload_seed: "0x0070"}
    - scenario: eoatx
      config: {throughput: 25}
ethereum_genesis_generator_params:
  image: ethpandaops/ethereum-genesis-generator:5.3.1
additional_services: [spamoor, assertoor]
assertoor_params:
  run_stability_check: true
  run_block_proposal_check: true
  image: ethpandaops/assertoor:v0.0.17
