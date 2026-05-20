participants:
  - cl_type: caplin
    cl_image: test/erigon:glamsterdam-caplin
    cl_log_level: "debug"
    el_type: erigon
    el_image: test/erigon:glamsterdam-caplin
    el_log_level: "debug"
    el_extra_params: ["--experimental.bal"]
    use_separate_vc: true
    vc_type: lighthouse
    vc_image: ethpandaops/lighthouse:glamsterdam-devnet-4
    count: 1
global_log_level: 'debug'
network_params:
  preset: minimal
  seconds_per_slot: 6
  genesis_delay: 20
  fulu_fork_epoch: 0
  gloas_fork_epoch: 1
ethereum_genesis_generator_params:
  image: ethpandaops/ethereum-genesis-generator:5.3.5
additional_services: [assertoor]
assertoor_params:
  image: ethpandaops/assertoor:master
  run_stability_check: false
  run_block_proposal_check: true
