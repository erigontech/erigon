participants:
  - cl_type: lighthouse
    cl_image: ethpandaops/lighthouse:glamsterdam-devnet-5
    el_type: erigon
    el_image: test/erigon:current
    el_log_level: "debug"
    el_extra_params: ["--experimental.bal"]
    supernode: true
    count: 1
  - cl_type: prysm
    cl_image: ethpandaops/prysm-beacon-chain:glamsterdam-devnet-5-tmp-minimal
    el_type: erigon
    el_image: test/erigon:current
    el_log_level: "debug"
    el_extra_params: ["--experimental.bal"]
    use_separate_vc: true
    vc_type: prysm
    vc_image: ethpandaops/prysm-validator:glamsterdam-devnet-5-tmp-minimal
    count: 1
  - cl_type: caplin
    cl_image: test/erigon:current
    cl_log_level: "debug"
    cl_extra_params: ["--local-discovery", "--caplin.subscribe-all-topics"]
    el_type: erigon
    el_image: test/erigon:current
    el_log_level: "debug"
    el_extra_params: ["--experimental.bal"]
    use_separate_vc: true
    vc_type: lighthouse
    vc_image: ethpandaops/lighthouse:glamsterdam-devnet-5
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
  run_stability_check: false
  run_block_proposal_check: true
  image: ethpandaops/assertoor:master
