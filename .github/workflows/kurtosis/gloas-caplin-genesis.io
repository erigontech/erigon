participants:
  - cl_type: caplin
    cl_image: test/erigon:current
    cl_log_level: "debug"
    cl_extra_params: ["--local-discovery", "--caplin.subscribe-all-topics", "--beacon.api=beacon,validator,node,config,debug"]
    el_type: erigon
    el_image: test/erigon:current
    el_log_level: "debug"
    el_extra_params: ["--experimental.bal"]
    use_separate_vc: true
    vc_type: lighthouse
    vc_image: ethpandaops/lighthouse:glamsterdam-devnet-8
    count: 1
global_log_level: "debug"
network_params:
  preset: mainnet
  seconds_per_slot: 6
  genesis_delay: 20
  altair_fork_epoch: 0
  bellatrix_fork_epoch: 0
  capella_fork_epoch: 0
  deneb_fork_epoch: 0
  electra_fork_epoch: 0
  fulu_fork_epoch: 0
  gloas_fork_epoch: 0
ethereum_genesis_generator_params:
  image: ethpandaops/ethereum-genesis-generator:6.2.0
additional_services: [assertoor]
assertoor_params:
  run_stability_check: false
  run_block_proposal_check: true
  image: ethpandaops/assertoor:v0.1.3
