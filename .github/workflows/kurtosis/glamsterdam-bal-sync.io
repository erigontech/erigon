# BAL download flow — 2-node sync test (see docs/plans/bal-downloader/testing.md).
#
# Node A (geth + lighthouse): the progressor and BAL-serving peer. Geth retains
# BALs and Lighthouse serves full, BAL-bearing execution-payload envelopes over
# CL p2p — a known-good source that sidesteps Erigon-Caplin's own envelope
# serving/retention gaps (out of scope for this change). Holds the validators.
#
# Node B (erigon + internal Caplin): the node under test. It joins after the
# chain has produced ~1000 blocks and must catch up by downloading the envelopes
# from Node A's Lighthouse and extracting each block's BAL into the Erigon EL.
# The staggered "join after ~1000 blocks" is an operational step (run the
# progressor first, then bring Node B up) — see testing.md. seconds_per_slot is
# 6 for fast local runs; bump to 8, then 12 (mainnet) if peering/sync is flaky.
participants:
  - el_type: geth
    el_image: ethpandaops/geth:glamsterdam-devnet-4
    el_log_level: "info"
    cl_type: lighthouse
    cl_image: ethpandaops/lighthouse:glamsterdam-devnet-4
    cl_log_level: "info"
    supernode: true
    count: 2
  - el_type: erigon
    el_image: test/erigon:current
    el_log_level: "debug"
    el_extra_params: ["--experimental.bal"]
    cl_type: caplin
    cl_image: test/erigon:current
    cl_log_level: "debug"
    cl_extra_params: ["--local-discovery", "--caplin.subscribe-all-topics"]
    validator_count: 0
    count: 1
global_log_level: 'info'
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
