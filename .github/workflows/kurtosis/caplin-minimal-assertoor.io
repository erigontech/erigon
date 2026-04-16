participants:
  - el_type: erigon
    el_image: test/erigon:current
    el_log_level: "debug"
    cl_type: caplin
    cl_image: test/erigon:current
    cl_log_level: "debug"
    use_separate_vc: true
    vc_type: lighthouse
    vc_image: sigp/lighthouse:v7.0.1
network_params:
  preset: "minimal"
  deneb_fork_epoch: 0
additional_services:
  - assertoor
assertoor_params:
  run_stability_check: true
  run_block_proposal_check: true
  image: ethpandaops/assertoor:v0.0.17
  tests:
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/synchronized-check.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/block-proposal-check.yaml
    # eoa-transactions-test omitted: the upstream test uses 100 child wallets
    # per phase, which is too aggressive for a single-node minimal-preset
    # environment. The legacy phase drains child wallet balances, causing the
    # subsequent dynfee phase to fail with "insufficient funds" (maxFeePerGas *
    # gasLimit exceeds remaining balance). EOA transactions are already covered
    # by the regular assertoor suite.
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/stability-check.yaml
