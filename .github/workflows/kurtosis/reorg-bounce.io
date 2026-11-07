participants:
  - el_type: erigon
    el_image: test/erigon:current
    el_extra_params: ["--prune.mode=archive", "--prune.include-commitment-history"]
    cl_type: lighthouse
    cl_image: sigp/lighthouse:v8.3.2
    count: 2
  - el_type: erigon
    el_image: test/erigon:current
    el_extra_params: ["--prune.mode=archive", "--prune.include-commitment-history"]
    cl_type: teku
    cl_image: consensys/teku:26.10.3
    count: 2
network_params:
  seconds_per_slot: 6
  genesis_delay: 30
  electra_fork_epoch: 0
  fulu_fork_epoch: 0
  gloas_fork_epoch: 10000000
additional_services: [disruptoor]
ethereum_genesis_generator_params:
  image: ethpandaops/ethereum-genesis-generator:6.1.4
  extra_env:
    SLOT_DURATION_IN_SECONDS: 6
disruptoor_params:
  image: ethpandaops/disruptoor:v1.1.1
spamoor_params:
  image: ethpandaops/spamoor:v1.2.4
  spammers:
    - name: storage
      scenario: storagerefundtx
      config: {throughput: 1, slots_per_call: 32}
    - name: transfers
      scenario: eoatx
      config: {throughput: 1}
