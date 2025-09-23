participants_matrix:
  el:
    - el_type: geth
      el_image: ethpandaops/geth:fusaka-devnet-0
    - el_type: erigon
      el_image: test/erigon:current
      el_log_level: "debug"
  cl:
    - cl_type: teku
      cl_image: ethpandaops/teku:das
      supernode: true

network_params:
  fulu_fork_epoch: 1
  bpo_1_epoch: 1
  bpo_1_max_blobs: 12
  bpo_1_target_blobs: 9
  bpo_2_epoch: 2
  bpo_2_max_blobs: 6
  bpo_2_target_blobs: 4
  bpo_3_epoch: 3
  bpo_3_max_blobs: 9
  bpo_3_target_blobs: 6
  bpo_4_epoch: 4
  bpo_4_max_blobs: 18
  bpo_4_target_blobs: 12
  bpo_5_epoch: 5
  bpo_5_max_blobs: 9
  bpo_5_target_blobs: 6
  withdrawal_type: "0x01"
  genesis_delay: 30
  seconds_per_slot: 3

snooper_enabled: true

additional_services:
  - dora
  - spamoor

spamoor_params:
  image: ethpandaops/spamoor:master
  max_mem: 4000
  spammers:
    - scenario: eoatx
      config:
        throughput: 200
    - scenario: blobs
      config:
        throughput: 20
