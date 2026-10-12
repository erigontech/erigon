id: caplin-glamsterdam-block-production
name: "Caplin builds Glamsterdam blocks and execution payloads"
timeout: 10m
tasks:
  - name: check_clients_are_healthy
    title: "Wait for Caplin and Erigon"
    timeout: 2m
    config:
      minClientCount: 1

  - name: get_consensus_specs
    id: specs
    title: "Read the Glamsterdam fork epoch"

  - name: check_consensus_slot_range
    title: "Wait for the Glamsterdam fork"
    timeout: 3m
    configVars:
      minEpochNumber: ".tasks.specs.outputs.specs.GLOAS_FORK_EPOCH | tonumber"

  - name: check_consensus_block_proposals
    title: "Wait for three new Caplin blocks with execution payloads"
    timeout: 3m
    config:
      checkLookback: 0
      blockCount: 3
      validatorNamePattern: "caplin"
      extraDataPattern: "^caplin-glamsterdam$"

  - name: check_execution_sync_status
    title: "Check Erigon imports blocks after Glamsterdam"
    timeout: 2m
    config:
      waitForChainProgression: true
    configVars:
      minBlockHeight: ".tasks.specs.outputs.specs | (.GLOAS_FORK_EPOCH | tonumber) * (.SLOTS_PER_EPOCH | tonumber) + 1"
