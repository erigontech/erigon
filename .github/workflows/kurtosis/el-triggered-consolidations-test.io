id: el-triggered-consolidation
name: "EL-triggered consolidation test"
timeout: 45m
config:
  walletPrivkey: ""
  validatorMnemonic: ""
  sourceValidatorIndex: 22
  targetValidatorIndex: 23

tasks:
- name: check_clients_are_healthy
  title: "Wait for a ready client"
  timeout: 5m
  config:
    minClientCount: 1

- name: get_consensus_specs
  id: specs
  title: "Get fork and validator-age requirements"

- name: check_consensus_slot_range
  title: "Wait for Electra and eligible genesis validators"
  timeout: 10m
  configVars:
    minEpochNumber: "| [.tasks.specs.outputs.specs.ELECTRA_FORK_EPOCH, .tasks.specs.outputs.specs.SHARD_COMMITTEE_PERIOD] | map(tonumber) | max"

- name: generate_child_wallet
  id: wallet
  title: "Fund the consolidation sender"
  config:
    walletSeed: "pectra-consolidation"
    prefundMinBalance: 2000000000000000000
  configVars:
    privateKey: "walletPrivkey"

- name: run_task_matrix
  title: "Set source and target withdrawal addresses"
  configVars:
    matrixValues: "| [.sourceValidatorIndex, .targetValidatorIndex]"
  config:
    matrixVar: "validatorIndex"
    runConcurrent: true
    task:
      name: run_tasks
      title: "Prepare validator ${validatorIndex}"
      config:
        tasks:
        - name: check_consensus_validator_status
          title: "Require an active genesis validator"
          timeout: 2m
          config:
            validatorStatus: [active_ongoing]
            withdrawalCredsPrefix: "0x00"
            minValidatorBalance: 32000000000
            validatorPubKeyResultVar: "validatorPubKey"
          configVars:
            validatorIndex: "validatorIndex"
        - name: generate_bls_changes
          title: "Authorize the consolidation sender"
          config:
            limitTotal: 1
            indexCount: 1
          configVars:
            mnemonic: "validatorMnemonic"
            startIndex: "validatorIndex"
            targetAddress: "tasks.wallet.outputs.childWallet.address"
        - name: check_consensus_block_proposals
          title: "Wait for the authorized BLS change in a beacon block"
          timeout: 3m
          config:
            checkLookback: 8
            minBlsChangeCount: 1
          configVars:
            expectBlsChanges: "| [{publicKey: .validatorPubKey, address: .tasks.wallet.outputs.childWallet.address}]"

- name: generate_consolidations
  title: "Switch the target to compounding credentials"
  config:
    limitTotal: 1
    sourceIndexCount: 1
    failOnReject: true
    awaitReceipt: true
  configVars:
    walletPrivkey: "tasks.wallet.outputs.childWallet.privkey"
    sourceStartValidatorIndex: "targetValidatorIndex"
    targetValidatorIndex: "targetValidatorIndex"

- name: check_consensus_validator_status
  title: "Wait for the compounding target"
  timeout: 10m
  config:
    validatorStatus: [active_ongoing]
  configVars:
    validatorIndex: "targetValidatorIndex"
    withdrawalCredsPrefix: "| \"0x020000000000000000000000\" + (.tasks.wallet.outputs.childWallet.address | ltrimstr(\"0x\"))"

- name: generate_consolidations
  title: "Consolidate validator ${sourceValidatorIndex} into ${targetValidatorIndex}"
  config:
    limitTotal: 1
    sourceIndexCount: 1
    failOnReject: true
    awaitReceipt: true
  configVars:
    walletPrivkey: "tasks.wallet.outputs.childWallet.privkey"
    sourceStartValidatorIndex: "sourceValidatorIndex"
    targetValidatorIndex: "targetValidatorIndex"

# An EL receipt also succeeds for a request ignored by the consensus layer.
- name: check_consensus_validator_status
  title: "Wait for the source balance to be transferred"
  timeout: 30m
  config:
    validatorStatus: [withdrawal_done]
    maxValidatorBalance: 0
  configVars:
    validatorIndex: "sourceValidatorIndex"

- name: check_consensus_validator_status
  title: "Require the target to receive the source stake"
  timeout: 10m
  config:
    validatorStatus: [active_ongoing]
    withdrawalCredsPrefix: "0x02"
    minValidatorBalance: 63000000000
  configVars:
    validatorIndex: "targetValidatorIndex"
