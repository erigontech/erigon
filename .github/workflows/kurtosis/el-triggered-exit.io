id: el-triggered-exit
name: "EL-triggered exit test"
timeout: 45m
config:
  walletPrivkey: ""
  validatorMnemonic: ""
  validatorIndex: 20

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
  title: "Fund the exit sender"
  config:
    walletSeed: "pectra-exit"
    prefundMinBalance: 1000000000000000000
  configVars:
    privateKey: "walletPrivkey"

- name: check_consensus_validator_status
  title: "Require an active genesis validator"
  timeout: 2m
  config:
    validatorStatus: [active_ongoing]
    withdrawalCredsPrefix: "0x00"
    validatorPubKeyResultVar: "validatorPubKey"
  configVars:
    validatorIndex: "validatorIndex"

- name: generate_bls_changes
  title: "Authorize the exit sender"
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

- name: run_task_background
  title: "Request an exit and wait for the full withdrawal"
  timeout: 30m
  config:
    onBackgroundComplete: failOrIgnore
    backgroundTask:
      name: generate_withdrawal_requests
      title: "Exit validator ${validatorIndex} via EL"
      config:
        limitTotal: 1
        sourceIndexCount: 1
        withdrawAmount: 0
        awaitReceipt: true
        failOnReject: true
      configVars:
        walletPrivkey: "tasks.wallet.outputs.childWallet.privkey"
        sourceStartValidatorIndex: "validatorIndex"
    foregroundTask:
      name: check_consensus_block_proposals
      title: "Require the validator's full stake to reach its withdrawal address"
      config:
        checkLookback: 0
        minWithdrawalCount: 1
      configVars:
        expectWithdrawals: "| [{publicKey: .validatorPubKey, address: .tasks.wallet.outputs.childWallet.address, minAmount: 31000000000}]"

- name: check_consensus_validator_status
  title: "Require the exited validator to have no remaining balance"
  timeout: 10m
  config:
    validatorStatus: [withdrawal_done]
    maxValidatorBalance: 0
  configVars:
    validatorIndex: "validatorIndex"
