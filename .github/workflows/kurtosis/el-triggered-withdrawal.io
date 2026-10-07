id: el-triggered-withdrawal
name: "EL-triggered partial withdrawal test"
timeout: 45m
config:
  walletPrivkey: ""
  validatorMnemonic: ""
  validatorIndex: 21
  # Withdraw one Gwei of earned rewards; no new validator or deposit is needed.
  withdrawAmount: 1

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
  title: "Fund the withdrawal sender"
  config:
    walletSeed: "pectra-partial-withdrawal"
    prefundMinBalance: 2000000000000000000
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
  title: "Authorize the withdrawal sender"
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
  title: "Enable requested partial withdrawals with a self-consolidation"
  config:
    limitTotal: 1
    sourceIndexCount: 1
    failOnReject: true
    awaitReceipt: true
  configVars:
    walletPrivkey: "tasks.wallet.outputs.childWallet.privkey"
    sourceStartValidatorIndex: "validatorIndex"
    targetValidatorIndex: "validatorIndex"

- name: check_consensus_validator_status
  title: "Wait for compounding credentials and enough earned rewards"
  timeout: 10m
  config:
    validatorStatus: [active_ongoing]
  configVars:
    validatorIndex: "validatorIndex"
    minValidatorBalance: "| 32000000000 + .withdrawAmount"
    withdrawalCredsPrefix: "| \"0x020000000000000000000000\" + (.tasks.wallet.outputs.childWallet.address | ltrimstr(\"0x\"))"

- name: run_task_background
  title: "Request a partial withdrawal and wait for its payout"
  timeout: 30m
  config:
    onBackgroundComplete: failOrIgnore
    backgroundTask:
      name: generate_withdrawal_requests
      title: "Request ${withdrawAmount} Gwei from validator ${validatorIndex}"
      config:
        limitTotal: 1
        sourceIndexCount: 1
        awaitReceipt: true
        failOnReject: true
      configVars:
        walletPrivkey: "tasks.wallet.outputs.childWallet.privkey"
        sourceStartValidatorIndex: "validatorIndex"
        withdrawAmount: "withdrawAmount"
    foregroundTask:
      name: check_consensus_block_proposals
      title: "Require the exact partial withdrawal in an execution payload"
      config:
        checkLookback: 0
        minWithdrawalCount: 1
      configVars:
        expectWithdrawals: "| [{publicKey: .validatorPubKey, address: .tasks.wallet.outputs.childWallet.address, minAmount: .withdrawAmount, maxAmount: .withdrawAmount}]"

- name: check_consensus_validator_status
  title: "Require the partially withdrawn validator to remain active"
  timeout: 2m
  config:
    validatorStatus: [active_ongoing]
    withdrawalCredsPrefix: "0x02"
    minValidatorBalance: 32000000000
  configVars:
    validatorIndex: "validatorIndex"
