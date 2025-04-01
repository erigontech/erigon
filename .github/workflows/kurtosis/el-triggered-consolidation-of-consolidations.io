
id: el-triggered-consolidation-of-consolidation
name: "Consolidation of Consolidations"
timeout: 1h
config:
  #walletPrivkey: ""
  initialSourceValidatorIndex: 10
  initialTargetValidatorIndex: 11
  finalTargetValidatorIndex: 12
  waitForSlot: 41
tasks:
  - name: check_clients_are_healthy
    title: "Check if at least one client is ready"
    timeout: 5m
    config:
      minClientCount: 1 
  - name: check_consensus_slot_range
    title: "Wait for slot >= ${waitForSlot}"
    timeout: 1h
    configVars:
      minSlotNumber: "waitForSlot"
  - name: run_task_options
    title: "Generate consolidations"
    config:
      task:
        name: generate_consolidations
        title: "Consolidate Validator ${sourceValidatorIndex} to ${targetValidatorIndex}"
        config:
          sourceIndexCount: 1
          failOnReject: true
          awaitReceipt: true
          consolidationContract: "0x0000BBdDc7CE488642fb579F8B00f3a590007251"

        configVars:
          walletPrivkey: "walletPrivkey"
          sourceStartValidatorIndex: "initialSourceValidatorIndex"
          targetValidatorIndex: "initialTargetValidatorIndex"

  - name: check_consensus_validator_status
    title: "Wait for validator consolidation to start by checking if validator (${initialSourceValidatorIndex}) is exiting"
    timeout: 1h
    config:
      validatorStatus:
        - active_exiting
    configVars:
      validatorIndex: "initialSourceValidatorIndex"

  - name: check_consensus_validator_status
    title: "Wait for validator to exit (${initialSourceValidatorIndex}) to check consolidation status"
    timeout: 1h
    config:
      validatorStatus:
        - exited_unslashed
        - withdrawal_possible
        - withdrawal_done
    configVars:
      validatorIndex: "initialSourceValidatorIndex"

  - name: run_task_options
    title: "Generate consolidations for second validator"
    config:
      task:
        name: generate_consolidations
        title: "Consolidate Validator ${sourceValidatorIndex} to ${targetValidatorIndex}"
        config:
          sourceIndexCount: 1
          failOnReject: true
          awaitReceipt: true
          consolidationContract: "0x0000BBdDc7CE488642fb579F8B00f3a590007251"

        configVars:
          walletPrivkey: "walletPrivkey"
          sourceStartValidatorIndex: "initialTargetValidatorIndex"
          targetValidatorIndex: "finalTargetValidatorIndex"

  - name: check_consensus_validator_status
    title: "Wait for validator consolidation to start by checking if validator (${initialSourceValidatorIndex}) is exiting"
    timeout: 1h
    config:
      validatorStatus:
        - active_exiting
    configVars:
      validatorIndex: "initialTargetValidatorIndex"

  - name: check_consensus_validator_status
    title: "Wait for validator to exit (${initialSourceValidatorIndex}) to check consolidation status"
    timeout: 1h
    config:
      validatorStatus:
        - exited_unslashed
        - withdrawal_possible
        - withdrawal_done
    configVars:
      validatorIndex: "initialTargetValidatorIndex"