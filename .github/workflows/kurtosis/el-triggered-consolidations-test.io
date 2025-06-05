
id: el-triggered-consolidation
name: "EL-triggered consolidation test"
timeout: 1h
config:
  #walletPrivkey: ""
  sourceValidatorIndex: 6610
  targetValidatorIndex: 6611
  waitForSlot: 41

tasks:
- name: check_clients_are_healthy
  title: "Check if at least one client is ready"
  timeout: 5m
  config:
    minClientCount: 1

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
        sourceStartValidatorIndex: "sourceValidatorIndex"
        targetValidatorIndex: "targetValidatorIndex"