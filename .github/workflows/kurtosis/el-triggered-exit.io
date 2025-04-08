
id: el-triggered-exit
name: "EL-triggered exit test"
timeout: 1h
config:
  #walletPrivkey: ""
  validatorIndex: 20
  waitForSlot: 41

tasks:
- name: check_clients_are_healthy
  title: "Check if at least one client is ready"
  timeout: 5m
  config:
    minClientCount: 1

- name: check_consensus_validator_status
  title: "Get status for validator ${validatorIndex}"
  timeout: 1h
  config:
    validatorStatus:
    - active_ongoing
    validatorPubKeyResultVar: "validatorPubKey"
  configVars:
    validatorIndex: "validatorIndex"

- name: generate_transaction
  title: "Exit Validator ${validatorIndex} via EL"
  config:
    targetAddress: "0x00000961Ef480Eb55e80D19ad83579A64c007002"
    feeCap: 50000000000 # 50 gwei
    gasLimit: 1000000
    amount: "500000000000000000" # 0.5 ETH
    failOnReject: true
  configVars:
    privateKey: "walletPrivkey"
    # 0x0000000000000000 is the amount as uint64,  0 means full withdrawal / exit
    callData: "| .validatorPubKey + \"0000000000000000\""
