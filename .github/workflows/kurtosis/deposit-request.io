id: pectra-massive-deposit-0x02
name: "Massive deposit with 0x02 creds"
timeout: 1h
config:
  #walletPrivkey: ""
  depositContract: "0x4242424242424242424242424242424242424242"
  targetAddress: "0x65D08a056c17Ae13370565B04cF77D2AfA1cB9FA"

tasks:
- name: generate_random_mnemonic
  title: "Generate random mnemonic"
  config:
    mnemonicResultVar: "validatorMnemonic"
- name: generate_child_wallet
  title: "Generate wallet for lifecycle test"
  config:
    prefundMinBalance: 2501000000000000000000 # ensure 2501 ETH
    walletAddressResultVar: "depositorAddress"
    walletPrivateKeyResultVar: "depositorPrivateKey"
  configVars:
    privateKey: "walletPrivkey"
- name: sleep
  title: "wait for child wallet availablility"
  config:
    duration: 12s # wait 1 slot to ensure all ELs have the proper child wallet balance

# generate deposits & wait for activation
- name: run_tasks
  title: "Generate normal deposits & track inclusion"
  config:
    stopChildOnResult: false
    tasks:
    - name: generate_deposits
      title: "Generate 2 deposits with 1000 ETH each"
      config:
        limitTotal: 8
        limitPerSlot: 2
        limitPending: 4
        depositAmount: 32
        awaitReceipt: true
        failOnReject: true
        validatorPubkeysResultVar: "validatorPubkeys"
      configVars:
        walletPrivkey: "depositorPrivateKey"
        mnemonic: "validatorMnemonic"
        depositContract: "depositContract"
        withdrawalCredentials: "| \"0x020000000000000000000000\" + (.targetAddress | capture(\"(0x)?(?<addr>.+)\").addr)"

# generate deposits & wait for activation
- name: run_tasks
  title: "Generate deposits & track inclusion"
  config:
    stopChildOnResult: false
    tasks:
    - name: generate_deposits
      title: "Generate 2 deposits with 1000 ETH each"
      config:
        limitTotal: 2
        limitPerSlot: 1
        limitPending: 1
        depositAmount: 1000
        awaitReceipt: true
        failOnReject: true
        validatorPubkeysResultVar: "validatorPubkeys"
      configVars:
        walletPrivkey: "depositorPrivateKey"
        mnemonic: "validatorMnemonic"
        depositContract: "depositContract"
        withdrawalCredentials: "| \"0x020000000000000000000000\" + (.targetAddress | capture(\"(0x)?(?<addr>.+)\").addr)"
