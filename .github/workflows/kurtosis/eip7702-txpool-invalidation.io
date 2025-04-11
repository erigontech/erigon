id: eip7702-txpool-invalidation
name: "EIP7702 txpool invalidation"
timeout: 30m
config:
  walletCount: 10 # Number of child wallets to create
  txPerWallet: 2 # Number of transactions per child wallet
  invalidationScenario: 1 # 0: all, 1: regular txs, 2: delegated deployments
  #walletPrivkey: ""
  waitForSlot: 41

tasks:
- name: check_clients_are_healthy
  title: "Check if at least one client is ready"
  timeout: 5m
  config:
    minClientCount: 1

# wait for electra activation
- name: get_consensus_specs
  id: get_specs
  title: "Get consensus chain specs"
- name: check_consensus_slot_range
  title: "Wait for electra activation"
  timeout: 5m
  configVars:
    minEpochNumber: "tasks.get_specs.outputs.specs.ELECTRA_FORK_EPOCH"


# prepare wallets
- name: run_tasks_concurrent
  title: "Prepare wallets"
  config:
    newVariableScope: false
    tasks:
    - name: run_task_matrix
      id: create_child_wallets
      title: "Generate child wallets"
      configVars:
        matrixValues: "| [range(.walletCount)]"
      config:
        matrixVar: "walletIndex"
        runConcurrent: true
        task:
          name: generate_child_wallet
          id: create_wallet
          title: "Generate child wallet ${walletIndex}"
          config:
            prefundMinBalance: 1000000000000000000 # 1 ETH
            randomSeed: true
          configVars:
            privateKey: "walletPrivkey"

    - name: generate_child_wallet
      id: create_eoa_wallet
      title: "Generate wallet for EIP7702 transaction"
      config:
        prefundMinBalance: 2000000000000000000 # 2 ETH
        randomSeed: true
      configVars:
        privateKey: "walletPrivkey"

- name: sleep
  title: "Wait for wallet funding"
  config:
    duration: 12s

# SCENARIO 1: invalidation via a 7702 authorization
- name: run_tasks
  if: "| (.invalidationScenario == 0 or .invalidationScenario == 1)"
  title: "Scenario 1: Send regular txs and invalidate them via a 7702 authorization"
  config:
    tasks:
    - name: check_consensus_block_proposals
      title: "Wait for next block proposal (ensure we're at the beginning of the slot)"
    - name: run_tasks_concurrent
      title: "Send regular transactions from child wallets and invalidate them via a 7702 authorization"
      config:
        succeedTaskCount: 1 # only one task needs to succeed, the other fails as the txpool is invalidated
        tasks:
        - name: run_task_matrix
          title: "Send regular transactions from child wallets"
          configVars:
            matrixValues: "| [range(.walletCount)]"
            succeedTaskCount: "walletCount + 1"
          config:
            matrixVar: "walletIndex"
            runConcurrent: true
            task:
              name: generate_eoa_transactions
              title: "Send tx from wallet ${walletIndex}"
              config:
                feeCap: 5000000000 # 5 gwei
                gasLimit: 21000
                value: 100000000000000000 # 0.1 ETH
                targetAddress: "0x1111111111111111111111111111111111111111"
                awaitReceipt: true
              configVars:
                limitTotal: "txPerWallet"
                privateKey: "tasks.create_child_wallets.outputs.childScopes.[.walletIndex | tostring].tasks.create_wallet.outputs.childWallet.privkey"

        - name: run_tasks
          title: "Send 7702 tx after delay"
          config:
            tasks:
            # wait 2 secs to give the regular tx some time to propagate across the network before being invalidated
            - name: sleep
              title: "Wait before sending 7702 tx"
              config:
                duration: 2s

            - name: run_task_matrix
              title: "Send 7702 transaction to invalidate txpool"
              configVars:
                matrixValues: "| [range(.txPerWallet)]"
                succeedTaskCount: "txPerWallet + 1"
              config:
                matrixVar: "txIndex"
                runConcurrent: true
                task:
                  name: generate_transaction
                  title: "Send 7702 transaction ${txIndex}"
                  config:
                    feeCap: 10000000000 # 10 gwei
                    gasLimit: 15000000
                    tipCap: 5000000000
                    value: 0 # 0 ETH
                    targetAddress: "0x2222222222222222222222222222222222222222"
                    setCodeTxType: true
                  configVars:
                    nonce: "txIndex"
                    privateKey: "tasks.create_eoa_wallet.outputs.childWallet.privkey"
                    authorizations: "| . as $root | [range(.walletCount) | {codeAddress: \"0x0000000000000000000000000000000000001234\", nonce: $root.txIndex, signerPrivkey: $root.tasks.create_child_wallets.outputs.childScopes.[. | tostring].tasks.create_wallet.outputs.childWallet.privkey}]"


# SCENARIO 2: invalidation via delegated deployments
- name: run_tasks
  if: "| (.invalidationScenario == 0 or .invalidationScenario == 2)"
  title: "Scenario 2: Send regular txs and invalidate them via delegated deployments"
  config:
    tasks:

    # delpoy eip7701 test contract & test delegate
    - name: run_tasks_concurrent
      title: "Prepare eip7702 invalidation test contracts"
      config:
        newVariableScope: false
        tasks:
        - name: generate_transaction
          id: deploy_eip7701_test_contract
          title: "Deploy eip7701 test contract"
          config:
            feeCap: 5000000000 # 5 gwei
            gasLimit: 1000000
            contractDeployment: true
            callData: "6080604052348015600e575f80fd5b5061047c8061001c5f395ff3fe608060405234801561000f575f80fd5b506004361061004a575f3560e01c806333ff495a1461004e5780637d23e8571461005857806382fe1c9b14610076578063be4ca1e014610080575b5f80fd5b6100566100b0565b005b610060610111565b60405161006d919061036f565b60405180910390f35b61007e61019b565b005b61009a600480360381019061009591906103c6565b61024e565b6040516100a79190610400565b60405180910390f35b5f33908060018154018082558091505060019003905f5260205f20015f9091909190916101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff160217905550565b60605f80548060200260200160405190810160405280929190818152602001828054801561019157602002820191905f5260205f20905b815f9054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610148575b5050505050905090565b5f5b5f8054905081101561024b575f81815481106101bc576101bb610419565b5b905f5260205f20015f9054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff166382fe1c9b6040518163ffffffff1660e01b81526004015f604051808303815f87803b158015610228575f80fd5b505af115801561023a573d5f803e3d5ffd5b50505050808060010191505061019d565b50565b5f818154811061025c575f80fd5b905f5260205f20015f915054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b5f81519050919050565b5f82825260208201905092915050565b5f819050602082019050919050565b5f73ffffffffffffffffffffffffffffffffffffffff82169050919050565b5f6102da826102b1565b9050919050565b6102ea816102d0565b82525050565b5f6102fb83836102e1565b60208301905092915050565b5f602082019050919050565b5f61031d82610288565b6103278185610292565b9350610332836102a2565b805f5b8381101561036257815161034988826102f0565b975061035483610307565b925050600181019050610335565b5085935050505092915050565b5f6020820190508181035f8301526103878184610313565b905092915050565b5f80fd5b5f819050919050565b6103a581610393565b81146103af575f80fd5b50565b5f813590506103c08161039c565b92915050565b5f602082840312156103db576103da61038f565b5b5f6103e8848285016103b2565b91505092915050565b6103fa816102d0565b82525050565b5f6020820190506104135f8301846103f1565b92915050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52603260045260245ffdfea264697066735822122009cb00d85292b3405714243f4b0ce4f3e3511b206a3c0ee8b4ae7d0eecd27a2e64736f6c634300081a0033"
            failOnReject: true
          configVars:
            privateKey: "tasks.create_eoa_wallet.outputs.childWallet.privkey"
        - name: generate_transaction
          id: deploy_eip7701_test_delegate
          title: "Deploy eip7701 test delegate"
          config:
            feeCap: 5000000000 # 5 gwei
            gasLimit: 1000000
            contractDeployment: true
            callData: "6080604052348015600e575f80fd5b5061022d8061001c5f395ff3fe6080604052348015600e575f80fd5b50600436106026575f3560e01c806382fe1c9b14602a575b5f80fd5b60306032565b005b5f6040518060200160419060c8565b6020820181038252601f19601f8201166040525090505f8151602083015ff09050803b606b575f80fd5b8073ffffffffffffffffffffffffffffffffffffffff166383197ef06040518163ffffffff1660e01b81526004015f604051808303815f87803b15801560af575f80fd5b505af115801560c0573d5f803e3d5ffd5b505050505050565b610122806100d68339019056fe6080604052348015600e575f80fd5b503373ffffffffffffffffffffffffffffffffffffffff167f1449abf21e49fd025f33495e77f7b1461caefdd3d4bb646424a3f445c4576a5b60405160405180910390a260c48061005e5f395ff3fe6080604052348015600e575f80fd5b50600436106026575f3560e01c806383197ef014602a575b5f80fd5b60306032565b005b3373ffffffffffffffffffffffffffffffffffffffff167f7dec311f70bce33f6997a1cc140bcb6149f9ee83d6be656e848b00d170c9820060405160405180910390a23373ffffffffffffffffffffffffffffffffffffffff16fffea2646970667358221220ca3fe2fd8ee8ebce4dc80ef81aa0ddd6378445f1cb812f1da843da64220a827c64736f6c634300081a0033a26469706673582212207b7486e77106264abec073f512d5c29f5fb636f5af344973fe13e44881be61e064736f6c634300081a0033"
            failOnReject: true
          configVars:
            privateKey: "tasks.create_eoa_wallet.outputs.childWallet.privkey"

    # register all child wallets to the controller
    - name: run_task_matrix
      title: "Register child wallets to controller"
      configVars:
        matrixValues: "| [range(.walletCount)]"
        succeedTaskCount: "walletCount + 1"
      config:
        matrixVar: "walletIndex"
        runConcurrent: true
        task:
          name: generate_transaction
          title: "Send tx from wallet ${walletIndex} (register to controller)"
          config:
            feeCap: 5000000000 # 5 gwei
            gasLimit: 150000
            value: 0 # 0.1 ETH
            callData: "0x33ff495a" # registerWallet()
            failOnReject: true
          configVars:
            privateKey: "tasks.create_child_wallets.outputs.childScopes.[.walletIndex | tostring].tasks.create_wallet.outputs.childWallet.privkey"
            targetAddress: "tasks.deploy_eip7701_test_contract.outputs.contractAddress"

    # prepare child wallets (delegate all to eip7701 test delegate)
    - name: generate_transaction
      title: "Send 7702 transaction to set code for all child wallets to eip7701 test delegate"
      config:
        feeCap: 10000000000 # 10 gwei
        gasLimit: 15000000
        tipCap: 2000000000
        value: 100000000000000000 # 0.1 ETH
        targetAddress: "0x1111111111111111111111111111111111111111"
        setCodeTxType: true
        awaitReceipt: true
      configVars:
        privateKey: "tasks.create_eoa_wallet.outputs.childWallet.privkey"
        authorizations: "| . as $root | [range(.walletCount) | {codeAddress: $root.tasks.deploy_eip7701_test_delegate.outputs.contractAddress, signerPrivkey: $root.tasks.create_child_wallets.outputs.childScopes.[. | tostring].tasks.create_wallet.outputs.childWallet.privkey}]"

    - name: check_consensus_block_proposals
      title: "Wait for next block proposal (ensure we're at the beginning of the slot)"

    # send regular txs and invalidate them via delegated deployments
    - name: run_tasks_concurrent
      title: "Send regular txs and invalidate them via delegated deployments"
      config:
        succeedTaskCount: 1 # only one task needs to succeed, the other fails as the txpool is invalidated
        tasks:
        - name: run_task_matrix
          title: "Send regular transactions from child wallets"
          configVars:
            matrixValues: "| [range(.walletCount)]"
            succeedTaskCount: "walletCount + 1"
          config:
            matrixVar: "walletIndex"
            runConcurrent: true
            task:
              name: generate_eoa_transactions
              title: "Send tx from wallet ${walletIndex}"
              config:
                feeCap: 5000000000 # 5 gwei
                gasLimit: 21000
                value: 100000000000000000 # 0.1 ETH
                targetAddress: "0x1111111111111111111111111111111111111111"
                awaitReceipt: true
              configVars:
                limitTotal: "txPerWallet"
                privateKey: "tasks.create_child_wallets.outputs.childScopes.[.walletIndex | tostring].tasks.create_wallet.outputs.childWallet.privkey"

        - name: run_tasks
          title: "Send mass delegated deployment that invalidates all previous transactions"
          config:
            tasks:
            # wait 2 secs to give the regular tx some time to propagate across the network before being invalidated
            - name: sleep
              title: "Wait before sending mass delegated deployment"
              config:
                duration: 2s

            - name: run_task_matrix
              title: "Send 7702 transaction to invalidate txpool"
              configVars:
                matrixValues: "| [range(.txPerWallet)]"
                succeedTaskCount: "txPerWallet + 1"
              config:
                matrixVar: "txIndex"
                runConcurrent: true
                task:
                  name: generate_transaction
                  title: "Send mass delegated deployment to invalidate tx ${txIndex}/${txPerWallet}"
                  config:
                    feeCap: 10000000000 # 10 gwei
                    gasLimit: 20000000
                    tipCap: 2000000000
                    value: 0 # 0 ETH
                    callData: "0x82fe1c9b" # runTest()
                  configVars:
                    privateKey: "tasks.create_eoa_wallet.outputs.childWallet.privkey"
                    targetAddress: "tasks.deploy_eip7701_test_contract.outputs.contractAddress"

