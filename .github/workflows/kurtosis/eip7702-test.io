
id: eip7702-test
name: "EIP7702 test"
timeout: 30m
config:
  #walletPrivkey: ""
  waitForSlot: 41
tasks:
- name: check_clients_are_healthy
  title: "Check if at least one client is ready"
  timeout: 5m
  config:
    minClientCount: 1

- name: generate_child_wallet
  title: "Generate wallet for EIP7702 test"
  config:
    prefundMinBalance: 10000000000000000000 # ensure 10 ETH
    walletAddressResultVar: "eoaAddress"
    walletPrivateKeyResultVar: "eoaPrivateKey"
    randomSeed: true
  configVars:
    privateKey: "walletPrivkey"
- name: sleep
  title: "wait for child wallet availablility"
  config:
    duration: 12s # wait 1 slot to ensure all ELs have the proper child wallet balance

- name: generate_transaction
  title: "Deploy EIP-7702 delegate contract"
  config:
    feeCap: 5000000000 # 5 gwei
    gasLimit: 2500000
    contractDeployment: true
    callData: "0x6080604052348015600e575f80fd5b50607580601a5f395ff3fe608060405236601057600e6018565b005b60166018565b005b365f80375f5160601c5f80601436036014845af43d5f803e805f8114603b573d5ff35b3d5ffdfea264697066735822122041f98880dc8582e942863e8dc7b6c126a09ff7ec9c2fd36d59df9196170dcf4664736f6c634300081a0033"
    failOnReject: true
    contractAddressResultVar: "delegateContractAddr"
  configVars:
    privateKey: "eoaPrivateKey"
  
- name: generate_transaction
  title: "Deploy EIP-7702 test contract"
  config:
    feeCap: 5000000000 # 5 gwei
    gasLimit: 2500000
    contractDeployment: true
    callData: "0x6080604052348015600e575f80fd5b506102198061001c5f395ff3fe608060405234801561000f575f80fd5b506004361061003f575f3560e01c80635ec8c7ac146100435780636b59084d1461004d578063b895c74a14610055575b5f80fd5b61004b61006f565b005b61004b6100c2565b61005d5f5481565b60405190815260200160405180910390f35b5f8054908061007d8361016b565b90915550505f5460408051328152602081019290925233917fb65d14ceb0bd10fc227373cb4bf5c54ece032fe7a118847459d3bd1eb4681b8f910160405180910390a2565b60645f9081556040805160048152602481018252602080820180516001600160e01b03166317b231eb60e21b179052915133926101039230929091016101a6565b60408051601f198184030181529082905261011d916101d1565b5f604051808303815f865af19150503d805f8114610156576040519150601f19603f3d011682016040523d82523d5f602084013e61015b565b606091505b5050905080610168575f80fd5b50565b5f6001820161018857634e487b7160e01b5f52601160045260245ffd5b5060010190565b5f81518060208401855e5f93019283525090919050565b6bffffffffffffffffffffffff198360601b1681525f6101c9601483018461018f565b949350505050565b5f6101dc828461018f565b939250505056fea26469706673582212203c5b729a024e2276896980d56840e87ce5c2cac7308780d5eb6553200eb09e2364736f6c634300081a0033"
    failOnReject: true
    contractAddressResultVar: "testContractAddr"
  configVars:
    privateKey: "eoaPrivateKey"

- name: generate_transaction
  title: "Call test1 from EOA1 with authorization for EOA1 to delegate contract"
  config:
    feeCap: 5000000000 # 5 gwei
    gasLimit: 1000000
    callData: "0x6b59084d"
    setCodeTxType: true
    awaitReceipt: true
  configVars:
    privateKey: "eoaPrivateKey"
    targetAddress: "testContractAddr"
    authorizations: "| [ { codeAddress: .delegateContractAddr, signerPrivkey: .eoaPrivateKey, nonce: 3 } ]"
