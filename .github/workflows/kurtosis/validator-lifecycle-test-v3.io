id: validator-lifecycle-test-v3
name: "Validator Lifecycle Test (v3)"
timeout: 672h
config:
  walletPrivkey: ""
  depositContract: "0x4242424242424242424242424242424242424242"

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
  timeout: 1h
  configVars:
    minEpochNumber: "tasks.get_specs.outputs.specs.ELECTRA_FORK_EPOCH"

# prepare new mnemonic & child wallet
- name: get_random_mnemonic
  id: test_mnemonic
  title: "Generate random mnemonic"

- name: generate_child_wallet
  id: depositor_wallet
  title: "Generate wallet for lifecycle test"
  config:
    walletSeed: "validator-lifecycle-test-v3"
    prefundMinBalance: 500000000000000000000 # ensure 500 ETH
  configVars:
    privateKey: "walletPrivkey"

##
## STEP 1: generate deposits & wait for activation
##
- name: run_tasks
  title: "Generate deposits & track inclusion"
  config:
    stopChildOnResult: false
    tasks:

    # 4 deposits, 32 ETH each, 0x00 withdrawal creds
    - name: generate_deposits
      title: "Generate 4 deposits with 32 ETH each and 0x00 withdrawal creds (index 0-3)"
      id: deposit_0
      config:
        limitTotal: 4
        awaitReceipt: true
        failOnReject: true
        validatorPubkeysResultVar: "validatorPubkeys"
      configVars:
        walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"
        mnemonic: "tasks.test_mnemonic.outputs.mnemonic"
        depositContract: "depositContract"
    
    # 1 deposit, 24 ETH, 0x00 withdrawal creds
    - name: generate_deposits
      title: "Generate 1 deposit with 24 ETH and 0x00 withdrawal creds (index 4)"
      id: deposit_1
      config:
        limitTotal: 1
        startIndex: 4
        awaitReceipt: true
        failOnReject: true
        depositAmount: 24
      configVars:
        walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"
        mnemonic: "tasks.test_mnemonic.outputs.mnemonic"
        depositContract: "depositContract"

    # 1 deposit, 32 ETH, 0x00 withdrawal creds
    - name: generate_deposits
      title: "Generate 1 deposit with 32 ETH and 0x00 withdrawal creds (index 0)"
      config:
        limitTotal: 1
        awaitReceipt: true
        failOnReject: true
      configVars:
        walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"
        mnemonic: "tasks.test_mnemonic.outputs.mnemonic"
        depositContract: "depositContract"
    
    # 1 deposit, 1 ETH, 0x02 withdrawal creds
    - name: generate_deposits
      title: "Generate 1 deposit with 1 ETH and 0x02 withdrawal creds (index 1)"
      config:
        limitTotal: 1
        startIndex: 1
        awaitReceipt: true
        failOnReject: true
      configVars:
        walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"
        mnemonic: "tasks.test_mnemonic.outputs.mnemonic"
        depositContract: "depositContract"
        withdrawalCredentials: "| \"0x020000000000000000000000\" + (.tasks.depositor_wallet.outputs.childWallet.address | capture(\"(0x)?(?<addr>.+)\").addr)"
    
    # 2 deposits, 32 ETH each, 0x01 withdrawal creds (index 5-6)
    - name: generate_deposits
      title: "Generate 2 deposits with 32 ETH each and 0x01 withdrawal creds (index 5-6)"
      id: deposit_2
      config:
        limitTotal: 2
        startIndex: 5
        awaitReceipt: true
        failOnReject: true
      configVars:
        walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"
        mnemonic: "tasks.test_mnemonic.outputs.mnemonic"
        depositContract: "depositContract"
        withdrawalCredentials: "| \"0x010000000000000000000000\" + (.tasks.depositor_wallet.outputs.childWallet.address | capture(\"(0x)?(?<addr>.+)\").addr)"
    
    # 1 deposit, 24 ETH, 0x02 withdrawal creds (index 7)
    - name: generate_deposits
      title: "Generate 1 deposit with 24 ETH and 0x02 withdrawal creds (index 7)"
      id: deposit_3
      config:
        limitTotal: 1
        startIndex: 7
        awaitReceipt: true
        failOnReject: true
        depositAmount: 24
      configVars:
        walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"
        mnemonic: "tasks.test_mnemonic.outputs.mnemonic"
        depositContract: "depositContract"
        withdrawalCredentials: "| \"0x020000000000000000000000\" + (.tasks.depositor_wallet.outputs.childWallet.address | capture(\"(0x)?(?<addr>.+)\").addr)"

    # 2 deposits, 64 ETH each, 0x02 withdrawal creds (index 8-9)
    - name: generate_deposits
      title: "Generate 2 deposits with 64 ETH each and 0x02 withdrawal creds (index 8-9)"
      id: deposit_4
      config:
        limitTotal: 2
        startIndex: 8
        awaitReceipt: true
        failOnReject: true
        depositAmount: 64
      configVars:
        walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"
        mnemonic: "tasks.test_mnemonic.outputs.mnemonic"
        depositContract: "depositContract"
        withdrawalCredentials: "| \"0x020000000000000000000000\" + (.tasks.depositor_wallet.outputs.childWallet.address | capture(\"(0x)?(?<addr>.+)\").addr)"

    # 1 deposit, 24 ETH, 0x02 withdrawal creds (index 4)
    - name: generate_deposits
      title: "Generate 1 deposit with 24 ETH and 0x02 withdrawal creds (index 4)"
      config:
        limitTotal: 1
        startIndex: 4
        awaitReceipt: true
        failOnReject: true
        depositAmount: 24
      configVars:
        walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"
        mnemonic: "tasks.test_mnemonic.outputs.mnemonic"
        depositContract: "depositContract"
        withdrawalCredentials: "| \"0x020000000000000000000000\" + (.tasks.depositor_wallet.outputs.childWallet.address | capture(\"(0x)?(?<addr>.+)\").addr)"
    
    # 1 deposit, 24 ETH, 0x02 withdrawal creds (index 7)
    - name: generate_deposits
      title: "Generate 1 deposit with 24 ETH and 0x02 withdrawal creds (index 7)"
      config:
        limitTotal: 1
        startIndex: 7
        awaitReceipt: true
        failOnReject: true
        depositAmount: 24
      configVars:
        walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"
        mnemonic: "tasks.test_mnemonic.outputs.mnemonic"
        depositContract: "depositContract"
        withdrawalCredentials: "| \"0x020000000000000000000000\" + (.tasks.depositor_wallet.outputs.childWallet.address | capture(\"(0x)?(?<addr>.+)\").addr)"

    - name: run_shell
      title: "Collect deposited validator pubkeys"
      id: all_pubkeys
      config:
        envVars:
          validatorPubkeys0: "tasks.deposit_0.outputs.validatorPubkeys"
          validatorPubkeys1: "tasks.deposit_1.outputs.validatorPubkeys"
          validatorPubkeys2: "tasks.deposit_2.outputs.validatorPubkeys"
          validatorPubkeys3: "tasks.deposit_3.outputs.validatorPubkeys"
          validatorPubkeys4: "tasks.deposit_4.outputs.validatorPubkeys"
        command: |
          echo "validatorPubkeys 0: $(echo $validatorPubkeys0 | jq)"
          echo "validatorPubkeys 1: $(echo $validatorPubkeys1 | jq)"
          echo "validatorPubkeys 2: $(echo $validatorPubkeys2 | jq)"
          echo "validatorPubkeys 3: $(echo $validatorPubkeys3 | jq)"
          echo "validatorPubkeys 4: $(echo $validatorPubkeys4 | jq)"
          all_pubkeys=$(echo -e "$validatorPubkeys0\n$validatorPubkeys1\n$validatorPubkeys2\n$validatorPubkeys3\n$validatorPubkeys4" | jq -c -s 'flatten')
          echo "::set-output-json pubkeys $all_pubkeys"

    - name: run_task_matrix
      title: "Wait for activation of all deposited keys"
      timeout: 2h
      configVars:
        matrixValues: "tasks.all_pubkeys.outputs.pubkeys"
      config:
        runConcurrent: true
        matrixVar: "validatorPubkey"
        task:
          name: check_consensus_validator_status
          title: "Wait for validator to become activated (${validatorPubkey})"
          config:
            validatorStatus:
            - active_ongoing
          configVars:
            validatorPubKey: "validatorPubkey"

    # check validator status
    - name: run_tasks
      title: "Check validator status"
      config:
        tasks:
        # validator #0: 64 ETH (32 ETH EB), 0x00 withdrawal creds
        - name: check_consensus_validator_status
          title: "Get validator #0 info"
          id: validator_info_0
          timeout: 5m
          config:
            validatorStatus:
            - active_ongoing
          configVars:
            validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[0]"
        - name: run_shell
          title: "Check validator #0 status: 64 ETH (32 ETH EB), 0x00 withdrawal creds"
          config:
            envVars: 
              validator: "tasks.validator_info_0.outputs.validator"
            command: |
              balance=$(echo "$validator" | jq -r '.balance')
              if [ "$balance" -lt 63900000000 ]; then
                echo "Balance too low! expected: > 63.9 ETH, got: $balance Gwei"
                exit 1
              fi

              effective_balance=$(echo "$validator" | jq -r '.validator.effective_balance')
              if [ "$effective_balance" != "32000000000" ]; then
                echo "Effective balance too low! expected: 32 ETH, got: $effective_balance Gwei"
                exit 1
              fi

              withdrawal_creds=$(echo "$validator" | jq -r '.validator.withdrawal_credentials')
              if [[ ! "$withdrawal_creds" == "0x00"* ]]; then
                echo "Invalid withdrawal credentials! expected: 0x00*, got: $withdrawal_creds"
                exit 1
              fi
        
        # validator #1: 33 ETH (32 ETH EB), 0x00 withdrawal creds
        - name: check_consensus_validator_status
          title: "Get validator #1 info"
          id: validator_info_1
          timeout: 5m
          config:
            validatorStatus:
            - active_ongoing
          configVars:
            validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[1]"
        - name: run_shell
          title: "Check validator #1 status: 33 ETH (32 ETH EB), 0x00 withdrawal creds"
          config:
            envVars: 
              validator: "tasks.validator_info_1.outputs.validator"
            command: |
              balance=$(echo "$validator" | jq -r '.balance')
              if [ "$balance" -lt 32900000000 ]; then
                echo "Balance too low! expected: > 32.9 ETH, got: $balance Gwei"
                exit 1
              fi

              effective_balance=$(echo "$validator" | jq -r '.validator.effective_balance')
              if [ "$effective_balance" != "32000000000" ]; then
                echo "Effective balance too low! expected: 32 ETH, got: $effective_balance Gwei"
                exit 1
              fi

              withdrawal_creds=$(echo "$validator" | jq -r '.validator.withdrawal_credentials')
              if [[ ! "$withdrawal_creds" == "0x00"* ]]; then
                echo "Invalid withdrawal credentials! expected: 0x00*, got: $withdrawal_creds"
                exit 1
              fi
        
        # validator #4: 48 ETH (32 ETH EB), 0x00 withdrawal creds
        - name: check_consensus_validator_status
          title: "Get validator #4 info"
          id: validator_info_4
          timeout: 5m
          config:
            validatorStatus:
            - active_ongoing
          configVars:
            validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[4]"
        - name: run_shell
          title: "Check validator #4 status: 48 ETH (32 ETH EB), 0x00 withdrawal creds"
          config:
            envVars: 
              validator: "tasks.validator_info_4.outputs.validator"
            command: |
              balance=$(echo "$validator" | jq -r '.balance')
              if [ "$balance" -lt 47900000000 ]; then
                echo "Balance too low! expected: > 47.9 ETH, got: $balance Gwei"
                exit 1
              fi

              effective_balance=$(echo "$validator" | jq -r '.validator.effective_balance')
              if [ "$effective_balance" != "32000000000" ]; then
                echo "Effective balance too low! expected: 32 ETH, got: $effective_balance Gwei"
                exit 1
              fi

              withdrawal_creds=$(echo "$validator" | jq -r '.validator.withdrawal_credentials')
              if [[ ! "$withdrawal_creds" == "0x00"* ]]; then
                echo "Invalid withdrawal credentials! expected: 0x00*, got: $withdrawal_creds"
                exit 1
              fi
        
        # validator #5: 32 ETH (32 ETH EB), 0x01 withdrawal creds
        - name: check_consensus_validator_status
          title: "Get validator #5 info"
          id: validator_info_5
          timeout: 5m
          config:
            validatorStatus:
            - active_ongoing
          configVars:
            validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[5]"
        - name: run_shell
          title: "Check validator #5 status: 32 ETH (32 ETH EB), 0x01 withdrawal creds"
          config:
            envVars: 
              validator: "tasks.validator_info_5.outputs.validator"
            command: |
              balance=$(echo "$validator" | jq -r '.balance')
              if [ "$balance" -lt 31900000000 ]; then
                echo "Balance too low! expected: > 31.9 ETH, got: $balance Gwei"
                exit 1
              fi

              effective_balance=$(echo "$validator" | jq -r '.validator.effective_balance')
              if [ "$effective_balance" != "32000000000" ]; then
                echo "Effective balance too low! expected: 32 ETH, got: $effective_balance Gwei"
                exit 1
              fi

              withdrawal_creds=$(echo "$validator" | jq -r '.validator.withdrawal_credentials')
              if [[ ! "$withdrawal_creds" == "0x01"* ]]; then
                echo "Invalid withdrawal credentials! expected: 0x01*, got: $withdrawal_creds"
                exit 1
              fi

        # validator #6: 32 ETH (32 ETH EB), 0x01 withdrawal creds
        - name: check_consensus_validator_status
          title: "Get validator #6 info"
          id: validator_info_6
          timeout: 5m
          config:
            validatorStatus:
            - active_ongoing
          configVars:
            validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[6]"
        - name: run_shell
          title: "Check validator #6 status: 32 ETH (32 ETH EB), 0x01 withdrawal creds"
          config:
            envVars: 
              validator: "tasks.validator_info_6.outputs.validator"
            command: |
              balance=$(echo "$validator" | jq -r '.balance')
              if [ "$balance" -lt 31900000000 ]; then
                echo "Balance too low! expected: > 31.9 ETH, got: $balance Gwei"
                exit 1
              fi

              effective_balance=$(echo "$validator" | jq -r '.validator.effective_balance')
              if [ "$effective_balance" != "32000000000" ]; then
                echo "Effective balance too low! expected: 32 ETH, got: $effective_balance Gwei"
                exit 1
              fi

              withdrawal_creds=$(echo "$validator" | jq -r '.validator.withdrawal_credentials')
              if [[ ! "$withdrawal_creds" == "0x01"* ]]; then
                echo "Invalid withdrawal credentials! expected: 0x01*, got: $withdrawal_creds"
                exit 1
              fi
        
        # validator #7: 48 ETH (48 ETH EB), 0x02 withdrawal creds
        - name: check_consensus_validator_status
          title: "Get validator #7 info"
          id: validator_info_7
          timeout: 5m
          config:
            validatorStatus:
            - active_ongoing
          configVars:
            validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[7]"
        - name: run_shell
          title: "Check validator #7 status: 48 ETH (48 ETH EB), 0x02 withdrawal creds"
          config:
            envVars: 
              validator: "tasks.validator_info_7.outputs.validator"
            command: |
              balance=$(echo "$validator" | jq -r '.balance')
              if [ "$balance" -lt 47900000000 ]; then
                echo "Balance too low! expected: > 47.9 ETH, got: $balance Gwei"
                exit 1
              fi

              effective_balance=$(echo "$validator" | jq -r '.validator.effective_balance')
              if [ "$effective_balance" != "48000000000" ]; then
                echo "Effective balance too low! expected: 48 ETH, got: $effective_balance Gwei"
                exit 1
              fi

              withdrawal_creds=$(echo "$validator" | jq -r '.validator.withdrawal_credentials')
              if [[ ! "$withdrawal_creds" == "0x02"* ]]; then
                echo "Invalid withdrawal credentials! expected: 0x02*, got: $withdrawal_creds"
                exit 1
              fi
        
        # validator #8: 64 ETH (64 ETH EB), 0x02 withdrawal creds
        - name: check_consensus_validator_status
          title: "Get validator #8 info"
          id: validator_info_8
          timeout: 5m
          config:
            validatorStatus:
            - active_ongoing
          configVars:
            validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[8]"
        - name: run_shell
          title: "Check validator #8 status: 64 ETH (64 ETH EB), 0x02 withdrawal creds"
          config:
            envVars: 
              validator: "tasks.validator_info_8.outputs.validator"
            command: |
              balance=$(echo "$validator" | jq -r '.balance')
              if [ "$balance" -lt 63900000000 ]; then
                echo "Balance too low! expected: > 63.9 ETH, got: $balance Gwei"
                exit 1
              fi

              effective_balance=$(echo "$validator" | jq -r '.validator.effective_balance')
              if [ "$effective_balance" != "64000000000" ]; then
                echo "Effective balance too low! expected: 64 ETH, got: $effective_balance Gwei"
                exit 1
              fi

              withdrawal_creds=$(echo "$validator" | jq -r '.validator.withdrawal_credentials')
              if [[ ! "$withdrawal_creds" == "0x02"* ]]; then
                echo "Invalid withdrawal credentials! expected: 0x02*, got: $withdrawal_creds"
                exit 1
              fi

        # validator #9: 64 ETH (64 ETH EB), 0x02 withdrawal creds
        - name: check_consensus_validator_status
          title: "Get validator #9 info"
          id: validator_info_9
          timeout: 5m
          config:
            validatorStatus:
            - active_ongoing
          configVars:
            validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[9]"
        - name: run_shell
          title: "Check validator #9 status: 64 ETH (64 ETH EB), 0x02 withdrawal creds"
          config:
            envVars: 
              validator: "tasks.validator_info_9.outputs.validator"
            command: |
              balance=$(echo "$validator" | jq -r '.balance')
              if [ "$balance" -lt 63900000000 ]; then
                echo "Balance too low! expected: > 63.9 ETH, got: $balance Gwei"
                exit 1
              fi

              effective_balance=$(echo "$validator" | jq -r '.validator.effective_balance')
              if [ "$effective_balance" != "64000000000" ]; then
                echo "Effective balance too low! expected: 64 ETH, got: $effective_balance Gwei"
                exit 1
              fi

              withdrawal_creds=$(echo "$validator" | jq -r '.validator.withdrawal_credentials')
              if [[ ! "$withdrawal_creds" == "0x02"* ]]; then
                echo "Invalid withdrawal credentials! expected: 0x02*, got: $withdrawal_creds"
                exit 1
              fi

# key status:
# index 0: active  64 ETH (32 ETH EB), 0x00 withdrawal creds
# index 1: active  33 ETH (32 ETH EB), 0x00 withdrawal creds
# index 2: active  32 ETH (32 ETH EB), 0x00 withdrawal creds
# index 3: active  32 ETH (32 ETH EB), 0x00 withdrawal creds
# index 4: active  48 ETH (32 ETH EB), 0x00 withdrawal creds
# index 5: active  32 ETH (32 ETH EB), 0x01 withdrawal creds
# index 6: active  32 ETH (32 ETH EB), 0x01 withdrawal creds
# index 7: active  48 ETH (48 ETH EB), 0x02 withdrawal creds
# index 8: active  64 ETH (64 ETH EB), 0x02 withdrawal creds
# index 9: active  64 ETH (64 ETH EB), 0x02 withdrawal creds

##
## STEP 2: generate BLS changes & wait for inclusion
##

- name: run_tasks
  title: "Generate BLS changes & track inclusion"
  config:
    stopChildOnResult: false
    tasks:

    - name: run_task_background
      title: "Generate BLS changes & track inclusion"
      config:
        onBackgroundComplete: failOrIgnore
        backgroundTask:
          name: run_tasks
          title: "Generate BLS changes & track inclusion"
          config:
            stopChildOnResult: false
            tasks:
            - name: generate_bls_changes
              title: "Generate BLS change for index 1-4 (0x01 creds)"
              config:
                limitTotal: 4
                startIndex: 1
                indexCount: 4
              configVars:
                mnemonic: "tasks.test_mnemonic.outputs.mnemonic"
                targetAddress: "tasks.depositor_wallet.outputs.childWallet.address"
        
        foregroundTask:
          name: run_tasks_concurrent
          title: "Wait for inclusion of all BLS changes & partial withdrawals"
          config:
            tasks:
            - name: run_task_matrix
              title: "Wait for inclusion of BLS changes for keys 1-4"
              timeout: 1h
              configVars:
                matrixValues: "tasks.all_pubkeys.outputs.pubkeys[1:5]"
              config:
                runConcurrent: true
                matrixVar: "validatorPubkey"
                task:
                  name: check_consensus_block_proposals
                  title: "Wait for BLS change inclusion for ${validatorPubkey}"
                  config:
                    minBlsChangeCount: 1
                  configVars:
                    expectBlsChanges: "| [{publicKey: .validatorPubkey, address: .tasks.depositor_wallet.outputs.childWallet.address}]"
            - name: check_consensus_block_proposals
              title: "Wait for partial withdrawal (>0.9 ETH) for key 1"
              config:
                minWithdrawalCount: 1
              configVars:
                expectWithdrawals: "| [{publicKey: .tasks.all_pubkeys.outputs.pubkeys[1], address: .tasks.depositor_wallet.outputs.childWallet.address, minAmount: 900000000}]"
            - name: check_consensus_block_proposals
              title: "Wait for partial withdrawal (>15.9 ETH) for key 4"
              config:
                minWithdrawalCount: 1
              configVars:
                expectWithdrawals: "| [{publicKey: .tasks.all_pubkeys.outputs.pubkeys[4], address: .tasks.depositor_wallet.outputs.childWallet.address, minAmount: 15900000000}]"

# key status:
# index 0: active  64 ETH (32 ETH EB), 0x00 withdrawal creds
# index 1: active  32 ETH (32 ETH EB), 0x01 withdrawal creds
# index 2: active  32 ETH (32 ETH EB), 0x01 withdrawal creds
# index 3: active  32 ETH (32 ETH EB), 0x01 withdrawal creds
# index 4: active  32 ETH (32 ETH EB), 0x01 withdrawal creds
# index 5: active  32 ETH (32 ETH EB), 0x01 withdrawal creds
# index 6: active  32 ETH (32 ETH EB), 0x01 withdrawal creds
# index 7: active  48 ETH (48 ETH EB), 0x02 withdrawal creds
# index 8: active  64 ETH (64 ETH EB), 0x02 withdrawal creds
# index 9: active  64 ETH (64 ETH EB), 0x02 withdrawal creds

##
## STEP 3: generate consolidations & wait for inclusion
##

# generate consolidations
- name: run_tasks
  title: "Generate consolidations & track inclusion"
  config:
    tasks:

    # generate self consolidation for key 5 & wait for inclusion
    - name: run_task_background
      title: "Generate self consolidation for key 5"
      timeout: 10m
      config:
        onBackgroundComplete: failOrIgnore
        backgroundTask:
          name: generate_consolidations
          title: "Generate 1 self consolidation (key 5)"
          config:
            limitTotal: 1
            sourceStartIndex: 5
            sourceIndexCount: 1
            awaitReceipt: true
            failOnReject: true
            consolidationContract: "0x0000BBdDc7CE488642fb579F8B00f3a590007251"
          configVars:
            sourceMnemonic: "tasks.test_mnemonic.outputs.mnemonic"
            targetPublicKey: "tasks.all_pubkeys.outputs.pubkeys[5]"
            walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"
        
        foregroundTask:
          name: check_consensus_block_proposals
          title: "Wait for inclusion of self consolidation request for key 5"
          config:
            minConsolidationRequestCount: 1
          configVars:
            expectConsolidationRequests: "| [{sourceAddress: .tasks.depositor_wallet.outputs.childWallet.address, sourcePubkey: .tasks.all_pubkeys.outputs.pubkeys[5]}]"

    # wait for exitability
    - name: run_tasks
      title: "Wait for validators to be exitable (${{ tasks.get_specs.outputs.specs.SHARD_COMMITTEE_PERIOD }} epochs)"
      config:
        stopChildOnResult: true
        tasks:
        - name: check_consensus_validator_status
          title: "Get validator info for last key"
          id: last_validator_info
          timeout: 1m
          config:
            validatorStatus:
            - active_ongoing
          configVars:
            validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[-1]"
        - name: check_consensus_slot_range
          title: "Wait for validators to be exitable (epoch >= ${{ |(.tasks.last_validator_info.outputs.validator.validator.activation_epoch | tonumber) + (.tasks.get_specs.outputs.specs.SHARD_COMMITTEE_PERIOD | tonumber) }})"
          timeout: 28h  # 256 epochs = ~27.3h
          configVars:
            minEpochNumber: "|(.tasks.last_validator_info.outputs.validator.validator.activation_epoch | tonumber) + (.tasks.get_specs.outputs.specs.SHARD_COMMITTEE_PERIOD | tonumber)"

    - name: run_task_background
      title: "Generate consolidations & track inclusion"
      config:
        onBackgroundComplete: failOrIgnore
        backgroundTask:
          name: run_tasks
          title: "Generate consolidations & track inclusion"
          config:
            stopChildOnResult: false
            tasks:
            - name: generate_consolidations
              title: "Generate 1 consolidation (index 0 -> index 6)"
              config:
                limitTotal: 1
                sourceStartIndex: 0
                sourceIndexCount: 1
                awaitReceipt: true
                failOnReject: true
                consolidationContract: "0x0000BBdDc7CE488642fb579F8B00f3a590007251"
              configVars:
                sourceMnemonic: "tasks.test_mnemonic.outputs.mnemonic"
                targetValidatorIndex: "tasks.validator_info_6.outputs.validator.index | tonumber"
                walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"
            
            - name: generate_consolidations
              title: "Generate 1 consolidation (index 1 -> index 5)"
              config:
                limitTotal: 1
                sourceStartIndex: 1
                sourceIndexCount: 1
                awaitReceipt: true
                failOnReject: true
                consolidationContract: "0x0000BBdDc7CE488642fb579F8B00f3a590007251"
              configVars:
                sourceMnemonic: "tasks.test_mnemonic.outputs.mnemonic"
                targetValidatorIndex: "tasks.validator_info_5.outputs.validator.index | tonumber"
                walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"
            
            - name: generate_consolidations
              title: "Generate 1 consolidation (index 5 -> index 7)"
              config:
                limitTotal: 1
                sourceStartIndex: 5
                sourceIndexCount: 1
                awaitReceipt: true
                failOnReject: true
                consolidationContract: "0x0000BBdDc7CE488642fb579F8B00f3a590007251"
              configVars:
                sourceMnemonic: "tasks.test_mnemonic.outputs.mnemonic"
                targetValidatorIndex: "tasks.validator_info_7.outputs.validator.index | tonumber"
                walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"

            - name: generate_consolidations
              title: "Generate 2 consolidation (index 3+4 -> index 7)"
              config:
                limitTotal: 2
                sourceStartIndex: 3
                sourceIndexCount: 2
                awaitReceipt: true
                failOnReject: true
                consolidationContract: "0x0000BBdDc7CE488642fb579F8B00f3a590007251"
              configVars:
                sourceMnemonic: "tasks.test_mnemonic.outputs.mnemonic"
                targetValidatorIndex: "tasks.validator_info_7.outputs.validator.index | tonumber"
                walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"
        
        foregroundTask:
          name: run_tasks_concurrent
          title: "Wait for inclusion of all consolidations"
          timeout: 30m
          config:
            tasks:
            - name: check_consensus_block_proposals
              title: "Wait for consolidation request 0->6 inclusion"
              config:
                minConsolidationRequestCount: 1
              configVars:
                expectConsolidationRequests: "| [{sourceAddress: .tasks.depositor_wallet.outputs.childWallet.address, sourcePubkey: .tasks.all_pubkeys.outputs.pubkeys[0]}]"
            - name: check_consensus_block_proposals
              title: "Wait for consolidation request 1->5 inclusion"
              config:
                minConsolidationRequestCount: 1
              configVars:
                expectConsolidationRequests: "| [{sourceAddress: .tasks.depositor_wallet.outputs.childWallet.address, sourcePubkey: .tasks.all_pubkeys.outputs.pubkeys[1]}]"
            - name: check_consensus_block_proposals
              title: "Wait for consolidation request 5->7 inclusion"
              config:
                minConsolidationRequestCount: 1
              configVars:
                expectConsolidationRequests: "| [{sourceAddress: .tasks.depositor_wallet.outputs.childWallet.address, sourcePubkey: .tasks.all_pubkeys.outputs.pubkeys[5]}]"
            - name: check_consensus_block_proposals
              title: "Wait for consolidation request 3->7 inclusion"
              config:
                minConsolidationRequestCount: 1
              configVars:
                expectConsolidationRequests: "| [{sourceAddress: .tasks.depositor_wallet.outputs.childWallet.address, sourcePubkey: .tasks.all_pubkeys.outputs.pubkeys[3]}]"
            - name: check_consensus_block_proposals
              title: "Wait for consolidation request 4->7 inclusion"
              config:
                minConsolidationRequestCount: 1
              configVars:
                expectConsolidationRequests: "| [{sourceAddress: .tasks.depositor_wallet.outputs.childWallet.address, sourcePubkey: .tasks.all_pubkeys.outputs.pubkeys[4]}]"
      
    # slash validator #3 before consolidation started
    - name: run_task_background
      title: "Generate slashing for key #3 & track inclusion"
      config:
        onBackgroundComplete: failOrIgnore
        backgroundTask:
          name: run_tasks
          title: "Generate slashing for key 3"
          config:
            stopChildOnResult: false
            tasks:
            - name: generate_slashings
              title: "Generate attester slashing for key 3"
              config:
                slashingType: "attester"
                limitTotal: 1
                startIndex: 3
                indexCount: 1
              configVars:
                mnemonic: "tasks.test_mnemonic.outputs.mnemonic"
        foregroundTask:
          name: check_consensus_block_proposals
          title: "Wait for slashing inclusion for key 3"
          config:
            minSlashingCount: 1
            minAttesterSlashingCount: 1
          configVars:
            expectSlashings: "| [{publicKey: .tasks.all_pubkeys.outputs.pubkeys[3], slashingType: \"attester\"}]"

    # wait for validator #1 exit
    - name: check_consensus_validator_status
      title: "Wait for validator #1 to exit (start of consolidation 1->5)"
      timeout: 28h
      config:
        validatorStatus:
        - exited_unslashed
        - withdrawal_possible
        - withdrawal_done
      configVars:
        validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[1]"
      
    # check validator #0 still active (consolidation ignored due to 0x00 credentials)
    - name: check_consensus_validator_status
      title: "Check validator #0 is still active (consolidation ignored)"
      timeout: 10s
      config:
        validatorStatus:
        - active_ongoing
      configVars:
        validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[0]"

    # wait for other consolidations
    - name: run_task_matrix
      title: "Wait for start of consolidations for keys 4-5"
      timeout: 1h
      configVars:
        matrixValues: "tasks.all_pubkeys.outputs.pubkeys[4:5]"
      config:
        runConcurrent: true
        matrixVar: "validatorPubkey"
        task:
          name: check_consensus_validator_status
          title: "Wait for validator to exit (${validatorPubkey})"
          config:
            validatorStatus:
            - exited_unslashed
            - withdrawal_possible
            - withdrawal_done
          configVars:
            validatorPubKey: "validatorPubkey"

# key status:
# index 0: active  64 ETH (32 ETH EB), 0x00 withdrawal creds
# index 1: exited  32 ETH (32 ETH EB), 0x01 withdrawal creds (consolidating to 5)
# index 2: active  32 ETH (32 ETH EB), 0x01 withdrawal creds
# index 3: slashed  31 ETH (31 ETH EB), 0x01 withdrawal creds (slashed, consolidating to 7)
# index 4: exited  32 ETH (0 ETH EB), 0x01 withdrawal creds (consolidating to 7)
# index 5: exited  32 ETH (0 ETH EB), 0x02 withdrawal creds (consolidating to 7)
# index 6: active  32 ETH (32 ETH EB), 0x01 withdrawal creds
# index 7: active  48 ETH (48 ETH EB), 0x02 withdrawal creds
# index 8: active  64 ETH (64 ETH EB), 0x02 withdrawal creds
# index 9: active  64 ETH (64 ETH EB), 0x02 withdrawal creds

##
## STEP 4: generate el triggered withdrawals & wait for inclusion
##

# generate EL triggered withdrawals
- name: run_tasks
  title: "Generate EL triggered withdrawals & track inclusion"
  config:
    stopChildOnResult: false
    tasks:

    - name: run_task_background
      title: "Generate EL triggered withdrawal for key 8 & track inclusion"
      config:
        onBackgroundComplete: failOrIgnore
        backgroundTask:
          name: run_tasks
          title: "Generate EL triggered partial withdrawal for key 8"
          config:
            stopChildOnResult: false
            tasks:
            - name: run_shell
              id: el_withdrawal_8
              title: "Build calldata (withdraw 32 ETH)"
              config:
                envVars:
                  pubkey: "tasks.all_pubkeys.outputs.pubkeys[8]"
                command: |
                  amount="32"
                  amount=$(expr ${amount:-1} \* 1000000000)
                  amount=$(printf "%016x" $amount)

                  pubkey=$(echo $pubkey | jq -r .)

                  echo "Pubkey: $pubkey"
                  echo "Amount: $amount"
                  echo "::set-out txCalldata $pubkey$amount"
            - name: generate_transaction
              title: "Send partial withdrawal transaction for key 8"
              config:
                targetAddress: "0x00000961Ef480Eb55e80D19ad83579A64c007002"
                feeCap: 50000000000 # 50 gwei
                gasLimit: 1000000
                amount: "500000000000000000" # 0.5 ETH
                failOnReject: true
              configVars:
                privateKey: "tasks.depositor_wallet.outputs.childWallet.privkey"
                callData: "tasks.el_withdrawal_8.outputs.txCalldata"
        foregroundTask:
          name: run_tasks_concurrent
          title: "Wait for inclusion of EL triggered withdrawal for key 8"
          config:
            tasks:
            - name: check_consensus_block_proposals
              title: "Wait for EL triggered withdrawal request for key 8"
              config:
                minWithdrawalRequestCount: 1
              configVars:
                expectWithdrawalRequests: "| [{validatorPubkey: .tasks.all_pubkeys.outputs.pubkeys[8], sourceAddress: .tasks.depositor_wallet.outputs.childWallet.address, amount: 32000000000}]"
            - name: check_consensus_block_proposals
              title: "Wait for EL triggered withdrawal for key 8"
              config:
                minWithdrawalCount: 1
              configVars:
                expectWithdrawals: "| [{publicKey: .tasks.all_pubkeys.outputs.pubkeys[8], address: .tasks.depositor_wallet.outputs.childWallet.address, minAmount: 31900000000}]"

# generate EL triggered exit
- name: run_tasks
  title: "Generate EL triggered exit & track inclusion"
  config:
    stopChildOnResult: false
    tasks:

    - name: run_task_background
      title: "Generate EL triggered withdrawal for key 0 + 2 + 9 & track inclusion"
      config:
        onBackgroundComplete: failOrIgnore
        backgroundTask:
          name: run_tasks
          title: "Generate EL triggered exit for key 0 + 2 + 8"
          config:
            stopChildOnResult: false
            tasks:
            - name: run_shell
              id: el_exit_0
              title: "Build calldata (exit key 0)"
              config:
                envVars:
                  pubkey: "tasks.all_pubkeys.outputs.pubkeys[0]"
                command: |
                  amount="0"
                  amount=$(expr ${amount:-1} \* 1000000000)
                  amount=$(printf "%016x" $amount)

                  pubkey=$(echo $pubkey | jq -r .)

                  echo "Pubkey: $pubkey"
                  echo "Amount: $amount"
                  echo "::set-out txCalldata $pubkey$amount"
            - name: generate_transaction
              title: "Send exit transaction for key 0"
              config:
                targetAddress: "0x00000961Ef480Eb55e80D19ad83579A64c007002"
                feeCap: 50000000000 # 50 gwei
                gasLimit: 1000000
                amount: "500000000000000000" # 0.5 ETH
                failOnReject: true
              configVars:
                privateKey: "tasks.depositor_wallet.outputs.childWallet.privkey"
                callData: "tasks.el_exit_0.outputs.txCalldata"
            - name: run_shell
              id: el_exit_2
              title: "Build calldata (exit key 2)"
              config:
                envVars:
                  pubkey: "tasks.all_pubkeys.outputs.pubkeys[2]"
                command: |
                  amount="0"
                  amount=$(expr ${amount:-1} \* 1000000000)
                  amount=$(printf "%016x" $amount)

                  pubkey=$(echo $pubkey | jq -r .)

                  echo "Pubkey: $pubkey"
                  echo "Amount: $amount"
                  echo "::set-out txCalldata $pubkey$amount"
            - name: generate_transaction
              title: "Send exit transaction for key 2"
              config:
                targetAddress: "0x00000961Ef480Eb55e80D19ad83579A64c007002"
                feeCap: 50000000000 # 50 gwei
                gasLimit: 1000000
                amount: "500000000000000000" # 0.5 ETH
                failOnReject: true
              configVars:
                privateKey: "tasks.depositor_wallet.outputs.childWallet.privkey"
                callData: "tasks.el_exit_2.outputs.txCalldata"
            - name: run_shell
              id: el_exit_9
              title: "Build calldata (exit key 9)"
              config:
                envVars:
                  pubkey: "tasks.all_pubkeys.outputs.pubkeys[9]"
                command: |
                  amount="0"
                  amount=$(expr ${amount:-1} \* 1000000000)
                  amount=$(printf "%016x" $amount)

                  pubkey=$(echo $pubkey | jq -r .)

                  echo "Pubkey: $pubkey"
                  echo "Amount: $amount"
                  echo "::set-out txCalldata $pubkey$amount"
            - name: generate_transaction
              title: "Send exit transaction for key 9"
              config:
                targetAddress: "0x00000961Ef480Eb55e80D19ad83579A64c007002"
                feeCap: 50000000000 # 50 gwei
                gasLimit: 1000000
                amount: "500000000000000000" # 0.5 ETH
                failOnReject: true
              configVars:
                privateKey: "tasks.depositor_wallet.outputs.childWallet.privkey"
                callData: "tasks.el_exit_9.outputs.txCalldata"
        foregroundTask:
          name: run_tasks_concurrent
          title: "Wait for inclusion of EL triggered exit for key 0+9"
          config:
            tasks:
            - name: check_consensus_block_proposals
              title: "Wait for EL triggered exit request for key 0"
              config:
                minWithdrawalRequestCount: 1
              configVars:
                expectWithdrawalRequests: "| [{validatorPubkey: .tasks.all_pubkeys.outputs.pubkeys[0], sourceAddress: .tasks.depositor_wallet.outputs.childWallet.address, amount: 0}]"
            - name: check_consensus_block_proposals
              title: "Wait for EL triggered exit request for key 2"
              config:
                minWithdrawalRequestCount: 1
              configVars:
                expectWithdrawalRequests: "| [{validatorPubkey: .tasks.all_pubkeys.outputs.pubkeys[2], sourceAddress: .tasks.depositor_wallet.outputs.childWallet.address, amount: 0}]"
            - name: check_consensus_validator_status
              title: "Wait for validator #2 to exit"
              timeout: 1h
              config:
                validatorStatus:
                - exited_unslashed
                - withdrawal_possible
                - withdrawal_done
              configVars:
                validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[2]"
            - name: check_consensus_block_proposals
              title: "Wait for EL triggered exit request for key 9"
              config:
                minWithdrawalRequestCount: 1
              configVars:
                expectWithdrawalRequests: "| [{validatorPubkey: .tasks.all_pubkeys.outputs.pubkeys[9], sourceAddress: .tasks.depositor_wallet.outputs.childWallet.address, amount: 0}]"
            - name: check_consensus_validator_status
              title: "Wait for validator #9 to exit"
              timeout: 1h
              config:
                validatorStatus:
                - exited_unslashed
                - withdrawal_possible
                - withdrawal_done
              configVars:
                validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[9]"


# key status:
# index 0: active  64 ETH (32 ETH EB), 0x00 withdrawal creds
# index 1: exited  32 ETH (32 ETH EB), 0x01 withdrawal creds (consolidating to 5)
# index 2: exited  32 ETH (32 ETH EB), 0x01 withdrawal creds
# index 3: slashed  31 ETH (31 ETH EB), 0x01 withdrawal creds (slashed, consolidating to 7)
# index 4: exited  32 ETH (0 ETH EB), 0x01 withdrawal creds (consolidating to 7)
# index 5: exited  32 ETH (0 ETH EB), 0x02 withdrawal creds (consolidating to 7)
# index 6: active  32 ETH (32 ETH EB), 0x01 withdrawal creds
# index 7: active  48 ETH (48 ETH EB), 0x02 withdrawal creds
# index 8: active  32 ETH (32 ETH EB), 0x02 withdrawal creds
# index 9: exited  64 ETH (64 ETH EB), 0x02 withdrawal creds


##
## STEP 5: await consolidation completion & check validator status
##

# check validator status
- name: run_tasks
  title: "Await consolidation completion & check validator status"
  config:
    tasks:
    # validator #0: 64 ETH (32 ETH EB), 0x00 withdrawal creds
    - name: check_consensus_validator_status
      title: "Get validator #0 info"
      id: validator_info_0
      timeout: 5m
      config:
        validatorStatus:
        - active_ongoing
      configVars:
        validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[0]"
    - name: run_shell
      title: "Check validator #0 status: 64 ETH (32 ETH EB), 0x00 withdrawal creds"
      config:
        envVars: 
          validator: "tasks.validator_info_0.outputs.validator"
        command: |
          balance=$(echo "$validator" | jq -r '.balance')
          if [ "$balance" -lt 63900000000 ]; then
            echo "Balance too low! expected: > 63.9 ETH, got: $balance Gwei"
            exit 1
          fi

          effective_balance=$(echo "$validator" | jq -r '.validator.effective_balance')
          if [ "$effective_balance" != "32000000000" ]; then
            echo "Effective balance too low! expected: 32 ETH, got: $effective_balance Gwei"
            exit 1
          fi

          withdrawal_creds=$(echo "$validator" | jq -r '.validator.withdrawal_credentials')
          if [[ ! "$withdrawal_creds" == "0x00"* ]]; then
            echo "Invalid withdrawal credentials! expected: 0x00*, got: $withdrawal_creds"
            exit 1
          fi
    
    # validator #1: 0 ETH (0 ETH EB), 0x01 withdrawal creds
    - name: check_consensus_validator_status
      title: "Get validator #1 info"
      id: validator_info_1
      timeout: 1h
      config:
        validatorStatus:
        - withdrawal_done
      configVars:
        validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[1]"
    - name: run_shell
      title: "Check validator #1 status: 0 ETH (0 ETH EB), 0x00 withdrawal creds"
      config:
        envVars: 
          validator: "tasks.validator_info_1.outputs.validator"
        command: |
          balance=$(echo "$validator" | jq -r '.balance')
          if [ "$balance" -gt 100000000 ]; then
            echo "Balance too high! expected: < 0.1 ETH, got: $balance Gwei"
            exit 1
          fi

          effective_balance=$(echo "$validator" | jq -r '.validator.effective_balance')
          if [ "$effective_balance" != "0" ]; then
            echo "Effective balance too high! expected: 0 ETH, got: $effective_balance Gwei"
            exit 1
          fi

          withdrawal_creds=$(echo "$validator" | jq -r '.validator.withdrawal_credentials')
          if [[ ! "$withdrawal_creds" == "0x01"* ]]; then
            echo "Invalid withdrawal credentials! expected: 0x01*, got: $withdrawal_creds"
            exit 1
          fi
    
    # validator #4: 0 ETH (0 ETH EB), 0x00 withdrawal creds
    - name: check_consensus_validator_status
      title: "Get validator #4 info"
      id: validator_info_4
      timeout: 1h
      config:
        validatorStatus:
        - withdrawal_done
      configVars:
        validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[4]"
    - name: run_shell
      title: "Check validator #4 status: 0 ETH (0 ETH EB), 0x00 withdrawal creds"
      config:
        envVars: 
          validator: "tasks.validator_info_4.outputs.validator"
        command: |
          balance=$(echo "$validator" | jq -r '.balance')
          if [ "$balance" -gt 100000000 ]; then
            echo "Balance too high! expected: < 0.1 ETH, got: $balance Gwei"
            exit 1
          fi

          effective_balance=$(echo "$validator" | jq -r '.validator.effective_balance')
          if [ "$effective_balance" != "0" ]; then
            echo "Effective balance too high! expected: 0 ETH, got: $effective_balance Gwei"
            exit 1
          fi

          withdrawal_creds=$(echo "$validator" | jq -r '.validator.withdrawal_credentials')
          if [[ ! "$withdrawal_creds" == "0x01"* ]]; then
            echo "Invalid withdrawal credentials! expected: 0x01*, got: $withdrawal_creds"
            exit 1
          fi
    
    # validator #5: 0 ETH (0 ETH EB), 0x02 withdrawal creds
    - name: check_consensus_validator_status
      title: "Get validator #5 info"
      id: validator_info_5
      timeout: 1h
      config:
        validatorStatus:
        - withdrawal_done
      configVars:
        validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[5]"
    - name: run_shell
      title: "Check validator #5 status: 0 ETH (0 ETH EB), 0x01 withdrawal creds"
      config:
        envVars: 
          validator: "tasks.validator_info_5.outputs.validator"
        command: |
          balance=$(echo "$validator" | jq -r '.balance')
          if [ "$balance" -gt 100000000 ]; then
            echo "Balance too high! expected: < 0.1 ETH, got: $balance Gwei"
            exit 1
          fi

          effective_balance=$(echo "$validator" | jq -r '.validator.effective_balance')
          if [ "$effective_balance" != "0" ]; then
            echo "Effective balance too high! expected: 0 ETH, got: $effective_balance Gwei"
            exit 1
          fi

          withdrawal_creds=$(echo "$validator" | jq -r '.validator.withdrawal_credentials')
          if [[ ! "$withdrawal_creds" == "0x02"* ]]; then
            echo "Invalid withdrawal credentials! expected: 0x02*, got: $withdrawal_creds"
            exit 1
          fi
    
    # validator #7: 111.9 ETH (111 ETH EB), 0x02 withdrawal creds
    - name: check_consensus_validator_status
      title: "Get validator #7 info"
      id: validator_info_7
      timeout: 8h
      config:
        validatorStatus:
        - active_ongoing
        minValidatorBalance: 111800000000
      configVars:
        validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[7]"
    - name: run_shell
      title: "Check validator #7 status: 111.9 ETH (111 ETH EB), 0x02 withdrawal creds"
      config:
        envVars: 
          validator: "tasks.validator_info_7.outputs.validator"
        command: |
          balance=$(echo "$validator" | jq -r '.balance')
          if [ "$balance" -lt 111800000000 ]; then
            echo "Balance too low! expected: > 111.8 ETH, got: $balance Gwei"
            exit 1
          fi

          effective_balance=$(echo "$validator" | jq -r '.validator.effective_balance')
          if [ "$effective_balance" != "111000000000" ]; then
            echo "Effective balance too low! expected: 111 ETH, got: $effective_balance Gwei"
            exit 1
          fi

          withdrawal_creds=$(echo "$validator" | jq -r '.validator.withdrawal_credentials')
          if [[ ! "$withdrawal_creds" == "0x02"* ]]; then
            echo "Invalid withdrawal credentials! expected: 0x02*, got: $withdrawal_creds"
            exit 1
          fi

# key status:
# index 0: active  64 ETH (32 ETH EB), 0x00 withdrawal creds
# index 1: exited  0 ETH (0 ETH EB), 0x01 withdrawal creds (consolidated to 5)
# index 2: exited  0 ETH (0 ETH EB), 0x01 withdrawal creds
# index 3: slashed  31 ETH (31 ETH EB), 0x01 withdrawal creds (slashed, consolidated to 7)
# index 4: exited  0 ETH (0 ETH EB), 0x01 withdrawal creds (consolidated to 7)
# index 5: exited  0 ETH (0 ETH EB), 0x02 withdrawal creds (consolidated to 7)
# index 6: active  32 ETH (32 ETH EB), 0x01 withdrawal creds
# index 7: active  111.9 ETH (111 ETH EB), 0x02 withdrawal creds
# index 8: active  32 ETH (32 ETH EB), 0x02 withdrawal creds
# index 9: exited  0 ETH (0 ETH EB), 0x02 withdrawal creds


##
## STEP 6: change withdrawal credentials (0x01->0x02) via self consolidation & wait for inclusion
##

# change withdrawal credentials
- name: run_tasks
  title: "Change withdrawal credentials (0x01->0x02) via self consolidation & track inclusion"
  config:
    tasks:

    - name: run_task_background
      title: "Change withdrawal credentials (0x01->0x02) via self consolidation & track inclusion"
      config:
        onBackgroundComplete: failOrIgnore
        backgroundTask:
          name: generate_consolidations
          title: "Generate 1 self consolidation (index 6 -> index 6)"
          config:
            limitTotal: 1
            sourceStartIndex: 6
            sourceIndexCount: 1
            awaitReceipt: true
            failOnReject: true
            consolidationContract: "0x0000BBdDc7CE488642fb579F8B00f3a590007251"
          configVars:
            sourceMnemonic: "tasks.test_mnemonic.outputs.mnemonic"
            targetValidatorIndex: "tasks.validator_info_6.outputs.validator.index | tonumber"
            walletPrivkey: "tasks.depositor_wallet.outputs.childWallet.privkey"
        
        foregroundTask:
          name: run_tasks_concurrent
          title: "Wait for inclusion of self consolidation request"
          config:
            tasks:
            - name: check_consensus_block_proposals
              title: "Wait for inclusion of self consolidation request for key 6"
              config:
                minConsolidationRequestCount: 1
              configVars:
                expectConsolidationRequests: "| [{sourceAddress: .tasks.depositor_wallet.outputs.childWallet.address, sourcePubkey: .tasks.all_pubkeys.outputs.pubkeys[6]}]"

    # validator #6: 42 ETH (42 ETH EB), 0x02 withdrawal creds
    - name: check_consensus_validator_status
      title: "Get validator #6 info"
      id: validator_info_6_b
      timeout: 1h
      config:
        validatorStatus:
        - active_ongoing
        minValidatorBalance: 31900000000
        withdrawalCredsPrefix: "0x02"
      configVars:
        validatorPubKey: "tasks.all_pubkeys.outputs.pubkeys[6]"
    - name: run_shell
      title: "Check validator #6 status: 31.9 ETH (32 ETH EB), 0x00 withdrawal creds"
      config:
        envVars: 
          validator: "tasks.validator_info_6_b.outputs.validator"
        command: |
          balance=$(echo "$validator" | jq -r '.balance')
          if [ "$balance" -lt 31900000000 ]; then
            echo "Balance too high! expected: > 31.9 ETH, got: $balance Gwei"
            exit 1
          fi

          effective_balance=$(echo "$validator" | jq -r '.validator.effective_balance')
          if [ "$effective_balance" != "32000000000" ]; then
            echo "Effective balance too high! expected: 32 ETH, got: $effective_balance Gwei"
            exit 1
          fi

          withdrawal_creds=$(echo "$validator" | jq -r '.validator.withdrawal_credentials')
          if [[ ! "$withdrawal_creds" == "0x02"* ]]; then
            echo "Invalid withdrawal credentials! expected: 0x02*, got: $withdrawal_creds"
            exit 1
          fi



cleanupTasks:
- name: run_tasks
  title: "Generate BLS changes, & exit all test validators"
  config:
    stopChildOnResult: false
    tasks:
    - name: generate_bls_changes
      title: "Generate bls changes for all test validators"
      config:
        limitTotal: 10
        indexCount: 10
      configVars:
        mnemonic: "tasks.test_mnemonic.outputs.mnemonic"
        targetAddress: "tasks.depositor_wallet.outputs.childWallet.address"
    - name: generate_exits
      title: "Exit all test validators"
      config:
        limitTotal: 10
        indexCount: 10
      configVars:
        mnemonic: "tasks.test_mnemonic.outputs.mnemonic"
