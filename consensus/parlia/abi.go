package parlia

const validatorSetABIBeforeLuban = `
[
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "amount",
          "type": "uint256"
        }
      ],
      "name": "batchTransfer",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "uint256",
          "name": "amount",
          "type": "uint256"
        },
        {
          "indexed": false,
          "internalType": "string",
          "name": "reason",
          "type": "string"
        }
      ],
      "name": "batchTransferFailed",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "uint256",
          "name": "amount",
          "type": "uint256"
        },
        {
          "indexed": false,
          "internalType": "bytes",
          "name": "reason",
          "type": "bytes"
        }
      ],
      "name": "batchTransferLowerFailed",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address",
          "name": "validator",
          "type": "address"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "amount",
          "type": "uint256"
        }
      ],
      "name": "deprecatedDeposit",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address payable",
          "name": "validator",
          "type": "address"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "amount",
          "type": "uint256"
        }
      ],
      "name": "directTransfer",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address payable",
          "name": "validator",
          "type": "address"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "amount",
          "type": "uint256"
        }
      ],
      "name": "directTransferFail",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": false,
          "internalType": "string",
          "name": "message",
          "type": "string"
        }
      ],
      "name": "failReasonWithStr",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "amount",
          "type": "uint256"
        }
      ],
      "name": "feeBurned",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": false,
          "internalType": "string",
          "name": "key",
          "type": "string"
        },
        {
          "indexed": false,
          "internalType": "bytes",
          "name": "value",
          "type": "bytes"
        }
      ],
      "name": "paramChange",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "amount",
          "type": "uint256"
        }
      ],
      "name": "systemTransfer",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": false,
          "internalType": "uint8",
          "name": "channelId",
          "type": "uint8"
        },
        {
          "indexed": false,
          "internalType": "bytes",
          "name": "msgBytes",
          "type": "bytes"
        }
      ],
      "name": "unexpectedPackage",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address",
          "name": "validator",
          "type": "address"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "amount",
          "type": "uint256"
        }
      ],
      "name": "validatorDeposit",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address",
          "name": "validator",
          "type": "address"
        }
      ],
      "name": "validatorEmptyJailed",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address",
          "name": "validator",
          "type": "address"
        }
      ],
      "name": "validatorEnterMaintenance",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address",
          "name": "validator",
          "type": "address"
        }
      ],
      "name": "validatorExitMaintenance",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address",
          "name": "validator",
          "type": "address"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "amount",
          "type": "uint256"
        }
      ],
      "name": "validatorFelony",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address",
          "name": "validator",
          "type": "address"
        }
      ],
      "name": "validatorJailed",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address",
          "name": "validator",
          "type": "address"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "amount",
          "type": "uint256"
        }
      ],
      "name": "validatorMisdemeanor",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [],
      "name": "validatorSetUpdated",
      "type": "event"
    },
    {
      "inputs": [],
      "name": "BIND_CHANNELID",
      "outputs": [
        {
          "internalType": "uint8",
          "name": "",
          "type": "uint8"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "BURN_ADDRESS",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "BURN_RATIO_SCALE",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "CODE_OK",
      "outputs": [
        {
          "internalType": "uint32",
          "name": "",
          "type": "uint32"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "CROSS_CHAIN_CONTRACT_ADDR",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "DUSTY_INCOMING",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "EPOCH",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "ERROR_FAIL_CHECK_VALIDATORS",
      "outputs": [
        {
          "internalType": "uint32",
          "name": "",
          "type": "uint32"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "ERROR_FAIL_DECODE",
      "outputs": [
        {
          "internalType": "uint32",
          "name": "",
          "type": "uint32"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "ERROR_LEN_OF_VAL_MISMATCH",
      "outputs": [
        {
          "internalType": "uint32",
          "name": "",
          "type": "uint32"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "ERROR_RELAYFEE_TOO_LARGE",
      "outputs": [
        {
          "internalType": "uint32",
          "name": "",
          "type": "uint32"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "ERROR_UNKNOWN_PACKAGE_TYPE",
      "outputs": [
        {
          "internalType": "uint32",
          "name": "",
          "type": "uint32"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "EXPIRE_TIME_SECOND_GAP",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "GOV_CHANNELID",
      "outputs": [
        {
          "internalType": "uint8",
          "name": "",
          "type": "uint8"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "GOV_HUB_ADDR",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "INCENTIVIZE_ADDR",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "INIT_BURN_RATIO",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "INIT_MAINTAIN_SLASH_SCALE",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "INIT_MAX_NUM_OF_MAINTAINING",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "INIT_NUM_OF_CABINETS",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "INIT_VALIDATORSET_BYTES",
      "outputs": [
        {
          "internalType": "bytes",
          "name": "",
          "type": "bytes"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "JAIL_MESSAGE_TYPE",
      "outputs": [
        {
          "internalType": "uint8",
          "name": "",
          "type": "uint8"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "LIGHT_CLIENT_ADDR",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "MAX_NUM_OF_VALIDATORS",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "PRECISION",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "RELAYERHUB_CONTRACT_ADDR",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "SLASH_CHANNELID",
      "outputs": [
        {
          "internalType": "uint8",
          "name": "",
          "type": "uint8"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "SLASH_CONTRACT_ADDR",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "STAKING_CHANNELID",
      "outputs": [
        {
          "internalType": "uint8",
          "name": "",
          "type": "uint8"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "SYSTEM_ADDRESS",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "SYSTEM_REWARD_ADDR",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "TOKEN_HUB_ADDR",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "TOKEN_MANAGER_ADDR",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "TRANSFER_IN_CHANNELID",
      "outputs": [
        {
          "internalType": "uint8",
          "name": "",
          "type": "uint8"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "TRANSFER_OUT_CHANNELID",
      "outputs": [
        {
          "internalType": "uint8",
          "name": "",
          "type": "uint8"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "VALIDATORS_UPDATE_MESSAGE_TYPE",
      "outputs": [
        {
          "internalType": "uint8",
          "name": "",
          "type": "uint8"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "VALIDATOR_CONTRACT_ADDR",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "alreadyInit",
      "outputs": [
        {
          "internalType": "bool",
          "name": "",
          "type": "bool"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "bscChainID",
      "outputs": [
        {
          "internalType": "uint16",
          "name": "",
          "type": "uint16"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "burnRatio",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "burnRatioInitialized",
      "outputs": [
        {
          "internalType": "bool",
          "name": "",
          "type": "bool"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "name": "currentValidatorSet",
      "outputs": [
        {
          "internalType": "address",
          "name": "consensusAddress",
          "type": "address"
        },
        {
          "internalType": "address payable",
          "name": "feeAddress",
          "type": "address"
        },
        {
          "internalType": "address",
          "name": "BBCFeeAddress",
          "type": "address"
        },
        {
          "internalType": "uint64",
          "name": "votingPower",
          "type": "uint64"
        },
        {
          "internalType": "bool",
          "name": "jailed",
          "type": "bool"
        },
        {
          "internalType": "uint256",
          "name": "incoming",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "name": "currentValidatorSetMap",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "expireTimeSecondGap",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "maintainSlashScale",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "maxNumOfCandidates",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "maxNumOfMaintaining",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "maxNumOfWorkingCandidates",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "numOfCabinets",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "numOfJailed",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "numOfMaintaining",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "totalInComing",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "address",
          "name": "valAddr",
          "type": "address"
        },
        {
          "internalType": "address",
          "name": "slashAddr",
          "type": "address"
        },
        {
          "internalType": "address",
          "name": "rewardAddr",
          "type": "address"
        },
        {
          "internalType": "address",
          "name": "lightAddr",
          "type": "address"
        },
        {
          "internalType": "address",
          "name": "tokenHubAddr",
          "type": "address"
        },
        {
          "internalType": "address",
          "name": "incentivizeAddr",
          "type": "address"
        },
        {
          "internalType": "address",
          "name": "relayerHubAddr",
          "type": "address"
        },
        {
          "internalType": "address",
          "name": "govHub",
          "type": "address"
        },
        {
          "internalType": "address",
          "name": "tokenManagerAddr",
          "type": "address"
        },
        {
          "internalType": "address",
          "name": "crossChain",
          "type": "address"
        }
      ],
      "name": "updateContractAddr",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "name": "validatorExtraSet",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "enterMaintenanceHeight",
          "type": "uint256"
        },
        {
          "internalType": "bool",
          "name": "isMaintaining",
          "type": "bool"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "init",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "uint8",
          "name": "",
          "type": "uint8"
        },
        {
          "internalType": "bytes",
          "name": "msgBytes",
          "type": "bytes"
        }
      ],
      "name": "handleSynPackage",
      "outputs": [
        {
          "internalType": "bytes",
          "name": "responsePayload",
          "type": "bytes"
        }
      ],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "uint8",
          "name": "channelId",
          "type": "uint8"
        },
        {
          "internalType": "bytes",
          "name": "msgBytes",
          "type": "bytes"
        }
      ],
      "name": "handleAckPackage",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "uint8",
          "name": "channelId",
          "type": "uint8"
        },
        {
          "internalType": "bytes",
          "name": "msgBytes",
          "type": "bytes"
        }
      ],
      "name": "handleFailAckPackage",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "address",
          "name": "valAddr",
          "type": "address"
        }
      ],
      "name": "deposit",
      "outputs": [],
      "stateMutability": "payable",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "getMiningValidators",
      "outputs": [
        {
          "internalType": "address[]",
          "name": "",
          "type": "address[]"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "getValidators",
      "outputs": [
        {
          "internalType": "address[]",
          "name": "",
          "type": "address[]"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "uint256",
          "name": "index",
          "type": "uint256"
        }
      ],
      "name": "isWorkingValidator",
      "outputs": [
        {
          "internalType": "bool",
          "name": "",
          "type": "bool"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "address",
          "name": "validator",
          "type": "address"
        }
      ],
      "name": "getIncoming",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "address",
          "name": "validator",
          "type": "address"
        }
      ],
      "name": "isCurrentValidator",
      "outputs": [
        {
          "internalType": "bool",
          "name": "",
          "type": "bool"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "address",
          "name": "validator",
          "type": "address"
        }
      ],
      "name": "misdemeanor",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "address",
          "name": "validator",
          "type": "address"
        }
      ],
      "name": "felony",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "address",
          "name": "_validator",
          "type": "address"
        }
      ],
      "name": "getCurrentValidatorIndex",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "uint256",
          "name": "index",
          "type": "uint256"
        }
      ],
      "name": "canEnterMaintenance",
      "outputs": [
        {
          "internalType": "bool",
          "name": "",
          "type": "bool"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "enterMaintenance",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "exitMaintenance",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "string",
          "name": "key",
          "type": "string"
        },
        {
          "internalType": "bytes",
          "name": "value",
          "type": "bytes"
        }
      ],
      "name": "updateParam",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "address",
          "name": "validator",
          "type": "address"
        }
      ],
      "name": "isValidatorExist",
      "outputs": [
        {
          "internalType": "bool",
          "name": "",
          "type": "bool"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "getMaintainingValidators",
      "outputs": [
        {
          "internalType": "address[]",
          "name": "maintainingValidators",
          "type": "address[]"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    }
]
`

const validatorSetABI = `
[
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":false,
                "internalType":"uint256",
                "name":"amount",
                "type":"uint256"
            }
        ],
        "name":"batchTransfer",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":true,
                "internalType":"uint256",
                "name":"amount",
                "type":"uint256"
            },
            {
                "indexed":false,
                "internalType":"string",
                "name":"reason",
                "type":"string"
            }
        ],
        "name":"batchTransferFailed",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":true,
                "internalType":"uint256",
                "name":"amount",
                "type":"uint256"
            },
            {
                "indexed":false,
                "internalType":"bytes",
                "name":"reason",
                "type":"bytes"
            }
        ],
        "name":"batchTransferLowerFailed",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":true,
                "internalType":"address",
                "name":"validator",
                "type":"address"
            },
            {
                "indexed":false,
                "internalType":"uint256",
                "name":"amount",
                "type":"uint256"
            }
        ],
        "name":"deprecatedDeposit",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":true,
                "internalType":"address",
                "name":"validator",
                "type":"address"
            },
            {
                "indexed":false,
                "internalType":"uint256",
                "name":"amount",
                "type":"uint256"
            }
        ],
        "name":"deprecatedFinalityRewardDeposit",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":true,
                "internalType":"address payable",
                "name":"validator",
                "type":"address"
            },
            {
                "indexed":false,
                "internalType":"uint256",
                "name":"amount",
                "type":"uint256"
            }
        ],
        "name":"directTransfer",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":true,
                "internalType":"address payable",
                "name":"validator",
                "type":"address"
            },
            {
                "indexed":false,
                "internalType":"uint256",
                "name":"amount",
                "type":"uint256"
            }
        ],
        "name":"directTransferFail",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":false,
                "internalType":"string",
                "name":"message",
                "type":"string"
            }
        ],
        "name":"failReasonWithStr",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":false,
                "internalType":"uint256",
                "name":"amount",
                "type":"uint256"
            }
        ],
        "name":"feeBurned",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":true,
                "internalType":"address",
                "name":"validator",
                "type":"address"
            },
            {
                "indexed":false,
                "internalType":"uint256",
                "name":"amount",
                "type":"uint256"
            }
        ],
        "name":"finalityRewardDeposit",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":false,
                "internalType":"string",
                "name":"key",
                "type":"string"
            },
            {
                "indexed":false,
                "internalType":"bytes",
                "name":"value",
                "type":"bytes"
            }
        ],
        "name":"paramChange",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":false,
                "internalType":"uint256",
                "name":"amount",
                "type":"uint256"
            }
        ],
        "name":"systemTransfer",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":false,
                "internalType":"uint8",
                "name":"channelId",
                "type":"uint8"
            },
            {
                "indexed":false,
                "internalType":"bytes",
                "name":"msgBytes",
                "type":"bytes"
            }
        ],
        "name":"unexpectedPackage",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":true,
                "internalType":"address",
                "name":"validator",
                "type":"address"
            },
            {
                "indexed":false,
                "internalType":"uint256",
                "name":"amount",
                "type":"uint256"
            }
        ],
        "name":"validatorDeposit",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":true,
                "internalType":"address",
                "name":"validator",
                "type":"address"
            }
        ],
        "name":"validatorEmptyJailed",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":true,
                "internalType":"address",
                "name":"validator",
                "type":"address"
            }
        ],
        "name":"validatorEnterMaintenance",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":true,
                "internalType":"address",
                "name":"validator",
                "type":"address"
            }
        ],
        "name":"validatorExitMaintenance",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":true,
                "internalType":"address",
                "name":"validator",
                "type":"address"
            },
            {
                "indexed":false,
                "internalType":"uint256",
                "name":"amount",
                "type":"uint256"
            }
        ],
        "name":"validatorFelony",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":true,
                "internalType":"address",
                "name":"validator",
                "type":"address"
            }
        ],
        "name":"validatorJailed",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[
            {
                "indexed":true,
                "internalType":"address",
                "name":"validator",
                "type":"address"
            },
            {
                "indexed":false,
                "internalType":"uint256",
                "name":"amount",
                "type":"uint256"
            }
        ],
        "name":"validatorMisdemeanor",
        "type":"event"
    },
    {
        "anonymous":false,
        "inputs":[

        ],
        "name":"validatorSetUpdated",
        "type":"event"
    },
    {
        "inputs":[

        ],
        "name":"BIND_CHANNELID",
        "outputs":[
            {
                "internalType":"uint8",
                "name":"",
                "type":"uint8"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"BURN_ADDRESS",
        "outputs":[
            {
                "internalType":"address",
                "name":"",
                "type":"address"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"BURN_RATIO_SCALE",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"CODE_OK",
        "outputs":[
            {
                "internalType":"uint32",
                "name":"",
                "type":"uint32"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"CROSS_CHAIN_CONTRACT_ADDR",
        "outputs":[
            {
                "internalType":"address",
                "name":"",
                "type":"address"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"CROSS_STAKE_CHANNELID",
        "outputs":[
            {
                "internalType":"uint8",
                "name":"",
                "type":"uint8"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"DUSTY_INCOMING",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"EPOCH",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"ERROR_FAIL_CHECK_VALIDATORS",
        "outputs":[
            {
                "internalType":"uint32",
                "name":"",
                "type":"uint32"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"ERROR_FAIL_DECODE",
        "outputs":[
            {
                "internalType":"uint32",
                "name":"",
                "type":"uint32"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"ERROR_LEN_OF_VAL_MISMATCH",
        "outputs":[
            {
                "internalType":"uint32",
                "name":"",
                "type":"uint32"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"ERROR_RELAYFEE_TOO_LARGE",
        "outputs":[
            {
                "internalType":"uint32",
                "name":"",
                "type":"uint32"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"ERROR_UNKNOWN_PACKAGE_TYPE",
        "outputs":[
            {
                "internalType":"uint32",
                "name":"",
                "type":"uint32"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"EXPIRE_TIME_SECOND_GAP",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"GOV_CHANNELID",
        "outputs":[
            {
                "internalType":"uint8",
                "name":"",
                "type":"uint8"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"GOV_HUB_ADDR",
        "outputs":[
            {
                "internalType":"address",
                "name":"",
                "type":"address"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"INCENTIVIZE_ADDR",
        "outputs":[
            {
                "internalType":"address",
                "name":"",
                "type":"address"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"INIT_BURN_RATIO",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"INIT_FINALITY_REWARD_RATIO",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"INIT_MAINTAIN_SLASH_SCALE",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"INIT_MAX_NUM_OF_MAINTAINING",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"INIT_NUM_OF_CABINETS",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"INIT_VALIDATORSET_BYTES",
        "outputs":[
            {
                "internalType":"bytes",
                "name":"",
                "type":"bytes"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"JAIL_MESSAGE_TYPE",
        "outputs":[
            {
                "internalType":"uint8",
                "name":"",
                "type":"uint8"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"LIGHT_CLIENT_ADDR",
        "outputs":[
            {
                "internalType":"address",
                "name":"",
                "type":"address"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"MAX_NUM_OF_VALIDATORS",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"PRECISION",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"RELAYERHUB_CONTRACT_ADDR",
        "outputs":[
            {
                "internalType":"address",
                "name":"",
                "type":"address"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"SLASH_CHANNELID",
        "outputs":[
            {
                "internalType":"uint8",
                "name":"",
                "type":"uint8"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"SLASH_CONTRACT_ADDR",
        "outputs":[
            {
                "internalType":"address",
                "name":"",
                "type":"address"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"STAKING_CHANNELID",
        "outputs":[
            {
                "internalType":"uint8",
                "name":"",
                "type":"uint8"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"STAKING_CONTRACT_ADDR",
        "outputs":[
            {
                "internalType":"address",
                "name":"",
                "type":"address"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"SYSTEM_REWARD_ADDR",
        "outputs":[
            {
                "internalType":"address",
                "name":"",
                "type":"address"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"TOKEN_HUB_ADDR",
        "outputs":[
            {
                "internalType":"address",
                "name":"",
                "type":"address"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"TOKEN_MANAGER_ADDR",
        "outputs":[
            {
                "internalType":"address",
                "name":"",
                "type":"address"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"TRANSFER_IN_CHANNELID",
        "outputs":[
            {
                "internalType":"uint8",
                "name":"",
                "type":"uint8"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"TRANSFER_OUT_CHANNELID",
        "outputs":[
            {
                "internalType":"uint8",
                "name":"",
                "type":"uint8"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"VALIDATORS_UPDATE_MESSAGE_TYPE",
        "outputs":[
            {
                "internalType":"uint8",
                "name":"",
                "type":"uint8"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"VALIDATOR_CONTRACT_ADDR",
        "outputs":[
            {
                "internalType":"address",
                "name":"",
                "type":"address"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"alreadyInit",
        "outputs":[
            {
                "internalType":"bool",
                "name":"",
                "type":"bool"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"bscChainID",
        "outputs":[
            {
                "internalType":"uint16",
                "name":"",
                "type":"uint16"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"burnRatio",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"burnRatioInitialized",
        "outputs":[
            {
                "internalType":"bool",
                "name":"",
                "type":"bool"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"uint256",
                "name":"index",
                "type":"uint256"
            }
        ],
        "name":"canEnterMaintenance",
        "outputs":[
            {
                "internalType":"bool",
                "name":"",
                "type":"bool"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "name":"currentValidatorSet",
        "outputs":[
            {
                "internalType":"address",
                "name":"consensusAddress",
                "type":"address"
            },
            {
                "internalType":"address payable",
                "name":"feeAddress",
                "type":"address"
            },
            {
                "internalType":"address",
                "name":"BBCFeeAddress",
                "type":"address"
            },
            {
                "internalType":"uint64",
                "name":"votingPower",
                "type":"uint64"
            },
            {
                "internalType":"bool",
                "name":"jailed",
                "type":"bool"
            },
            {
                "internalType":"uint256",
                "name":"incoming",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"address",
                "name":"",
                "type":"address"
            }
        ],
        "name":"currentValidatorSetMap",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"address",
                "name":"valAddr",
                "type":"address"
            }
        ],
        "name":"deposit",
        "outputs":[

        ],
        "stateMutability":"payable",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"address[]",
                "name":"valAddrs",
                "type":"address[]"
            },
            {
                "internalType":"uint256[]",
                "name":"weights",
                "type":"uint256[]"
            }
        ],
        "name":"distributeFinalityReward",
        "outputs":[

        ],
        "stateMutability":"nonpayable",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"enterMaintenance",
        "outputs":[

        ],
        "stateMutability":"nonpayable",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"exitMaintenance",
        "outputs":[

        ],
        "stateMutability":"nonpayable",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"expireTimeSecondGap",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"address",
                "name":"validator",
                "type":"address"
            }
        ],
        "name":"felony",
        "outputs":[

        ],
        "stateMutability":"nonpayable",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"finalityRewardRatio",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"address",
                "name":"_validator",
                "type":"address"
            }
        ],
        "name":"getCurrentValidatorIndex",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"address",
                "name":"validator",
                "type":"address"
            }
        ],
        "name":"getIncoming",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"getLivingValidators",
        "outputs":[
            {
                "internalType":"address[]",
                "name":"",
                "type":"address[]"
            },
            {
                "internalType":"bytes[]",
                "name":"",
                "type":"bytes[]"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"getMiningValidators",
        "outputs":[
            {
                "internalType":"address[]",
                "name":"",
                "type":"address[]"
            },
            {
                "internalType":"bytes[]",
                "name":"",
                "type":"bytes[]"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"getValidators",
        "outputs":[
            {
                "internalType":"address[]",
                "name":"",
                "type":"address[]"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"getWorkingValidatorCount",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"workingValidatorCount",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"uint8",
                "name":"channelId",
                "type":"uint8"
            },
            {
                "internalType":"bytes",
                "name":"msgBytes",
                "type":"bytes"
            }
        ],
        "name":"handleAckPackage",
        "outputs":[

        ],
        "stateMutability":"nonpayable",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"uint8",
                "name":"channelId",
                "type":"uint8"
            },
            {
                "internalType":"bytes",
                "name":"msgBytes",
                "type":"bytes"
            }
        ],
        "name":"handleFailAckPackage",
        "outputs":[

        ],
        "stateMutability":"nonpayable",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"uint8",
                "name":"",
                "type":"uint8"
            },
            {
                "internalType":"bytes",
                "name":"msgBytes",
                "type":"bytes"
            }
        ],
        "name":"handleSynPackage",
        "outputs":[
            {
                "internalType":"bytes",
                "name":"responsePayload",
                "type":"bytes"
            }
        ],
        "stateMutability":"nonpayable",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"init",
        "outputs":[

        ],
        "stateMutability":"nonpayable",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"address",
                "name":"validator",
                "type":"address"
            }
        ],
        "name":"isCurrentValidator",
        "outputs":[
            {
                "internalType":"bool",
                "name":"",
                "type":"bool"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"uint256",
                "name":"index",
                "type":"uint256"
            }
        ],
        "name":"isWorkingValidator",
        "outputs":[
            {
                "internalType":"bool",
                "name":"",
                "type":"bool"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"maintainSlashScale",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"maxNumOfCandidates",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"maxNumOfMaintaining",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"maxNumOfWorkingCandidates",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"address",
                "name":"validator",
                "type":"address"
            }
        ],
        "name":"misdemeanor",
        "outputs":[

        ],
        "stateMutability":"nonpayable",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"numOfCabinets",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"numOfJailed",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"numOfMaintaining",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"previousHeight",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[

        ],
        "name":"totalInComing",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"string",
                "name":"key",
                "type":"string"
            },
            {
                "internalType":"bytes",
                "name":"value",
                "type":"bytes"
            }
        ],
        "name":"updateParam",
        "outputs":[

        ],
        "stateMutability":"nonpayable",
        "type":"function"
    },
    {
        "inputs":[
            {
                "internalType":"uint256",
                "name":"",
                "type":"uint256"
            }
        ],
        "name":"validatorExtraSet",
        "outputs":[
            {
                "internalType":"uint256",
                "name":"enterMaintenanceHeight",
                "type":"uint256"
            },
            {
                "internalType":"bool",
                "name":"isMaintaining",
                "type":"bool"
            },
            {
                "internalType":"bytes",
                "name":"voteAddress",
                "type":"bytes"
            }
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "stateMutability":"payable",
        "type":"receive"
    }
]
`

const slashABI = `
[
    {
      "anonymous": false,
      "inputs": [],
      "name": "indicatorCleaned",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address",
          "name": "validator",
          "type": "address"
        }
      ],
      "name": "validatorSlashed",
      "type": "event"
    },
    {
      "inputs": [],
      "name": "FELONY_THRESHOLD",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "MISDEMEANOR_THRESHOLD",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "SYSTEM_ADDRESS",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "VALIDATOR_CONTRACT_ADDR",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "previousHeight",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "address",
          "name": "validator",
          "type": "address"
        }
      ],
      "name": "slash",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "clean",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "address",
          "name": "validator",
          "type": "address"
        }
      ],
      "name": "getSlashIndicator",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    }
  ]
`
