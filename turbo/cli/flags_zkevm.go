package cli

import (
	"fmt"
	"math"

	"strings"

	"time"

	"strconv"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	utils2 "github.com/ledgerwatch/erigon/zk/utils"
	"github.com/urfave/cli/v2"
)

var DeprecatedFlags = map[string]string{
	"zkevm.gasless":       "zkevm.allow-free-transactions",
	"zkevm.rpc-ratelimit": "",
}

func ApplyFlagsForZkConfig(ctx *cli.Context, cfg *ethconfig.Config) {
	checkFlag := func(flagName string, value interface{}) {
		switch v := value.(type) {
		case string:
			if v == "" {
				panic(fmt.Sprintf("Flag not set: %s", flagName))
			}
		case uint64:
			if v == 0 {
				panic(fmt.Sprintf("Flag not set: %s", flagName))
			}
		case uint32:
			if v == 0 {
				panic(fmt.Sprintf("Flag not set: %s", flagName))
			}
		case uint:
			if v == 0 {
				panic(fmt.Sprintf("Flag not set: %s", flagName))
			}
		case int:
			if v == 0 {
				panic(fmt.Sprintf("Flag not set: %s", flagName))
			}
		case []string:
			if len(v) == 0 {
				panic(fmt.Sprintf("Flag not set: %s", flagName))
			}
		case libcommon.Address:
			if v == (libcommon.Address{}) {
				panic(fmt.Sprintf("Flag not set: %s", flagName))
			}
		case time.Duration:
			if v == 0 {
				panic(fmt.Sprintf("Flag not set: %s", flagName))
			}
		case bool:
			// nothing to check
		default:
			panic(fmt.Sprintf("Unsupported type for flag check: %T", value))
		}
	}

	l2DataStreamTimeoutVal := ctx.String(utils.L2DataStreamerTimeout.Name)
	l2DataStreamTimeout, err := time.ParseDuration(l2DataStreamTimeoutVal)
	if err != nil {
		panic(fmt.Sprintf("could not parse l2 datastreamer timeout value %s", l2DataStreamTimeoutVal))
	}

	l2ShortCircuitToVerifiedBatchVal := ctx.Bool(utils.L2ShortCircuitToVerifiedBatchFlag.Name)

	sequencerBlockSealTimeVal := ctx.String(utils.SequencerBlockSealTime.Name)
	sequencerBlockSealTime, err := time.ParseDuration(sequencerBlockSealTimeVal)
	if err != nil {
		panic(fmt.Sprintf("could not parse sequencer block seal time timeout value %s", sequencerBlockSealTimeVal))
	}

	sequencerBatchSealTimeVal := ctx.String(utils.SequencerBatchSealTime.Name)
	sequencerBatchSealTime, err := time.ParseDuration(sequencerBatchSealTimeVal)
	if err != nil {
		panic(fmt.Sprintf("could not parse sequencer batch seal time timeout value %s", sequencerBatchSealTimeVal))
	}

	sequencerBatchVerificationTimeoutVal := ctx.String(utils.SequencerBatchVerificationTimeout.Name)
	sequencerBatchVerificationTimeout, err := time.ParseDuration(sequencerBatchVerificationTimeoutVal)
	if err != nil {
		panic(fmt.Sprintf("could not parse sequencer batch seal time timeout value %s", sequencerBatchSealTimeVal))
	}

	sequencerTimeoutOnEmptyTxPoolVal := ctx.String(utils.SequencerTimeoutOnEmptyTxPool.Name)
	sequencerTimeoutOnEmptyTxPool, err := time.ParseDuration(sequencerTimeoutOnEmptyTxPoolVal)
	if err != nil {
		panic(fmt.Sprintf("could not parse sequencer batch seal time timeout value %s", sequencerBatchSealTimeVal))
	}

	effectiveGasPriceForEthTransferVal := ctx.Float64(utils.EffectiveGasPriceForEthTransfer.Name)
	effectiveGasPriceForErc20TransferVal := ctx.Float64(utils.EffectiveGasPriceForErc20Transfer.Name)
	effectiveGasPriceForContractInvocationVal := ctx.Float64(utils.EffectiveGasPriceForContractInvocation.Name)
	effectiveGasPriceForContractDeploymentVal := ctx.Float64(utils.EffectiveGasPriceForContractDeployment.Name)
	if effectiveGasPriceForEthTransferVal < 0 || effectiveGasPriceForEthTransferVal > 1 {
		panic("Effective gas price for eth transfer must be in interval [0; 1]")
	}
	if effectiveGasPriceForErc20TransferVal < 0 || effectiveGasPriceForErc20TransferVal > 1 {
		panic("Effective gas price for erc20 transfer must be in interval [0; 1]")
	}
	if effectiveGasPriceForContractInvocationVal < 0 || effectiveGasPriceForContractInvocationVal > 1 {
		panic("Effective gas price for contract invocation must be in interval [0; 1]")
	}
	if effectiveGasPriceForContractDeploymentVal < 0 || effectiveGasPriceForContractDeploymentVal > 1 {
		panic("Effective gas price for contract deployment must be in interval [0; 1]")
	}

	witnessMemSize := utils.DatasizeFlagValue(ctx, utils.WitnessMemdbSize.Name)

	badBatchStrings := strings.Split(ctx.String(utils.BadBatches.Name), ",")
	badBatches := make([]uint64, 0)
	for _, s := range badBatchStrings {
		if s == "" {
			// if there are no entries then we can just ignore it and move on
			continue
		}
		// parse the string as uint64
		val, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("could not parse bad batch number %s", s))
		}
		badBatches = append(badBatches, val)
	}

	cfg.Zk = &ethconfig.Zk{
		L2ChainId:                              ctx.Uint64(utils.L2ChainIdFlag.Name),
		L2RpcUrl:                               ctx.String(utils.L2RpcUrlFlag.Name),
		L2DataStreamerUrl:                      ctx.String(utils.L2DataStreamerUrlFlag.Name),
		L2DataStreamerTimeout:                  l2DataStreamTimeout,
		L2ShortCircuitToVerifiedBatch:          l2ShortCircuitToVerifiedBatchVal,
		L1SyncStartBlock:                       ctx.Uint64(utils.L1SyncStartBlock.Name),
		L1SyncStopBatch:                        ctx.Uint64(utils.L1SyncStopBatch.Name),
		L1ChainId:                              ctx.Uint64(utils.L1ChainIdFlag.Name),
		L1RpcUrl:                               ctx.String(utils.L1RpcUrlFlag.Name),
		L1CacheEnabled:                         ctx.Bool(utils.L1CacheEnabledFlag.Name),
		L1CachePort:                            ctx.Uint(utils.L1CachePortFlag.Name),
		AddressSequencer:                       libcommon.HexToAddress(ctx.String(utils.AddressSequencerFlag.Name)),
		AddressAdmin:                           libcommon.HexToAddress(ctx.String(utils.AddressAdminFlag.Name)),
		AddressRollup:                          libcommon.HexToAddress(ctx.String(utils.AddressRollupFlag.Name)),
		AddressZkevm:                           libcommon.HexToAddress(ctx.String(utils.AddressZkevmFlag.Name)),
		AddressGerManager:                      libcommon.HexToAddress(ctx.String(utils.AddressGerManagerFlag.Name)),
		L1RollupId:                             ctx.Uint64(utils.L1RollupIdFlag.Name),
		L1BlockRange:                           ctx.Uint64(utils.L1BlockRangeFlag.Name),
		L1QueryDelay:                           ctx.Uint64(utils.L1QueryDelayFlag.Name),
		L1HighestBlockType:                     ctx.String(utils.L1HighestBlockTypeFlag.Name),
		L1MaticContractAddress:                 libcommon.HexToAddress(ctx.String(utils.L1MaticContractAddressFlag.Name)),
		L1FirstBlock:                           ctx.Uint64(utils.L1FirstBlockFlag.Name),
		L1FinalizedBlockRequirement:            ctx.Uint64(utils.L1FinalizedBlockRequirementFlag.Name),
		L1ContractAddressCheck:                 ctx.Bool(utils.L1ContractAddressCheckFlag.Name),
		L1ContractAddressRetrieve:              ctx.Bool(utils.L1ContractAddressRetrieveFlag.Name),
		RpcGetBatchWitnessConcurrencyLimit:     ctx.Int(utils.RpcGetBatchWitnessConcurrencyLimitFlag.Name),
		DatastreamVersion:                      ctx.Int(utils.DatastreamVersionFlag.Name),
		RebuildTreeAfter:                       ctx.Uint64(utils.RebuildTreeAfterFlag.Name),
		IncrementTreeAlways:                    ctx.Bool(utils.IncrementTreeAlways.Name),
		SmtRegenerateInMemory:                  ctx.Bool(utils.SmtRegenerateInMemory.Name),
		SequencerBlockSealTime:                 sequencerBlockSealTime,
		SequencerBatchSealTime:                 sequencerBatchSealTime,
		SequencerBatchVerificationTimeout:      sequencerBatchVerificationTimeout,
		SequencerBatchVerificationRetries:      ctx.Int(utils.SequencerBatchVerificationRetries.Name),
		SequencerTimeoutOnEmptyTxPool:          sequencerTimeoutOnEmptyTxPool,
		SequencerHaltOnBatchNumber:             ctx.Uint64(utils.SequencerHaltOnBatchNumber.Name),
		SequencerResequence:                    ctx.Bool(utils.SequencerResequence.Name),
		SequencerResequenceStrict:              ctx.Bool(utils.SequencerResequenceStrict.Name),
		SequencerResequenceReuseL1InfoIndex:    ctx.Bool(utils.SequencerResequenceReuseL1InfoIndex.Name),
		ExecutorUrls:                           strings.Split(strings.ReplaceAll(ctx.String(utils.ExecutorUrls.Name), " ", ""), ","),
		ExecutorStrictMode:                     ctx.Bool(utils.ExecutorStrictMode.Name),
		ExecutorRequestTimeout:                 ctx.Duration(utils.ExecutorRequestTimeout.Name),
		DatastreamNewBlockTimeout:              ctx.Duration(utils.DatastreamNewBlockTimeout.Name),
		WitnessMemdbSize:                       *witnessMemSize,
		ExecutorMaxConcurrentRequests:          ctx.Int(utils.ExecutorMaxConcurrentRequests.Name),
		Limbo:                                  ctx.Bool(utils.Limbo.Name),
		AllowFreeTransactions:                  ctx.Bool(utils.AllowFreeTransactions.Name),
		AllowPreEIP155Transactions:             ctx.Bool(utils.AllowPreEIP155Transactions.Name),
		EffectiveGasPriceForEthTransfer:        uint8(math.Round(effectiveGasPriceForEthTransferVal * 255.0)),
		EffectiveGasPriceForErc20Transfer:      uint8(math.Round(effectiveGasPriceForErc20TransferVal * 255.0)),
		EffectiveGasPriceForContractInvocation: uint8(math.Round(effectiveGasPriceForContractInvocationVal * 255.0)),
		EffectiveGasPriceForContractDeployment: uint8(math.Round(effectiveGasPriceForContractDeploymentVal * 255.0)),
		DefaultGasPrice:                        ctx.Uint64(utils.DefaultGasPrice.Name),
		MaxGasPrice:                            ctx.Uint64(utils.MaxGasPrice.Name),
		GasPriceFactor:                         ctx.Float64(utils.GasPriceFactor.Name),
		WitnessFull:                            ctx.Bool(utils.WitnessFullFlag.Name),
		SyncLimit:                              ctx.Uint64(utils.SyncLimit.Name),
		DebugTimers:                            ctx.Bool(utils.DebugTimers.Name),
		DebugNoSync:                            ctx.Bool(utils.DebugNoSync.Name),
		DebugLimit:                             ctx.Uint64(utils.DebugLimit.Name),
		DebugStep:                              ctx.Uint64(utils.DebugStep.Name),
		DebugStepAfter:                         ctx.Uint64(utils.DebugStepAfter.Name),
		PoolManagerUrl:                         ctx.String(utils.PoolManagerUrl.Name),
		TxPoolRejectSmartContractDeployments:   ctx.Bool(utils.TxPoolRejectSmartContractDeployments.Name),
		DisableVirtualCounters:                 ctx.Bool(utils.DisableVirtualCounters.Name),
		ExecutorPayloadOutput:                  ctx.String(utils.ExecutorPayloadOutput.Name),
		DAUrl:                                  ctx.String(utils.DAUrl.Name),
		DataStreamHost:                         ctx.String(utils.DataStreamHost.Name),
		DataStreamPort:                         ctx.Uint(utils.DataStreamPort.Name),
		DataStreamWriteTimeout:                 ctx.Duration(utils.DataStreamWriteTimeout.Name),
		DataStreamInactivityTimeout:            ctx.Duration(utils.DataStreamInactivityTimeout.Name),
		VirtualCountersSmtReduction:            ctx.Float64(utils.VirtualCountersSmtReduction.Name),
		BadBatches:                             badBatches,
		InitialBatchCfgFile:                    ctx.String(utils.InitialBatchCfgFile.Name),
		ACLPrintHistory:                        ctx.Int(utils.ACLPrintHistory.Name),
		InfoTreeUpdateInterval:                 ctx.Duration(utils.InfoTreeUpdateInterval.Name),
		SealBatchImmediatelyOnOverflow:         ctx.Bool(utils.SealBatchImmediatelyOnOverflow.Name),
	}

	utils2.EnableTimer(cfg.DebugTimers)

	checkFlag(utils.L2ChainIdFlag.Name, cfg.L2ChainId)
	if !sequencer.IsSequencer() {
		checkFlag(utils.L2RpcUrlFlag.Name, cfg.Zk.L2RpcUrl)
		checkFlag(utils.L2DataStreamerUrlFlag.Name, cfg.L2DataStreamerUrl)
	} else {
		checkFlag(utils.ExecutorUrls.Name, cfg.ExecutorUrls)
		checkFlag(utils.ExecutorStrictMode.Name, cfg.ExecutorStrictMode)
		checkFlag(utils.DataStreamHost.Name, cfg.DataStreamHost)
		checkFlag(utils.DataStreamPort.Name, cfg.DataStreamPort)
		checkFlag(utils.DataStreamWriteTimeout.Name, cfg.DataStreamWriteTimeout)

		if cfg.DeprecatedTxPool.Disable {
			panic("You need tx-pool in order to run a sequencer. Enable it using txpool.disable: false")
		}

		// if we are running in strict mode, the default, and we have no executor URLs then we panic
		if cfg.ExecutorStrictMode && !cfg.HasExecutors() {
			panic("You must set executor urls when running in executor strict mode (zkevm.executor-strict)")
		}

		if cfg.ExecutorStrictMode && cfg.DisableVirtualCounters {
			panic("You cannot disable virtual counters when running in strict mode")
		}

		if len(cfg.ExecutorUrls) > 0 && cfg.ExecutorUrls[0] != "" && cfg.DisableVirtualCounters {
			panic("You cannot disable virtual counters when running with executors")
		}
	}

	checkFlag(utils.AddressZkevmFlag.Name, cfg.AddressZkevm)

	checkFlag(utils.L1ChainIdFlag.Name, cfg.L1ChainId)
	checkFlag(utils.L1RpcUrlFlag.Name, cfg.L1RpcUrl)
	checkFlag(utils.L1MaticContractAddressFlag.Name, cfg.L1MaticContractAddress.Hex())
	checkFlag(utils.L1FirstBlockFlag.Name, cfg.L1FirstBlock)
	checkFlag(utils.RpcGetBatchWitnessConcurrencyLimitFlag.Name, cfg.RpcGetBatchWitnessConcurrencyLimit)
	checkFlag(utils.RebuildTreeAfterFlag.Name, cfg.RebuildTreeAfter)
	checkFlag(utils.L1BlockRangeFlag.Name, cfg.L1BlockRange)
	checkFlag(utils.L1QueryDelayFlag.Name, cfg.L1QueryDelay)
	checkFlag(utils.TxPoolRejectSmartContractDeployments.Name, cfg.TxPoolRejectSmartContractDeployments)
	checkFlag(utils.L1ContractAddressCheckFlag.Name, cfg.L1ContractAddressCheck)
	checkFlag(utils.L1ContractAddressRetrieveFlag.Name, cfg.L1ContractAddressCheck)
}
