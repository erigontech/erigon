package cli

import (
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/turbo/logging"
	"github.com/erigontech/erigon/zk/sequencer"
	utils2 "github.com/erigontech/erigon/zk/utils"

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

	verifyAddressFlag := func(name, value string) string {
		if strings.Count(value, ":") == 0 {
			return value
		}
		_, _, err := net.SplitHostPort(value)
		if err != nil {
			panic(fmt.Sprintf("invalid address for flag %s: %s", name, value))
		}
		return value
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

	var sequencerEmptyBlockSealTime time.Duration
	sequencerEmptyBlockSealTimeVal := ctx.String(utils.SequencerEmptyBlockSealTime.Name)
	if sequencerEmptyBlockSealTimeVal == "" {
		sequencerEmptyBlockSealTime = sequencerBlockSealTime
	} else {
		sequencerEmptyBlockSealTime, err = time.ParseDuration(sequencerEmptyBlockSealTimeVal)
		if err != nil {
			panic(fmt.Sprintf("could not parse sequencer empty block seal time timeout value %s", sequencerEmptyBlockSealTimeVal))
		}
		if sequencerEmptyBlockSealTime < sequencerBlockSealTime {
			panic(fmt.Sprintf("sequencer empty block seal time (%s) must be greater than or equal to sequencer block seal time (%s)", sequencerEmptyBlockSealTime, sequencerBlockSealTime))
		}
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
	witnessUnwindLimit := ctx.Uint64(utils.WitnessUnwindLimit.Name)

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

	// witness cache flags
	// if dicabled, set limit to 0 and only check for it to be 0 or not
	witnessCacheEnabled := ctx.Bool(utils.WitnessCacheEnable.Name)
	witnessCachePurge := ctx.Bool(utils.WitnessCachePurge.Name)
	var witnessInclusion []libcommon.Address
	for _, s := range strings.Split(ctx.String(utils.WitnessContractInclusion.Name), ",") {
		if s == "" {
			// if there are no entries then we can just ignore it and move on
			continue
		}
		witnessInclusion = append(witnessInclusion, libcommon.HexToAddress(s))
	}

	logLevel, lErr := logging.TryGetLogLevel(ctx.String(logging.LogConsoleVerbosityFlag.Name))
	if lErr != nil {
		// try verbosity flag
		logLevel, lErr = logging.TryGetLogLevel(ctx.String(logging.LogVerbosityFlag.Name))
		if lErr != nil {
			logLevel = log.LvlInfo
		}
	}

	hardfork := ethconfig.Hardfork(ctx.String(utils.Hardfork.Name))
	if !hardfork.IsValid() {
		panic(fmt.Sprintf("Invalid hardfork: %s. Must be one of: %s", ctx.String(utils.Hardfork.Name), hardfork.ValidHardforks()))
	}

	commitment := ethconfig.Commitment(ctx.String(utils.Commitment.Name))
	if !commitment.IsValid() {
		panic(fmt.Sprintf("Invalid commitment: %s. Must be one of: %s", ctx.String(utils.Commitment.Name), commitment.ValidCommitments()))
	}

	var l1InfoTreeOffset *ethconfig.L1InfoTreeOffset
	infoTreeOffsetStr := ctx.String(utils.SequencerResequenceInfoTreeOffset.Name)
	if infoTreeOffsetStr != "" {
		parts := strings.Split(infoTreeOffsetStr, ":")
		if len(parts) != 3 {
			panic(fmt.Sprintf("Invalid info tree offset format: %s, should be <index>:<offset>:<expected_ger_hash>", infoTreeOffsetStr))
		}
		index, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			panic(fmt.Sprintf("Invalid info tree offset format: %s", infoTreeOffsetStr))
		}
		offset, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			panic(fmt.Sprintf("Invalid info tree offset format: %s", infoTreeOffsetStr))
		}
		hashStr := parts[2]
		if !strings.HasPrefix(hashStr, "0x") {
			panic(fmt.Sprintf("Invalid info tree offset format: %s, expected_ger_hash should start with 0x", infoTreeOffsetStr))
		}
		if _, err := hex.DecodeString(hashStr[2:]); err != nil {
			panic(fmt.Sprintf("Invalid info tree offset format: %s, expected_ger_hash should be a valid hex string", infoTreeOffsetStr))
		}
		if len(hashStr) != 66 {
			panic(fmt.Sprintf("Invalid info tree offset format: %s, expected_ger_hash should be 66 characters long", infoTreeOffsetStr))
		}
		expectedGerHash := libcommon.HexToHash(hashStr)
		l1InfoTreeOffset = &ethconfig.L1InfoTreeOffset{
			Index:           index,
			Offset:          offset,
			ExpectedGerHash: expectedGerHash,
		}
	}

	cfg.Zk = &ethconfig.Zk{
		L2ChainId:                              ctx.Uint64(utils.L2ChainIdFlag.Name),
		L2RpcUrl:                               ctx.String(utils.L2RpcUrlFlag.Name),
		L2DataStreamerUrl:                      ctx.String(utils.L2DataStreamerUrlFlag.Name),
		L2DataStreamerMaxEntryChan:             ctx.Uint64(utils.L2DataStreamerMaxEntryChanFlag.Name),
		L2DataStreamerUseTLS:                   ctx.Bool(utils.L2DataStreamerUseTLSFlag.Name),
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
		RebuildTreeAfter:                       ctx.Uint64(utils.RebuildTreeAfterFlag.Name),
		IncrementTreeAlways:                    ctx.Bool(utils.IncrementTreeAlways.Name),
		SmtRegenerateInMemory:                  ctx.Bool(utils.SmtRegenerateInMemory.Name),
		SequencerBlockSealTime:                 sequencerBlockSealTime,
		SequencerEmptyBlockSealTime:            sequencerEmptyBlockSealTime,
		SequencerBatchSealTime:                 sequencerBatchSealTime,
		SequencerBatchVerificationTimeout:      sequencerBatchVerificationTimeout,
		SequencerBatchVerificationRetries:      ctx.Int(utils.SequencerBatchVerificationRetries.Name),
		SequencerTimeoutOnEmptyTxPool:          sequencerTimeoutOnEmptyTxPool,
		SequencerHaltOnBatchNumber:             ctx.Uint64(utils.SequencerHaltOnBatchNumber.Name),
		SequencerResequence:                    ctx.Bool(utils.SequencerResequence.Name),
		SequencerResequenceStrict:              ctx.Bool(utils.SequencerResequenceStrict.Name),
		SequencerResequenceReuseL1InfoIndex:    ctx.Bool(utils.SequencerResequenceReuseL1InfoIndex.Name),
		SequencerDecodedTxCacheSize:            ctx.Int(utils.SequencerDecodedTxCacheSize.Name),
		SequencerDecodedTxCacheTTL:             ctx.Duration(utils.SequencerDecodedTxCacheTTL.Name),
		SequencerResequenceInfoTreeOffset:      l1InfoTreeOffset,
		ExecutorUrls:                           strings.Split(strings.ReplaceAll(ctx.String(utils.ExecutorUrls.Name), " ", ""), ","),
		ExecutorStrictMode:                     ctx.Bool(utils.ExecutorStrictMode.Name),
		ExecutorRequestTimeout:                 ctx.Duration(utils.ExecutorRequestTimeout.Name),
		ExecutorEnabled:                        ctx.Bool(utils.ExecutorEnabled.Name),
		DatastreamNewBlockTimeout:              ctx.Duration(utils.DatastreamNewBlockTimeout.Name),
		WitnessMemdbSize:                       *witnessMemSize,
		WitnessUnwindLimit:                     witnessUnwindLimit,
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
		SyncLimitVerifiedEnabled:               ctx.Bool(utils.SyncLimitVerifiedEnabled.Name),
		SyncLimitUnverifiedCount:               ctx.Uint64(utils.SyncLimitUnverifiedCount.Name),
		DebugTimers:                            ctx.Bool(utils.DebugTimers.Name),
		DebugNoSync:                            ctx.Bool(utils.DebugNoSync.Name),
		DebugLimit:                             ctx.Uint64(utils.DebugLimit.Name),
		DebugStep:                              ctx.Uint64(utils.DebugStep.Name),
		DebugStepAfter:                         ctx.Uint64(utils.DebugStepAfter.Name),
		DebugDisableStateRootCheck:             ctx.Bool(utils.DebugDisableStateRootCheck.Name),
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
		IgnoreBadBatchesCheck:                  ctx.Bool(utils.IgnoreBadBatchesCheck.Name),
		InitialBatchCfgFile:                    ctx.String(utils.InitialBatchCfgFile.Name),
		ACLPrintHistory:                        ctx.Int(utils.ACLPrintHistory.Name),
		ACLJsonLocation:                        ctx.String(utils.ACLJsonLocation.Name),
		PrioritySendersJsonLocation:            ctx.String(utils.PrioritySendersJsonLocation.Name),
		InfoTreeUpdateInterval:                 ctx.Duration(utils.InfoTreeUpdateInterval.Name),
		SealBatchImmediatelyOnOverflow:         ctx.Bool(utils.SealBatchImmediatelyOnOverflow.Name),
		MockWitnessGeneration:                  ctx.Bool(utils.MockWitnessGeneration.Name),
		WitnessCacheEnabled:                    witnessCacheEnabled,
		WitnessCachePurge:                      witnessCachePurge,
		WitnessCacheBatchAheadOffset:           ctx.Uint64(utils.WitnessCacheBatchAheadOffset.Name),
		WitnessCacheBatchBehindOffset:          ctx.Uint64(utils.WitnessCacheBatchBehindOffset.Name),
		WitnessContractInclusion:               witnessInclusion,
		AlwaysGenerateBatchL2Data:              ctx.Bool(utils.AlwaysGenerateBatchL2Data.Name),
		GasPriceCheckFrequency:                 ctx.Duration(utils.GasPriceCheckFrequency.Name),
		GasPriceHistoryCount:                   ctx.Uint64(utils.GasPriceHistoryCount.Name),
		RejectLowGasPriceTransactions:          ctx.Bool(utils.RejectLowGasPriceTransactions.Name),
		RejectLowGasPriceTolerance:             ctx.Float64(utils.RejectLowGasPriceTolerance.Name),
		LogLevel:                               logLevel,
		PanicOnReorg:                           ctx.Bool(utils.PanicOnReorg.Name),
		ShadowSequencer:                        ctx.Bool(utils.ShadowSequencer.Name),
		BadTxAllowance:                         ctx.Uint64(utils.BadTxAllowance.Name),
		BadTxStoreValue:                        ctx.Uint64(utils.BadTxStoreValue.Name),
		BadTxPurge:                             ctx.Bool(utils.BadTxPurge.Name),
		L2InfoTreeUpdatesBatchSize:             ctx.Uint64(utils.L2InfoTreeUpdatesBatchSize.Name),
		L2InfoTreeUpdatesEnabled:               ctx.Bool(utils.L2InfoTreeUpdatesEnabled.Name),
		Hardfork:                               hardfork,
		Commitment:                             commitment,
		InjectGers:                             ctx.Bool(utils.InjectGers.Name),
	}

	utils2.EnableTimer(cfg.DebugTimers)

	checkFlag(utils.L2ChainIdFlag.Name, cfg.L2ChainId)
	if !sequencer.IsSequencer() {
		checkFlag(utils.L2RpcUrlFlag.Name, cfg.Zk.L2RpcUrl)
		checkFlag(utils.L2DataStreamerUrlFlag.Name, cfg.L2DataStreamerUrl)
	} else {
		checkFlag(utils.ExecutorUrls.Name, cfg.ExecutorUrls)
		checkFlag(utils.ExecutorStrictMode.Name, cfg.ExecutorStrictMode)
		checkFlag(utils.ExecutorEnabled.Name, cfg.ExecutorEnabled)
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

		if cfg.UseExecutors() && cfg.DisableVirtualCounters {
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

	verifyAddressFlag(utils.L2DataStreamerUrlFlag.Name, cfg.L2DataStreamerUrl)
}
