package cli

import (
	"fmt"

	"strings"

	"time"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/urfave/cli/v2"
)

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
		}
	}

	l2DataStreamTimeoutVal := ctx.String(utils.L2DataStreamerTimeout.Name)
	l2DataStreamTimeout, err := time.ParseDuration(l2DataStreamTimeoutVal)
	if err != nil {
		panic(fmt.Sprintf("could not parse l2 datastreamer timeout value %s", l2DataStreamTimeoutVal))
	}

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

	cfg.Zk = &ethconfig.Zk{
		L2ChainId:                  ctx.Uint64(utils.L2ChainIdFlag.Name),
		L2RpcUrl:                   ctx.String(utils.L2RpcUrlFlag.Name),
		L2DataStreamerUrl:          ctx.String(utils.L2DataStreamerUrlFlag.Name),
		L2DataStreamerTimeout:      l2DataStreamTimeout,
		L1ChainId:                  ctx.Uint64(utils.L1ChainIdFlag.Name),
		L1RpcUrl:                   ctx.String(utils.L1RpcUrlFlag.Name),
		AddressSequencer:           libcommon.HexToAddress(ctx.String(utils.AddressSequencerFlag.Name)),
		AddressAdmin:               libcommon.HexToAddress(ctx.String(utils.AddressAdminFlag.Name)),
		AddressRollup:              libcommon.HexToAddress(ctx.String(utils.AddressRollupFlag.Name)),
		AddressZkevm:               libcommon.HexToAddress(ctx.String(utils.AddressZkevmFlag.Name)),
		AddressGerManager:          libcommon.HexToAddress(ctx.String(utils.AddressGerManagerFlag.Name)),
		L1RollupId:                 ctx.Uint64(utils.L1RollupIdFlag.Name),
		L1BlockRange:               ctx.Uint64(utils.L1BlockRangeFlag.Name),
		L1QueryDelay:               ctx.Uint64(utils.L1QueryDelayFlag.Name),
		L1MaticContractAddress:     libcommon.HexToAddress(ctx.String(utils.L1MaticContractAddressFlag.Name)),
		L1FirstBlock:               ctx.Uint64(utils.L1FirstBlockFlag.Name),
		RpcRateLimits:              ctx.Int(utils.RpcRateLimitsFlag.Name),
		DatastreamVersion:          ctx.Int(utils.DatastreamVersionFlag.Name),
		RebuildTreeAfter:           ctx.Uint64(utils.RebuildTreeAfterFlag.Name),
		SequencerInitialForkId:     ctx.Uint64(utils.SequencerInitialForkId.Name),
		SequencerBlockSealTime:     sequencerBlockSealTime,
		SequencerBatchSealTime:     sequencerBatchSealTime,
		ExecutorUrls:               strings.Split(ctx.String(utils.ExecutorUrls.Name), ","),
		ExecutorStrictMode:         ctx.Bool(utils.ExecutorStrictMode.Name),
		AllowFreeTransactions:      ctx.Bool(utils.AllowFreeTransactions.Name),
		AllowPreEIP155Transactions: ctx.Bool(utils.AllowPreEIP155Transactions.Name),
		WitnessFull:                ctx.Bool(utils.WitnessFullFlag.Name),
		DebugLimit:                 ctx.Uint64(utils.DebugLimit.Name),
		DebugStep:                  ctx.Uint64(utils.DebugStep.Name),
		DebugStepAfter:             ctx.Uint64(utils.DebugStepAfter.Name),
	}

	checkFlag(utils.L2ChainIdFlag.Name, cfg.Zk.L2ChainId)
	if !sequencer.IsSequencer() {
		checkFlag(utils.L2RpcUrlFlag.Name, cfg.Zk.L2RpcUrl)
		checkFlag(utils.L2DataStreamerUrlFlag.Name, cfg.Zk.L2DataStreamerUrl)
		checkFlag(utils.L2DataStreamerTimeout.Name, cfg.Zk.L2DataStreamerTimeout)
	} else {
		checkFlag(utils.SequencerInitialForkId.Name, cfg.Zk.SequencerInitialForkId)
		checkFlag(utils.ExecutorUrls.Name, cfg.Zk.ExecutorUrls)
		checkFlag(utils.ExecutorStrictMode.Name, cfg.Zk.ExecutorStrictMode)

		// if we are running in strict mode, the default, and we have no executor URLs then we panic
		if cfg.Zk.ExecutorStrictMode && (len(cfg.Zk.ExecutorUrls) == 0 || cfg.Zk.ExecutorUrls[0] == "") {
			panic("You must set executor urls when running in executor strict mode (zkevm.executor-strict)")
		}
	}

	checkFlag(utils.AddressSequencerFlag.Name, cfg.Zk.AddressSequencer)
	checkFlag(utils.AddressAdminFlag.Name, cfg.Zk.AddressAdmin)
	checkFlag(utils.AddressRollupFlag.Name, cfg.Zk.AddressRollup)
	checkFlag(utils.AddressZkevmFlag.Name, cfg.Zk.AddressZkevm)
	checkFlag(utils.AddressGerManagerFlag.Name, cfg.Zk.AddressGerManager)

	checkFlag(utils.L1ChainIdFlag.Name, cfg.Zk.L1ChainId)
	checkFlag(utils.L1RpcUrlFlag.Name, cfg.Zk.L1RpcUrl)
	checkFlag(utils.L1MaticContractAddressFlag.Name, cfg.Zk.L1MaticContractAddress.Hex())
	checkFlag(utils.L1FirstBlockFlag.Name, cfg.Zk.L1FirstBlock)
	checkFlag(utils.RpcRateLimitsFlag.Name, cfg.Zk.RpcRateLimits)
	checkFlag(utils.RebuildTreeAfterFlag.Name, cfg.Zk.RebuildTreeAfter)
	checkFlag(utils.L1BlockRangeFlag.Name, cfg.Zk.L1BlockRange)
	checkFlag(utils.L1QueryDelayFlag.Name, cfg.Zk.L1QueryDelay)
}
