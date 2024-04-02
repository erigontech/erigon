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

	cfg.Zk = &ethconfig.Zk{
		L2ChainId:                   ctx.Uint64(utils.L2ChainIdFlag.Name),
		L2RpcUrl:                    ctx.String(utils.L2RpcUrlFlag.Name),
		L2DataStreamerUrl:           ctx.String(utils.L2DataStreamerUrlFlag.Name),
		L2DataStreamerTimeout:       l2DataStreamTimeout,
		L1ChainId:                   ctx.Uint64(utils.L1ChainIdFlag.Name),
		L1RpcUrl:                    ctx.String(utils.L1RpcUrlFlag.Name),
		L1PolygonRollupManager:      libcommon.HexToAddress(ctx.String(utils.L1PolygonRollupManagerFlag.Name)),
		L1Rollup:                    libcommon.HexToAddress(ctx.String(utils.L1RollupFlag.Name)),
		L1RollupId:                  ctx.Uint64(utils.L1RollupIdFlag.Name),
		L1TopicVerification:         libcommon.HexToHash(ctx.String(utils.L1TopicVerificationFlag.Name)),
		L1TopicSequence:             libcommon.HexToHash(ctx.String(utils.L1TopicSequenceFlag.Name)),
		L1BlockRange:                ctx.Uint64(utils.L1BlockRangeFlag.Name),
		L1QueryDelay:                ctx.Uint64(utils.L1QueryDelayFlag.Name),
		L1MaticContractAddress:      libcommon.HexToAddress(ctx.String(utils.L1MaticContractAddressFlag.Name)),
		L1GERManagerContractAddress: libcommon.HexToAddress(ctx.String(utils.L1GERManagerContractAddressFlag.Name)),
		L1FirstBlock:                ctx.Uint64(utils.L1FirstBlockFlag.Name),
		RpcRateLimits:               ctx.Int(utils.RpcRateLimitsFlag.Name),
		DatastreamVersion:           ctx.Int(utils.DatastreamVersionFlag.Name),
		RebuildTreeAfter:            ctx.Uint64(utils.RebuildTreeAfterFlag.Name),
		SequencerInitialForkId:      ctx.Uint64(utils.SequencerInitialForkId.Name),
		SequencerAddress:            libcommon.HexToAddress(ctx.String(utils.SequencerAddressFlag.Name)),
		ExecutorUrls:                strings.Split(ctx.String(utils.ExecutorUrls.Name), ","),
		ExecutorStrictMode:          ctx.Bool(utils.ExecutorStrictMode.Name),
		AllowFreeTransactions:       ctx.Bool(utils.AllowFreeTransactions.Name),
		AllowPreEIP155Transactions:  ctx.Bool(utils.AllowPreEIP155Transactions.Name),
		WitnessFull:                 ctx.Bool(utils.WitnessFullFlag.Name),
	}

	checkFlag(utils.L2ChainIdFlag.Name, cfg.Zk.L2ChainId)
	if !sequencer.IsSequencer() {
		checkFlag(utils.L2RpcUrlFlag.Name, cfg.Zk.L2RpcUrl)
		checkFlag(utils.L2DataStreamerUrlFlag.Name, cfg.Zk.L2DataStreamerUrl)
		checkFlag(utils.L2DataStreamerTimeout.Name, cfg.Zk.L2DataStreamerTimeout)
	} else {
		checkFlag(utils.SequencerInitialForkId.Name, cfg.Zk.SequencerInitialForkId)
		checkFlag(utils.SequencerAddressFlag.Name, cfg.Zk.SequencerAddress)
		checkFlag(utils.ExecutorUrls.Name, cfg.Zk.ExecutorUrls)
		checkFlag(utils.ExecutorStrictMode.Name, cfg.Zk.ExecutorStrictMode)

		// if we are running in strict mode, the default, and we have no executor URLs then we panic
		if cfg.Zk.ExecutorStrictMode && (len(cfg.Zk.ExecutorUrls) == 0 || cfg.Zk.ExecutorUrls[0] == "") {
			panic("You must set executor urls when running in executor strict mode (zkevm.executor-strict)")
		}
	}
	checkFlag(utils.L1ChainIdFlag.Name, cfg.Zk.L1ChainId)
	checkFlag(utils.L1RpcUrlFlag.Name, cfg.Zk.L1RpcUrl)
	checkFlag(utils.L1PolygonRollupManagerFlag.Name, cfg.Zk.L1PolygonRollupManager.Hex())
	checkFlag(utils.L1RollupFlag.Name, cfg.Zk.L1Rollup.Hex())
	checkFlag(utils.L1TopicVerificationFlag.Name, cfg.Zk.L1TopicVerification.Hex())
	checkFlag(utils.L1TopicSequenceFlag.Name, cfg.Zk.L1TopicSequence.Hex())
	checkFlag(utils.L1MaticContractAddressFlag.Name, cfg.Zk.L1MaticContractAddress.Hex())
	checkFlag(utils.L1GERManagerContractAddressFlag.Name, cfg.Zk.L1GERManagerContractAddress.Hex())
	checkFlag(utils.L1FirstBlockFlag.Name, cfg.Zk.L1FirstBlock)
	checkFlag(utils.RpcRateLimitsFlag.Name, cfg.Zk.RpcRateLimits)
	checkFlag(utils.RebuildTreeAfterFlag.Name, cfg.Zk.RebuildTreeAfter)
	checkFlag(utils.L1BlockRangeFlag.Name, cfg.Zk.L1BlockRange)
	checkFlag(utils.L1QueryDelayFlag.Name, cfg.Zk.L1QueryDelay)
}
