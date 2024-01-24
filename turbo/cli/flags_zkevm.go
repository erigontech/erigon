package cli

import (
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/urfave/cli/v2"
	"fmt"
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

	cfg.Zk = &ethconfig.Zk{
		L2ChainId:                   ctx.Uint64(utils.L2ChainIdFlag.Name),
		L2RpcUrl:                    ctx.String(utils.L2RpcUrlFlag.Name),
		L2DataStreamerUrl:           ctx.String(utils.L2DataStreamerUrlFlag.Name),
		L1ChainId:                   ctx.Uint64(utils.L1ChainIdFlag.Name),
		L1RpcUrl:                    ctx.String(utils.L1RpcUrlFlag.Name),
		L1ContractAddress:           libcommon.HexToAddress(ctx.String(utils.L1ContractAddressFlag.Name)),
		L1MaticContractAddress:      libcommon.HexToAddress(ctx.String(utils.L1MaticContractAddressFlag.Name)),
		L1GERManagerContractAddress: libcommon.HexToAddress(ctx.String(utils.L1GERManagerContractAddressFlag.Name)),
		L1FirstBlock:                ctx.Uint64(utils.L1FirstBlockFlag.Name),
		RpcRateLimits:               ctx.Int(utils.RpcRateLimitsFlag.Name),
		RebuildTreeAfter:            ctx.Uint64(utils.RebuildTreeAfterFlag.Name),
		L1BlockRange:                ctx.Uint64(utils.L1BlockRangeFlag.Name),
		L1QueryDelay:                ctx.Uint64(utils.L1QueryDelayFlag.Name),
	}

	checkFlag(utils.L2ChainIdFlag.Name, cfg.Zk.L2ChainId)
	if !sequencer.IsSequencer() {
		checkFlag(utils.L2RpcUrlFlag.Name, cfg.Zk.L2RpcUrl)
		checkFlag(utils.L2DataStreamerUrlFlag.Name, cfg.Zk.L2DataStreamerUrl)
	}
	checkFlag(utils.L1ChainIdFlag.Name, cfg.Zk.L1ChainId)
	checkFlag(utils.L1RpcUrlFlag.Name, cfg.Zk.L1RpcUrl)
	checkFlag(utils.L1ContractAddressFlag.Name, cfg.Zk.L1ContractAddress.Hex())
	checkFlag(utils.L1MaticContractAddressFlag.Name, cfg.Zk.L1MaticContractAddress.Hex())
	checkFlag(utils.L1GERManagerContractAddressFlag.Name, cfg.Zk.L1GERManagerContractAddress.Hex())
	checkFlag(utils.L1FirstBlockFlag.Name, cfg.Zk.L1FirstBlock)
	checkFlag(utils.RpcRateLimitsFlag.Name, cfg.Zk.RpcRateLimits)
	checkFlag(utils.RebuildTreeAfterFlag.Name, cfg.Zk.RebuildTreeAfter)
	checkFlag(utils.L1BlockRangeFlag.Name, cfg.Zk.L1BlockRange)
	checkFlag(utils.L1QueryDelayFlag.Name, cfg.Zk.L1QueryDelay)
}
