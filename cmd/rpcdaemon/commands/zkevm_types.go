package commands

import (
	types "github.com/ledgerwatch/erigon/zk/rpcdaemon"
	"github.com/gateway-fm/cdk-erigon-lib/common"
)

type ZkExitRoots struct {
	BlockNumber     types.ArgUint64 `json:"blockNumber"`
	Timestamp       types.ArgUint64 `json:"timestamp"`
	MainnetExitRoot common.Hash     `json:"mainnetExitRoot"`
	RollupExitRoot  common.Hash     `json:"rollupExitRoot"`
}
