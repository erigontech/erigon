package jsonrpc

import (
	"github.com/ledgerwatch/erigon-lib/common"
	types "github.com/ledgerwatch/erigon/zk/rpcdaemon"
)

type ZkExitRoots struct {
	BlockNumber     types.ArgUint64 `json:"blockNumber"`
	Timestamp       types.ArgUint64 `json:"timestamp"`
	MainnetExitRoot common.Hash     `json:"mainnetExitRoot"`
	RollupExitRoot  common.Hash     `json:"rollupExitRoot"`
}
