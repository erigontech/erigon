package jsonrpc

import (
	"github.com/erigontech/erigon-lib/common"
	types "github.com/erigontech/erigon/zk/rpcdaemon"
)

type ZkExitRoots struct {
	BlockNumber     types.ArgUint64 `json:"blockNumber"`
	Timestamp       types.ArgUint64 `json:"timestamp"`
	MainnetExitRoot common.Hash     `json:"mainnetExitRoot"`
	RollupExitRoot  common.Hash     `json:"rollupExitRoot"`
}
