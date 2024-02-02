package ethconfig

import "github.com/ledgerwatch/erigon-lib/common"

type Zk struct {
	L2ChainId                   uint64
	L2RpcUrl                    string
	L2DataStreamerUrl           string
	L1ChainId                   uint64
	L1RpcUrl                    string
	L1PolygonRollupManager      common.Address
	L1Rollup                    common.Address
	L1TopicVerification         common.Hash
	L1TopicSequence             common.Hash
	L1BlockRange                uint64
	L1QueryDelay                uint64
	L1MaticContractAddress      common.Address
	L1GERManagerContractAddress common.Address
	L1FirstBlock                uint64
	RpcRateLimits               int
	DatastreamVersion           int

	RebuildTreeAfter uint64
}
