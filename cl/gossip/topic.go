package gossip

import (
	"github.com/erigontech/erigon/common"
)

type GossipTopic struct {
	ForkDigest common.Bytes4
	Name       string
	CodecStr   string
}
