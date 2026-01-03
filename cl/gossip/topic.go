package gossip

import (
	"fmt"

	"github.com/erigontech/erigon/common"
)

type GossipTopic struct {
	ForkDigest common.Bytes4
	Name       string
	CodecStr   string
}

func (t *GossipTopic) Topic() string {
	return fmt.Sprintf("/eth2/%x/%s/%s", t.ForkDigest, t.Name, t.CodecStr)
}
