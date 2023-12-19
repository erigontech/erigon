package sync

import (
	"errors"
	"math/big"

	"github.com/ledgerwatch/erigon/core/types"
)

var (
	peerMissingBlocksErr = errors.New("peer missing blocks")
	badPeerErr           = errors.New("bad peer")
)

type p2pLayer interface {
	PeerCount() int
	DownloadBlocks(fromBlockNum *big.Int, toBlockNum *big.Int, peerIndex int) ([]*types.Block, error)
	Penalize(peerIndex int)
}
