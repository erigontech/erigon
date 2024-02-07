package p2p

import (
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
)

func PeerIdFromH512(h512 *types.H512) PeerId {
	return gointerfaces.ConvertH512ToHash(h512)
}

type PeerId [64]byte

func (pid PeerId) H512() *types.H512 {
	return gointerfaces.ConvertHashToH512(pid)
}
