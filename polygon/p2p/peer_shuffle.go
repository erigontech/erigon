package p2p

import "github.com/ledgerwatch/erigon-lib/common"

type PeerShuffle func(peerIds []*PeerId)

func RandPeerShuffle(peerIds []*PeerId) {
	common.SliceShuffle(peerIds)
}

func PreservingPeerShuffle(_ []*PeerId) {
	// no-op
}
