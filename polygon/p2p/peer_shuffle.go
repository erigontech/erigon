package p2p

import "math/rand"

type PeerShuffle func(peerIds []*PeerId)

func RandPeerShuffle(peerIds []*PeerId) {
	rand.Shuffle(len(peerIds), func(i, j int) {
		peerIds[i], peerIds[j] = peerIds[j], peerIds[i]
	})
}

func PreservingPeerShuffle(_ []*PeerId) {
	// no-op
}
