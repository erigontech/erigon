package p2p

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
)

func PeerIdFromH512(peerId *types.H512) PeerId {
	return gointerfaces.ConvertH512ToHash(peerId)
}

// PeerIdFromUint64 is useful for testing and that is its main intended purpose
func PeerIdFromUint64(peerId uint64) PeerId {
	var peerIdBytes [64]byte
	binary.BigEndian.PutUint64(peerIdBytes[:8], peerId)
	return peerIdBytes
}

type PeerId [64]byte

func (pid PeerId) H512() *types.H512 {
	return gointerfaces.ConvertHashToH512(pid)
}

func (pid PeerId) String() string {
	return hex.EncodeToString(pid[:])
}
