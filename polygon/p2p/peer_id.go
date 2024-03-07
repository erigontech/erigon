package p2p

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	erigonlibtypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
)

func PeerIdFromH512(h512 *erigonlibtypes.H512) *PeerId {
	peerId := PeerId(gointerfaces.ConvertH512ToHash(h512))
	return &peerId
}

// PeerIdFromUint64 is useful for testing and that is its main intended purpose
func PeerIdFromUint64(num uint64) *PeerId {
	peerId := PeerId{}
	binary.BigEndian.PutUint64(peerId[:8], num)
	return &peerId
}

type PeerId [64]byte

func (pid *PeerId) H512() *erigonlibtypes.H512 {
	return gointerfaces.ConvertHashToH512(*pid)
}

func (pid *PeerId) String() string {
	return hex.EncodeToString(pid[:])
}

func (pid *PeerId) Equal(other *PeerId) bool {
	return *pid == *other
}
