package p2p

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
)

func PeerIdFromH512(pid *types.H512) PeerId {
	return gointerfaces.ConvertH512ToHash(pid)
}

// PeerIdFromUint64 is useful for testing and that is its main intended purpose
func PeerIdFromUint64(pid uint64) PeerId {
	var pidBytes [64]byte
	binary.BigEndian.PutUint64(pidBytes[:8], pid)
	return pidBytes
}

type PeerId [64]byte

func (pid PeerId) H512() *types.H512 {
	return gointerfaces.ConvertHashToH512(pid)
}

func (pid PeerId) String() string {
	return hex.EncodeToString(pid[:])[:20]
}
