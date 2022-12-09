package state

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon/common"
)

// Uint64Root retrieve thr root of uint64 fields
func Uint64Root(val uint64) common.Hash {
	var root common.Hash
	binary.LittleEndian.PutUint64(root[:], val)
	return root
}
