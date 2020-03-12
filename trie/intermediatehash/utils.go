package intermediatehash

import (
	"bytes"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
)

// IsBefore - kind of bytes.Compare, but nil is the last key. And return
func IsBefore(triePrefix, accOrStorageKey []byte) (bool, []byte) {
	if triePrefix == nil {
		return false, accOrStorageKey
	}

	if accOrStorageKey == nil {
		return true, triePrefix
	}

	switch CmpWithoutIncarnation(triePrefix, accOrStorageKey) {
	case -1, 0:
		return true, triePrefix
	default:
		return false, accOrStorageKey
	}
}

// CmpWithoutIncarnation - removing incarnation from 2nd key if necessary
func CmpWithoutIncarnation(triePrefix, accOrStorageKey []byte) int {
	if triePrefix == nil {
		return 1
	}

	if accOrStorageKey == nil {
		return -1
	}

	if len(accOrStorageKey) <= common.HashLength {
		return bytes.Compare(triePrefix, accOrStorageKey)
	}

	if len(accOrStorageKey) <= common.HashLength+8 {
		return bytes.Compare(triePrefix, accOrStorageKey[:common.HashLength])
	}

	buf := pool.GetBuffer(256)
	defer pool.PutBuffer(buf)
	buf.B = append(buf.B[:0], accOrStorageKey[:common.HashLength]...)
	buf.B = append(buf.B, accOrStorageKey[common.HashLength+8:]...)

	return bytes.Compare(triePrefix, buf.B)
}

// NextSubtree does []byte++. Returns false if overflow.
func NextSubtree(in []byte) ([]byte, bool) {
	r := make([]byte, len(in))
	copy(r, in)
	for i := len(r) - 1; i >= 0; i-- {
		if r[i] != 255 {
			r[i]++
			return r, true
		}

		r[i] = 0
	}
	return nil, false
}
