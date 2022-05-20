package iavl

import (
	"bytes"
	"sort"
)

func maxInt8(a, b int8) int8 {
	if a > b {
		return a
	}
	return b
}

func cp(bz []byte) (ret []byte) {
	ret = make([]byte, len(bz))
	copy(ret, bz)
	return ret
}

// Returns a slice of the same length (big endian)
// except incremented by one.
// Appends 0x00 if bz is all 0xFF.
// CONTRACT: len(bz) > 0
func cpIncr(bz []byte) (ret []byte) {
	ret = cp(bz)
	for i := len(bz) - 1; i >= 0; i-- {
		if ret[i] < byte(0xFF) {
			ret[i]++
			return
		}
		ret[i] = byte(0x00)
		if i == 0 {
			return append(ret, 0x00)
		}
	}
	return []byte{0x00}
}

type byteslices [][]byte

func (bz byteslices) Len() int {
	return len(bz)
}

func (bz byteslices) Less(i, j int) bool {
	switch bytes.Compare(bz[i], bz[j]) {
	case -1:
		return true
	case 0, 1:
		return false
	default:
		panic("should not happen")
	}
}

func (bz byteslices) Swap(i, j int) {
	bz[j], bz[i] = bz[i], bz[j]
}

func sortByteSlices(src [][]byte) [][]byte {
	bzz := byteslices(src)
	sort.Sort(bzz)
	return bzz
}
