package dbutils

import (
	"bytes"
)

// EncodeTimestamp has the property: if a < b, then Encoding(a) < Encoding(b) lexicographically
func EncodeTimestamp(timestamp uint64) []byte {
	var suffix []byte
	var limit uint64 = 32

	for bytecount := 1; bytecount <= 8; bytecount++ {
		if timestamp < limit {
			suffix = make([]byte, bytecount)
			b := timestamp
			for i := bytecount - 1; i > 0; i-- {
				suffix[i] = byte(b & 0xff)
				b >>= 8
			}
			suffix[0] = byte(b) | (byte(bytecount) << 5) // 3 most significant bits of the first byte are bytecount
			break
		}
		limit <<= 8
	}
	return suffix
}

func DecodeTimestamp(suffix []byte) (uint64, []byte) {
	bytecount := int(suffix[0] >> 5)
	timestamp := uint64(suffix[0] & 0x1f)
	for i := 1; i < bytecount; i++ {
		timestamp = (timestamp << 8) | uint64(suffix[i])
	}
	return timestamp, suffix[bytecount:]
}

func ChangeSetByIndexBucket(b []byte) []byte {
	if bytes.Equal(b, AccountsHistoryBucket) {
		return AccountChangeSetBucket
	}
	if bytes.Equal(b, StorageHistoryBucket) {
		return StorageChangeSetBucket
	}
	panic("wrong bucket")
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

func NextS(in []byte, out *[]byte) bool {
	tmp := *out
	if cap(tmp) < len(in) {
		tmp = make([]byte, len(in))
	}
	tmp = tmp[:len(in)]
	copy(tmp, in)
	for i := len(tmp) - 1; i >= 0; i-- {
		if tmp[i] != 255 {
			tmp[i]++
			*out = tmp
			return true
		}
		tmp[i] = 0
	}
	*out = tmp
	return false
}
