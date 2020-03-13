package dbutils

import "bytes"

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

// Cmp - like bytes.Compare, but nil - means "bucket over" and has highest order.
func Cmp(k1, k2 []byte) int {
	if k1 == nil && k2 == nil {
		return 0
	}
	if k1 == nil {
		return 1
	}
	if k2 == nil {
		return -1
	}

	return bytes.Compare(k1, k2)
}

// IsBefore - kind of bytes.Compare, but nil is the last key. And return
func IsBefore(k1, k2 []byte) (bool, []byte) {
	switch Cmp(k1, k2) {
	case -1, 0:
		return true, k1
	default:
		return false, k2
	}
}
