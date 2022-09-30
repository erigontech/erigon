package utils

import (
	"encoding/binary"
	"encoding/hex"
)

func Uint32ToBytes4(n uint32) (ret [4]byte) {
	binary.BigEndian.PutUint32(ret[:], n)
	return
}

func BytesToBytes4(b []byte) (ret [4]byte) {
	copy(ret[:], b)
	return
}

func BytesToHex(b []byte) string {
	return hex.EncodeToString(b)
}
