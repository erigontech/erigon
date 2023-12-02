package base_encoding

import (
	"encoding/binary"
)

func Encode64ToBytes4(x uint64) (out []byte) {
	// little endian
	out = make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(x))
	return
}

func Decode64FromBytes4(buf []byte) (x uint64) {
	// little endian
	return uint64(binary.BigEndian.Uint32(buf))
}
