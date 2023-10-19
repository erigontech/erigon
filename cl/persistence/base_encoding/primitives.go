package base_encoding

import (
	"math/bits"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

// Encode64 takes x, the uint to encode, and buf, the buffer to write to.
func Encode64(x uint64) (out []byte) {
	l := libcommon.BitLenToByteLen(bits.Len64(x))
	out = make([]byte, l)
	return Encode64InPlace(x, out)
}

// Encode64 takes x, the uint to encode, and buf, the buffer to write to.
func Encode64InPlace(x uint64, buf []byte) (out []byte) {
	l := libcommon.BitLenToByteLen(bits.Len64(x))
	out = buf[:l]

	for i := l; i > 0; i-- {
		out[i-1] = byte(x)
		x >>= 8
	}
	return out
}

// Encode64 takes x, the uint to encode, and buf, the buffer to write to.
func Decode64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}
