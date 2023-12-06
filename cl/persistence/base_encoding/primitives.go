package base_encoding

import (
	"encoding/binary"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
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

// IndexAndPeriodKey encodes index and period (can be epoch/slot/epoch period) into 8 bytes
func IndexAndPeriodKey(index, timeframe uint64) (out []byte) {
	out = make([]byte, 8)
	binary.BigEndian.PutUint32(out[:4], uint32(index))
	binary.BigEndian.PutUint32(out[4:], uint32(timeframe))
	return
}

// Encode a number with least amount of bytes
func EncodeCompactUint64(x uint64) (out []byte) {
	for x >= 0x80 {
		out = append(out, byte(x)|0x80)
		x >>= 7
	}
	out = append(out, byte(x))
	return
}

// DecodeCompactUint64 decodes a number encoded with EncodeCompactUint64
func DecodeCompactUint64(buf []byte) (x uint64) {
	for i := 0; i < len(buf); i++ {
		x |= uint64(buf[i]&0x7f) << (7 * uint(i))
		if buf[i]&0x80 == 0 {
			return
		}
	}
	return
}

func EncodePeriodAndRoot(period uint32, root libcommon.Hash) []byte {
	out := make([]byte, 36)
	binary.BigEndian.PutUint32(out[:4], period)
	copy(out[4:], root[:])
	return out
}
