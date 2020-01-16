package ethdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Maximum length (in bytes of encoded timestamp)
const MaxTimestampLength = 8

func encodingLen8to7(b []byte) int { //nolint
	return (len(b)*8 + 6) / 7
}

// Transforms b into encoding where only
// 7 bits of each byte are used to encode the bits of b
// The most significant bit is left empty, for other purposes
func encode8to7(b []byte) []byte {
	// Calculate number of bytes in the output
	inbytes := len(b)
	outbytes := (inbytes*8 + 6) / 7
	out := make([]byte, outbytes)
	for i := 0; i < outbytes; i++ {
		out[i] = 0x80
	}
	outidx := 0
	for inidx := 0; inidx < inbytes; inidx++ {
		bb := b[inidx]
		switch inidx % 7 {
		case 0:
			out[outidx] |= bb >> 1
			out[outidx+1] |= (bb & 0x1) << 6
		case 1:
			out[outidx+1] |= (bb >> 2) & 0x3f
			out[outidx+2] |= (bb & 0x3) << 5
		case 2:
			out[outidx+2] |= (bb >> 3) & 0x1f
			out[outidx+3] |= (bb & 0x7) << 4
		case 3:
			out[outidx+3] |= (bb >> 4) & 0xf
			out[outidx+4] |= (bb & 0xf) << 3
		case 4:
			out[outidx+4] |= (bb >> 5) & 0x7
			out[outidx+5] |= (bb & 0x1f) << 2
		case 5:
			out[outidx+5] |= (bb >> 6) & 0x3
			out[outidx+6] |= (bb & 0x3f) << 1
		case 6:
			out[outidx+6] |= bb >> 7
			out[outidx+7] |= bb & 0x7f
			outidx += 8
		}
	}
	return out
}

func decode7to8(b []byte) []byte {
	inbytes := len(b)
	outbytes := inbytes * 7 / 8
	out := make([]byte, outbytes)
	inidx := 0
	for outidx := 0; outidx < outbytes; outidx++ {
		switch outidx % 7 {
		case 0:
			out[outidx] = ((b[inidx] & 0x7f) << 1) | ((b[inidx+1] >> 6) & 0x1)
		case 1:
			out[outidx] = ((b[inidx+1] & 0x3f) << 2) | ((b[inidx+2] >> 5) & 0x3)
		case 2:
			out[outidx] = ((b[inidx+2] & 0x1f) << 3) | ((b[inidx+3] >> 4) & 0x7)
		case 3:
			out[outidx] = ((b[inidx+3] & 0xf) << 4) | ((b[inidx+4] >> 3) & 0xf)
		case 4:
			out[outidx] = ((b[inidx+4] & 0x7) << 5) | ((b[inidx+5] >> 2) & 0x1f)
		case 5:
			out[outidx] = ((b[inidx+5] & 0x3) << 6) | ((b[inidx+6] >> 1) & 0x3f)
		case 6:
			out[outidx] = ((b[inidx+6] & 0x1) << 7) | (b[inidx+7] & 0x7f)
			inidx += 8
		}
	}
	return out
}

// addToChangeSet is not part the AccountChangeSet API, and it is only used in the test settings.
// In the production settings, ChangeSets encodings are never modified.
// In production settings (mutation.PutS) we always first populate AccountChangeSet object,
// then encode it once, and then only work with the encoding
func addToChangeSet(b []byte, key []byte, value []byte) ([]byte, error) {
	var m int
	var n int

	if len(b) == 0 {
		m = len(key)
		n = 0
	} else {
		n = int(binary.BigEndian.Uint32(b[0:4]))
		m = int(binary.BigEndian.Uint32(b[4:8]))
		if len(key) != m {
			return nil, fmt.Errorf("wrong key size in AccountChangeSet: expected %d, actual %d", m, len(key))
		}
	}
	pos := 4
	var buffer bytes.Buffer
	// Encode n
	intArr := make([]byte, 4)
	binary.BigEndian.PutUint32(intArr, uint32(n+1))
	buffer.Write(intArr)
	// KeySize should be the same
	if n == 0 {
		binary.BigEndian.PutUint32(intArr, uint32(len(key)))
		buffer.Write(intArr)
	} else {
		buffer.Write(b[pos : pos+4])
	}

	pos += 4
	// append key
	if n == 0 {
		buffer.Write(key)
		pos += len(key)
	} else {
		buffer.Write(b[pos : pos+n*m])
		buffer.Write(key)
		pos += n * m
	}
	// Append Index
	if n == 0 {
		binary.BigEndian.PutUint32(intArr, uint32(len(value)))
		buffer.Write(intArr)
	} else {
		buffer.Write(b[pos : pos+4*n])
		pos += 4 * n
		prev := int(binary.BigEndian.Uint32(b[pos-4 : pos]))
		binary.BigEndian.PutUint32(intArr, uint32(prev+len(value)))
		buffer.Write(intArr)
		buffer.Write(b[pos:])
	}
	// Append Value
	buffer.Write(value)
	return buffer.Bytes(), nil
}
