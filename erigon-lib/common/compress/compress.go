package compress

import (
	"fmt"

	"github.com/golang/snappy"
)

// growslice ensures b has the wanted length by either expanding it to its capacity
// or allocating a new slice if b has insufficient capacity.
func growslice(b []byte, wantLength int) []byte {
	if cap(b) >= wantLength {
		return b[:wantLength]
	}
	return make([]byte, wantLength)
}

func EncodeSnappyIfNeed(buf, v []byte, enabled bool) ([]byte, []byte) {
	if !enabled {
		return buf, v
	}
	buf = growslice(buf, snappy.MaxEncodedLen(len(v)))
	buf = snappy.Encode(buf, v)
	return buf, buf
}

const maxUint24 = int(^uint32(0) >> 8)

func DecodeSnappyIfNeed(buf, v []byte, enabled bool) ([]byte, []byte, error) {
	if !enabled {
		return buf, v, nil
	}
	actualSize, err := snappy.DecodedLen(v)
	if err != nil {
		return buf, nil, fmt.Errorf("snappy.decode1: %w", err)
	}
	if actualSize > maxUint24 {
		return buf, nil, fmt.Errorf("snappy.decode2: too large msg: %d", actualSize)
	}
	//buf = growslice(buf, actualSize)
	buf, err = snappy.Decode(nil, v) //todo: `erigon seg decompress` doesn't work if use buffer
	if err != nil {
		return buf, nil, fmt.Errorf("snappy.decode3: %w", err)
	}
	return buf, buf, nil
}
