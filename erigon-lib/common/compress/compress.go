package compress

import (
	"github.com/golang/snappy"
)

// growslice ensures b has the wanted length by either expanding it to its capacity
// or allocating a new slice if b has insufficient capacity.
func growslice(b []byte, wantLength int) []byte {
	if len(b) >= wantLength {
		return b
	}
	if cap(b) >= wantLength {
		return b[:cap(b)]
	}
	return make([]byte, wantLength)
}

func EncodeSnappyIfNeed(buf, v []byte, enabled bool) ([]byte, []byte) {
	if !enabled {
		return buf, v
	}
	buf = snappy.Encode(growslice(buf, snappy.MaxEncodedLen(len(v))), v)
	return buf, buf
}

func DecodeSnappyIfNeed(buf, v []byte, enabled bool) ([]byte, []byte, error) {
	if !enabled {
		return buf, v, nil
	}
	actualSize, err := snappy.DecodedLen(v)
	if err != nil {
		return buf, nil, err
	}
	buf, err = snappy.Decode(growslice(buf, actualSize), v)
	if err != nil {
		return buf, nil, err
	}
	return buf, buf, nil
}
