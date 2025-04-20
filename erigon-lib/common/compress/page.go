package compress

import (
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

func DecodeSnappyIfNeed(buf, v []byte, enabled bool) ([]byte, []byte, error) {
	if !enabled {
		return buf, v, nil
	}
	actualSize, err := snappy.DecodedLen(v)
	if err != nil {
		return buf, nil, err
	}
	buf = growslice(buf, actualSize)
	buf, err = snappy.Decode(buf, v)
	if err != nil {
		return buf, nil, err
	}
	return buf, buf, nil
}
