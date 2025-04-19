package compress

import (
	"bytes"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
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

func EncodeZstdIfNeed(buf, v []byte, enabled bool) ([]byte, []byte) {
	if !enabled {
		return buf, v
	}
	buf = growslice(buf, snappy.MaxEncodedLen(len(v)))
	bb := bytes.NewBuffer(buf)
	wr, err := zstd.NewWriter(
		bb,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(16), // we implicitly get this concurrency level if we run on 16 core CPU
		zstd.WithLowerEncoderMem(false),
		zstd.WithWindowSize(1<<20),
	)
	if err != nil {
		panic(err)
	}
	wr.Write(v)
	wr.Flush()
	wr.Close()
	buf = bb.Bytes()
	return buf, buf
}

func DecodeZstdIfNeed(buf, v []byte, enabled bool) ([]byte, []byte, error) {
	if !enabled {
		return buf, v, nil
	}
	actualSize, err := snappy.DecodedLen(v)
	if err != nil {
		return buf, nil, err
	}
	buf = growslice(buf, actualSize)
	r, err := zstd.NewReader(
		bytes.NewReader(v),
	)
	if err != nil {
		panic(err)
	}
	if _, err = io.ReadFull(r, buf); err != nil && err != io.ErrUnexpectedEOF {
		return nil, nil, err
	}
	if err != nil {
		return buf, nil, err
	}
	return buf, buf, nil
}
