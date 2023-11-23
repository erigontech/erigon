package base_encoding

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"sync"
)

// make a sync.pool of compressors (zlib)
var compressorPool = sync.Pool{
	New: func() interface{} {
		return zlib.NewWriter(nil)
	},
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

// ComputeSerializedUint64ListDiff computes the difference between two uint64 lists, it assumes the new list to be always greater in length than the old one.
func ComputeCompressedSerializedUint64ListDiff(old, new []uint64, dst []byte) ([]byte, error) {
	if len(old) > len(new) {
		return nil, fmt.Errorf("old list is longer than new list")
	}
	if cap(dst) < len(new)*8 {
		dst = make([]byte, len(new)*8)
	}
	dst = dst[:len(new)*8]
	for i := 0; i < len(new); i++ {
		if i >= len(old) {
			binary.BigEndian.PutUint64(dst[i*8:], new[i])
			continue
		}
		binary.BigEndian.PutUint64(dst[i*8:], new[i]-old[i])
	}
	// get a temporary buffer from the pool
	buffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buffer)
	buffer.Reset()

	compressor := compressorPool.Get().(*zlib.Writer)
	defer compressorPool.Put(compressor)
	compressor.Reset(buffer)

	if _, err := compressor.Write(dst); err != nil {
		return nil, err
	}
	if err := compressor.Flush(); err != nil {
		return nil, err
	}
	return dst, nil
}
