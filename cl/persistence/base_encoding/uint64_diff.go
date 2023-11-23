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
func ComputeCompressedSerializedUint64ListDiff(old, new []uint64, out []byte) ([]byte, error) {
	if len(old) > len(new) {
		return nil, fmt.Errorf("old list is longer than new list")
	}

	// get a temporary buffer from the pool
	buffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buffer)
	buffer.Reset()
	bytes8 := make([]byte, 8)

	compressor := compressorPool.Get().(*zlib.Writer)
	defer compressorPool.Put(compressor)
	compressor.Reset(buffer)

	for i := 0; i < len(new); i++ {
		if i >= len(old) {
			binary.BigEndian.PutUint64(bytes8, new[i])
			if _, err := compressor.Write(bytes8); err != nil {
				return nil, err
			}
			continue
		}
		//binary.BigEndian.PutUint64(dst[i*8:], new[i]-old[i])
		binary.BigEndian.PutUint64(bytes8, new[i]-old[i])
		if _, err := compressor.Write(bytes8); err != nil {
			return nil, err
		}
	}

	if err := compressor.Flush(); err != nil {
		return nil, err
	}
	bufLen := buffer.Len()
	if bufLen > cap(out) {
		out = make([]byte, bufLen)
	}
	out = out[:bufLen]
	copy(out, buffer.Bytes())
	return out, nil
}
