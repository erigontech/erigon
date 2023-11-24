package base_encoding

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
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
func ComputeCompressedSerializedUint64ListDiff(w io.Writer, old, new []uint64) error {
	if len(old) > len(new) {
		return fmt.Errorf("old list is longer than new list")
	}

	bytes8 := make([]byte, 8)

	compressor := compressorPool.Get().(*zlib.Writer)
	defer compressorPool.Put(compressor)
	compressor.Reset(w)

	if err := binary.Write(w, binary.BigEndian, uint32(len(new))); err != nil {
		return err
	}

	for i := 0; i < len(new); i++ {
		if i >= len(old) {
			binary.BigEndian.PutUint64(bytes8, new[i])
			if _, err := compressor.Write(bytes8); err != nil {
				return err
			}
			continue
		}
		//binary.BigEndian.PutUint64(dst[i*8:], new[i]-old[i])
		binary.BigEndian.PutUint64(bytes8, new[i]-old[i])
		if _, err := compressor.Write(bytes8); err != nil {
			return err
		}
	}

	if err := compressor.Flush(); err != nil {
		return err
	}

	return nil
}

// ApplyCompressedSerializedUint64ListDiff applies the difference between two uint64 lists, it assumes the new list to be always greater in length than the old one.
func ApplyCompressedSerializedUint64ListDiff(old, out []uint64, diff []byte) ([]uint64, error) {
	out = out[:0]
	// get a temporary buffer from the pool
	buffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buffer)
	buffer.Reset()

	if _, err := buffer.Write(diff); err != nil {
		return nil, err
	}

	var length uint32
	if err := binary.Read(buffer, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	decompressor, err := zlib.NewReader(buffer)
	if err != nil {
		return nil, err
	}
	defer decompressor.Close()

	bytes8 := make([]byte, 8)
	for i := 0; i < int(length); i++ {
		if _, err := decompressor.Read(bytes8); err != nil {
			return nil, err
		}
		if i >= len(old) {
			out = append(out, binary.BigEndian.Uint64(bytes8))
			continue
		}
		fmt.Println("a")
		out = append(out, old[i]+binary.BigEndian.Uint64(bytes8))
	}

	return out, nil
}
