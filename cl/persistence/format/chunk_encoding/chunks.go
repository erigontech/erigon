package chunk_encoding

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ledgerwatch/erigon/cl/utils"
)

type DataType int

const (
	ChunkDataType   DataType = 0
	PointerDataType DataType = 1
)

// writeChunk writes a chunk to the writer.
func WriteChunk(w io.Writer, buf []byte, t DataType, snappy bool) error {
	if snappy {
		buf = utils.CompressSnappy(buf)
	}
	// prefix is type of chunk + length of chunk
	prefix := make([]byte, 8)
	binary.BigEndian.PutUint64(prefix, uint64(len(buf)))
	prefix[0] = byte(t)
	if _, err := w.Write(prefix); err != nil {
		return err
	}
	if _, err := w.Write(buf); err != nil {
		return err
	}
	return nil
}

func ReadChunk(r io.Reader, snappy bool) (buf []byte, t DataType, err error) {
	prefix := make([]byte, 8)
	if _, err := r.Read(prefix); err != nil {
		return nil, DataType(0), err
	}
	t = DataType(prefix[0])
	prefix[0] = 0
	fmt.Println(binary.BigEndian.Uint64(prefix))
	buf = make([]byte, binary.BigEndian.Uint64(prefix))
	if _, err := r.Read(buf); err != nil {
		return nil, t, err
	}
	if snappy {
		buf, err = utils.DecompressSnappy(buf)
	}
	return buf, t, err
}
