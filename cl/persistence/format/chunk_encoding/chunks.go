package chunk_encoding

import (
	"encoding/binary"
	"io"
)

type DataType int

const (
	ChunkDataType   DataType = 0
	PointerDataType DataType = 1
)

// writeChunk writes a chunk to the writer.
func WriteChunk(w io.Writer, buf []byte, t DataType) error {

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

func ReadChunk(r io.Reader, out io.Writer) (t DataType, err error) {
	prefix := make([]byte, 8)
	if _, err := r.Read(prefix); err != nil {
		return DataType(0), err
	}
	t = DataType(prefix[0])
	prefix[0] = 0

	bufLen := binary.BigEndian.Uint64(prefix)
	if bufLen == 0 {
		return
	}

	if _, err = io.CopyN(out, r, int64(bufLen)); err != nil {
		return
	}
	return
}

func ReadChunkToBytes(r io.Reader) (b []byte, t DataType, err error) {
	prefix := make([]byte, 8)
	if _, err := r.Read(prefix); err != nil {
		return nil, DataType(0), err
	}
	t = DataType(prefix[0])
	prefix[0] = 0

	bufLen := binary.BigEndian.Uint64(prefix)
	if bufLen == 0 {
		return
	}
	b = make([]byte, bufLen)

	if _, err = r.Read(b); err != nil {
		return
	}
	return
}
