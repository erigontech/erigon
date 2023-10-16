package snapshot_format

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ledgerwatch/erigon/cl/clparams"
)

type dataType int

const (
	chunkDataType   dataType = 0
	pointerDataType dataType = 1
)

// writeChunk writes a chunk to the writer.
func writeChunk(w io.Writer, buf []byte, t dataType) error {
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

func readChunk(r io.Reader) (buf []byte, t dataType, err error) {
	prefix := make([]byte, 8)
	fmt.Println("A")
	if _, err = r.Read(prefix); err != nil {
		return
	}

	t = dataType(prefix[0])
	prefix[0] = 0
	buf = make([]byte, binary.BigEndian.Uint64(prefix))
	if _, err = r.Read(buf); err != nil {
		return
	}
	return
}

func readMetadataForBlock(r io.Reader) (clparams.StateVersion, error) {
	b := []byte{0}
	if _, err := r.Read(b); err != nil {
		return 0, err
	}
	return clparams.StateVersion(b[0]), nil
}
