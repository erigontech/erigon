package snapshot_format

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
)

type dataType int

const (
	chunkDataType   dataType = 0
	pointerDataType dataType = 1
)

// writeChunk writes a chunk to the writer.
func writeChunk(w io.Writer, buf []byte, t dataType, snappy bool) error {
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

func readChunk(r io.Reader, snappy bool) (buf []byte, t dataType, err error) {
	prefix := make([]byte, 8)
	if _, err := r.Read(prefix); err != nil {
		return nil, dataType(0), err
	}
	t = dataType(prefix[0])
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

func readMetadataForBlock(r io.Reader) (clparams.StateVersion, error) {
	b := []byte{0}
	if _, err := r.Read(b); err != nil {
		return 0, err
	}
	return clparams.StateVersion(b[0]), nil
}
