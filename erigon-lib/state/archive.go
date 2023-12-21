package state

import (
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
)

type FileCompression uint8

const (
	CompressNone FileCompression = 0b0  // no compression
	CompressKeys FileCompression = 0b1  // compress keys only
	CompressVals FileCompression = 0b10 // compress values only
)

type getter struct {
	*compress.Getter
	nextValue bool            // if nextValue true then getter.Next() expected to return value
	c         FileCompression // compressed
}

func NewArchiveGetter(g *compress.Getter, c FileCompression) ArchiveGetter {
	return &getter{Getter: g, c: c}
}

func (g *getter) MatchPrefix(prefix []byte) bool {
	if g.c&CompressKeys != 0 {
		return g.Getter.MatchPrefix(prefix)
	}
	return g.Getter.MatchPrefixUncompressed(prefix) == 0
}

func (g *getter) Next(buf []byte) ([]byte, uint64) {
	fl := CompressKeys
	if g.nextValue {
		fl = CompressVals
		g.nextValue = false
	} else {
		g.nextValue = true
	}

	if g.c&fl != 0 {
		return g.Getter.Next(buf)
	}
	return g.Getter.NextUncompressed()
}

func (g *getter) Reset(offset uint64) {
	g.nextValue = false
	g.Getter.Reset(offset)
}
func (g *getter) Skip() (uint64, int) {
	fl := CompressKeys
	if g.nextValue {
		fl = CompressVals
		g.nextValue = false
	} else {
		g.nextValue = true
	}

	if g.c&fl != 0 {
		return g.Getter.Skip()
	}
	return g.Getter.SkipUncompressed()

}

// ArchiveGetter hides if the underlying compress.Getter is compressed or not
type ArchiveGetter interface {
	HasNext() bool
	FileName() string
	MatchPrefix(prefix []byte) bool
	Skip() (uint64, int)
	Size() int
	Next(buf []byte) ([]byte, uint64)
	Reset(offset uint64)
}

type ArchiveWriter interface {
	AddWord(word []byte) error
	Count() int
	Compress() error
	DisableFsync()
	Close()
}

type compWriter struct {
	*compress.Compressor
	keyWritten bool
	c          FileCompression
}

func NewArchiveWriter(kv *compress.Compressor, compress FileCompression) ArchiveWriter {
	return &compWriter{kv, false, compress}
}

func (c *compWriter) AddWord(word []byte) error {
	fl := CompressKeys
	if c.keyWritten {
		fl = CompressVals
		c.keyWritten = false
	} else {
		c.keyWritten = true
	}

	if c.c&fl != 0 {
		return c.Compressor.AddWord(word)
	}
	return c.Compressor.AddUncompressedWord(word)
}

func (c *compWriter) Close() {
	if c.Compressor != nil {
		c.Compressor.Close()
	}
}

func SaveExecV3PruneProgress(db kv.Putter, prunedTblName string, step uint64, prunedKey []byte) error {
	return db.Put(kv.TblPruningProgress, []byte(prunedTblName), append(encodeBigEndian(step), prunedKey...))
}

// GetExecV3PruneProgress retrieves saved progress of given table pruning from the database
// ts==0 && prunedKey==nil means that pruning is finished, next prune could start
// For domains make more sense to store inverted step to have 0 as empty value meaning no progress saved
func GetExecV3PruneProgress(db kv.Getter, prunedTblName string) (ts uint64, pruned []byte, err error) {
	v, err := db.GetOne(kv.TblPruningProgress, []byte(prunedTblName))
	if err != nil {
		return 0, nil, err
	}
	return unmarshalData(v)
}

func unmarshalData(data []byte) (uint64, []byte, error) {
	switch {
	case len(data) < 8 && len(data) > 0:
		return 0, nil, fmt.Errorf("value must be at least 8 bytes, got %d", len(data))
	case len(data) == 8:
		// we want to preserve guarantee that if step==0 && prunedKey==nil then pruning is finished
		// If return data[8:] - result will be empty array which is a valid key to prune and does not
		// mean that pruning is finished.
		return binary.BigEndian.Uint64(data[:8]), nil, nil
	case len(data) > 8:
		return binary.BigEndian.Uint64(data[:8]), data[8:], nil
	default:
		return 0, nil, nil
	}
}

func encodeBigEndian(n uint64) []byte {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], n)
	return v[:]
}
