package state

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/seg"
)

type FileCompression uint8

const (
	CompressNone FileCompression = 0b0  // no compression
	CompressKeys FileCompression = 0b1  // compress keys only
	CompressVals FileCompression = 0b10 // compress values only
)

func ParseFileCompression(s string) (FileCompression, error) {
	switch s {
	case "none", "":
		return CompressNone, nil
	case "k":
		return CompressKeys, nil
	case "v":
		return CompressVals, nil
	case "kv":
		return CompressKeys | CompressVals, nil
	default:
		return 0, fmt.Errorf("invalid file compression type: %s", s)
	}
}

type getter struct {
	*seg.Getter
	nextValue bool            // if nextValue true then getter.Next() expected to return value
	c         FileCompression // compressed
}

func NewArchiveGetter(g *seg.Getter, c FileCompression) ArchiveGetter {
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

// ArchiveGetter hides if the underlying seg.Getter is compressed or not
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
	*seg.Compressor
	keyWritten bool
	c          FileCompression
}

func NewArchiveWriter(kv *seg.Compressor, compress FileCompression) ArchiveWriter {
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

// SaveExecV3PruneProgress saves latest pruned key in given table to the database.
// nil key also allowed and means that latest pruning run has been finished.
func SaveExecV3PruneProgress(db kv.Putter, prunedTblName string, prunedKey []byte) error {
	empty := make([]byte, 1)
	if prunedKey != nil {
		empty[0] = 1
	}
	return db.Put(kv.TblPruningProgress, []byte(prunedTblName), append(empty, prunedKey...))
}

// GetExecV3PruneProgress retrieves saved progress of given table pruning from the database.
// For now it is latest pruned key in prunedTblName
func GetExecV3PruneProgress(db kv.Getter, prunedTblName string) (pruned []byte, err error) {
	v, err := db.GetOne(kv.TblPruningProgress, []byte(prunedTblName))
	if err != nil {
		return nil, err
	}
	switch len(v) {
	case 0:
		return nil, nil
	case 1:
		if v[0] == 1 {
			return []byte{}, nil
		}
		// nil values returned an empty key which actually is a value
		return nil, nil
	default:
		return v[1:], nil
	}
}
