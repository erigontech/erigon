package state

import "github.com/ledgerwatch/erigon-lib/compress"

type getter struct {
	*compress.Getter
	c bool // compressed
}

func NewArchiveGetter(g *compress.Getter, c bool) ArchiveGetter {
	return &getter{Getter: g, c: c}
}

func (g *getter) MatchPrefix(prefix []byte) bool {
	if g.c {
		return g.Getter.MatchPrefix(prefix)
	}
	return g.Getter.MatchPrefixUncompressed(prefix) == 0
}

func (g *getter) Next(buf []byte) ([]byte, uint64) {
	if g.c {
		return g.Getter.Next(buf)
	}
	return g.Getter.NextUncompressed()
}

// ArchiveGetter hides if the underlying compress.Getter is compressed or not
type ArchiveGetter interface {
	HasNext() bool
	FileName() string
	MatchPrefix(prefix []byte) bool
	Skip() (uint64, int)
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
	c bool
}

func NewArchiveWriter(kv *compress.Compressor, compress bool) ArchiveWriter {
	return &compWriter{kv, compress}
}

func (c *compWriter) AddWord(word []byte) error {
	if c.c {
		return c.Compressor.AddWord(word)
	}
	return c.Compressor.AddUncompressedWord(word)
}

func (c *compWriter) Close() {
	if c.Compressor != nil {
		c.Compressor.Close()
	}
}
