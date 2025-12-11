// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package seg

//Reader and Writer - decorators on Getter and Compressor - which
//can auto-use Next/NextUncompressed and Write/AddUncompressedWord - based on `FileCompression` passed to constructor

// Maybe in future will add support of io.Reader/Writer interfaces to this decorators
// Maybe in future will merge decorators into it's parents

type Reader struct {
	*Getter
	nextValue bool            // if nextValue true then getter.Next() expected to return value
	c         FileCompression // compressed
}

func NewReader(g *Getter, c FileCompression) *Reader {
	return &Reader{Getter: g, c: c}
}

func (g *Reader) MatchPrefix(prefix []byte) bool {
	if g.c.Has(CompressKeys) {
		return g.Getter.MatchPrefix(prefix)
	}
	return g.Getter.MatchPrefixUncompressed(prefix)
}

func (g *Reader) MatchCmp(prefix []byte) int {
	if g.c.Has(CompressKeys) {
		return g.Getter.MatchCmp(prefix)
	}
	return g.Getter.MatchCmpUncompressed(prefix)
}

func (g *Reader) MadvNormal() MadvDisabler {
	g.d.MadvNormal()
	return g
}
func (g *Reader) DisableReadAhead() { g.d.DisableReadAhead() }
func (g *Reader) FileName() string  { return g.Getter.FileName() }
func (g *Reader) Next(buf []byte) ([]byte, uint64) {
	fl := CompressKeys
	if g.nextValue {
		fl = CompressVals
		g.nextValue = false
	} else {
		g.nextValue = true
	}

	if g.c.Has(fl) {
		return g.Getter.Next(buf)
	}
	return g.Getter.NextUncompressed()
}

func (g *Reader) Reset(offset uint64) {
	g.nextValue = false
	g.Getter.Reset(offset)
}
func (g *Reader) Skip() (uint64, int) {
	fl := CompressKeys
	if g.nextValue {
		fl = CompressVals
		g.nextValue = false
	} else {
		g.nextValue = true
	}

	if g.c.Has(fl) {
		return g.Getter.Skip()
	}
	return g.Getter.SkipUncompressed()

}

type Writer struct {
	*Compressor
	keyWritten bool
	c          FileCompression
}

func NewWriter(kv *Compressor, compress FileCompression) *Writer {
	return &Writer{kv, false, compress}
}

func (c *Writer) Write(word []byte) (n int, err error) {
	fl := CompressKeys
	if c.keyWritten {
		fl = CompressVals
		c.keyWritten = false
	} else {
		c.keyWritten = true
	}

	if c.c&fl != 0 {
		return len(word), c.Compressor.AddWord(word)
	}
	return len(word), c.Compressor.AddUncompressedWord(word)
}

func (c *Writer) ReadFrom(r *Reader) error {
	var v []byte
	for r.HasNext() {
		v, _ = r.Next(v[:0])
		if _, err := c.Write(v); err != nil {
			return err
		}
	}
	return nil
}

func (c *Writer) Close() {
	if c.Compressor != nil {
		c.Compressor.Close()
	}
}

func DetectCompressType(getter *Getter) (compressed FileCompression) {
	keyCompressed := func() (compressed bool) {
		defer func() {
			if rec := recover(); rec != nil {
				compressed = true
			}
		}()
		getter.Reset(0)
		for i := 0; i < 100; i++ {
			if getter.HasNext() {
				_, _ = getter.SkipUncompressed()
			}
			if getter.HasNext() {
				_, _ = getter.Skip()
			}
		}
		return compressed
	}()

	valCompressed := func() (compressed bool) {
		defer func() {
			if rec := recover(); rec != nil {
				compressed = true
			}
		}()
		getter.Reset(0)
		for i := 0; i < 100; i++ {
			if getter.HasNext() {
				_, _ = getter.Skip()
			}
			if getter.HasNext() {
				_, _ = getter.SkipUncompressed()
			}
		}
		return compressed
	}()
	getter.Reset(0)

	if keyCompressed {
		compressed |= CompressKeys
	}
	if valCompressed {
		compressed |= CompressVals
	}
	return compressed
}
