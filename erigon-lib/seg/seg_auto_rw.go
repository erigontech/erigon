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

import (
	"fmt"

	"github.com/erigontech/erigon-lib/common/page"
)

//Reader and Writer - decorators on Getter and Compressor - which
//can auto-use Next/NextUncompressed and Write/AddUncompressedWord - based on `FileCompression` passed to constructor

// Maybe in future will add support of io.Reader/Writer interfaces to this decorators
// Maybe in future will merge decorators into it's parents

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

func (c FileCompression) String() string {
	switch c {
	case CompressNone:
		return "none"
	case CompressKeys:
		return "k"
	case CompressVals:
		return "v"
	case CompressKeys | CompressVals:
		return "kv"
	default:
		return ""
	}
}

type Reader struct {
	*Getter
	nextValue bool            // if nextValue true then getter.Next() expected to return value
	c         FileCompression // compressed
}

func NewReader(g *Getter, c FileCompression) *Reader {
	return &Reader{Getter: g, c: c}
}

func (g *Reader) MatchPrefix(prefix []byte) bool {
	if g.c&CompressKeys != 0 {
		return g.Getter.MatchPrefix(prefix)
	}
	return g.Getter.MatchPrefixUncompressed(prefix)
}

func (g *Reader) MatchCmp(prefix []byte) int {
	if g.c&CompressKeys != 0 {
		return g.Getter.MatchCmp(prefix)
	}
	return g.Getter.MatchCmpUncompressed(prefix)
}

func (g *Reader) FileName() string { return g.Getter.FileName() }
func (g *Reader) Next(buf []byte) ([]byte, uint64) {
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

	if g.c&fl != 0 {
		return g.Getter.Skip()
	}
	return g.Getter.SkipUncompressed()

}

type ReaderI interface {
	Next(buf []byte) ([]byte, uint64)
	Reset(offset uint64)
	HasNext() bool
	Skip() (uint64, int)
	FileName() string
	BinarySearch(seek []byte, count int, getOffset func(i uint64) (offset uint64)) (foundOffset uint64, ok bool)
}

type PagedReader struct {
	file                   ReaderI
	snappy                 bool
	valuesOnCompressedPage int
	page                   *page.Reader

	currentPageOffset, nextPageOffset uint64
}

func NewPagedReader(r ReaderI, valuesOnCompressedPage int, snappy bool) *PagedReader {
	if valuesOnCompressedPage == 0 {
		valuesOnCompressedPage = 1
	}
	return &PagedReader{file: r, valuesOnCompressedPage: valuesOnCompressedPage, snappy: snappy, page: &page.Reader{}}
}

func (g *PagedReader) Reset(offset uint64) {
	if g.valuesOnCompressedPage <= 1 {
		g.file.Reset(offset)
		return
	}
	if g.currentPageOffset == offset { // don't reset internal state in this case: likely user just iterating over all values
		return
	}

	g.file.Reset(offset)
	g.currentPageOffset = offset
	g.nextPageOffset = offset
	g.page = &page.Reader{} // TODO: optimize
}
func (g *PagedReader) FileName() string { return g.file.FileName() }
func (g *PagedReader) HasNext() bool {
	return (g.valuesOnCompressedPage > 1 && g.page.HasNext()) || g.file.HasNext()
}
func (g *PagedReader) Next(buf []byte) ([]byte, uint64) {
	if g.valuesOnCompressedPage <= 1 {
		return g.file.Next(buf)
	}

	if g.page.HasNext() {
		_, v := g.page.Next()
		if g.page.HasNext() {
			return v, g.currentPageOffset
		}
		return v, g.nextPageOffset
	}
	g.currentPageOffset = g.nextPageOffset
	var pageV []byte
	pageV, g.nextPageOffset = g.file.Next(buf)
	g.page.Reset(pageV, g.snappy)
	_, v := g.page.Next()
	return v, g.currentPageOffset
}
func (g *PagedReader) Next2(buf []byte) (k, v, bufOut []byte, pageOffset uint64) {
	if g.valuesOnCompressedPage <= 1 {
		buf, pageOffset = g.file.Next(buf)
		return nil, buf, buf, pageOffset
	}

	if g.page.HasNext() {
		k, v = g.page.Next()
		return k, v, buf, g.currentPageOffset
	}
	g.currentPageOffset = g.nextPageOffset
	buf, g.nextPageOffset = g.file.Next(buf[:0])
	g.page.Reset(buf, g.snappy)
	k, v = g.page.Next()
	return k, v, buf, g.currentPageOffset
}
func (g *PagedReader) Skip() (uint64, int) {
	v, offset := g.Next(nil)
	return offset, len(v)
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
