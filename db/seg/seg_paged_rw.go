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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/erigontech/erigon/db/compress"
)

var be = binary.BigEndian

func GetFromPage(key, compressedPage []byte, compressionBuf []byte, compressionEnabled bool) (v []byte, compressionBufOut []byte) {
	var err error
	var page []byte
	compressionBuf, page, err = compress.DecodeZstdIfNeed(compressionBuf[:0], compressedPage, compressionEnabled)
	if err != nil {
		panic(err)
	}

	cnt := int(page[0])
	if cnt == 0 {
		return nil, compressionBuf
	}
	meta, data := page[1:1+cnt*4*2], page[1+cnt*4*2:]
	kLens, vLens := meta[:cnt*4], meta[cnt*4:]
	var kOffset, vOffset uint32
	for i := 0; i < cnt*4; i += 4 {
		vOffset += be.Uint32(kLens[i:])
	}
	keys := data[:vOffset]
	vals := data[vOffset:]
	vOffset = 0

	for i := 0; i < cnt*4; i += 4 {
		kLen, vLen := be.Uint32(kLens[i:]), be.Uint32(vLens[i:])
		foundKey := keys[kOffset : kOffset+kLen]
		if bytes.Equal(key, foundKey) {
			return vals[vOffset : vOffset+vLen], compressionBuf
		} else {
			_ = data
		}
		kOffset += kLen
		vOffset += vLen
	}
	return nil, compressionBuf
}

type Page struct {
	i, limit           int
	kLens, vLens, data []byte
	kOffset, vOffset   uint32

	compressionBuf []byte
}

func FromBytes(buf []byte, compressionEnabled bool) *Page {
	r := &Page{}
	r.Reset(buf, compressionEnabled)
	return r
}

func (r *Page) Reset(v []byte, compressionEnabled bool) (n int) {
	var err error
	r.compressionBuf, v, err = compress.DecodeZstdIfNeed(r.compressionBuf[:0], v, compressionEnabled)
	if err != nil {
		panic(fmt.Errorf("len(v): %d, %w", len(v), err))
	}

	r.i, r.kOffset, r.vOffset = 0, 0, 0
	r.limit = int(v[0])
	meta, data := v[1:1+r.limit*4*2], v[1+r.limit*4*2:]
	r.kLens, r.vLens, r.data = meta[:r.limit*4], meta[r.limit*4:r.limit*4*2], data

	for i := 0; i < r.limit*4; i += 4 {
		r.vOffset += be.Uint32(r.kLens[i:])
	}
	return
}
func (r *Page) HasNext() bool { return r.limit > r.i }
func (r *Page) Next() (k, v []byte) {
	kLen := be.Uint32(r.kLens[r.i*4:])
	k = r.data[r.kOffset : r.kOffset+kLen]
	vLen := be.Uint32(r.vLens[r.i*4:])
	v = r.data[r.vOffset : r.vOffset+vLen]
	r.i++
	r.kOffset += kLen
	r.vOffset += vLen
	return k, v
}

func WordsAmount2PagesAmount(wordsAmount int, pageSize int) (pagesAmount int) {
	pagesAmount = wordsAmount
	if wordsAmount == 0 {
		return 0
	}
	if pageSize > 0 {
		pagesAmount = (wordsAmount-1)/pageSize + 1 //amount of pages
	}
	return pagesAmount
}

type PagedReader struct {
	file         ReaderI
	isCompressed bool
	pageSize     int
	page         *Page

	currentPageOffset, nextPageOffset uint64
}

func NewPagedReader(r ReaderI, pageSize int, snappy bool) *PagedReader {
	if pageSize == 0 {
		pageSize = 1
	}
	return &PagedReader{file: r, pageSize: pageSize, isCompressed: snappy, page: &Page{}}
}

func (g *PagedReader) Reset(offset uint64) {
	if g.pageSize <= 1 {
		g.file.Reset(offset)
		return
	}
	if g.currentPageOffset == offset { // don't reset internal state in this case: likely user just iterating over all values
		return
	}

	g.file.Reset(offset)
	g.currentPageOffset = offset
	g.nextPageOffset = offset
	g.page = &Page{} // TODO: optimize
	if g.file.HasNext() {
		g.NextPage()
	}
}

//	func (g *PagedReader) PrintPages() {
//		if g.pageSize <= 1 {
//			return
//		}
//		g.Reset(0)
//		i := 0
//		for {
//			var keys [][]byte
//			fst, _ := g.page.First()
//			fst = common.Copy(fst)
//			lst, _ := g.page.Last()
//			lst = common.Copy(lst)
//			fmt.Printf("page: %d, offset=%d, keys: %d %x-%x\n", i, g.currentPageOffset, len(keys), fst, lst)
//			i++
//			if !g.HasNextPage() {
//				break
//			}
//			g.NextPage()
//		}
//	}
func (g *PagedReader) MadvNormal() *PagedReader {
	g.file.MadvNormal()
	return g
}
func (g *PagedReader) DisableReadAhead()   { g.file.DisableReadAhead() }
func (g *PagedReader) FileName() string    { return g.file.FileName() }
func (g *PagedReader) Count() int          { return g.file.Count() }
func (g *PagedReader) Size() int           { return g.file.Size() }
func (g *PagedReader) HasNextOnPage() bool { return g.pageSize > 1 && g.page.HasNext() }
func (g *PagedReader) HasNextPage() bool   { return g.file.HasNext() }
func (g *PagedReader) HasNext() bool       { return g.HasNextOnPage() || g.HasNextPage() }
func (g *PagedReader) Next(buf []byte) ([]byte, uint64) {
	if g.pageSize <= 1 {
		return g.file.Next(buf)
	}

	if g.page.HasNext() {
		_, v := g.page.Next()
		if g.page.HasNext() {
			return v, g.currentPageOffset
		}
		return v, g.nextPageOffset
	}
	g.NextPage()
	_, v := g.page.Next()
	return v, g.currentPageOffset
}

func (g *PagedReader) NextPage() {
	g.currentPageOffset = g.nextPageOffset
	var pageV []byte
	pageV, g.nextPageOffset = g.file.Next(nil)
	g.page.Reset(pageV, g.isCompressed)
}

func (g *PagedReader) Next2(buf []byte) (k, v, bufOut []byte, pageOffset uint64) {
	if g.pageSize <= 1 {
		buf, pageOffset = g.file.Next(buf)
		return nil, buf, buf, pageOffset
	}

	if g.page.HasNext() {
		k, v = g.page.Next()
		return k, v, buf, g.currentPageOffset
	}
	g.NextPage()
	k, v = g.page.Next()
	return k, v, buf, g.currentPageOffset
}
func (g *PagedReader) Skip() (uint64, int) {
	v, offset := g.Next(nil)
	return offset, len(v)
}

func NewPagedWriter(parent CompressorI, pageSize int, compressionEnabled bool) *PagedWriter {
	return &PagedWriter{parent: parent, pageSize: pageSize, compressionEnabled: compressionEnabled}
}

type CompressorI interface {
	io.Writer
	Close()
	Compress() error
	Count() int
	FileName() string
}
type PagedWriter struct {
	parent             CompressorI
	pageSize           int
	keys, vals         []byte
	kLengths, vLengths []int

	compressionBuf     []byte
	compressionEnabled bool

	pairs int
}

func (c *PagedWriter) Empty() bool { return c.pairs == 0 }
func (c *PagedWriter) Close()      { c.parent.Close() }
func (c *PagedWriter) Compress() error {
	if err := c.Flush(); err != nil {
		return err
	}
	return c.parent.Compress()
}
func (c *PagedWriter) Count() int {
	if c.pageSize <= 1 {
		return c.parent.Count()
	}
	return c.pairs * 2
}
func (c *PagedWriter) FileName() string { return c.parent.FileName() }

func (c *PagedWriter) writePage() error {
	bts, ok := c.bytes()
	c.resetPage()
	if !ok {
		return nil
	}
	_, err := c.parent.Write(bts)
	return err
}
func (c *PagedWriter) Add(k, v []byte) (err error) {
	if c.pageSize <= 1 {
		_, err = c.parent.Write(v)
		return err
	}

	c.pairs++
	c.kLengths = append(c.kLengths, len(k))
	c.vLengths = append(c.vLengths, len(v))
	c.keys = append(c.keys, k...)
	c.vals = append(c.vals, v...)
	isFull := c.pairs%c.pageSize == 0
	if isFull {
		return c.writePage()
	}
	return nil
}

func (c *PagedWriter) resetPage() {
	c.kLengths, c.vLengths = c.kLengths[:0], c.vLengths[:0]
	c.keys, c.vals = c.keys[:0], c.vals[:0]
}
func (c *PagedWriter) Flush() error {
	if c.pageSize <= 1 {
		return nil
	}
	defer c.resetPage()
	return c.writePage()
}

func (c *PagedWriter) bytes() (wholePage []byte, notEmpty bool) {
	if len(c.kLengths) == 0 {
		return nil, false
	}
	//TODO: alignment,compress+alignment

	c.keys = append(c.keys, c.vals...)
	keysAndVals := c.keys

	c.vals = growslice(c.vals[:0], 1+len(c.kLengths)*2*4)
	wholePage = c.vals
	clear(wholePage)
	wholePage[0] = uint8(len(c.kLengths)) // first byte is amount of vals
	lensBuf := wholePage[1:]
	for i, l := range c.kLengths {
		binary.BigEndian.PutUint32(lensBuf[i*4:(i+1)*4], uint32(l))
	}
	lensBuf = lensBuf[len(c.kLengths)*4:]
	for i, l := range c.vLengths {
		binary.BigEndian.PutUint32(lensBuf[i*4:(i+1)*4], uint32(l))
	}

	wholePage = append(wholePage, keysAndVals...)
	c.compressionBuf, wholePage = compress.EncodeZstdIfNeed(c.compressionBuf[:0], wholePage, c.compressionEnabled)

	return wholePage, true
}

func (c *PagedWriter) DisableFsync() {
	if casted, ok := c.parent.(disableFsycn); ok {
		casted.DisableFsync()
	}
}

type disableFsycn interface {
	DisableFsync()
}

// growslice ensures b has the wanted length by either expanding it to its capacity
// or allocating a new slice if b has insufficient capacity.
func growslice(b []byte, wantLength int) []byte {
	if cap(b) >= wantLength {
		return b[:wantLength]
	}
	return make([]byte, wantLength)
}
