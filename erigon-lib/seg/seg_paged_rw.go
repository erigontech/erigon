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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/compress"
)

var be = binary.BigEndian

func GetFromPage(key, compressedPage []byte, compressionBuf []byte, compressionEnabled bool) (v []byte, compressionBufOut []byte) {
	var err error
	var page []byte
	compressionBuf, page, err = compress.DecodeZstdIfNeed(compressionBuf, compressedPage, compressionEnabled)
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

func (r *Page) Reset(v []byte, compressionEnabled bool) {
	var err error
	r.compressionBuf, v, err = compress.DecodeZstdIfNeed(r.compressionBuf, v, compressionEnabled)
	if err != nil {
		panic(fmt.Errorf("len(v): %d, %w", len(v), err))
	}

	r.limit = int(v[0])
	meta, data := v[1:1+r.limit*4*2], v[1+r.limit*4*2:]
	r.kLens, r.vLens, r.data = meta[:r.limit*4], meta[r.limit*4:r.limit*4*2], data
	r.reset()
}
func (r *Page) reset() {
	r.i, r.kOffset, r.vOffset = 0, 0, 0
	for i := 0; i < r.limit*4; i += 4 {
		r.vOffset += be.Uint32(r.kLens[i:])
	}
}

func (r *Page) HasNext() bool { return r.limit > r.i }
func (r *Page) Current() (k, v []byte) {
	if r.i >= r.limit {
		return nil, nil
	}
	kLen := be.Uint32(r.kLens[r.i*4:])
	k = r.data[r.kOffset : r.kOffset+kLen]
	vLen := be.Uint32(r.vLens[r.i*4:])
	v = r.data[r.vOffset : r.vOffset+vLen]
	return k, v
}
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
func (r *Page) Seek(seekKey []byte) (k, v []byte) {
	r.reset()
	for r.HasNext() {
		k, v = r.Next()
		if bytes.Compare(k, seekKey) >= 0 {
			return k, v
		}
	}
	return nil, nil
}
func (r *Page) Get(getKey []byte) (v []byte) {
	r.reset()

	var k []byte
	for r.HasNext() {
		k, v = r.Next()
		if bytes.Equal(getKey, k) {
			return v
		}
	}
	return nil
}
func (r *Page) First() (k, v []byte) {
	r.reset()
	if r.HasNext() {
		return r.Next()
	}
	return nil, nil
}
func (r *Page) Last() (k, v []byte) {
	r.reset()
	for r.HasNext() {
		k, v = r.Next()
	}
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
	PageLvlCfg
	file ReaderI
	page *Page

	currentPageOffset, nextPageOffset uint64
}

func NewPagedReader(r ReaderI, cfg PageLvlCfg) *PagedReader {
	if cfg.PageSize > MaxPageSize {
		panic("assert: current `Page` implementation using 1 byte to store keys amount, means max pageSize is 256")
	}
	return &PagedReader{PageLvlCfg: cfg, file: r, page: &Page{}}
}

func (g *PagedReader) Reset(offset uint64) {
	if g.PageSize <= 1 {
		g.file.Reset(offset)
		return
	}
	//if g.currentPageOffset == offset { // don't reset internal state in this case: likely user just iterating over all values
	//	return
	//}

	g.file.Reset(offset)
	g.currentPageOffset = offset
	g.nextPageOffset = offset
	g.page = &Page{} // TODO: optimize
	if g.file.HasNext() {
		g.NextPage()
	}
}
func (g *PagedReader) PrintPages() {
	if g.PageSize <= 1 {
		return
	}
	g.Reset(0)
	i := 0
	for {
		var keys [][]byte
		fst, _ := g.page.First()
		fst = common.Copy(fst)
		lst, _ := g.page.Last()
		lst = common.Copy(lst)
		fmt.Printf("page: %d, offset=%d, keys: %d %x-%x\n", i, g.currentPageOffset, len(keys), fst, lst)
		i++
		if !g.HasNextPage() {
			break
		}
		g.NextPage()
	}
}
func (g *PagedReader) MadvNormal() *PagedReader {
	g.file.MadvNormal()
	return g
}
func (g *PagedReader) DisableReadAhead()   { g.file.DisableReadAhead() }
func (g *PagedReader) FileName() string    { return g.file.FileName() }
func (g *PagedReader) Count() int          { return g.file.Count() }
func (g *PagedReader) Size() int           { return g.file.Size() }
func (g *PagedReader) HasNextOnPage() bool { return g.PageSize > 1 && g.page.HasNext() }
func (g *PagedReader) HasNextPage() bool   { return g.file.HasNext() }
func (g *PagedReader) HasNext() bool       { return g.HasNextOnPage() || g.HasNextPage() }

//func (g *PagedReader) Next(buf []byte) ([]byte, uint64) {
//	panic("use Next2")
//	if g.pageSize <= 1 {
//		return g.file.Next(buf)
//	}
//
//	if g.page.HasNext() {
//		_, v := g.page.Next()
//		if g.page.HasNext() {
//			return v, g.currentPageOffset
//		}
//		return v, g.nextPageOffset
//	}
//	g.NextPage()
//	_, v := g.page.Next()
//	return v, g.currentPageOffset
//}

func (g *PagedReader) NextPage() {
	g.currentPageOffset = g.nextPageOffset
	var pageV []byte
	pageV, g.nextPageOffset = g.file.Next(nil)
	g.page.Reset(pageV, g.Compress)
}

func (g *PagedReader) ResetAndSeekForward(seekKey []byte, offset uint64) (k, v []byte) {
	g.Reset(offset)
	if g.PageSize <= 1 {
		k, _ = g.file.Next(nil)
		v, _ = g.file.Next(nil)
		return k, v
	}

	if !g.page.HasNext() {
		g.NextPage()
	}

	k, v = g.page.Seek(seekKey)
	if k != nil {
		return k, v
	}

	return g.page.Last()
}
func (g *PagedReader) ResetAndGetOnPage(getKey []byte, offset uint64) (v []byte) {
	g.Reset(offset)
	if g.PageSize <= 1 {
		_, _ = g.file.Skip()
		v, _ = g.file.Next(nil)
		return v
	}
	return g.page.Get(getKey)
}

func (g *PagedReader) FindOnPageForHistory(key []byte, offset uint64) (v []byte) {
	if g.PageSize <= 1 {
		g.Reset(offset)
		v, _ = g.file.Next(nil)
		return v
	}

	g.Reset(offset)
	for i := 0; i < g.PageSize && g.HasNext(); i++ {
		k, v, _, _ := g.Next2ForHistory(nil)
		if bytes.Equal(key, k) {
			return v
		}
	}
	return
}
func (g *PagedReader) Next2ForHistory(buf []byte) (k, v, bufOut []byte, pageOffset uint64) {
	if g.PageSize <= 1 {
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
func (g *PagedReader) Next2Copy(kBuf, vBuf []byte) (k, v, kBufOut, vBufOut []byte, nextKeyOffset uint64) {
	k, v, kBuf, vBuf, nextKeyOffset = g.Next2(kBuf, vBuf)
	return bytes.Clone(k), bytes.Clone(v), kBuf, vBuf, nextKeyOffset
}
func (g *PagedReader) Next2(kBuf, vBuf []byte) (k, v, kBufOut, vBufOut []byte, nextKeyOffset uint64) {
	if g.PageSize <= 1 {
		if g.NoKeysMode {
			vBuf, nextKeyOffset = g.file.Next(vBuf[:0])
			return nil, vBuf, kBuf, vBuf, nextKeyOffset
		}
		kBuf, _ = g.file.Next(kBuf[:0])
		vBuf, nextKeyOffset = g.file.Next(vBuf[:0])
		return kBuf, vBuf, kBuf, vBuf, nextKeyOffset
	}

	if g.page.HasNext() {
		k, v = g.page.Next()
		return k, v, kBuf, vBuf, g.currentPageOffset
	}
	if !g.page.HasNext() {
		g.NextPage()
	}

	k, v = g.page.Next()
	nextKeyOffset = g.currentPageOffset
	if !g.page.HasNext() {
		nextKeyOffset = g.nextPageOffset
	}
	return k, v, kBuf, vBuf, nextKeyOffset
}
func (g *PagedReader) Cmp(k []byte, offset uint64) (cmp int, key []byte) {
	g.Reset(offset)
	if g.PageSize <= 1 {
		//TODO: use `b.getter.Match` after https://github.com/erigontech/erigon/issues/7855
		key, _ = g.file.Next(nil)
		if k == nil {
			return -1, key
		}
		return bytes.Compare(key, k), key
	}

	if !g.page.HasNext() {
		g.currentPageOffset = g.nextPageOffset
		var pageBuf []byte
		pageBuf, g.nextPageOffset = g.file.Next(pageBuf[:0])
		g.page.Reset(pageBuf, g.Compress)
	}
	if !g.page.HasNext() {
		return -1, nil
	}

	// If requested key is nil, return last key from page
	if k == nil {
		key, _ := g.page.Last()
		return -1, key
	}

	fst, _ := g.page.First()
	lst, _ := g.page.Last()

	if cmp = bytes.Compare(k, fst); cmp < 0 { // key is before page
		return -1, fst
	}
	if cmp = bytes.Compare(k, lst); cmp > 0 { // key is after page
		return 1, lst
	}
	key, _ = g.page.Seek(k) // key is within page: Seek it
	//cmp = bytes.Compare(seekKey, k)
	return 0, key
}

func (g *PagedReader) NextKey(kBuf []byte) (k, kBufOut []byte, nextKeyOffset uint64) {
	if g.PageSize <= 1 {
		kBuf, _ = g.file.Next(kBuf[:0])
		nextKeyOffset, _ = g.file.Skip()
		return kBuf, kBuf, nextKeyOffset
	}

	if !g.page.HasNext() {
		if !g.HasNextPage() {
			return nil, kBuf, 0
		}
		g.NextPage()
		if !g.page.HasNext() {
			panic(fmt.Sprintf("assert: empty page %t, %t\n", g.HasNextOnPage(), g.HasNextPage()))
		}
	}
	k, _ = g.page.Next()

	nextKeyOffset = g.currentPageOffset
	if !g.page.HasNext() {
		nextKeyOffset = g.nextPageOffset
	}
	return common.Copy(k), kBuf, nextKeyOffset
}

func (g *PagedReader) Skip() (uint64, int) {
	if g.PageSize <= 1 {
		return g.file.Skip()
	}

	k, v, _, _, nextKeyOffset := g.Next2(nil, nil)
	return nextKeyOffset, len(k) + len(v)
}

const MaxPageSize = 256

type CompressorI interface {
	io.Writer
	Close()
	Compress() error
	CompressWithCustomMetadata(count, emptyCount uint64) error
	Count() int
	FileName() string
}

func NewPagedWriter(parent CompressorI, cfg PageLvlCfg) *PagedWriter {
	if cfg.PageSize > MaxPageSize {
		panic("assert: current `Page` implementation using 1 byte to store keys amount, means max pageSize is 256")
	}
	return &PagedWriter{parent: parent, PageLvlCfg: cfg}
}

type PagedWriter struct {
	PageLvlCfg
	parent             CompressorI
	keys, vals         []byte
	kLengths, vLengths []int

	compressionBuf []byte

	pairs int // words amount
}

func (c *PagedWriter) ReadFrom(g *PagedReader) error {
	var kBuf, vBuf []byte
	var k, v []byte
	for g.HasNext() {
		k, v, kBuf, vBuf, _ = g.Next2(kBuf[:0], vBuf[:0])
		if err := c.Add(k, v); err != nil {
			return err
		}
	}
	return nil
}
func (c *PagedWriter) Empty() bool { return c.pairs == 0 }
func (c *PagedWriter) Close()      { c.parent.Close() }
func (c *PagedWriter) Compress() error {
	if err := c.Flush(); err != nil {
		return err
	}
	if c.PageSize <= 1 {
		return c.parent.Compress()
	}

	return c.parent.CompressWithCustomMetadata(uint64(c.pairs*2), 0)
}
func (c *PagedWriter) Count() int {
	if c.PageSize <= 1 {
		return c.parent.Count()
	}
	return c.pairs * 2
}
func (c *PagedWriter) FileName() string { return c.parent.FileName() }

func (c *PagedWriter) AddForHistory(k, v []byte) (err error) {
	c.pairs++
	if c.PageSize <= 1 {
		if _, err = c.parent.Write(v); err != nil {
			return err
		}
		return nil
	}

	c.kLengths = append(c.kLengths, len(k))
	c.vLengths = append(c.vLengths, len(v))
	c.keys = append(c.keys, k...)
	c.vals = append(c.vals, v...)
	isFull := c.pairs%c.PageSize == 0
	if isFull {
		return c.writePage()
	}
	return nil
}
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
	c.pairs++
	if c.PageSize <= 1 {
		if _, err = c.parent.Write(k); err != nil {
			return err
		}
		if _, err = c.parent.Write(v); err != nil {
			return err
		}
		return nil
	}

	c.kLengths = append(c.kLengths, len(k))
	c.vLengths = append(c.vLengths, len(v))
	c.keys = append(c.keys, k...)
	c.vals = append(c.vals, v...)
	isFull := c.pairs%c.PageSize == 0
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
	if c.PageSize <= 1 {
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
	//TODO: valLengths can replace by valOffsets - then reader will do less math

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
	c.compressionBuf, wholePage = compress.EncodeZstdIfNeed(c.compressionBuf, wholePage, c.PageLvlCfg.Compress)

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
