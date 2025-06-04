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
	"encoding/binary"
	"io"

	"github.com/erigontech/erigon-lib/common/compress"
	"github.com/erigontech/erigon-lib/common/page"
)

func NewPagedWriter(parent io.Writer, limit int, compressionEnabled bool) *PagedWriter {
	return &PagedWriter{parent: parent, limit: limit, compressionEnabled: compressionEnabled}
}

type PagedWriter struct {
	parent             io.Writer
	i, limit           int
	keys, vals         []byte
	kLengths, vLengths []int

	compressionBuf     []byte
	compressionEnabled bool
}

func (c *PagedWriter) Empty() bool { return c.i == 0 }
func (c *PagedWriter) Add(k, v []byte) (err error) {
	if c.limit <= 1 {
		_, err = c.parent.Write(v)
		return err
	}

	c.i++
	c.kLengths = append(c.kLengths, len(k))
	c.vLengths = append(c.vLengths, len(v))
	c.keys = append(c.keys, k...)
	c.vals = append(c.vals, v...)
	isFull := c.i%c.limit == 0
	//fmt.Printf("[dbg] write: %x, %x\n", k, v)
	if isFull {
		//fmt.Printf("[dbg] write--\n")
		bts := c.bytesAndReset()
		_, err = c.parent.Write(bts)
		return err
	}
	return nil
}

func (c *PagedWriter) Reset() {
	c.i = 0
	c.kLengths, c.vLengths = c.kLengths[:0], c.vLengths[:0]
	c.keys, c.vals = c.keys[:0], c.vals[:0]
}
func (c *PagedWriter) Flush() error {
	if c.limit <= 1 {
		return nil
	}

	defer c.Reset()
	if !c.Empty() {
		bts := c.bytesAndReset()
		_, err := c.parent.Write(bts)
		return err
	}
	return nil
}

func (c *PagedWriter) bytesAndReset() []byte {
	v := c.bytes()
	c.Reset()
	return v
}

func (c *PagedWriter) bytes() []byte {
	//TODO: alignment,compress+alignment

	c.keys = append(c.keys, c.vals...)
	keysAndVals := c.keys

	c.vals = growslice(c.vals[:0], 1+len(c.kLengths)*2*4)
	for i := range c.vals {
		c.vals[i] = 0
	}
	c.vals[0] = uint8(len(c.kLengths)) // first byte is amount of vals
	lensBuf := c.vals[1:]
	for i, l := range c.kLengths {
		binary.BigEndian.PutUint32(lensBuf[i*4:(i+1)*4], uint32(l))
	}
	lensBuf = lensBuf[len(c.kLengths)*4:]
	for i, l := range c.vLengths {
		binary.BigEndian.PutUint32(lensBuf[i*4:(i+1)*4], uint32(l))
	}

	c.vals = append(c.vals, keysAndVals...)
	lengthsAndKeysAndVals := c.vals

	c.compressionBuf, lengthsAndKeysAndVals = compress.EncodeZstdIfNeed(c.compressionBuf, lengthsAndKeysAndVals, c.compressionEnabled)

	return lengthsAndKeysAndVals
}

func (c *PagedWriter) DisableFsync() {
	if casted, ok := c.parent.(disableFsycn); ok {
		casted.DisableFsync()
	}
}

type disableFsycn interface {
	DisableFsync()
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
func (g *PagedReader) MadvNormal() *PagedReader {
	g.file.MadvNormal()
	return g
}
func (g *PagedReader) DisableReadAhead() { g.file.DisableReadAhead() }
func (g *PagedReader) FileName() string  { return g.file.FileName() }
func (g *PagedReader) Count() int        { return g.file.Count() }
func (g *PagedReader) Size() int         { return g.file.Size() }
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

// growslice ensures b has the wanted length by either expanding it to its capacity
// or allocating a new slice if b has insufficient capacity.
func growslice(b []byte, wantLength int) []byte {
	if cap(b) >= wantLength {
		return b[:wantLength]
	}
	return make([]byte, wantLength)
}
