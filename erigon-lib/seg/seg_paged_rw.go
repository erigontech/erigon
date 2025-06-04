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
	"github.com/erigontech/erigon-lib/common/page"
)

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
