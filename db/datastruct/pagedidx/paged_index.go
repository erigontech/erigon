// Copyright 2026 The Erigon Authors
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

// Package pagedidx maps a position to a value without storing one entry per
// item.
//
// It fits data written in a single pass as consecutive runs of items - run 0's
// items, then run 1's, and so on - where the value being indexed never
// decreases along that order and only changes once per page of items. For a
// file that means: the value is an offset into it, and a page is however many
// items share a record.
//
// Two Elias-Fano sequences replace the per-item table:
//
//	starts[r] = items before run r     -> ordinal = starts[r] + item
//	pages[p]  = value of page p        -> value   = pages[ordinal / pageSize]
//
// starts counts items, pages holds the values themselves, and the two have
// different lengths. Paging drops the stored count by pageSize, and Elias-Fano
// then encodes what is left in the bits its gaps need: 8 billion items at 64
// per page keep 125 million values.
package pagedidx

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/db/bufiopool"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
)

const version = 1

// header: version, page size, item count
const headerLen = 1 + 8 + 8

// Index resolves an item's value from its (run, item) position.
type Index struct {
	f         *os.File
	m         mmap.Ro
	starts    *eliasfano32.EliasFano
	pages     *eliasfano32.EliasFano
	pageSize  uint64
	itemCount uint64
}

func Open(path string) (*Index, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	idx := &Index{f: f}
	defer func() {
		if idx.m == nil {
			f.Close()
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.Size() < headerLen {
		return nil, fmt.Errorf("%s: truncated paged index", path)
	}
	m, err := mmap.OpenRo(f, int(fi.Size()))
	if err != nil {
		return nil, err
	}
	if v := m[0]; v != version {
		_ = m.Unmap()
		return nil, fmt.Errorf("%s: paged index version %d, expected %d", path, v, version)
	}
	idx.m = m
	idx.pageSize = binary.BigEndian.Uint64(m[1:])
	idx.itemCount = binary.BigEndian.Uint64(m[9:])
	if idx.pageSize == 0 {
		idx.Close()
		return nil, fmt.Errorf("%s: paged index page size is 0", path)
	}
	if fi.Size() > headerLen {
		starts, n := eliasfano32.ReadEliasFano(m[headerLen:])
		idx.starts = starts
		idx.pages, _ = eliasfano32.ReadEliasFano(m[headerLen+n:])
	}
	return idx, nil
}

// Get returns the value of a run's item, and false when the index holds no such
// position. Callers take the position from a separate file, so out-of-range
// means the two files do not belong together.
func (i *Index) Get(run, item uint64) (uint64, bool) {
	if i.pages == nil || run >= i.starts.Count() {
		return 0, false
	}
	page := (i.starts.Get(run) + item) / i.pageSize
	if page >= i.pages.Count() {
		return 0, false
	}
	return i.pages.Get(page), true
}

// Run returns the run owning the item at ordinal, and false when the index
// holds no such item. An empty run owns nothing, so the search looks for the
// first run starting after the ordinal and steps back one.
func (i *Index) Run(ordinal uint64) (uint64, bool) {
	if i.starts == nil || ordinal >= i.itemCount {
		return 0, false
	}
	last := i.starts.Count() - 1
	if ordinal >= i.starts.Get(last) {
		return last, true
	}
	_, pos, _ := i.starts.Seek(ordinal + 1)
	return pos - 1, true
}

// Page returns the page holding value, and false when value falls before the
// first page.
func (i *Index) Page(value uint64) (uint64, bool) {
	if i.pages == nil || value < i.pages.Get(0) {
		return 0, false
	}
	last := i.pages.Count() - 1
	if value >= i.pages.Get(last) {
		return last, true
	}
	_, pos, _ := i.pages.Seek(value + 1)
	return pos - 1, true
}

func (i *Index) Empty() bool { return i == nil || i.pages == nil }

func (i *Index) Close() {
	if i == nil {
		return
	}
	if i.m != nil {
		_ = i.m.Unmap()
		i.m = nil
		i.starts, i.pages = nil, nil
	}
	if i.f != nil {
		_ = i.f.Close()
		i.f = nil
	}
}

// Writer builds an Index. AddRun is called once per run in order, AddPage
// once per page in order; the two may be interleaved.
type Writer struct {
	path      string
	starts    *eliasfano32.EliasFano
	pages     *eliasfano32.EliasFano
	items     uint64
	pageSize  uint64
	itemCount uint64
	noFsync   bool
}

// NewWriter sizes the two sequences up front, which is all Elias-Fano needs.
// maxValue only has to be an upper bound.
func NewWriter(path string, pageSize, runCount, itemCount, maxValue uint64) (*Writer, error) {
	if pageSize == 0 {
		return nil, fmt.Errorf("%s: paged index page size is 0", path)
	}
	w := &Writer{path: path, pageSize: pageSize, itemCount: itemCount}
	if runCount == 0 || itemCount == 0 { // nothing to address: header only
		return w, nil
	}
	pages := (itemCount + pageSize - 1) / pageSize
	w.starts = eliasfano32.NewEliasFano(runCount, itemCount)
	w.pages = eliasfano32.NewEliasFano(pages, max(maxValue, 1))
	return w, nil
}

func (w *Writer) NoFsync() { w.noFsync = true }

// AddRun records a run holding the given number of items.
func (w *Writer) AddRun(items uint64) {
	w.starts.AddOffset(w.items)
	w.items += items
}

// AddPage records the value shared by the next page of items.
func (w *Writer) AddPage(value uint64) { w.pages.AddOffset(value) }

func (w *Writer) Build() error {
	if w.pages != nil {
		w.starts.Build()
		w.pages.Build()
	}

	f, err := dir.CreateTemp(w.path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufiopool.Writer(f)
	defer bufiopool.PutWriter(bw)

	var header [headerLen]byte
	header[0] = version
	binary.BigEndian.PutUint64(header[1:], w.pageSize)
	binary.BigEndian.PutUint64(header[9:], w.itemCount)
	if _, err := bw.Write(header[:]); err != nil {
		return err
	}
	if w.pages != nil {
		if err := w.starts.Write(bw); err != nil {
			return err
		}
		if err := w.pages.Write(bw); err != nil {
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if !w.noFsync {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), w.path)
}
