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

// Package pagedidx indexes values that grow along a known order and repeat in
// runs, without storing one entry per item.
//
// It fits data laid out as consecutive groups of items - group 0's items, then
// group 1's, and so on - where the value being indexed never decreases along
// that order and only changes once per page of items. Both of those hold for a
// file written in a single pass: the value is an offset into it, and a page is
// however many items share a record.
//
// Two Elias-Fano sequences replace the per-item table: one holds the number of
// items before each group, the other one value per page. A file with 8 billion
// items and 64 items per page keeps 125 million values instead of 8 billion,
// and Elias-Fano then encodes those in the bits their gaps need.
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

const version = 2

// header: version, page size
const headerLen = 1 + 8

// Index resolves an item's value from its (group, member) position.
type Index struct {
	f        *os.File
	m        mmap.Ro
	groups   *eliasfano32.EliasFano
	values   *eliasfano32.EliasFano
	pageSize uint64
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
	if idx.pageSize == 0 {
		idx.Close()
		return nil, fmt.Errorf("%s: paged index page size is 0", path)
	}
	if fi.Size() > headerLen {
		groups, n := eliasfano32.ReadEliasFano(m[headerLen:])
		idx.groups = groups
		idx.values, _ = eliasfano32.ReadEliasFano(m[headerLen+n:])
	}
	return idx, nil
}

// Get returns the value of a group's member.
func (i *Index) Get(group, member uint64) uint64 {
	return i.values.Get((i.groups.Get(group) + member) / i.pageSize)
}

func (i *Index) Empty() bool { return i == nil || i.values == nil }

func (i *Index) Close() {
	if i == nil {
		return
	}
	if i.m != nil {
		_ = i.m.Unmap()
		i.m = nil
		i.groups, i.values = nil, nil
	}
	if i.f != nil {
		_ = i.f.Close()
		i.f = nil
	}
}

// Writer builds an Index. AddGroup is called once per group in order, AddPage
// once per page in order; the two may be interleaved.
type Writer struct {
	path     string
	groups   *eliasfano32.EliasFano
	values   *eliasfano32.EliasFano
	items    uint64
	pageSize uint64
	noFsync  bool
}

// NewWriter sizes the two sequences up front, which is all Elias-Fano needs.
// maxValue only has to be an upper bound.
func NewWriter(path string, pageSize, groupCount, itemCount, maxValue uint64) (*Writer, error) {
	if pageSize == 0 {
		return nil, fmt.Errorf("%s: paged index page size is 0", path)
	}
	w := &Writer{path: path, pageSize: pageSize}
	if groupCount == 0 || itemCount == 0 { // nothing to address: header only
		return w, nil
	}
	pages := (itemCount + pageSize - 1) / pageSize
	w.groups = eliasfano32.NewEliasFano(groupCount, itemCount)
	w.values = eliasfano32.NewEliasFano(pages, max(maxValue, 1))
	return w, nil
}

func (w *Writer) NoFsync() { w.noFsync = true }

// AddGroup records a group holding the given number of items.
func (w *Writer) AddGroup(items uint64) {
	w.groups.AddOffset(w.items)
	w.items += items
}

// AddPage records the value shared by the next page of items.
func (w *Writer) AddPage(value uint64) { w.values.AddOffset(value) }

func (w *Writer) Build() error {
	if w.values != nil {
		w.groups.Build()
		w.values.Build()
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
	if _, err := bw.Write(header[:]); err != nil {
		return err
	}
	if w.values != nil {
		if err := w.groups.Write(bw); err != nil {
			return err
		}
		if err := w.values.Write(bw); err != nil {
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
