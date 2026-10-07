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

// Package posidx answers what the layout of the data already implies, so it
// stores neither the keys nor one entry per item.
//
// It cannot find anything by key. recsplit answers "where is the record for
// this hash"; posidx answers "where is item j of run i", and the caller must
// already know i and j - usually because another index just produced them.
//
// Reach for it when data is written in one pass as consecutive runs of items -
// run 0's items, then run 1's, and so on - and the value being indexed never
// decreases along that order. Two uses in Erigon:
//
//	history .vi     run = a key in .ef, item = the rank of a txNum in that
//	                key's list, value = the offset in .v that holds it
//	txn -> block    run = a block, item = a transaction, no value at all: the
//	                question is only which run owns an item
//
// Values may change once per page of items rather than once per item, and a
// page is however many items share a record in the file. An index may also
// carry no values, as the second example does.
//
// Two Elias-Fano sequences replace the per-item table:
//
//	starts[r] = items before run r     -> ordinal = starts[r] + item
//	pages[p]  = value of page p        -> value   = pages[ordinal / pageSize]
//
// starts counts items, pages holds the values themselves, and the two have
// different lengths. Paging drops the
// stored count by pageSize, and Elias-Fano then encodes what is left in the
// bits its gaps need: 8 billion items at 64 per page keep 125 million values.
package posidx

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/db/bufiopool"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
)

const version = 1

// Header layout. The padding after the version is load-bearing: the sequences
// that follow are read as []uint64 straight out of the mapping, so the header
// has to be a multiple of 8 or every read is unaligned.
//
//	0      version
//	1..7   padding
//	8..15  page size
//	16..23 item count
const (
	offVersion   = 0
	offPageSize  = 8
	offItemCount = 16
	headerLen    = 24
)

// ErrCorrupt marks a file that cannot be parsed, as opposed to one that cannot
// be read. Accessors are rebuildable, so callers recover from it by rebuilding.
var ErrCorrupt = errors.New("corrupt paged index")

// Index resolves an item's value from its (run, item) position. Lookups only
// read the mapping and the two sequences, so they are safe to call concurrently.
type Index struct {
	f         *os.File
	m         mmap.Ro
	starts    *eliasfano32.EliasFano
	pages     *eliasfano32.EliasFano
	pageSize  uint64
	itemCount uint64

	readAheadRefcnt atomic.Int32
}

func Open(path string) (*Index, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	idx := &Index{f: f}
	defer func() {
		if idx.m == nil && idx.f != nil { // Close already took it on the error paths below
			f.Close()
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.Size() < headerLen {
		return nil, fmt.Errorf("%w: %s: shorter than a header", ErrCorrupt, path)
	}
	m, err := mmap.OpenRo(f, int(fi.Size()))
	if err != nil {
		return nil, err
	}
	if v := m[offVersion]; v != version {
		_ = m.Unmap()
		return nil, fmt.Errorf("%w: %s: version %d, expected %d", ErrCorrupt, path, v, version)
	}
	idx.m = m
	idx.pageSize = binary.BigEndian.Uint64(m[offPageSize:])
	idx.itemCount = binary.BigEndian.Uint64(m[offItemCount:])
	if idx.pageSize == 0 {
		idx.Close()
		return nil, fmt.Errorf("%w: %s: page size is 0", ErrCorrupt, path)
	}
	if fi.Size() > headerLen {
		starts, n, err := readEliasFano(m[headerLen:])
		if err != nil {
			idx.Close()
			return nil, fmt.Errorf("%w: %s: starts: %w", ErrCorrupt, path, err)
		}
		pages, _, err := readEliasFano(m[headerLen+n:])
		if err != nil {
			idx.Close()
			return nil, fmt.Errorf("%w: %s: pages: %w", ErrCorrupt, path, err)
		}
		idx.starts, idx.pages = starts, pages
		// The two sequences and the header describe the same data three ways;
		// disagreement means the file is not what its header claims. Both checks
		// read only sequence headers: decoding Elias-Fano values trusts the bit
		// stream, so a corrupt one must not be walked to validate itself.
		if starts.Max() != idx.itemCount {
			idx.Close()
			return nil, fmt.Errorf("%w: %s: runs span %d items, header says %d", ErrCorrupt, path, starts.Max(), idx.itemCount)
		}
		if want := (idx.itemCount + idx.pageSize - 1) / idx.pageSize; pages.Count() != want {
			idx.Close()
			return nil, fmt.Errorf("%w: %s: %d pages for %d items at page size %d, expected %d",
				ErrCorrupt, path, pages.Count(), idx.itemCount, idx.pageSize, want)
		}
	}
	return idx, nil
}

// readEliasFano rejects a sequence that does not fit in what is left of the
// file. ReadEliasFano itself reads a 16-byte header unchecked, and silently
// re-allocates off the mapping when the header asks for more words than are
// there.
func readEliasFano(b []byte) (*eliasfano32.EliasFano, int, error) {
	ef, n, err := eliasfano32.ReadEliasFanoChecked(b)
	if err != nil {
		return nil, 0, err
	}
	if ef.Count() == 0 {
		return nil, 0, errors.New("empty sequence")
	}
	return ef, n, nil
}

// Get returns the value of a run's item, and false when the index holds no such
// position. Callers take the position from a separate file, so out-of-range
// means the two files do not belong together.
func (i *Index) Get(run, item uint64) (uint64, bool) {
	if i.pages == nil || run >= i.starts.Count() {
		return 0, false
	}
	ordinal := i.starts.Get(run) + item
	if ordinal >= i.runEnd(run) {
		return 0, false
	}
	return i.pages.Get(ordinal / i.pageSize), true
}

// runEnd is where run's items stop: the next run's start, or the item count for
// the last run.
func (i *Index) runEnd(run uint64) uint64 {
	if run+1 < i.starts.Count() {
		return i.starts.Get(run + 1)
	}
	return i.itemCount
}

func (i *Index) Empty() bool { return i == nil || i.pages == nil }

// DisableReadAhead - usage: `defer i.MadvSequential().DisableReadAhead()`.
func (i *Index) DisableReadAhead() {
	if i == nil || i.m == nil {
		return
	}
	if left := i.readAheadRefcnt.Add(-1); left == 0 {
		_ = mmap.MadviseRandom(i.m)
	}
}

func (i *Index) MadvSequential() *Index {
	if i == nil || i.m == nil {
		return i
	}
	i.readAheadRefcnt.Add(1)
	_ = mmap.MadviseSequential(i.m)
	return i
}

func (i *Index) MadvNormal() *Index {
	if i == nil || i.m == nil {
		return i
	}
	i.readAheadRefcnt.Add(1)
	_ = mmap.MadviseNormal(i.m)
	return i
}

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
// once per page in order; the two may be interleaved. Close releases the
// builders, and must be called whether or not Build succeeds.
type Writer struct {
	path      string
	runCount  uint64
	pageCount uint64
	runs      uint64
	added     uint64
	startsB   *eliasfano32.OffHeapBuilder
	pagesB    *eliasfano32.OffHeapBuilder
	starts    *eliasfano32.EliasFano
	pages     *eliasfano32.EliasFano
	items     uint64
	pageSize  uint64
	itemCount uint64
	noFsync   bool
}

// NewWriter sizes the two sequences up front, which is all Elias-Fano needs.
// maxValue only has to be an upper bound. Both sequences are as large as the
// index itself, so they are built in tmpDir rather than on the heap.
func NewWriter(path, tmpDir string, pageSize, runCount, itemCount, maxValue uint64) (*Writer, error) {
	if pageSize == 0 {
		return nil, fmt.Errorf("%s: paged index page size is 0", path)
	}
	if (runCount == 0) != (itemCount == 0) {
		return nil, fmt.Errorf("%s: paged index sized for %d runs and %d items", path, runCount, itemCount)
	}
	w := &Writer{path: path, pageSize: pageSize, runCount: runCount, itemCount: itemCount}
	if runCount == 0 { // nothing to address: header only
		return w, nil
	}
	var err error
	if w.startsB, err = eliasfano32.NewEliasFanoOffHeap(runCount, itemCount, tmpDir); err != nil {
		w.Close()
		return nil, err
	}
	w.pageCount = (itemCount + pageSize - 1) / pageSize
	if w.pagesB, err = eliasfano32.NewEliasFanoOffHeap(w.pageCount, max(maxValue, 1), tmpDir); err != nil {
		w.Close()
		return nil, err
	}
	w.starts, w.pages = w.startsB.EliasFano, w.pagesB.EliasFano
	return w, nil
}

func (w *Writer) Close() {
	if w.startsB != nil {
		w.startsB.Close()
		w.startsB = nil
	}
	if w.pagesB != nil {
		w.pagesB.Close()
		w.pagesB = nil
	}
	w.starts, w.pages = nil, nil
}

func (w *Writer) NoFsync() { w.noFsync = true }

// AddRun records a run holding the given number of items.
func (w *Writer) AddRun(items uint64) {
	w.starts.AddOffset(w.items)
	w.items += items
	w.runs++
}

// AddPage records the value shared by the next page of items.
func (w *Writer) AddPage(value uint64) {
	w.pages.AddOffset(value)
	w.added++
}

func (w *Writer) Build() error {
	if w.runs != w.runCount || w.items != w.itemCount || w.added != w.pageCount {
		return fmt.Errorf("%s: added %d runs/%d items/%d pages, sized for %d/%d/%d",
			w.path, w.runs, w.items, w.added, w.runCount, w.itemCount, w.pageCount)
	}
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
	header[offVersion] = version
	binary.BigEndian.PutUint64(header[offPageSize:], w.pageSize)
	binary.BigEndian.PutUint64(header[offItemCount:], w.itemCount)
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
