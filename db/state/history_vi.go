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

package state

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/db/bufiopool"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/version"
)

// History values live in the .v file in (key order, then txNum order), so a
// value's position is cumValues[keyOrdinal]+rank and its offset only advances
// once per compressed page. Both sequences are monotone, which is what lets
// this replace a perfect hash over every (txNum,key).
//
// keyOrdinal comes from the matching .ef file, so this format needs the .efi
// built with Enums and the .ef and .v step ranges to line up.
const historyValueIndexVersion = 2

const historyValueIndexHeaderLen = 1 + 8

// HistoryValueIndex resolves a history value's .v offset from the key's ordinal
// in the .ef file and the rank of its txNum in that key's txNum list.
type HistoryValueIndex struct {
	f           *os.File
	m           mmap.Ro
	cumValues   *eliasfano32.EliasFano
	pageOffsets *eliasfano32.EliasFano
	pageSize    uint64
	filePath    string

	// v1 files address a value by txNum+key through a perfect hash. They are
	// still read as-is: rpcdaemon and other read-only consumers cannot rebuild
	// accessors, so a datadir must keep working until the files are replaced.
	legacy       *recsplit.Index
	legacyReader *recsplit.IndexReader
}

func OpenHistoryValueIndex(path string, fileVer version.Version) (*HistoryValueIndex, error) {
	if fileVer.Less(version.V2_0) {
		idx, err := recsplit.OpenIndex(path)
		if err != nil {
			return nil, err
		}
		return &HistoryValueIndex{filePath: path, legacy: idx, legacyReader: recsplit.NewIndexReader(idx)}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	idx := &HistoryValueIndex{f: f, filePath: path}
	defer func() {
		if idx.m == nil {
			f.Close()
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.Size() < historyValueIndexHeaderLen {
		return nil, fmt.Errorf("%s: truncated history value index", path)
	}
	m, err := mmap.OpenRo(f, int(fi.Size()))
	if err != nil {
		return nil, err
	}
	if v := m[0]; v != historyValueIndexVersion {
		_ = m.Unmap()
		return nil, fmt.Errorf("%s: history value index version %d, expected %d", path, v, historyValueIndexVersion)
	}
	idx.m = m
	idx.pageSize = binary.BigEndian.Uint64(m[1:])
	if idx.pageSize == 0 {
		idx.Close()
		return nil, fmt.Errorf("%s: history value index page size is 0", path)
	}
	if fi.Size() > historyValueIndexHeaderLen {
		var n int
		idx.cumValues, n = eliasfano32.ReadEliasFano(m[historyValueIndexHeaderLen:])
		idx.pageOffsets, _ = eliasfano32.ReadEliasFano(m[historyValueIndexHeaderLen+n:])
	}
	return idx, nil
}

// Lookup returns the offset in the .v file of the page holding the value.
// keyOrdinal is the key's position in the .ef file, rank the position of the
// txNum in that key's txNum list. txNum and key are only read by v1 files,
// which address the value by txNum+key rather than by position.
func (i *HistoryValueIndex) Lookup(keyOrdinal, rank, txNum uint64, key []byte) (uint64, bool) {
	if i.legacy != nil {
		var txNumKey [8]byte
		binary.BigEndian.PutUint64(txNumKey[:], txNum)
		return i.legacyReader.Lookup2(txNumKey[:], key)
	}
	if i.cumValues == nil {
		return 0, false
	}
	return i.pageOffsets.Get((i.cumValues.Get(keyOrdinal) + rank) / i.pageSize), true
}

func (i *HistoryValueIndex) PageSize() uint64 { return i.pageSize }
func (i *HistoryValueIndex) FilePath() string { return i.filePath }

func (i *HistoryValueIndex) Empty() bool {
	return i == nil || (i.legacy == nil && i.cumValues == nil)
}

func (i *HistoryValueIndex) KeyCount() uint64 {
	switch {
	case i.Empty():
		return 0
	case i.legacy != nil:
		return i.legacy.KeyCount()
	default:
		return i.cumValues.Count()
	}
}

func (i *HistoryValueIndex) Close() {
	if i == nil {
		return
	}
	if i.legacy != nil {
		i.legacy.Close()
		i.legacy, i.legacyReader = nil, nil
	}
	if i.m != nil {
		_ = i.m.Unmap()
		i.m = nil
		i.cumValues, i.pageOffsets = nil, nil
	}
	if i.f != nil {
		_ = i.f.Close()
		i.f = nil
	}
}

// HistoryValueIndexWriter builds a HistoryValueIndex. AddKey is called once per
// .ef key in key order, AddPageOffset once per page in .v order; the two may be
// interleaved.
type HistoryValueIndexWriter struct {
	path        string
	cumValues   *eliasfano32.EliasFano
	pageOffsets *eliasfano32.EliasFano
	cum         uint64
	pageSize    uint64
	noFsync     bool
}

// vFileSize bounds the page offsets. It is the .v file size rather than the
// last page's offset, which is not known before the walk; Elias-Fano only needs
// an upper bound to size itself.
func NewHistoryValueIndexWriter(path string, pageSize, keyCount, valueCount, vFileSize uint64) (*HistoryValueIndexWriter, error) {
	if pageSize == 0 {
		return nil, fmt.Errorf("%s: history value index page size is 0", path)
	}
	w := &HistoryValueIndexWriter{path: path, pageSize: pageSize}
	if keyCount == 0 || valueCount == 0 { // empty .v: header only, nothing to look up
		return w, nil
	}
	pageCount := (valueCount + pageSize - 1) / pageSize
	w.cumValues = eliasfano32.NewEliasFano(keyCount, valueCount)
	w.pageOffsets = eliasfano32.NewEliasFano(pageCount, max(vFileSize, 1))
	return w, nil
}

func (w *HistoryValueIndexWriter) NoFsync() { w.noFsync = true }

func (w *HistoryValueIndexWriter) AddKey(values uint64) {
	w.cumValues.AddOffset(w.cum)
	w.cum += values
}

func (w *HistoryValueIndexWriter) AddPageOffset(offset uint64) {
	w.pageOffsets.AddOffset(offset)
}

func (w *HistoryValueIndexWriter) Build() error {
	if w.cumValues != nil {
		w.cumValues.Build()
		w.pageOffsets.Build()
	}

	f, err := dir.CreateTemp(w.path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufiopool.Writer(f)
	defer bufiopool.PutWriter(bw)

	var header [historyValueIndexHeaderLen]byte
	header[0] = historyValueIndexVersion
	binary.BigEndian.PutUint64(header[1:], w.pageSize)
	if _, err := bw.Write(header[:]); err != nil {
		return err
	}
	if w.cumValues != nil {
		if err := w.cumValues.Write(bw); err != nil {
			return err
		}
		if err := w.pageOffsets.Write(bw); err != nil {
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
