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

	"github.com/erigontech/erigon/db/datastruct/pagedidx"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/version"
)

// HistoryValueIndex resolves a history value's offset in the .v file.
//
// buildVI writes .v by walking .ef in key order and each key's txNums in order,
// so a value's position is (key ordinal in .ef, rank of its txNum) - which is
// what pagedidx addresses. That needs the .efi built with Enums, and the .ef
// and .v step ranges to line up.
//
// v1 files instead hold a perfect hash over txNum+key. They are still read as
// they are: rpcdaemon and other read-only consumers cannot rebuild accessors,
// so a datadir has to keep working until its files are replaced.
type HistoryValueIndex struct {
	paged  *pagedidx.Index
	legacy *recsplit.Index
	reader *recsplit.IndexReader
}

func OpenHistoryValueIndex(path string, fileVer version.Version) (*HistoryValueIndex, error) {
	if fileVer.Less(version.V2_0) {
		idx, err := recsplit.OpenIndex(path)
		if err != nil {
			return nil, err
		}
		return &HistoryValueIndex{legacy: idx, reader: recsplit.NewIndexReader(idx)}, nil
	}
	idx, err := pagedidx.Open(path)
	if err != nil {
		return nil, err
	}
	return &HistoryValueIndex{paged: idx}, nil
}

// Lookup returns the offset in the .v file of the page holding the value.
// txNum and key are only read by v1 files, which address by txNum+key rather
// than by position.
func (i *HistoryValueIndex) Lookup(keyOrdinal, rank, txNum uint64, key []byte) (uint64, bool) {
	if i.legacy != nil {
		var txNumKey [8]byte
		binary.BigEndian.PutUint64(txNumKey[:], txNum)
		return i.reader.Lookup2(txNumKey[:], key)
	}
	if i.paged.Empty() {
		return 0, false
	}
	return i.paged.Get(keyOrdinal, rank), true
}

func (i *HistoryValueIndex) Empty() bool {
	return i == nil || (i.legacy == nil && i.paged.Empty())
}

func (i *HistoryValueIndex) FilePath() string {
	if i.legacy != nil {
		return i.legacy.FilePath()
	}
	return i.paged.FilePath()
}

// KeyCount returns the number of .ef keys covered, or for a v1 file the number
// of values it indexes.
func (i *HistoryValueIndex) KeyCount() uint64 {
	switch {
	case i.Empty():
		return 0
	case i.legacy != nil:
		return i.legacy.KeyCount()
	default:
		return i.paged.GroupCount()
	}
}

func (i *HistoryValueIndex) Close() {
	if i == nil {
		return
	}
	if i.legacy != nil {
		i.legacy.Close()
		i.legacy, i.reader = nil, nil
	}
	i.paged.Close()
	i.paged = nil
}
