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

	"github.com/erigontech/erigon/db/datastruct/posidx"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/version"
)

// HistoryValueIndex resolves a history value's offset in the .v file.
//
// buildVI writes .v by walking .ef in key order and each key's txNums in order,
// so one key owns a run of consecutive values and a value's position is
// (key number in .ef, rank of its txNum in that key's list) - a posidx run
// and item. That needs the .efi built with Enums, and the .ef and .v step
// ranges to line up.
//
// v1 files instead hold a perfect hash over txNum+key. They are still read as
// they are: rpcdaemon and other read-only consumers cannot rebuild accessors,
// so a datadir has to keep working until its files are replaced.
type HistoryValueIndex struct {
	paged    *posidx.Index
	legacy   *recsplit.Index
	reader   *recsplit.IndexReader
	filePath string
}

func OpenHistoryValueIndex(path string, fileVer version.Version) (*HistoryValueIndex, error) {
	if fileVer.Less(version.V2_0) {
		idx, err := recsplit.OpenIndex(path)
		if err != nil {
			return nil, err
		}
		return &HistoryValueIndex{legacy: idx, reader: recsplit.NewIndexReader(idx), filePath: path}, nil
	}
	idx, err := posidx.Open(path)
	if err != nil {
		return nil, err
	}
	return &HistoryValueIndex{paged: idx, filePath: path}, nil
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
	return i.paged.Get(keyOrdinal, rank)
}

func (i *HistoryValueIndex) Empty() bool {
	if i == nil {
		return true
	}
	if i.legacy != nil {
		return i.legacy.Empty() // Lookup panics on a keyless recsplit index
	}
	return i.paged.Empty()
}

func (i *HistoryValueIndex) FilePath() string { return i.filePath }

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
