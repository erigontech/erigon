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
	"context"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
)

// ScanHistoryKeys calls visit with each key of the domain's history index file
// [fromTxNum, toTxNum), in key order. It reads the file sequentially.
func (at *AggregatorRoTx) ScanHistoryKeys(ctx context.Context, name kv.Domain, fromTxNum, toTxNum uint64, visit func(key []byte) error) error {
	ht := at.d[name].ht
	ef, err := historyIndexFile(ht, fromTxNum, toTxNum)
	if err != nil {
		return err
	}
	view, err := ef.src.decompressor.OpenSequentialView()
	if err != nil {
		return err
	}
	defer view.Close()
	r := seg.NewReader(view.MakeGetter(), ht.h.InvertedIndex.Compression)
	var key []byte
	for i := 0; r.HasNext(); i++ {
		key, _ = r.Next(key[:0])
		if !r.HasNext() {
			return fmt.Errorf("%s truncated at key %x", ef.Fullpath(), key)
		}
		r.Skip()
		if err := visit(key); err != nil {
			return err
		}
		if i%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

// ScanHistory calls visit with each change of the domain's history files [fromTxNum,
// toTxNum): the key, the txNum and the value before the change, in key and txNum order. It
// reads the .ef and .v files sequentially. key and before are valid only until visit returns.
func (at *AggregatorRoTx) ScanHistory(ctx context.Context, name kv.Domain, fromTxNum, toTxNum uint64, visit func(key []byte, txNum uint64, before []byte) error) error {
	ht := at.d[name].ht
	ef, err := historyIndexFile(ht, fromTxNum, toTxNum)
	if err != nil {
		return err
	}
	v, ok := ht.pairedFile(ef)
	if !ok {
		return fmt.Errorf("%s has no .v of the same range", ef.Fullpath())
	}
	efView, err := ef.src.decompressor.OpenSequentialView()
	if err != nil {
		return err
	}
	defer efView.Close()
	vView, err := v.src.decompressor.OpenSequentialView()
	if err != nil {
		return err
	}
	defer vView.Close()
	efReader := seg.NewReader(efView.MakeGetter(), ht.h.InvertedIndex.Compression)
	vReader := seg.NewPagedReader(seg.NewReader(vView.MakeGetter(), ht.h.Compression), int(ht.h.valuesPerPage(v.src.decompressor)), true)
	var (
		seq            multiencseq.SequenceReader
		it             multiencseq.SequenceIterator
		key, efVal, vb []byte
	)
	for i := 0; efReader.HasNext(); i++ {
		key, _ = efReader.Next(key[:0])
		if !efReader.HasNext() {
			return fmt.Errorf("%s truncated at key %x", ef.Fullpath(), key)
		}
		efVal, _ = efReader.Next(efVal[:0])
		seq.Reset(ef.startTxNum, efVal)
		it.Reset(&seq, 0)
		for it.HasNext() {
			txNum, err := it.Next()
			if err != nil {
				return err
			}
			if !vReader.HasNext() {
				return fmt.Errorf("%s has no value for %x at txNum %d", v.Fullpath(), key, txNum)
			}
			var before []byte
			_, before, vb, _ = vReader.Next2(vb[:0])
			if err := visit(key, txNum, before); err != nil {
				return err
			}
		}
		if i%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	if vReader.HasNext() {
		return fmt.Errorf("%s has values past the last .ef entry", v.Fullpath())
	}
	return nil
}

func historyIndexFile(ht *HistoryRoTx, fromTxNum, toTxNum uint64) (visibleFile, error) {
	i := slices.IndexFunc(ht.iit.files, func(f visibleFile) bool { return f.startTxNum == fromTxNum && f.endTxNum == toTxNum })
	if i < 0 {
		return visibleFile{}, fmt.Errorf("%s has no history index file %d-%d", ht.h.FilenameBase, fromTxNum, toTxNum)
	}
	return ht.iit.files[i], nil
}
