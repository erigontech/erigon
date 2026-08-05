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

package state

import (
	"encoding/binary"
	"errors"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
)

func encodeRange(txFrom, txTo uint64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], txFrom)
	binary.BigEndian.PutUint64(b[8:16], txTo)
	return b
}

func decodeRange(v []byte) (txFrom, txTo uint64, err error) {
	if len(v) == 0 {
		return 0, 0, nil
	}
	if len(v) < 16 {
		return 0, 0, errors.New("prune progress: short range value")
	}
	return binary.BigEndian.Uint64(v[0:8]), binary.BigEndian.Uint64(v[8:16]), nil
}

const (
	flagDone = 1 << 0
)

func encodeProgress(prg prune.Progress, last []byte) []byte {
	b := make([]byte, 1+len(last))
	if prg == prune.Done {
		b[0] = flagDone
	}
	copy(b[1:], last)
	return b
}

func decodeProgress(v []byte) (prg prune.Progress, last []byte, err error) {
	if len(v) == 0 {
		return prune.First, nil, nil
	}
	done := (v[0] & flagDone) != 0
	if len(v) > 1 {
		last = v[1:]
	}
	if done {
		prg = prune.Done
	} else {
		prg = prune.InProgress
	}
	return prg, last, nil
}

func SavePruneValProgress(db kv.Putter, prunedTblName string, st *prune.Stat) error {
	if err := db.Put(kv.TblPruningValsProg, []byte(prunedTblName+"range"), encodeRange(st.TxFrom, st.TxTo)); err != nil {
		return err
	}

	if err := db.Put(kv.TblPruningValsProg, []byte(prunedTblName+"keys"), encodeProgress(st.KeyProgress, st.LastPrunedKey)); err != nil {
		return err
	}

	if err := db.Put(kv.TblPruningValsProg, []byte(prunedTblName+"vals"), encodeProgress(st.ValueProgress, st.LastPrunedValue)); err != nil {
		return err
	}

	return nil
}

func InvalidatePruneProgress(db kv.Putter, prunedTblName string) error {
	if err := db.Delete(kv.TblPruningValsProg, []byte(prunedTblName+"range")); err != nil {
		return err
	}

	if err := db.Delete(kv.TblPruningValsProg, []byte(prunedTblName+"keys")); err != nil {
		return err
	}

	if err := db.Delete(kv.TblPruningValsProg, []byte(prunedTblName+"vals")); err != nil {
		return err
	}

	return nil
}

func GetPruneValProgress(db kv.Getter, tbl []byte) (*prune.Stat, error) {
	st := &prune.Stat{}

	r, err := db.GetOne(kv.TblPruningValsProg, append(tbl, "range"...))
	if err != nil {
		return nil, err
	}
	st.TxFrom, st.TxTo, err = decodeRange(r)
	if err != nil {
		return nil, err
	}

	v, err := db.GetOne(kv.TblPruningValsProg, append(tbl, "vals"...))
	if err != nil {
		return nil, err
	}
	st.ValueProgress, st.LastPrunedValue, err = decodeProgress(v)
	if err != nil {
		return nil, err
	}

	k, err := db.GetOne(kv.TblPruningValsProg, append(tbl, "keys"...))
	if err != nil {
		return nil, err
	}
	st.KeyProgress, st.LastPrunedKey, err = decodeProgress(k)
	if err != nil {
		return nil, err
	}

	return st, nil
}
