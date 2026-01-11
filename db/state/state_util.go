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

// SaveExecV3PruneProgress saves latest pruned key in given table to the database.
// nil key also allowed and means that latest pruning run has been finished.
func SaveExecV3PruneProgress(db kv.Putter, prunedTblName string, prunedKey []byte) error {
	empty := make([]byte, 1)
	if prunedKey != nil {
		empty[0] = 1
	}
	return db.Put(kv.TblPruningProgress, []byte(prunedTblName), append(empty, prunedKey...))
}

// GetExecV3PruneProgress retrieves saved progress of given table pruning from the database.
// For now it is latest pruned key in prunedTblName
func GetExecV3PruneProgress(db kv.Getter, prunedTblName string) (pruned []byte, err error) {
	v, err := db.GetOne(kv.TblPruningProgress, []byte(prunedTblName))
	if err != nil {
		return nil, err
	}
	switch len(v) {
	case 0:
		return nil, nil
	case 1:
		if v[0] == 1 {
			return []byte{}, nil
		}
		// nil values returned an empty key which actually is a value
		return nil, nil
	default:
		return v[1:], nil
	}
}

// SaveExecV3PrunableProgress saves latest pruned key in given table to the database.
func SaveExecV3PrunableProgress(db kv.RwTx, tbl []byte, step kv.Step) error {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(step))
	if err := db.Delete(kv.TblPruningProgress, append(kv.MinimumPrunableStepDomainKey, tbl...)); err != nil {
		return err
	}
	return db.Put(kv.TblPruningProgress, append(kv.MinimumPrunableStepDomainKey, tbl...), v)
}

// GetExecV3PrunableProgress retrieves saved progress of given table pruning from the database.
func GetExecV3PrunableProgress(db kv.Getter, tbl []byte) (step kv.Step, err error) {
	v, err := db.GetOne(kv.TblPruningProgress, append(kv.MinimumPrunableStepDomainKey, tbl...))
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	return kv.Step(binary.BigEndian.Uint64(v)), nil
}

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
