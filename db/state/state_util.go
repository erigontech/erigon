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

	"github.com/erigontech/erigon/db/kv"
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
