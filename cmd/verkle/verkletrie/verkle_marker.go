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

package verkletrie

import (
	"context"

	"github.com/erigontech/erigon/erigon-lib/kv"
	"github.com/erigontech/erigon/erigon-lib/kv/mdbx"
)

type VerkleMarker struct {
	db kv.RwDB
	tx kv.RwTx
}

//nolint:gocritic
func NewVerkleMarker(tempdir string) *VerkleMarker {
	markedSlotsDb, err := mdbx.NewTemporaryMdbx(context.TODO(), tempdir)
	if err != nil {
		panic(err)
	}

	tx, err := markedSlotsDb.BeginRw(context.TODO())
	if err != nil {
		panic(err)
	}

	return &VerkleMarker{
		db: markedSlotsDb,
		tx: tx,
	}
}

func (v *VerkleMarker) MarkAsDone(key []byte) error {
	return v.tx.Put(kv.Headers, key, []byte{0})
}

func (v *VerkleMarker) IsMarked(key []byte) (bool, error) {
	return v.tx.Has(kv.Headers, key)
}

func (v *VerkleMarker) Rollback() {
	v.tx.Rollback()
	v.db.Close()
}
