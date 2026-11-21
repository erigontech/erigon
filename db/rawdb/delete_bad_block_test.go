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

package rawdb_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/rawdb"
)

type failingDeleteTx struct {
	kv.RwTx
	table string
}

func (tx failingDeleteTx) Delete(table string, k []byte) error {
	if table == tx.table {
		return errors.New("delete failed")
	}
	return tx.RwTx.Delete(table, k)
}

// A failed delete fails the purge, so the caller rolls back instead of committing half of it.
func TestDeleteBadBlockReturnsEveryDeleteError(t *testing.T) {
	for _, table := range []string{kv.Senders, kv.BlockBody, kv.BlockAccessList, kv.Headers, kv.HeaderNumber} {
		t.Run(table, func(t *testing.T) {
			_, tx := mdbxtest.NewTestTx(t)
			err := rawdb.DeleteBadBlock(failingDeleteTx{RwTx: tx, table: table}, common.Hash{1}, 1)
			require.ErrorContains(t, err, "delete failed")
		})
	}
}
