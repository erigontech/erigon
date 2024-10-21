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

package membatch

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/log/v3"
)

func TestMapmutation_Flush_Close(t *testing.T) {
	db := memdb.NewTestDB(t, kv.ChainDB)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	batch := NewHashBatch(tx, nil, os.TempDir(), log.New())
	defer func() {
		batch.Close()
	}()
	assert.Equal(t, batch.size, 0)
	err = batch.Put(kv.ChaindataTables[0], []byte{1}, []byte{1})
	require.NoError(t, err)
	assert.Equal(t, batch.size, 2)
	err = batch.Put(kv.ChaindataTables[0], []byte{2}, []byte{2})
	require.NoError(t, err)
	assert.Equal(t, batch.size, 4)
	err = batch.Put(kv.ChaindataTables[0], []byte{1}, []byte{3, 2, 1, 0})
	require.NoError(t, err)
	assert.Equal(t, batch.size, 7)
	err = batch.Flush(context.Background(), tx)
	require.NoError(t, err)
	batch.Close()
	batch.Close()
}
