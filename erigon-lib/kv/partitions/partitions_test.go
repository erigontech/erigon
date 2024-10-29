// Copyright 2021 The Erigon Authors
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

package partitions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
)

func TestRotate(t *testing.T) {
	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	db := mdbx.NewMDBX(log.New()).InMem(dirs.Chaindata).MustOpen()
	t.Cleanup(db.Close)

	tx, err := db.BeginRw(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	primary, secondary, err := kv.TxLookup.Partitions(tx)
	require.NoError(err)
	require.Equal(kv.Partitions[kv.TxLookup][0], primary)
	require.Equal(kv.Partitions[kv.TxLookup][1], secondary)

	done, err := kv.TxLookup.Rotate(tx)
	require.NoError(err)
	require.True(done)

	primary, secondary, err = kv.TxLookup.Partitions(tx)
	require.NoError(err)
	require.Equal(kv.Partitions[kv.TxLookup][1], primary)
	require.Equal(kv.Partitions[kv.TxLookup][0], secondary)

	//write to primary
	err = tx.Put(primary, []byte{1}, []byte{1})
	require.NoError(err)
	cnt, err := tx.Count(primary)
	require.NoError(err)
	require.Equal(1, int(cnt))

	v, err := kv.TxLookup.GetOne(tx, []byte{1})
	require.NoError(err)
	require.Equal([]byte{1}, v)

	//see after rotate
	done, err = kv.TxLookup.Rotate(tx)
	require.NoError(err)
	require.True(done)

	v, err = kv.TxLookup.GetOne(tx, []byte{1})
	require.NoError(err)
	require.Equal([]byte{1}, v)

	primary, secondary, err = kv.TxLookup.Partitions(tx)
	require.NoError(err)

	cnt, err = tx.Count(primary)
	require.NoError(err)
	require.Equal(0, int(cnt))
}
