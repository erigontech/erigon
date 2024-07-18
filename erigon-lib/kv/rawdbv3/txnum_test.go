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

package rawdbv3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
)

func TestName(t *testing.T) {
	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	db := mdbx.NewMDBX(log.New()).InMem(dirs.Chaindata).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	t.Cleanup(db.Close)

	err := db.Update(context.Background(), func(tx kv.RwTx) error {
		require.NoError(TxNums.Append(tx, 0, 3))
		require.NoError(TxNums.Append(tx, 1, 99))
		require.NoError(TxNums.Append(tx, 2, 100))

		_, n, err := TxNums.FindBlockNum(tx, 10)
		require.NoError(err)
		require.Equal(1, int(n))

		_, n, err = TxNums.FindBlockNum(tx, 0)
		require.NoError(err)
		require.Equal(0, int(n))

		_, n, err = TxNums.FindBlockNum(tx, 3)
		require.NoError(err)
		require.Equal(0, int(n))
		_, n, err = TxNums.FindBlockNum(tx, 4)
		require.NoError(err)
		require.Equal(1, int(n))

		_, n, err = TxNums.FindBlockNum(tx, 99)
		require.NoError(err)
		require.Equal(1, int(n))

		_, n, err = TxNums.FindBlockNum(tx, 100)
		require.NoError(err)
		require.Equal(2, int(n))

		ok, _, err := TxNums.FindBlockNum(tx, 101)
		require.NoError(err)
		require.Equal(false, ok)
		return nil
	})
	require.NoError(err)
}
