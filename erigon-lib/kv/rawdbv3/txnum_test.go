/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package rawdbv3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/log/v3"
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
