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

package rpcdaemontest

import (
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func CreateTestBlockAccessListExecModule(t *testing.T) (*execmoduletester.ExecModuleTester, *blockgen.ChainPack) {
	t.Helper()
	var config chain.Config
	require.NoError(t, copier.CopyWithOption(&config, chain.AllProtocolChanges, copier.Option{DeepCopy: true}))
	config.AmsterdamTime = common.NewUint64(20)
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(&config))
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	gasPrice := uint256.NewInt(m.Genesis.BaseFee().Uint64())
	var nonce uint64
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 5, func(i int, b *blockgen.BlockGen) {
		if i != 1 && i != 3 {
			return
		}
		txn, err := types.SignTx(types.NewTransaction(nonce, common.Address{byte(i + 1)}, uint256.NewInt(1), 50_000, gasPrice, nil), *signer, m.Key)
		require.NoError(t, err)
		nonce++
		b.AddTx(txn)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	require.Nil(t, chainPack.Blocks[0].Header().BlockAccessListHash)
	require.NotEqual(t, []byte{0xc0}, chainPack.BlockAccessLists[1])
	require.NotEqual(t, []byte{0xc0}, chainPack.BlockAccessLists[3])
	err = m.DB.Update(t.Context(), func(tx kv.RwTx) error {
		for _, block := range []*types.Block{chainPack.Blocks[2], chainPack.Blocks[4]} {
			if err := rawdb.WriteBlockAccessListBytes(tx, block.Hash(), block.NumberU64(), []byte{0xc0}); err != nil {
				return err
			}
		}
		rawdb.WriteForkchoiceSafe(tx, chainPack.Blocks[1].Hash())
		rawdb.WriteForkchoiceFinalized(tx, chainPack.Blocks[3].Hash())
		return nil
	})
	require.NoError(t, err)
	chainPack.BlockAccessLists[2] = []byte{0xc0}
	chainPack.BlockAccessLists[4] = []byte{0xc0}
	pruneBlockAccessListHistory(t, m, chainPack.Blocks[3])
	return m, chainPack
}

func pruneBlockAccessListHistory(t *testing.T, m *execmoduletester.ExecModuleTester, block *types.Block) {
	t.Helper()
	err := m.DB.Update(t.Context(), func(tx kv.RwTx) error {
		if err := tx.Delete(kv.BlockAccessList, dbutils.BlockBodyKey(block.NumberU64(), block.Hash())); err != nil {
			return err
		}
		minTxNum, err := m.BlockReader.TxnumReader().Min(t.Context(), tx, block.NumberU64())
		if err != nil {
			return err
		}
		var historyStart [8]byte
		binary.BigEndian.PutUint64(historyStart[:], minTxNum+1)
		for _, table := range []struct {
			name     string
			valueLen int
		}{
			{name: kv.TblAccountHistoryKeys, valueLen: length.Addr},
			{name: kv.TblStorageHistoryKeys, valueLen: length.Addr + length.Hash},
			{name: kv.TblCodeHistoryKeys, valueLen: length.Addr},
		} {
			for {
				key, err := kv.FirstKey(tx, table.name)
				if err != nil {
					return err
				}
				if key == nil {
					break
				}
				if err := tx.Delete(table.name, key); err != nil {
					return err
				}
			}
			if err := tx.Put(table.name, historyStart[:], make([]byte, table.valueLen)); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
}
