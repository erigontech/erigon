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

package execmoduletester

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func newAccountDeletionTester(t *testing.T) (*ExecModuleTester, func(types.Transaction) types.Transaction, *uint256.Int) {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	cfg := *chain.AllProtocolChanges
	// Before Cancun, SELFDESTRUCT removes a contract even if an earlier transaction created it.
	cfg.CancunTime, cfg.PragueTime, cfg.OsakaTime, cfg.AmsterdamTime = nil, nil, nil, nil
	m := New(t, WithKey(key), WithGenesisSpec(&types.Genesis{
		Config: &cfg,
		Alloc:  types.GenesisAlloc{sender: {Balance: big.NewInt(1e18)}},
	}))
	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	sign := func(tx types.Transaction) types.Transaction {
		t.Helper()
		signed, err := types.SignTx(tx, signer, key)
		require.NoError(t, err)
		return signed
	}
	return m, sign, uint256.NewInt(m.Genesis.BaseFee().Uint64() * 2)
}

func TestForkchoiceReportsAccountDeletion(t *testing.T) {
	m, sign, gasPrice := newAccountDeletionTester(t)
	address := types.CreateAddress(m.Address, 0)
	blocks, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		// The constructor writes slot 0, then returns CALLER SELFDESTRUCT as runtime code.
		init := common.FromHex("60016000556002601160003960026000f333ff")
		b.AddTx(sign(types.NewContractCreation(0, uint256.NewInt(0), 300_000, gasPrice, init)))
		b.AddTx(sign(types.NewTransaction(1, address, uint256.NewInt(0), 100_000, gasPrice, nil)))
	})
	require.NoError(t, err)
	waitForHead := subscribeStateChangeHeads(t, m)
	status, err := m.InsertBlocks(t.Context(), blocks.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := m.UpdateForkChoice(t.Context(), blocks.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.WaitIdle(t.Context())
	batch := waitForHead(blocks.TopBlock.Hash())
	require.Len(t, batch.ChangeBatch, 1)
	var changes []*remoteproto.AccountChange
	for _, change := range batch.ChangeBatch[0].Changes {
		if gointerfaces.ConvertH160toAddress(change.Address) == address {
			changes = append(changes, change)
		}
	}
	require.Len(t, changes, 1)
	require.Equal(t, remoteproto.Action_REMOVE, changes[0].Action)
	require.Empty(t, changes[0].Data)
	require.Empty(t, changes[0].Code)
	require.Empty(t, changes[0].StorageChanges)

	cache := kvcache.New(kvcache.DefaultCoherentConfig)
	cache.OnNewBlock(batch)
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		account, _, err := tx.GetLatest(kv.AccountsDomain, address[:], kv.GetLatestOptions{})
		require.NoError(t, err)
		require.Empty(t, account, "committed state must contain no contract after SELFDESTRUCT")
		id, err := tx.ReadSequence(string(kv.PlainStateVersion))
		require.NoError(t, err)
		require.Equal(t, batch.StateVersionId, id)
		view, err := cache.View(t.Context(), tx)
		require.NoError(t, err)
		cached, err := view.Get(address[:])
		require.NoError(t, err)
		require.Empty(t, cached, "cache must agree with committed account deletion")
		return nil
	}))
}

func TestForkchoiceInvalidatesDeletedStorage(t *testing.T) {
	for _, sameBlockRefund := range []bool{false, true} {
		name := "later_refund"
		if sameBlockRefund {
			name = "same_block_refund"
		}
		t.Run(name, func(t *testing.T) {
			m, sign, gasPrice := newAccountDeletionTester(t)
			address := types.CreateAddress(m.Address, 0)
			refund := sign(types.NewTransaction(2, address, uint256.NewInt(1), 21_000, gasPrice, nil))
			blocks, err := m.GenerateChain(3, func(i int, b *blockgen.BlockGen) {
				switch i {
				case 0:
					// The constructor fills two slots; the runtime overwrites only slot 0 before SELFDESTRUCT.
					init := common.FromHex("600160005560026001556007601660003960076000f3600360005533ff")
					b.AddTx(sign(types.NewContractCreation(0, uint256.NewInt(0), 300_000, gasPrice, init)))
				case 1:
					b.AddTx(sign(types.NewTransaction(1, address, uint256.NewInt(0), 100_000, gasPrice, nil)))
					if sameBlockRefund {
						b.AddTx(refund)
					}
				case 2:
					if !sameBlockRefund {
						b.AddTx(refund)
					}
				}
			})
			require.NoError(t, err)
			for _, receipts := range blocks.Receipts {
				for _, receipt := range receipts {
					require.Equal(t, types.ReceiptStatusSuccessful, receipt.Status)
				}
			}
			waitForHead := subscribeStateChangeHeads(t, m)
			cache := kvcache.New(kvcache.DefaultCoherentConfig)
			var previousVersion uint64
			selectHead := func(block *types.Block, storagePresent bool) {
				t.Helper()
				result, err := m.UpdateForkChoice(t.Context(), block.Header())
				require.NoError(t, err)
				require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
				m.ExecModule.WaitIdle(t.Context())
				batch := waitForHead(block.Hash())
				if previousVersion != 0 {
					require.Equal(t, previousVersion+1, batch.StateVersionId, "cache generations must stay connected")
				}
				previousVersion = batch.StateVersionId
				cache.OnNewBlock(batch)
				require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
					view, err := cache.View(t.Context(), tx)
					require.NoError(t, err)
					account, _, err := tx.GetLatest(kv.AccountsDomain, address[:], kv.GetLatestOptions{})
					require.NoError(t, err)
					if block.NumberU64() == 2 && !sameBlockRefund {
						require.Empty(t, account)
					} else {
						require.NotEmpty(t, account)
					}
					cachedAccount, err := view.Get(address[:])
					require.NoError(t, err)
					require.True(t, bytes.Equal(account, cachedAccount), "account: stored %x cached %x", account, cachedAccount)
					for slot := range 2 {
						location := common.Hash{31: byte(slot)}
						storageKey := append(bytes.Clone(address[:]), location[:]...)
						stored, _, err := tx.GetLatest(kv.StorageDomain, storageKey, kv.GetLatestOptions{})
						require.NoError(t, err)
						if storagePresent {
							require.Equal(t, []byte{byte(slot + 1)}, stored)
						} else {
							require.Empty(t, stored)
						}
						cached, err := view.Get(storageKey)
						require.NoError(t, err)
						require.True(t, bytes.Equal(stored, cached), "head %d slot %d: stored %x cached %x", block.NumberU64(), slot, stored, cached)
					}
					return nil
				}))
			}
			for i, block := range blocks.Blocks {
				status, err := m.InsertBlocks(t.Context(), []*types.Block{block})
				require.NoError(t, err)
				require.Equal(t, execmodule.ExecutionStatusSuccess, status)
				selectHead(block, i == 0)
			}
			selectHead(blocks.Blocks[0], true)
			selectHead(blocks.TopBlock, false)
		})
	}
}
