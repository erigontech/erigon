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

func TestReorgReportsUntouchedAccountAcrossSteps(t *testing.T) {
	for _, validate := range []bool{false, true} {
		name := "ForkchoiceOnly"
		if validate {
			name = "NewPayload"
		}
		t.Run(name, func(t *testing.T) {
			const stepSize = uint64(8)
			key, err := crypto.GenerateKey()
			require.NoError(t, err)
			otherKey, err := crypto.GenerateKey()
			require.NoError(t, err)
			sender := crypto.PubkeyToAddress(key.PublicKey)
			otherSender := crypto.PubkeyToAddress(otherKey.PublicKey)
			m := New(t, WithKey(key), WithStepSize(stepSize), WithGenesisSpec(&types.Genesis{
				Config: chain.AllProtocolChanges,
				Alloc: types.GenesisAlloc{
					sender:      {Balance: big.NewInt(1e18)},
					otherSender: {Balance: big.NewInt(1e18)},
				},
			}))
			signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
			gasPrice := uint256.NewInt(m.Genesis.BaseFee().Uint64() * 2)
			var branches [2]*blockgen.ChainPack
			for branch := range branches {
				branches[branch], err = m.GenerateChain(7, func(i int, b *blockgen.BlockGen) {
					signingKey, nonce := key, uint64(i)
					if branch == 1 && i > 0 {
						signingKey, nonce = otherKey, uint64(i-1)
					}
					txn, err := types.SignTx(types.NewTransaction(nonce, common.Address{0xaa}, uint256.NewInt(1), 21_000, gasPrice, nil), signer, signingKey)
					require.NoError(t, err)
					b.AddTx(txn)
				})
				require.NoError(t, err)
			}
			canonical, side := branches[0], branches[1]
			require.Equal(t, canonical.Blocks[0].Hash(), side.Blocks[0].Hash())
			waitForHead := subscribeStateChangeHeads(t, m)
			require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), canonical.Blocks[:1]))
			m.ExecModule.WaitIdle(t.Context())
			waitForHead(canonical.Blocks[0].Hash())
			var want []byte
			require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				value, _, err := tx.GetLatest(kv.AccountsDomain, sender[:], kv.GetLatestOptions{})
				want = bytes.Clone(value)
				return err
			}))
			require.NotEmpty(t, want)
			require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), canonical.Blocks[1:]))
			m.ExecModule.WaitIdle(t.Context())
			waitForHead(canonical.TopBlock.Hash())
			require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				first, err := m.BlockReader.TxnumReader().Min(t.Context(), tx, 2)
				require.NoError(t, err)
				last, err := m.BlockReader.TxnumReader().Max(t.Context(), tx, canonical.TopBlock.NumberU64())
				require.NoError(t, err)
				require.Greater(t, last/stepSize, first/stepSize, "unwind must cross state steps")
				return nil
			}))
			status, err := m.InsertBlocks(t.Context(), side.Blocks[1:])
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			if validate {
				result, err := m.ValidateChain(t.Context(), side.TopBlock.Header())
				require.NoError(t, err)
				require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
				require.Equal(t, side.TopBlock.Hash(), m.ForkValidator.ExtendingForkHeadHash())
			} else {
				require.NotEqual(t, side.TopBlock.Hash(), m.ForkValidator.ExtendingForkHeadHash())
			}
			result, err := m.UpdateForkChoice(t.Context(), side.TopBlock.Header())
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
			m.ExecModule.WaitIdle(t.Context())
			batch := waitForHead(side.TopBlock.Hash())
			require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				value, _, err := tx.GetLatest(kv.AccountsDomain, sender[:], kv.GetLatestOptions{})
				require.NoError(t, err)
				require.Equal(t, want, value, "committed account must match the common ancestor")
				return nil
			}))
			var updates []*remoteproto.AccountChange
			for _, change := range batch.ChangeBatch {
				for _, account := range change.Changes {
					if gointerfaces.ConvertH160toAddress(account.Address) == sender {
						require.Equal(t, remoteproto.Direction_UNWIND, change.Direction, "the replacement branch must leave this account untouched")
						updates = append(updates, account)
					}
				}
			}
			require.Len(t, updates, 1)
			require.Equal(t, remoteproto.Action_UPSERT, updates[0].Action)
			require.Equal(t, want, updates[0].Data, "notification must match the restored account")
		})
	}
}

func TestForkchoiceReportsAccountDeletion(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	cfg := *chain.AllProtocolChanges
	// Before Cancun, SELFDESTRUCT removes a contract even if an earlier transaction created it.
	cfg.CancunTime, cfg.PragueTime, cfg.OsakaTime, cfg.AmsterdamTime = nil, nil, nil, nil
	m := New(t, WithKey(key), WithoutExperimentalBAL(), WithGenesisSpec(&types.Genesis{
		Config: &cfg,
		Alloc:  types.GenesisAlloc{sender: {Balance: big.NewInt(1e18)}},
	}))
	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	sign := func(tx types.Transaction) types.Transaction {
		signed, err := types.SignTx(tx, signer, key)
		require.NoError(t, err)
		return signed
	}
	address := types.CreateAddress(sender, 0)
	gasPrice := uint256.NewInt(m.Genesis.BaseFee().Uint64() * 2)
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
