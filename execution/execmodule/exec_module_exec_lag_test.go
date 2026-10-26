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

package execmodule_test

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// newExecLagTester leaves block 1 executed and blocks 2..1+lag canonical but
// unexecuted, the state a node restarts into after its execution stage fell
// behind its headers. It returns the next block, a payload whose parent is the
// canonical head.
func newExecLagTester(t *testing.T, loopBlockLimit uint, lag int) (*execmoduletester.ExecModuleTester, *types.Block) {
	t.Helper()
	ctx := t.Context()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	genesis := &types.Genesis{Config: chain.AllProtocolChanges, Alloc: types.GenesisAlloc{addr: {Balance: big.NewInt(1e18)}}}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key), execmoduletester.WithLoopBlockLimit(loopBlockLimit))
	chainPack, err := m.GenerateChain(lag+2, func(i int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(uint64(i), addr, uint256.NewInt(1), 50000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil), *types.LatestSignerForChainID(nil), key)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(ctx, chainPack.Blocks[:1]))

	lagging := chainPack.Blocks[1 : 1+lag]
	_, err = m.InsertBlocks(ctx, lagging)
	require.NoError(t, err)
	head := lagging[len(lagging)-1].NumberU64()
	require.NoError(t, m.DB.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		maxTxNum, err := rawdbv3.TxNums.Max(ctx, tx, 1)
		if err != nil {
			return err
		}
		for _, b := range lagging {
			maxTxNum += uint64(len(b.Transactions())) + 2
			if err := rawdbv3.TxNums.Append(tx, b.NumberU64(), maxTxNum); err != nil {
				return err
			}
			if err := rawdb.WriteCanonicalHash(tx, b.Hash(), b.NumberU64()); err != nil {
				return err
			}
		}
		for _, stage := range []stages.SyncStage{stages.Headers, stages.Bodies, stages.Senders} {
			if err := stages.SaveStageProgress(tx, stage, head); err != nil {
				return err
			}
		}
		return nil
	}))
	require.Equal(t, uint64(1), execProgress(t, m))

	payload := chainPack.Blocks[1+lag]
	_, err = m.InsertBlocks(ctx, []*types.Block{payload})
	require.NoError(t, err)
	return m, payload
}

func execProgress(t *testing.T, m *execmoduletester.ExecModuleTester) uint64 {
	t.Helper()
	var progress uint64
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) (err error) {
		progress, err = stages.GetStageProgress(tx, stages.Execution)
		return err
	}))
	return progress
}

func TestValidateChainAcceptsPayloadBeyondLoopBlockLimit(t *testing.T) {
	m, payload := newExecLagTester(t, 5, 12)

	for range 2 {
		result, err := m.ValidateChain(t.Context(), payload.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusMissingSegment, result.ValidationStatus)
	}
	require.Equal(t, uint64(1), execProgress(t, m))

	fcu, err := m.UpdateForkChoice(t.Context(), payload.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	require.Equal(t, payload.NumberU64(), execProgress(t, m))
}

func TestValidateChainExecutesLagWithinLoopBlockLimit(t *testing.T) {
	m, payload := newExecLagTester(t, 5, 3)

	result, err := m.ValidateChain(t.Context(), payload.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
}
