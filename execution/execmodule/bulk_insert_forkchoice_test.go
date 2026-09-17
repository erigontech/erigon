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
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// bulkInsertChain builds a tester plus a chain of length blocks, one
// value transfer per block so each block carries txNums to append.
func bulkInsertChain(t *testing.T, length int) (*execmoduletester.ExecModuleTester, *blockgen.ChainPack) {
	t.Helper()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)
	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(privKey))
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, length, func(i int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(
			types.NewTransaction(uint64(i), senderAddr, uint256.NewInt(1_000), 50_000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil),
			privKey,
		)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	require.Len(t, chainPack.Blocks, length)
	return m, chainPack
}

// assertForkChoiceAdvancesHead inserts the whole chain in one batch and
// drives a single forkchoice at its tip — the shape a CL backfill uses,
// as opposed to the per-block insert+FCU the other tests drive.
func assertForkChoiceAdvancesHead(t *testing.T, batch int) {
	t.Helper()
	ctx := context.Background()
	m, chainPack := bulkInsertChain(t, batch)

	status, err := insertBlocks(ctx, m.ExecModule, chainPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)

	tip := chainPack.Blocks[len(chainPack.Blocks)-1].Header()
	res, err := updateForkChoice(ctx, m.ExecModule, tip)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, res.Status,
		"forkchoice at the tip of a %d-block batch must advance the head, not report the head as a bad block", batch)

	require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		lastBlock, _, err := rawdbv3.TxNums.Last(tx)
		require.NoError(t, err)
		require.Equal(t, tip.Number.Uint64(), lastBlock,
			"TxNums must be extended to the forkchoice head, otherwise execution has nothing to do and the head never moves")
		return nil
	}))
}

// TestBulkInsertThenForkChoiceAdvancesHead pins that a batch larger than
// the 16-block overlay threshold still advances the head. Above it
// InsertBlocks flushes the block overlay to the DB and closes it, so the
// forkchoice runs with no overlay of its own — the path a CL backfill
// takes for every 1000-block batch.
func TestBulkInsertThenForkChoiceAdvancesHead(t *testing.T) {
	assertForkChoiceAdvancesHead(t, 20)
}

// TestSmallInsertThenForkChoiceAdvancesHead is the control: the same
// sequence below the overlay threshold, where the blocks stay in the
// overlay the forkchoice inherits.
func TestSmallInsertThenForkChoiceAdvancesHead(t *testing.T) {
	assertForkChoiceAdvancesHead(t, 10)
}
