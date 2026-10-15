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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// An administrative setHead moves the head below what the consensus layer has
// already finalised — that is the whole point of the operation, and the soak
// does it every iteration. The consensus layer keeps sending the forkchoice it
// was already on, so the first thing it does afterwards is ask for a head whose
// finalised block sits above where the node now is.
//
// The engine API requires refusing a state whose finalised block is not on the
// head's chain, and a refused forkchoice is retried unchanged every slot. So if
// an unwind can leave the node in that state there is no way out of it: soak
// cycle 6 sat through 137 identical refusals with execution healthy and the head
// frozen one block above where it started.
func TestSetHeadBelowFinalisedStillAcceptsForkchoice(t *testing.T) {
	const stepSize uint64 = 8

	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	gspec := &types.Genesis{
		Config:   chain.TestChainBerlinConfig,
		Alloc:    types.GenesisAlloc{addr: {Balance: big.NewInt(common.Ether)}},
		GasLimit: 10_000_000,
	}
	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(gspec),
		execmoduletester.WithKey(key),
		execmoduletester.WithStepSize(stepSize),
		execmoduletester.WithAdminUnwindWired(),
	)
	ctx := m.Ctx

	signer := types.LatestSignerForChainID(nil)
	to := common.Address{0x42}
	pack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 24, func(i int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(
			types.NewTransaction(uint64(i), to, uint256.NewInt(1_000_000), 21_000, new(uint256.Int), nil),
			*signer, key,
		)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))

	tip := pack.Blocks[len(pack.Blocks)-1].Header()
	// Finalise well behind the tip, as a consensus layer does.
	finalised := pack.Blocks[len(pack.Blocks)-9].Header()
	res, err := m.UpdateForkChoice(ctx, tip,
		execmoduletester.WithFinalisedHash(finalised.Hash()),
		execmoduletester.WithSafeHash(finalised.Hash()))
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, res.Status, "baseline forkchoice must be accepted")

	// Administrative unwind to below the finalised block.
	target := finalised.Number.Uint64() - 4
	require.NoError(t, m.ExecModule.SetHead(ctx, target))

	// The consensus layer is unaware and re-sends the forkchoice it was on.
	res, err = m.UpdateForkChoice(ctx, tip,
		execmoduletester.WithFinalisedHash(finalised.Hash()),
		execmoduletester.WithSafeHash(finalised.Hash()))
	require.NoError(t, err)
	require.NotEqual(t, execmodule.ExecutionStatusInvalidForkchoice, res.Status,
		"an unwind below the finalised block must not leave the node refusing every forkchoice: "+
			"the refusal is retried unchanged forever and the head never moves again")
}
