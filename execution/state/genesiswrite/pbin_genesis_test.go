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

package genesiswrite_test

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/types"
)

func withBinCommitment(t *testing.T, on bool) {
	t.Helper()
	orig := statecfg.ExperimentalBinCommitment
	t.Cleanup(func() { statecfg.ExperimentalBinCommitment = orig })
	statecfg.ExperimentalBinCommitment = on
}

// Code-free alloc: code chunking into the tree is not part of this task.
func pbinTestGenesis() *types.Genesis {
	return &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			common.HexToAddress("0x0000000000000000000000000000000000000042"): {Balance: big.NewInt(1)},
			common.HexToAddress("0x00000000000000000000000000000000000000ff"): {Balance: big.NewInt(0xdeadbeef), Nonce: 3},
		},
	}
}

// Genesis is the block-0 state root the executor is later checked against, so it
// must be computed on the variant the datadir uses, not always on the hex trie.
func TestPBinGenesisComputesBinaryRoot(t *testing.T) {
	// No t.Parallel: mutates process-global statecfg flags.
	logger := log.New()
	g := pbinTestGenesis()

	withBinCommitment(t, false)
	hexBlock, _, err := genesiswrite.GenesisToBlock(t, g, datadir.New(t.TempDir()), logger)
	require.NoError(t, err)

	withBinCommitment(t, true)
	binBlock, _, err := genesiswrite.GenesisToBlock(t, g, datadir.New(t.TempDir()), logger)
	require.NoError(t, err)

	require.NotEqual(t, hexBlock.Root(), binBlock.Root(), "genesis under the bin variant returned the hex root")
	require.Equal(t, common.BytesToHash(pbinGenesisRoot(t, g)), binBlock.Root())
}

// pbinGenesisRoot computes the genesis root over a SharedDomains explicitly
// running the bin trie, as an oracle for what GenesisToBlock must return.
func pbinGenesisRoot(t *testing.T, g *types.Genesis) []byte {
	t.Helper()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	require.Equal(t, commitment.VariantBinPatriciaTrie, sd.GetCommitmentCtx().Trie().Variant())

	head, _ := genesiswrite.GenesisWithoutStateToBlock(g)
	root, _, err := genesiswrite.ComputeGenesisCommitment(t.Context(), g, tx, sd, head)
	require.NoError(t, err)
	return root
}
