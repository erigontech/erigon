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

package executiontests

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// Asserts both serial and parallel reject a block whose tx-gas exceeds
// header.GasLimit. Regression for the parallel-only gap where preCheck's
// gp-branch is a no-op against the per-tx pool built in trace_worker.
func TestInsertChain_TxGasExceedsBlockGasLimit_RejectsOnBothPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	_, validChain, err := GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {getBlockTx(from, to, uint256.NewInt(1000)), fromKey},
	})
	require.NoError(t, err)
	require.Len(t, validChain.Blocks, 1)

	// Only divergence vs validChain: header.GasLimit dropped below the tx's
	// 21000, so the sole invariant the bad block violates is the new one.
	badHeader := types.CopyHeader(validChain.Headers[0])
	badHeader.GasLimit = 20000

	badBlock := types.NewBlock(badHeader, validChain.Blocks[0].Transactions(),
		validChain.Blocks[0].Uncles(), validChain.Receipts[0], nil)
	badChain := &blockgen.ChainPack{
		Blocks:   []*types.Block{badBlock},
		Headers:  []*types.Header{badHeader},
		TopBlock: badBlock,
	}

	cases := []struct {
		name string
		opts []execmoduletester.Option
	}{
		{"parallel", nil},
		{"serial", []execmoduletester.Option{execmoduletester.WithoutExperimentalBAL()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]execmoduletester.Option{
				execmoduletester.WithGenesisSpec(data.genesisSpec),
				execmoduletester.WithKey(fromKey),
			}, tc.opts...)
			m := execmoduletester.New(t, opts...)
			require.Error(t, m.InsertChain(badChain),
				"%s exec must reject tx-gas > block-gas-limit", tc.name)
		})
	}
}
