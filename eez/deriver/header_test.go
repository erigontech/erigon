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
package deriver

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/internal/stateless"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/protocol"
	eezrules "github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

func witnessOf(w *testfixtures.Witness) *stateless.Witness {
	conv := func(in []hexutil.Bytes) [][]byte {
		out := make([][]byte, len(in))
		for i, b := range in {
			out[i] = b
		}
		return out
	}
	return &stateless.Witness{State: conv(w.State), Codes: conv(w.Codes), Headers: conv(w.Headers)}
}

// TestDeriveWindows is T-DRV-1: the blocks of a recorded settlement, derived
// from its DA payload and executed on their recorded witnesses, hash to the
// recorded blocks. Window 468 rebuilds a Sync block with system transactions.
func TestDeriveWindows(t *testing.T) {
	systemtx.Register()
	for _, name := range []string{"captured-devnet-window-84", "captured-devnet-anchor-402", "captured-devnet-window-468"} {
		t.Run(name, func(t *testing.T) { deriveWindow(t, name) })
	}
}

func deriveWindow(t *testing.T, name string) {
	w, err := testfixtures.LoadWindow(name)
	require.NoError(t, err)
	var cfg chain.Config
	require.NoError(t, json.Unmarshal(w.ChainConfig, &cfg))
	var registry common.Address
	require.NoError(t, json.Unmarshal(w.Oracle["eez_address"], &registry))
	l2 := &config.L2Config{
		RollupID:      1,
		L1ChainID:     7331,
		Registry:      registry,
		BlockInterval: 2,
		L1BlockTime:   12,
		BlockGasLimit: 30_000_000,
	}
	cfg.L2 = l2
	engine, err := eezrules.New(&cfg)
	require.NoError(t, err)

	b, err := protocol.DecodePostAndVerifyBatch(w.PostBatch)
	require.NoError(t, err)
	p, err := DecodePayload(b, l2.RollupID)
	require.NoError(t, err)
	require.Equal(t, uint64(len(w.Blocks)), p.Blocks())

	parent := new(types.Header)
	witness := w.Blocks[0].Witness
	require.NoError(t, rlp.DecodeBytes(witness.Headers[len(witness.Headers)-1], parent))
	pre, err := w.OracleHash("window_pre_block_hash")
	require.NoError(t, err)
	require.Equal(t, pre, parent.Hash())

	for i, recorded := range w.Blocks {
		encoded := p.BlockTxs(i)
		if i == len(w.Blocks)-1 && len(p.Actions) > 0 {
			var block types.Block
			require.NoError(t, rlp.DecodeBytes(recorded.RLP, &block))
			// The system address nonce at the parent; execution checks it.
			nonce := block.Transactions()[0].GetNonce()
			sb, err := BuildSyncBlock(p, appliedAll(b), protocol.SyncTxContext{ChainID: cfg.ChainID.Uint64(), RollupID: l2.RollupID}, nonce)
			require.NoError(t, err)
			encoded = sb.Txs
		}
		txs, err := DecodeTxs(encoded)
		require.NoError(t, err)
		h := NewHeader(&cfg, parent, DeriveInputs(l2, parent, p.Span, i))
		witness := witnessOf(recorded.Witness)
		res, err := stateless.Execute(&cfg, engine, types.NewBlock(h, txs, nil, nil, []*types.Withdrawal{}, nil), witness,
			stateless.WithoutPostValidation())
		require.NoError(t, err, "block %d", recorded.Number)
		h.Root, h.GasUsed = res.StateRoot, res.GasUsed
		block := types.NewBlock(h, txs, nil, res.Receipts, []*types.Withdrawal{}, nil)
		require.Equal(t, recorded.Hash, block.Hash(), "block %d", recorded.Number)
		_, err = stateless.Verify(&cfg, engine, block, witness)
		require.NoError(t, err, "block %d", recorded.Number)
		parent = block.Header()
	}
	post, err := w.OracleHash("window_post_block_hash")
	require.NoError(t, err)
	require.Equal(t, post, parent.Hash())
}
