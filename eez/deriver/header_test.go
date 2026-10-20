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
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/internal/stateless"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/protocol"
	eezrules "github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
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
// recorded blocks. Window 468 rebuilds a Sync block with system transactions
// whose outbound calls pass the outbound gate.
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

	var outbound []protocol.ExecutionEntry
	var logs []*types.Log
	for i, recorded := range w.Blocks {
		var block types.Block
		require.NoError(t, rlp.DecodeBytes(recorded.RLP, &block))
		encoded := p.BlockTxs(i)
		if i == len(w.Blocks)-1 && len(p.Actions) > 0 {
			// The system address nonce at the parent; execution checks it.
			nonce := block.Transactions()[0].GetNonce()
			sb, err := BuildSyncBlock(p, appliedAll(b), protocol.SyncTxContext{ChainID: cfg.ChainID.Uint64(), RollupID: l2.RollupID}, nonce)
			require.NoError(t, err)
			encoded, outbound = sb.Txs, sb.Outbound
		}
		txs, err := DecodeTxs(encoded)
		require.NoError(t, err)
		derived := types.NewBlock(newHeader(&cfg, parent, DeriveInputs(l2, parent, p.Span, i)), txs, nil, nil, []*types.Withdrawal{}, nil)
		// Execution's outputs come from the recorded block; Verify checks each
		// against the derived block's execution.
		h := derived.Header()
		h.Root, h.GasUsed, h.ReceiptHash, h.Bloom = block.Root(), block.GasUsed(), block.ReceiptHash(), block.Bloom()
		derived = derived.WithSeal(h)
		require.Equal(t, recorded.Hash, derived.Hash(), "block %d", recorded.Number)
		res, err := stateless.Verify(&cfg, engine, derived, witnessOf(recorded.Witness))
		require.NoError(t, err, "block %d", recorded.Number)
		for _, r := range res.Receipts {
			logs = append(logs, r.Logs...)
		}
		parent = derived.Header()
	}
	post, err := w.OracleHash("window_post_block_hash")
	require.NoError(t, err)
	require.Equal(t, post, parent.Hash())

	// Each outbound entry is a call the Sync block made; a redirected one is
	// not.
	require.NoError(t, VerifyOutbound(outbound, logs, l2.RollupID))
	if len(outbound) > 0 {
		redirected := slices.Clone(outbound)
		redirected[0].L2ToL1Calls = slices.Clone(redirected[0].L2ToL1Calls)
		redirected[0].L2ToL1Calls[0].TargetAddress = common.Address{1}
		require.ErrorIs(t, VerifyOutbound(redirected, logs, l2.RollupID), ErrOutboundUnauthorized)
		require.ErrorIs(t, VerifyOutbound(append(outbound, outbound[0]), logs, l2.RollupID), ErrOutboundUnauthorized, "one call settles one entry")
		require.ErrorIs(t, VerifyOutbound(outbound, logs, l2.RollupID+1), ErrOutboundUnauthorized)
	}
}

// newHeader returns the header of the block in derives on parent, before
// execution fills in its roots, bloom and gas used: a post-merge header with
// zero prevRandao and beacon root, no blob gas and empty withdrawals and
// requests.
func newHeader(cfg *chain.Config, parent *types.Header, in HeaderInputs) *types.Header {
	h := &types.Header{
		ParentHash: in.ParentHash,
		UncleHash:  empty.UncleHash,
		Coinbase:   in.Coinbase,
		Number:     *uint256.NewInt(in.Number),
		GasLimit:   in.GasLimit,
		Time:       in.Time,
		Extra:      in.Extra,
		BaseFee:    misc.CalcBaseFee(cfg, parent),
	}
	if cfg.IsShanghai(in.Time) {
		h.WithdrawalsHash = &empty.RootHash
	}
	if cfg.IsCancun(in.Time) {
		zero := uint64(0)
		excess := misc.CalcExcessBlobGas(cfg, parent, in.Time)
		h.BlobGasUsed, h.ExcessBlobGas = &zero, &excess
		h.ParentBeaconBlockRoot = &common.Hash{}
	}
	if cfg.IsPrague(in.Time) {
		h.RequestsHash = &empty.RequestsHash
	}
	return h
}
