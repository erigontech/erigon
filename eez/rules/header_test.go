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
package rules

import (
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// headerReader serves the headers a test hands it.
type headerReader struct {
	rules.ChainHeaderReader
	config  *chain.Config
	headers map[common.Hash]*types.Header
}

func (r headerReader) Config() *chain.Config { return r.config }

func (r headerReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	if h := r.headers[hash]; h != nil && h.Number.Uint64() == number {
		return h
	}
	return nil
}

// windowTiming is the block interval and L1 block time of the deployment
// each window was recorded on: the Kurtosis devnet, or 1 s Chiado ones.
var windowTiming = map[string][2]uint64{
	"stateless-block-13":         {1, 5},
	"stateless-checkpoint-2175":  {1, 5},
	"nonzero-outbound-630":       {1, 5},
	"captured-devnet-window-84":  {2, 12},
	"captured-devnet-window-468": {2, 12},
	"captured-devnet-anchor-402": {2, 12},
}

// windowHeaders returns the headers of a recorded window, preceded by the
// parent of its first block from that block's witness.
func windowHeaders(t *testing.T, name string) (*chain.Config, []*types.Header) {
	t.Helper()
	systemtx.Register() // window 468 carries type-0x76 transactions
	w, err := testfixtures.LoadWindow(name)
	require.NoError(t, err)
	var cfg chain.Config
	require.NoError(t, json.Unmarshal(w.ChainConfig, &cfg))

	var parent types.Header
	require.NoError(t, rlp.DecodeBytes(w.Blocks[0].Witness.Headers[0], &parent))
	headers := []*types.Header{&parent}
	for _, b := range w.Blocks {
		var block types.Block
		require.NoError(t, rlp.DecodeBytes(b.RLP, &block))
		headers = append(headers, block.Header())
	}
	cfg.L2 = &config.L2Config{
		RollupID:      1,
		L1ChainID:     7331,
		Registry:      common.HexToAddress("0x01"),
		BlockInterval: windowTiming[name][0],
		L1BlockTime:   windowTiming[name][1],
		BlockGasLimit: headers[1].GasLimit,
	}
	return &cfg, headers
}

func newTestEngine(t *testing.T, cfg *chain.Config) (*Engine, headerReader) {
	t.Helper()
	e, err := New(cfg)
	require.NoError(t, err)
	return e, headerReader{config: cfg, headers: map[common.Hash]*types.Header{}}
}

// TestVerifyRecordedHeaders is T-EVM-4 on headers the reference client built.
func TestVerifyRecordedHeaders(t *testing.T) {
	for _, name := range testfixtures.WindowNames {
		t.Run(name, func(t *testing.T) {
			cfg, headers := windowHeaders(t, name)
			e, chain := newTestEngine(t, cfg)
			for _, h := range headers {
				chain.headers[h.Hash()] = h
			}
			for _, h := range headers[1:] {
				require.NoError(t, e.VerifyHeader(chain, h, true), "block %d", h.Number.Uint64())
			}
		})
	}
}

func TestVerifyHeaderRejects(t *testing.T) {
	cfg, headers := windowHeaders(t, "captured-devnet-window-84")
	e, reader := newTestEngine(t, cfg)
	parent, valid := headers[1], headers[2]
	reader.headers[parent.Hash()] = parent
	require.NoError(t, e.VerifyHeader(reader, valid, true))

	hash := func(b byte) *common.Hash { h := common.Hash{b}; return &h }
	one := uint64(1)
	for _, tc := range []struct {
		name   string
		mutate func(h *types.Header)
	}{
		{"number", func(h *types.Header) { h.Number.AddUint64(&h.Number, 1) }},
		{"number beyond uint64", func(h *types.Header) { h.Number.Add(&h.Number, new(uint256.Int).Lsh(uint256.NewInt(1), 64)) }},
		{"timestamp", func(h *types.Header) { h.Time++ }},
		{"gas limit", func(h *types.Header) { h.GasLimit-- }},
		{"gas used", func(h *types.Header) { h.GasUsed = h.GasLimit + 1 }},
		{"extra data", func(h *types.Header) { h.Extra = make([]byte, 33) }},
		{"difficulty", func(h *types.Header) { h.Difficulty.SetOne() }},
		{"nonce", func(h *types.Header) { h.Nonce[7] = 1 }},
		{"prevRandao", func(h *types.Header) { h.MixDigest = common.Hash{1} }},
		{"uncle hash", func(h *types.Header) { h.UncleHash = common.Hash{1} }},
		{"base fee", func(h *types.Header) { h.BaseFee = new(uint256.Int).AddUint64(h.BaseFee, 1) }},
		{"no withdrawals root", func(h *types.Header) { h.WithdrawalsHash = nil }},
		{"withdrawals root", func(h *types.Header) { h.WithdrawalsHash = hash(1) }},
		{"no blob gas used", func(h *types.Header) { h.BlobGasUsed = nil }},
		{"blob gas used", func(h *types.Header) { h.BlobGasUsed = &one }},
		{"excess blob gas", func(h *types.Header) { h.ExcessBlobGas = &one }},
		{"no beacon root", func(h *types.Header) { h.ParentBeaconBlockRoot = nil }},
		{"beacon root", func(h *types.Header) { h.ParentBeaconBlockRoot = hash(1) }},
		{"no requests hash", func(h *types.Header) { h.RequestsHash = nil }},
		{"requests hash", func(h *types.Header) { h.RequestsHash = hash(1) }},
		{"slot number", func(h *types.Header) { h.SlotNumber = &one }},
		{"block access list", func(h *types.Header) { h.BlockAccessListHash = hash(1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := types.CopyHeader(valid)
			tc.mutate(h)
			require.Error(t, e.VerifyHeader(reader, h, true))
		})
	}

	orphan := types.CopyHeader(valid)
	orphan.ParentHash = common.Hash{1}
	require.ErrorIs(t, e.VerifyHeader(reader, orphan, true), rules.ErrUnknownAncestor)
	genesis := types.CopyHeader(headers[0])
	genesis.Number.Clear()
	require.ErrorIs(t, e.VerifyHeader(reader, genesis, true), ErrInvalidHeader)
}

func TestNew(t *testing.T) {
	cfg, _ := windowHeaders(t, "captured-devnet-window-84")
	e, err := New(cfg)
	require.NoError(t, err)
	difficulty := e.CalcDifficulty(nil, 0, 0, *uint256.NewInt(1), 0, common.Hash{}, common.Hash{}, 0)
	require.True(t, difficulty.IsZero())
	h := &types.Header{Difficulty: *uint256.NewInt(1)}
	require.NoError(t, e.Prepare(nil, h, nil))
	require.True(t, h.Difficulty.IsZero())

	_, err = New(&chain.Config{})
	require.Error(t, err)
	_, err = New(&chain.Config{L2: &config.L2Config{}})
	require.ErrorIs(t, err, config.ErrInvalid)
}
