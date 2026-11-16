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

package composer_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/composer"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	eezrules "github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
)

// TestDraftMatchesTheBuilder: on the node's chain, a draft of the next block
// executes under the header the builder then gives it, to the receipts the
// built block gets.
func TestDraftMatchesTheBuilder(t *testing.T) {
	_, chain := derivertest.NewL2(t)
	c := derivertest.NewComposer(t)
	ctx := t.Context()
	cfg := derivertest.Config()
	engine, err := eezrules.New(cfg)
	require.NoError(t, err)
	l2 := cfg.L2.(*config.L2Config)
	genesis, err := chain.Head(ctx)
	require.NoError(t, err)
	in := deriver.HeaderInputs{
		Number: 1, ParentHash: genesis.Hash(), Time: genesis.Time + l2.BlockInterval,
		Coinbase: common.HexToAddress("0xa11ce"), Extra: []byte{}, GasLimit: l2.BlockGasLimit,
	}
	txs, err := deriver.DecodeTxs([][]byte{c.Transfer(common.HexToAddress("0xb0b"), 1), c.Transfer(common.HexToAddress("0xb0b"), 2)})
	require.NoError(t, err)

	var header *types.Header
	var receipts types.Receipts
	require.NoError(t, chain.ViewPreState(ctx, genesis, func(r state.StateReader, headers rules.ChainHeaderReader, blockHash func(uint64) (common.Hash, error)) error {
		var err error
		if header, err = composer.SyncHeader(cfg, engine, headers, genesis, in); err != nil {
			return err
		}
		d, err := composer.NewDraft(cfg, engine, header, composer.PreState{Reader: r, Headers: headers, BlockHash: blockHash})
		if err != nil {
			return err
		}
		for _, tx := range txs {
			if _, err := d.Apply(tx, nil); err != nil {
				return err
			}
		}
		receipts = d.Receipts()
		return nil
	}))

	built, err := chain.Extend(ctx, genesis, in, txs, genesis.Hash(), genesis.Hash())
	require.NoError(t, err)
	b := built.Block.Header()
	for name, eq := range map[string]bool{
		"parent":       header.ParentHash == b.ParentHash,
		"number":       header.Number.Eq(&b.Number),
		"time":         header.Time == b.Time,
		"coinbase":     header.Coinbase == b.Coinbase,
		"extra":        bytes.Equal(header.Extra, b.Extra),
		"gas limit":    header.GasLimit == b.GasLimit,
		"base fee":     header.BaseFee.Eq(b.BaseFee),
		"difficulty":   header.Difficulty.Eq(&b.Difficulty),
		"mix digest":   header.MixDigest == b.MixDigest,
		"beacon root":  (header.ParentBeaconBlockRoot == nil) == (b.ParentBeaconBlockRoot == nil),
		"excess blobs": (header.ExcessBlobGas == nil) == (b.ExcessBlobGas == nil),
	} {
		require.True(t, eq, name)
	}
	require.Len(t, receipts, len(built.Receipts))
	for i := range receipts {
		require.Equal(t, built.Receipts[i].Status, receipts[i].Status, "transaction %d", i)
		require.Equal(t, built.Receipts[i].GasUsed, receipts[i].GasUsed, "transaction %d", i)
		require.Equal(t, built.Receipts[i].CumulativeGasUsed, receipts[i].CumulativeGasUsed, "transaction %d", i)
		require.Len(t, receipts[i].Logs, len(built.Receipts[i].Logs), "transaction %d", i)
	}
}
