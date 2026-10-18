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
package stateless

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	eezrules "github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// windowTiming is the block interval and L1 block time of the deployment
// each window was recorded on.
var windowTiming = map[string][2]uint64{
	"stateless-block-13":         {1, 5},
	"stateless-checkpoint-2175":  {1, 5},
	"nonzero-outbound-630":       {1, 5},
	"captured-devnet-window-84":  {2, 12},
	"captured-devnet-window-468": {2, 12},
	"captured-devnet-anchor-402": {2, 12},
}

type window struct {
	*testfixtures.Window
	config *chain.Config
	engine *eezrules.Engine
	blocks []*types.Block
}

func loadWindow(t *testing.T, name string) *window {
	t.Helper()
	systemtx.Register() // window 468 carries type-0x76 transactions
	fw, err := testfixtures.LoadWindow(name)
	require.NoError(t, err)
	w := &window{Window: fw, config: new(chain.Config)}
	require.NoError(t, json.Unmarshal(fw.ChainConfig, w.config))
	for _, b := range fw.Blocks {
		block := new(types.Block)
		require.NoError(t, rlp.DecodeBytes(b.RLP, block))
		w.blocks = append(w.blocks, block)
	}
	w.config.L2 = &config.L2Config{
		RollupID:      1,
		L1ChainID:     7331,
		Registry:      common.HexToAddress("0x01"),
		BlockInterval: windowTiming[name][0],
		L1BlockTime:   windowTiming[name][1],
		BlockGasLimit: w.blocks[0].GasLimit(),
	}
	w.engine, err = eezrules.New(w.config)
	require.NoError(t, err)
	return w
}

func (w *window) witness(i int) *Witness {
	fw := w.Blocks[i].Witness
	conv := func(in []hexutil.Bytes) [][]byte {
		out := make([][]byte, len(in))
		for j, b := range in {
			out[j] = b
		}
		return out
	}
	return &Witness{State: conv(fw.State), Codes: conv(fw.Codes), Headers: conv(fw.Headers)}
}

// TestBlock13 is T-WIT-1.
func TestBlock13(t *testing.T) {
	w := loadWindow(t, "stateless-block-13")
	block := w.blocks[0]
	require.Equal(t, common.HexToHash("0x16b64a78e9b3e0d533cafe81f9121735f6a2c8122c69b0bb5994ee75fe7bface"), block.Hash())

	res, err := Verify(w.config, w.engine, block, w.witness(0))
	require.NoError(t, err)
	require.Equal(t, common.HexToHash("0xf09d8f7da5bc5036f8dd9536c953e2212390a46fb3e553ece2b7d419131537b1"), res.StateRoot)

	tampered := types.CopyHeader(block.Header())
	tampered.Root[0] ^= 1
	_, err = Verify(w.config, w.engine, block.WithSeal(tampered), w.witness(0))
	require.ErrorIs(t, err, ErrStateRoot)

	parent := new(types.Header)
	require.NoError(t, rlp.DecodeBytes(w.Blocks[0].Witness.Headers[len(w.Blocks[0].Witness.Headers)-1], parent))
	var rootNode []byte
	withoutRoot := w.witness(0)
	withoutRoot.State = nil
	for _, n := range w.witness(0).State {
		if crypto.Keccak256Hash(n) == parent.Root {
			rootNode = n
			continue
		}
		withoutRoot.State = append(withoutRoot.State, n)
	}
	require.NotNil(t, rootNode)
	_, err = Verify(w.config, w.engine, block, withoutRoot)
	require.ErrorIs(t, err, ErrMissingParentState)

	// Block 13 is empty: forged body commitments change neither state, receipts
	// nor gas.
	txHash := types.CopyHeader(block.Header())
	txHash.TxHash[0] ^= 1
	_, err = Verify(w.config, w.engine, block.WithSeal(txHash), w.witness(0))
	require.ErrorIs(t, err, ErrBodyMismatch)
	withUncle := types.NewBlockFromNetwork(block.Header(), &types.Body{Uncles: []*types.Header{parent}, Withdrawals: block.Withdrawals()}, nil)
	_, err = Verify(w.config, w.engine, withUncle, w.witness(0))
	require.ErrorIs(t, err, ErrBodyMismatch)

	onlyRoot := w.witness(0)
	onlyRoot.State = [][]byte{rootNode}
	_, err = Verify(w.config, w.engine, block, onlyRoot)
	require.ErrorIs(t, err, ErrIncompleteWitness)

	noHeaders := w.witness(0)
	noHeaders.Headers = nil
	_, err = Verify(w.config, w.engine, block, noHeaders)
	require.ErrorIs(t, err, ErrWitnessHeaders)
}

// TestWitnessReadError covers a read failure the surrounding call swallows:
// Merge.Initialize only logs a failed history storage call.
func TestWitnessReadError(t *testing.T) {
	w := loadWindow(t, "captured-devnet-window-84")
	block, witness := w.blocks[0], w.witness(0)
	parent := new(types.Header)
	require.NoError(t, rlp.DecodeBytes(witness.Headers[len(witness.Headers)-1], parent))
	ws, err := newWitnessState(witness.State, nil, parent.Root)
	require.NoError(t, err)
	history, err := ws.ReadAccountData(params.HistoryStorageAddress)
	require.NoError(t, err)
	require.NotNil(t, history)
	// reth leaves the system caller out; its absence is not proven.
	_, err = ws.ReadAccountData(params.SystemAddress)
	require.ErrorIs(t, err, ErrIncompleteWitness)

	n := len(witness.Codes)
	witness.Codes = slices.DeleteFunc(witness.Codes, func(code []byte) bool {
		return crypto.Keccak256Hash(code) == history.CodeHash.Value()
	})
	require.Len(t, witness.Codes, n-1)
	_, err = Execute(w.config, w.engine, block, witness)
	require.ErrorIs(t, err, ErrIncompleteWitness)
}
