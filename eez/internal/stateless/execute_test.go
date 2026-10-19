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
	"encoding/binary"
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	eezprotocol "github.com/erigontech/erigon/eez/protocol"
	eezrules "github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
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

// witnessTrie returns the state trie holding codes, one contract per address,
// as the root and the nodes proving every path.
func witnessTrie(t *testing.T, codes map[common.Address][]byte) (common.Hash, [][]byte) {
	t.Helper()
	tr := trie.New(common.Hash{})
	for addr, code := range codes {
		tr.UpdateAccount(crypto.Keccak256(addr[:]), &accounts.Account{
			Root:     empty.RootHash,
			CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code)),
		})
	}
	root := tr.Hash()
	var nodes [][]byte
	for addr := range codes {
		proof, err := tr.Prove(crypto.Keccak256(addr[:]), 0, false)
		require.NoError(t, err)
		for _, n := range proof {
			if !slices.ContainsFunc(nodes, func(m []byte) bool { return slices.Equal(m, n) }) {
				nodes = append(nodes, n)
			}
		}
	}
	return root, nodes
}

// commitBlock returns the block of txn on header whose gas used, receipts
// root and bloom are what executing it on w yields.
func commitBlock(t *testing.T, win *window, header *types.Header, txn types.Transaction, w *Witness) *types.Block {
	t.Helper()
	headers, err := newHeaderReader(win.config, types.NewBlockWithHeader(header, nil), w.Headers)
	require.NoError(t, err)
	ws, err := newWitnessState(w.State, w.Codes, headers.parent.Root)
	require.NoError(t, err)
	ibs := state.New(ws)
	defer ibs.Close()
	evm := protocol.CreateEVM(win.config, headers.blockHash, win.engine, accounts.InternAddress(header.Coinbase), ibs, header, vm.Config{})
	var gasUsed protocol.GasUsed
	ibs.SetTxContext(header.Number.Uint64(), 0)
	receipt, err := protocol.ApplyTransactionWithEVM(win.config, win.engine, new(protocol.GasPool).AddGas(header.GasLimit), ibs, ws,
		header, txn, &gasUsed, vm.Config{}, evm)
	require.NoError(t, err)
	h := types.CopyHeader(header)
	h.GasUsed = gasUsed.BlockGasUsed()
	return types.NewBlock(h, []types.Transaction{txn}, nil, types.Receipts{receipt}, []*types.Withdrawal{}, nil)
}

// TestMissingBlockhashAncestor covers a BLOCKHASH the witness headers cannot
// serve: the EVM reports it as a frame error, which a failed transaction or a
// caller frame absorbs.
func TestMissingBlockhashAncestor(t *testing.T) {
	win := loadWindow(t, "captured-devnet-window-468")
	var grandparent types.Header
	require.NoError(t, rlp.DecodeBytes(win.Blocks[0].Witness.Headers[len(win.Blocks[0].Witness.Headers)-1], &grandparent))
	// PUSH8 <grandparent> BLOCKHASH POP STOP
	blockhash := binary.BigEndian.AppendUint64([]byte{0x67}, grandparent.Number.Uint64())
	blockhash = append(blockhash, 0x40, 0x50, 0x00)
	callee := common.HexToAddress("0xb10c")
	// CALL(GAS, callee, 0, 0, 0, 0, 0) POP STOP
	call := append(append([]byte{0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x73}, callee[:]...), 0x5a, 0xf1, 0x50, 0x00)

	for _, tc := range []struct {
		name  string
		codes map[common.Address][]byte
	}{
		{"direct", map[common.Address][]byte{eezprotocol.EEZL2Address: blockhash}},
		{"nested call", map[common.Address][]byte{eezprotocol.EEZL2Address: call, callee: blockhash}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, nodes := witnessTrie(t, tc.codes)
			var codes [][]byte
			for _, code := range tc.codes {
				codes = append(codes, code)
			}
			parent := types.CopyHeader(win.blocks[0].Header())
			parent.Root = root
			encGrandparent, err := rlp.EncodeToBytes(&grandparent)
			require.NoError(t, err)
			encParent, err := rlp.EncodeToBytes(parent)
			require.NoError(t, err)
			header := types.CopyHeader(win.blocks[1].Header())
			header.ParentHash = parent.Hash()
			txn, err := systemtx.New(eezprotocol.SystemTx{ChainID: win.config.ChainID.Uint64(), To: eezprotocol.EEZL2Address})
			require.NoError(t, err)

			full := &Witness{State: nodes, Codes: codes, Headers: [][]byte{encGrandparent, encParent}}
			block := commitBlock(t, win, header, txn, full)
			_, err = Execute(win.config, win.engine, block, full)
			require.NoError(t, err)

			// The header commits to the execution a parent-only witness yields.
			parentOnly := &Witness{State: nodes, Codes: codes, Headers: [][]byte{encParent}}
			forged := commitBlock(t, win, header, txn, parentOnly)
			require.NotEqual(t, block.Hash(), forged.Hash())
			_, err = Execute(win.config, win.engine, forged, parentOnly)
			require.ErrorIs(t, err, ErrIncompleteWitness)
		})
	}
}
