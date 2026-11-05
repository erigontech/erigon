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

package rules_test

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	eezrules "github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
)

// creditingEngine credits an account no transaction touches after each one.
type creditingEngine struct {
	rules.Engine
	credited accounts.Address
}

func (e creditingEngine) PostProcessTx(_ *chain.Config, ibs *state.IntraBlockState, _ types.Transactions, _ int) error {
	return ibs.AddBalance(e.credited, *uint256.NewInt(7), tracing.BalanceChangeUnspecified)
}

// TestPostProcessorKeepsAccount checks that a post-processor writing one field of
// an account no transaction touched leaves the rest of it, under the parallel
// executor as serially. No reader shares the first block, so the first credit
// builds on the account's committed record.
func TestPostProcessorKeepsAccount(t *testing.T) {
	credited := common.HexToAddress("0x00000000000000000000000000000000000c0de1")
	reader := common.HexToAddress("0x00000000000000000000000000000000000c0de2")
	var code []byte
	for slot, op := range []vm.OpCode{vm.BALANCE, vm.EXTCODEHASH, vm.EXTCODESIZE} {
		code = append(code, byte(vm.PUSH20))
		code = append(code, credited[:]...)
		code = append(code, byte(op), byte(vm.PUSH1), byte(slot), byte(vm.SSTORE))
	}
	c := newGroupEndChain(t, types.GenesisAlloc{
		derivertest.UserAddress(): {Balance: uint256.NewInt(1e18).ToBig()},
		credited:                  {Code: []byte{byte(vm.STOP)}, Nonce: 5, Balance: uint256.NewInt(1).ToBig()},
		reader:                    {Code: code},
	})
	engine, err := eezrules.New(c.spec.Genesis.Config)
	require.NoError(t, err)
	withEngine := execmoduletester.WithEngine(creditingEngine{Engine: engine, credited: accounts.InternAddress(credited)})

	dbg.Exec3Parallel = false
	_, serial := derivertest.NewL2Of(t, c.spec, withEngine)
	parent, err := serial.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	var blocks []*types.Block
	var headers []*types.Header
	for i, txs := range [][]types.Transaction{
		{c.userTx(derivertest.UserKey, 0, common.Address{0xf1}, 0, 21_000)},
		{c.userTx(derivertest.UserKey, 1, reader, 0, 500_000)},
		{c.userTx(derivertest.UserKey, 2, reader, 0, 500_000)},
	} {
		built, err := serial.Extend(t.Context(), parent, deriver.HeaderInputs{
			Number:     uint64(i) + 1,
			ParentHash: parent.Hash(),
			Time:       parent.Time + c.l2().BlockInterval,
			Extra:      []byte{},
			GasLimit:   c.l2().BlockGasLimit,
		}, txs, common.Hash{}, common.Hash{})
		require.NoError(t, err)
		require.Equal(t, types.ReceiptStatusSuccessful, built.Receipts[len(txs)-1].Status)
		blocks, headers = append(blocks, built.Block), append(headers, built.Block.Header())
		parent = built.Block.Header()
	}
	for _, workers := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("workers %d", workers), func(t *testing.T) {
			dbg.Exec3Parallel = true
			ethconfig.Defaults.Sync.ExecWorkerCount = workers
			m, chain := derivertest.NewL2Of(t, c.spec, withEngine)
			require.NoError(t, m.InsertChain(&blockgen.ChainPack{Blocks: blocks, Headers: headers, TopBlock: blocks[len(blocks)-1]}))
			slot := func(block, slot uint64) common.Hash {
				v, err := chain.Storage(t.Context(), block, reader, common.BigToHash(new(big.Int).SetUint64(slot)))
				require.NoError(t, err)
				return v
			}
			// One credit per transaction before the reader's.
			require.Equal(t, common.BigToHash(big.NewInt(1+7)), slot(2, 0))
			require.Equal(t, common.BigToHash(big.NewInt(1+2*7)), slot(3, 0))
			require.Equal(t, crypto.Keccak256Hash([]byte{byte(vm.STOP)}), slot(2, 1))
			require.Equal(t, common.BigToHash(big.NewInt(1)), slot(2, 2))
			nonce, err := chain.Nonce(t.Context(), 3, credited)
			require.NoError(t, err)
			require.Equal(t, uint64(5), nonce)
		})
	}
}
