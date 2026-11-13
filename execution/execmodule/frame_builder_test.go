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
	"context"
	"math/big"
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/txnprovider"
)

func TestBuildFrameNonceAfterCreate(t *testing.T) {
	for _, tc := range []struct {
		name           string
		key            uint64
		batchSize      int
		successorFirst bool
		otherCreator   bool
	}{
		{name: "legacy same batch", batchSize: 50},
		{name: "keyed same batch", key: 5, batchSize: 50},
		{name: "legacy later batch", batchSize: 1},
		{name: "keyed later batch", key: 5, batchSize: 1},
		{name: "legacy successor offered first", batchSize: 1, successorFirst: true},
		{name: "keyed successor offered first", key: 5, batchSize: 1, successorFirst: true},
		{name: "other account creates", batchSize: 50, otherCreator: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := chain.AllProtocolChanges.Copy()
			cfg.BogotaTime = common.NewUint64(0)
			sender := common.HexToAddress("0x1000")
			creator := sender
			if tc.otherCreator {
				creator = common.HexToAddress("0x2000")
			}
			create := program.New().Push0().Push0().Push0().Op(vm.CREATE, vm.POP, vm.STOP).Bytes()
			code := append([]byte{byte(vm.CALLDATASIZE), byte(vm.PUSH1), byte(4 + len(create)), byte(vm.JUMPI)}, create...)
			code = append(code, program.New().Op(vm.JUMPDEST).Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes()...)
			genesis := &types.Genesis{
				Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
				Alloc: types.GenesisAlloc{
					sender:                  {Balance: big.NewInt(common.Ether), Code: code},
					types.FrameNonceManager: {Nonce: 1, Code: common.FromHex("60006000fd")},
				},
			}
			genesis.Alloc[creator] = types.GenesisAccount{Balance: big.NewInt(common.Ether), Code: code}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
			parentChain, err := m.GenerateChain(1, nil)
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(parentChain))
			parent := parentChain.TopBlock
			approval := types.Frame{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Data: []byte{1}, Limits: mdgas.MdGas{Execution: 1_000, State: params.StateGasPerStorageSet}}
			first := &types.FrameTransaction{
				ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{*uint256.NewInt(tc.key)}, SenderAddress: sender,
				Fees: types.FrameFees{MaxFee: uint256.Int{1}},
				Frames: []types.Frame{
					{Mode: types.FrameDefault, Target: &creator, Limits: mdgas.MdGas{Execution: 200_000, State: 1_000_000}},
					approval,
				},
			}
			if tc.key == 0 {
				first.NonceKeys = nil
			}
			first.Fees.MaxFee.Mul(&first.Fees.MaxFee, uint256.NewInt(first.GetGasLimit()))
			// CREATE advances the creator's nonce; legacy payment approval advances
			// it once more when the creator is also the transaction sender.
			secondNonce := uint64(1)
			if tc.key == 0 && creator == sender {
				secondNonce++
			}
			second := &types.FrameTransaction{
				ChainID: *cfg.ChainID, NonceKeys: nil, SenderAddress: creator, Nonce: secondNonce,
				Fees: types.FrameFees{MaxFee: uint256.Int{1}}, Frames: []types.Frame{approval},
			}
			second.Fees.MaxFee.Mul(&second.Fees.MaxFee, uint256.NewInt(second.GetGasLimit()))
			_, err = m.GenerateChainFrom(parent, 1, func(_ int, b *blockgen.BlockGen) {
				b.AddTx(first)
				b.AddTx(second)
			})
			require.NoError(t, err)
			ordered := []types.Transaction{first, second}
			if tc.successorFirst {
				ordered[0], ordered[1] = ordered[1], ordered[0]
			}
			interrupt := &atomic.Bool{}
			provider := &frameNonceTxnProvider{
				orderedTxnProvider: orderedTxnProvider{ordered: ordered},
				batchSize:          tc.batchSize, interrupt: interrupt,
			}
			var buildInterrupt *atomic.Bool
			if tc.batchSize == 1 {
				buildInterrupt = interrupt
			}
			beaconRoot := common.Hash{1}
			built, err := m.BlockBuilder.Build(t.Context(), &builder.Parameters{
				ParentHash: parent.Hash(), Timestamp: parent.Time() + 1,
				Withdrawals: []*types.Withdrawal{}, ParentBeaconBlockRoot: &beaconRoot,
				SlotNumber: syntheticSlotNumber(parent), CustomTxnProvider: provider,
			}, buildInterrupt)
			require.NoError(t, err)
			require.Len(t, built.Block.Transactions(), 2)
			require.Equal(t, first.Hash(), built.Block.Transactions()[0].Hash())
			require.Equal(t, second.Hash(), built.Block.Transactions()[1].Hash())
			require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), []*types.Block{built.Block}))
		})
	}
}

type frameNonceTxnProvider struct {
	orderedTxnProvider
	batchSize int
	interrupt *atomic.Bool
}

func (p *frameNonceTxnProvider) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	txns, err := p.orderedTxnProvider.ProvideTxns(ctx, append(opts, txnprovider.WithAmount(p.batchSize))...)
	if len(txns) == 0 {
		p.interrupt.Store(true)
	}
	return txns, err
}
