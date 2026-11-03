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

package deriver_test

import (
	"maps"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/types"
)

// TestExtendNativeGasLeft: a native transaction after more gas than its
// block can spare for the cap is built with the gas the block has left.
func TestExtendNativeGasLeft(t *testing.T) {
	spec := networks.Chiado699169
	genesis := *spec.Genesis
	genesis.Alloc = maps.Clone(genesis.Alloc)
	genesis.Alloc[derivertest.UserAddress()] = types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil)}
	spec.Genesis = &genesis
	_, chain := derivertest.NewL2Of(t, spec)
	l2 := genesis.Config.L2.(*config.L2Config)
	signer := types.LatestSignerForChainID(genesis.Config.ChainID)
	var nonce uint64
	userTx := func(to *common.Address, gas uint64, data []byte) types.Transaction {
		tx, err := types.SignTx(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: nonce, GasLimit: gas, To: to, Data: data},
			ChainID:  *genesis.Config.ChainID,
			FeeCap:   *uint256.NewInt(100 * common.GWei),
		}, *signer, derivertest.UserKey)
		require.NoError(t, err)
		nonce++
		return tx
	}
	extend := func(parent *types.Header, txs ...types.Transaction) *types.BlockWithReceipts {
		in := deriver.HeaderInputs{
			Number:     parent.Number.Uint64() + 1,
			ParentHash: parent.Hash(),
			Time:       parent.Time + l2.BlockInterval,
			Extra:      []byte{},
			GasLimit:   l2.BlockGasLimit,
		}
		built, err := chain.Extend(t.Context(), parent, in, txs, common.Hash{}, common.Hash{})
		require.NoError(t, err)
		return built
	}

	parent, err := chain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	// Its runtime code, JUMPDEST PUSH1 0 JUMP, burns all the gas it gets.
	burner := types.CreateAddress(crypto.PubkeyToAddress(derivertest.UserKey.PublicKey), 0)
	deployed := extend(parent, userTx(nil, 100_000, common.FromHex("0x635b6000566000526004601cf3")))
	require.Equal(t, types.ReceiptStatusSuccessful, deployed.Receipts[0].Status)

	const burnt = 14_000_000
	delivery, err := systemtx.NewNative(protocol.NativeTx{
		ChainID:  genesis.Config.ChainID.Uint64(),
		GasLimit: protocol.NativeTxGasCap,
		To:       protocol.RollupXEEZL2Address,
		Input:    protocol.ExecuteIncomingCrossChainCallSelector[:],
	})
	require.NoError(t, err)
	built := extend(deployed.Block.Header(), userTx(&burner, burnt, nil), delivery)
	require.Equal(t, uint64(burnt), built.Receipts[0].GasUsed)
	require.Less(t, l2.BlockGasLimit-burnt, protocol.NativeTxGasCap)
	require.Equal(t, l2.BlockGasLimit-burnt, built.Block.Transactions()[1].GetGasLimit())
}
