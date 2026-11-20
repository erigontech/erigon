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

package protocol

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

func TestTransactionFeeDistribution(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name                 string
		berlin, cancun, aura bool
		free                 bool
		feeCap, gasPrice     uint64
		wantTip, wantBurn    uint64
		wantBlobBurn         uint64
	}{
		{name: "pre-London", berlin: true, feeCap: 11, gasPrice: 5, wantTip: 5},
		{name: "tip cap", feeCap: 11, gasPrice: 5, wantTip: 3, wantBurn: 2},
		{name: "fee cap", feeCap: 4, gasPrice: 4, wantTip: 2, wantBurn: 2},
		{name: "free", free: true},
		{name: "AuRa before Prague", cancun: true, aura: true, feeCap: 11, gasPrice: 5, wantTip: 3, wantBurn: 2},
		{name: "AuRa after Prague", aura: true, feeCap: 11, gasPrice: 5, wantTip: 3, wantBurn: 2, wantBlobBurn: 7 * params.GasPerBlob},
	} {
		kinds := []string{"ordinary"}
		if !test.berlin && !test.cancun {
			kinds = append(kinds, "frame")
		}
		for _, kind := range kinds {
			for _, deferred := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/deferred=%t", test.name, kind, deferred), func(t *testing.T) {
					cfg := chain.AllProtocolChanges.Copy()
					cfg.BogotaTime = common.NewUint64(0)
					if test.berlin {
						cfg = chain.TestChainBerlinConfig.Copy()
					} else if test.cancun {
						cfg.PragueTime, cfg.OsakaTime, cfg.AmsterdamTime, cfg.BogotaTime = nil, nil, nil, nil
					}
					if test.aura {
						cfg.Aura = &chain.AuRaConfig{}
					}
					sender := accounts.InternAddress(common.HexToAddress("0x1000"))
					target := accounts.InternAddress(common.HexToAddress("0x2000"))
					coinbase := accounts.InternAddress(common.HexToAddress("0x3000"))
					burn := common.HexToAddress("0x4000")
					cfg.BurntContract = map[string]common.Address{"0": burn}
					ibs := state.New(state.NewNoopReader())
					defer ibs.Close()
					require.NoError(t, ibs.SetBalance(sender, uint256.Int{10_000_000}, tracing.BalanceChangeUnspecified))
					const blockGas = 30_000_000
					evm := newTestEVM(ibs, cfg, blockGas)
					evm.Context.Coinbase = coinbase
					evm.Context.BaseFee.SetUint64(2)
					evm.Context.BlobBaseFee.SetUint64(7)
					require.Equal(t, test.aura, evm.ChainRules().IsAura)
					tipCap := uint64(3)
					if test.free {
						tipCap = 0
					}
					var blobs []common.Hash
					if !test.berlin {
						blobs = []common.Hash{{1}}
					}
					msg := types.NewMessage(sender, target, 0, new(uint256.Int), 100_000,
						uint256.NewInt(test.gasPrice), uint256.NewInt(test.feeCap), uint256.NewInt(tipCap),
						nil, nil, true, true, true, test.free, uint256.NewInt(13))
					msg.SetBlobVersionedHashes(blobs)
					if kind == "frame" {
						require.NoError(t, ibs.SetCode(sender, program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
						tx := &types.FrameTransaction{
							ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{{}}, SenderAddress: sender.Value(),
							Frames:              []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}},
							Fees:                types.FrameFees{FeeCap: uint256.Int{test.feeCap}, TipCap: uint256.Int{tipCap}, BlobFeeCap: uint256.Int{13}},
							BlobVersionedHashes: blobs,
						}
						var err error
						msg, err = tx.AsMessage(types.Signer{}, &evm.Context.BaseFee, evm.ChainRules())
						require.NoError(t, err)
						msg.SetIsFree(test.free)
					}
					executor := NewTxnExecutor(evm, msg, NewGasPool(blockGas, params.GasPerBlob))
					executor.noFeeBurnAndTip = deferred
					result, err := executor.Execute(true, false)
					require.NoError(t, err)
					require.NoError(t, result.Err)
					wantTip := result.ReceiptGasUsed * test.wantTip
					wantBurn := result.ReceiptGasUsed*test.wantBurn + test.wantBlobBurn
					require.Equal(t, uint256.Int{wantTip}, result.FeeTipped)
					require.Equal(t, uint256.Int{wantBurn}, result.FeeBurnt)
					if test.wantBurn == 0 {
						require.True(t, result.BurntContractAddress.IsNil())
					} else {
						require.Equal(t, burn, result.BurntContractAddress.Value())
					}
					if deferred {
						wantTip, wantBurn = 0, 0
					}
					balance, err := ibs.GetBalance(coinbase)
					require.NoError(t, err)
					require.Equal(t, uint256.Int{wantTip}, balance)
					balance, err = ibs.GetBalance(accounts.InternAddress(burn))
					require.NoError(t, err)
					require.Equal(t, uint256.Int{wantBurn}, balance)
				})
			}
		}
	}
}
