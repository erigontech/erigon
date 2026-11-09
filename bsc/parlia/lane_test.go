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

package parlia

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/bsc/paymentlane"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

const jennerBlock, jennerTime = 139533776, 1793241000

var paymentLane = accounts.InternAddress(paymentlane.ContractAddress)

func staticSyscall(cfg *chain.Config) rules.SysCallCustom {
	return func(contract accounts.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
		blockContext := evmtypes.BlockContext{
			CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
			BlockNumber: header.Number.Uint64(),
			Time:        header.Time,
			GasLimit:    header.GasLimit,
		}
		evm := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, cfg, vm.Config{})
		ret, _, _, err := evm.StaticCall(accounts.ZeroAddress, contract, data, mdgas.MdGas{Execution: 30_000_000})
		return ret, err
	}
}

// laneTestState is the state after the first Jenner block: PaymentLane holds its code and
// no storage, so the ratio reads as the contract default 500.
func laneTestState(t *testing.T, cfg *chain.Config) *state.IntraBlockState {
	t.Helper()
	alloc, err := types.DecodeGenesisAlloc(cfg.Parlia.BlockAlloc["1793241000"])
	require.NoError(t, err)
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	require.NoError(t, ibs.SetCode(paymentLane, alloc[paymentlane.ContractAddress].Code, tracing.CodeChangeUnspecified))
	return ibs
}

func laneTestHeader(number, time, gasUsed uint64) *types.Header {
	return &types.Header{
		Number:   *uint256.NewInt(number),
		Time:     time,
		GasLimit: 100_000_000,
		GasUsed:  gasUsed,
		Coinbase: common.HexToAddress("0x1a3d9d7a717d64e6088ac937d5aacdd3e20ca963"),
	}
}

func initializeLaneBlock(t *testing.T, p *Parlia, cfg *chain.Config, header *types.Header, parentTime uint64) *state.IntraBlockState {
	t.Helper()
	ibs := laneTestState(t, cfg)
	parent := &types.Header{Number: *uint256.NewInt(header.Number.Uint64() - 1), Time: parentTime}
	require.NoError(t, p.Initialize(cfg, parentReader{parent: parent}, header, ibs, staticSyscall(cfg), log.New(), nil))
	return ibs
}

func TestInitializeLoadsPaymentLaneMetaFromTheBlockAfterJenner(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	cfg := spec.Config
	p := New(cfg, log.New())

	pasteur := laneTestHeader(jennerBlock-1, jennerTime-1, 0)
	initializeLaneBlock(t, p, cfg, pasteur, jennerTime-2)
	require.False(t, p.laneMeta.Contains(pasteur.Hash()))

	fork := laneTestHeader(jennerBlock, jennerTime, 0)
	initializeLaneBlock(t, p, cfg, fork, jennerTime-1)
	meta, ok := p.laneMeta.Get(fork.Hash())
	require.True(t, ok)
	require.Nil(t, meta)

	next := laneTestHeader(jennerBlock+1, jennerTime+1, 0)
	ibs := initializeLaneBlock(t, p, cfg, next, jennerTime)
	meta, ok = p.laneMeta.Get(next.Hash())
	require.True(t, ok)
	require.EqualValues(t, 5_000_000, meta.Quota(next.GasLimit))
	require.Empty(t, meta.Listed())

	code, err := ibs.GetCode(accounts.InternAddress(common.HexToAddress("0x7020000000000000000000000000000000000001")))
	require.NoError(t, err)
	require.Empty(t, code, "the Jenner installs run on the first Jenner block only")
}

func TestInitializeRejectsACorruptPaymentLane(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	cfg := spec.Config
	p := New(cfg, log.New())

	header := laneTestHeader(jennerBlock+1, jennerTime+1, 0)
	ibs := laneTestState(t, cfg)
	require.NoError(t, ibs.SetState(paymentLane, accounts.InternKey(common.Hash{}), *uint256.NewInt(paymentlane.MaxLaneRatio + 1)))
	parent := &types.Header{Number: *uint256.NewInt(jennerBlock), Time: jennerTime}
	err = p.Initialize(cfg, parentReader{parent: parent}, header, ibs, staticSyscall(cfg), log.New(), nil)
	require.ErrorIs(t, err, paymentlane.ErrCorruptConfig)
}

func TestVerifyLanes(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	cfg := spec.Config
	p := New(cfg, log.New())

	coinbase := common.HexToAddress("0x1a3d9d7a717d64e6088ac937d5aacdd3e20ca963")
	validatorSet := common.HexToAddress("0x0000000000000000000000000000000000001000")
	eoa, contract := common.HexToAddress("0xe0a"), common.HexToAddress("0xc0de")
	systemTx := &types.LegacyTx{CommonTx: types.CommonTx{To: &validatorSet, Value: *uint256.NewInt(1), GasLimit: 1 << 40}}
	systemTx.SetSender(accounts.InternAddress(coinbase))
	payment := &types.LegacyTx{CommonTx: types.CommonTx{To: &eoa, Value: *uint256.NewInt(1), GasLimit: 21000}, GasPrice: *uint256.NewInt(1)}
	toContract := &types.LegacyTx{CommonTx: types.CommonTx{To: &contract, Value: *uint256.NewInt(1), GasLimit: 50000}, GasPrice: *uint256.NewInt(1)}
	txs := types.Transactions{systemTx, payment, toContract}
	receipts := types.Receipts{{GasUsed: 30_000}, {GasUsed: 21_000}, {GasUsed: 40_000}}

	// Quota 5,000,000 of a 100,000,000 gas limit; 21,000 payment gas leaves 4,979,000 idle.
	const fullBlock = 100_000_000 - 4_979_000
	for _, tc := range []struct {
		name             string
		gasUsed          uint64
		recipientHasCode []bool
		wantErr          bool
	}{
		{"idle quota fits", fullBlock, []bool{false, false, true}, false},
		{"one gas over", fullBlock + 1, []bool{false, false, true}, true},
		{"a transfer to code is general gas", fullBlock, []bool{false, true, true}, true},
		{"a transfer to a codeless account is payment gas", fullBlock - 40_000, []bool{false, false, false}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			header := laneTestHeader(jennerBlock+1, jennerTime+1, tc.gasUsed)
			initializeLaneBlock(t, p, cfg, header, jennerTime)
			err := p.VerifyLanes(header, txs, receipts, tc.recipientHasCode)
			if tc.wantErr {
				require.ErrorIs(t, err, paymentlane.ErrViolated)
			} else {
				require.NoError(t, err)
			}
		})
	}

	t.Run("misaligned inputs", func(t *testing.T) {
		header := laneTestHeader(jennerBlock+1, jennerTime+1, 0)
		initializeLaneBlock(t, p, cfg, header, jennerTime)
		require.Error(t, p.VerifyLanes(header, txs, receipts, []bool{false}))
	})
	t.Run("no lane before the block after Jenner", func(t *testing.T) {
		fork := laneTestHeader(jennerBlock, jennerTime, 100_000_000)
		initializeLaneBlock(t, p, cfg, fork, jennerTime-1)
		require.NoError(t, p.VerifyLanes(fork, txs, receipts, []bool{false, false, true}))
		pasteur := laneTestHeader(jennerBlock-1, jennerTime-1, 100_000_000)
		require.NoError(t, p.VerifyLanes(pasteur, txs, receipts, []bool{false, false, true}))
	})
	t.Run("metadata not loaded", func(t *testing.T) {
		header := laneTestHeader(jennerBlock+2, jennerTime+2, 0)
		require.Error(t, p.VerifyLanes(header, txs, receipts, []bool{false, false, true}))
	})
}
