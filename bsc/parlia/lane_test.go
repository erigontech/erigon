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
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const jennerBlock, jennerTime = 139533776, 1793241000

var paymentLane = accounts.InternAddress(paymentlane.ContractAddress)

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

func loadTestLanes(t *testing.T, p *Parlia, ibs *state.IntraBlockState, header *types.Header, parentTime uint64) (any, error) {
	t.Helper()
	parent := &types.Header{Number: *uint256.NewInt(header.Number.Uint64() - 1), Time: parentTime}
	return p.LoadLanes(parentReader{parent: parent}, header, ibs)
}

func TestLoadLanesFromTheBlockAfterJenner(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	cfg := spec.Config
	p := New(cfg, log.New())

	require.False(t, p.HasLanes(laneTestHeader(jennerBlock-1, jennerTime-1, 0)))

	fork := laneTestHeader(jennerBlock, jennerTime, 0)
	require.True(t, p.HasLanes(fork))
	lanes, err := loadTestLanes(t, p, laneTestState(t, cfg), fork, jennerTime-1)
	require.NoError(t, err)
	require.Nil(t, lanes)

	next := laneTestHeader(jennerBlock+1, jennerTime+1, 0)
	lanes, err = loadTestLanes(t, p, laneTestState(t, cfg), next, jennerTime)
	require.NoError(t, err)
	meta, ok := lanes.(*paymentlane.Meta)
	require.True(t, ok)
	require.EqualValues(t, 5_000_000, meta.Quota(next.GasLimit))
	require.Empty(t, meta.Listed())
}

func TestLoadLanesRejectsACorruptPaymentLane(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	cfg := spec.Config
	p := New(cfg, log.New())

	ibs := laneTestState(t, cfg)
	require.NoError(t, ibs.SetState(paymentLane, accounts.InternKey(common.Hash{}), *uint256.NewInt(1001)))
	_, err = loadTestLanes(t, p, ibs, laneTestHeader(jennerBlock+1, jennerTime+1, 0), jennerTime)
	require.ErrorIs(t, err, paymentlane.ErrCorruptConfig)
}

func TestVerifyLanes(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	cfg := spec.Config
	p := New(cfg, log.New())

	header := laneTestHeader(jennerBlock+1, jennerTime+1, 0)
	lanes, err := loadTestLanes(t, p, laneTestState(t, cfg), header, jennerTime)
	require.NoError(t, err)

	coinbase := common.HexToAddress("0x1a3d9d7a717d64e6088ac937d5aacdd3e20ca963")
	validatorSet := common.HexToAddress("0x0000000000000000000000000000000000001000")
	eoa, contract := common.HexToAddress("0xe0a"), common.HexToAddress("0xc0de")
	systemTx := &types.LegacyTx{CommonTx: types.CommonTx{To: &validatorSet, Value: *uint256.NewInt(1), GasLimit: 1 << 40}}
	systemTx.SetSender(accounts.InternAddress(coinbase))
	payment := &types.LegacyTx{CommonTx: types.CommonTx{To: &eoa, Value: *uint256.NewInt(1), GasLimit: 21000}, GasPrice: *uint256.NewInt(1)}
	toContract := &types.LegacyTx{CommonTx: types.CommonTx{To: &contract, Value: *uint256.NewInt(1), GasLimit: 50000}, GasPrice: *uint256.NewInt(1)}
	txs := types.Transactions{systemTx, payment, toContract}
	laneTxs := func(recipientHasCode ...bool) []rules.LaneTx {
		gas := []uint64{30_000, 21_000, 40_000}
		out := make([]rules.LaneTx, len(recipientHasCode))
		for i, hasCode := range recipientHasCode {
			out[i] = rules.LaneTx{GasUsed: gas[i], RecipientHasCode: hasCode}
		}
		return out
	}

	// Quota 5,000,000 of a 100,000,000 gas limit; 21,000 payment gas leaves 4,979,000 idle.
	const fullBlock = 100_000_000 - 4_979_000
	for _, tc := range []struct {
		name    string
		gasUsed uint64
		laneTxs []rules.LaneTx
		wantErr bool
	}{
		{"idle quota fits", fullBlock, laneTxs(false, false, true), false},
		{"one gas over", fullBlock + 1, laneTxs(false, false, true), true},
		{"a transfer to code is general gas", fullBlock, laneTxs(false, true, true), true},
		{"a transfer to a codeless account is payment gas", fullBlock - 40_000, laneTxs(false, false, false), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := p.VerifyLanes(laneTestHeader(jennerBlock+1, jennerTime+1, tc.gasUsed), lanes, txs, tc.laneTxs)
			if tc.wantErr {
				require.ErrorIs(t, err, paymentlane.ErrViolated)
			} else {
				require.NoError(t, err)
			}
		})
	}

	t.Run("misaligned inputs", func(t *testing.T) {
		require.Error(t, p.VerifyLanes(header, lanes, txs, laneTxs(false)))
	})
	t.Run("no lanes loaded", func(t *testing.T) {
		require.NoError(t, p.VerifyLanes(laneTestHeader(jennerBlock+1, jennerTime+1, 100_000_000), nil, txs, laneTxs(false, false, true)))
	})
}
