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

package paymentlane

import (
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func TestQuotaIsTheRatioOfTheGasLimit(t *testing.T) {
	for _, tc := range []struct {
		ratio, gasLimit, want uint64
	}{
		{500, 55_000_000, 2_750_000},
		{1_000, 55_000_000, 5_500_000},
		{1, 55_000_000, 5_500},
		{500, 0, 0},
		{500, 30_000_001, 1_500_000},
		{1_000, math.MaxInt64, 922_337_203_685_477_580},
	} {
		require.Equalf(t, tc.want, Quota(tc.ratio, tc.gasLimit), "Quota(%d, %d)", tc.ratio, tc.gasLimit)
	}
}

func TestCheckRatioGuardsAtFullWidth(t *testing.T) {
	for _, ok := range []uint64{1, 500, MaxLaneRatio} {
		got, err := CheckRatio(new(big.Int).SetUint64(ok))
		require.NoError(t, err)
		require.Equal(t, ok, got)
	}
	wraps := new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(500))
	for _, bad := range []*big.Int{nil, big.NewInt(0), big.NewInt(MaxLaneRatio + 1), wraps} {
		_, err := CheckRatio(bad)
		require.ErrorIsf(t, err, ErrCorruptConfig, "ratio %v", bad)
	}
}

func TestLaneIsFloorNotCeiling(t *testing.T) {
	const limit, lane = 100, 20
	for _, tc := range []struct {
		name             string
		general, payment uint64
		wantErr          bool
	}{
		{"payment exactly fills the quota", 80, 20, false},
		{"payment one gas over, general one gas under", 79, 21, false},
		{"payment one gas short does not hand the freed quota to general", 81, 19, true},
		{"with no payment demand the quota idles", 80, 0, false},
		{"general does not get the idling quota", 81, 0, true},
		{"payment may take the whole block", 0, 100, false},
	} {
		err := CheckInequality(limit, tc.general+tc.payment, tc.payment, lane)
		if tc.wantErr {
			require.ErrorIsf(t, err, ErrViolated, tc.name)
		} else {
			require.NoErrorf(t, err, tc.name)
		}
	}
	require.ErrorIs(t, CheckInequality(limit, 0, 0, limit+1), ErrViolated)
	require.NoError(t, CheckInequality(limit, 0, 0, limit))
}

func TestOverflowIsNotAWayIn(t *testing.T) {
	const gasLimit = 70_000_000
	maxU := uint64(math.MaxUint64)
	for _, tc := range []struct{ gasUsed, payment, lane uint64 }{
		{maxU, 0, 0},
		{gasLimit, 0, maxU},
		{maxU/2 + 1, 0, maxU/2 + 1},
		{gasLimit + 1, 0, 0},
	} {
		require.ErrorIsf(t, CheckInequality(gasLimit, tc.gasUsed, tc.payment, tc.lane), ErrViolated,
			"gasUsed=%d payment=%d lane=%d", tc.gasUsed, tc.payment, tc.lane)
	}
	require.NoError(t, CheckInequality(gasLimit, 1000, maxU, maxU))
}

var (
	plainDest  = common.HexToAddress("0x00000000000000000000000000000000000f0001")
	listedDest = common.HexToAddress("0x00000000000000000000000000000000000f0002")
)

func newTx(t *testing.T, txType byte, to *common.Address, value uint64) types.Transaction {
	t.Helper()
	base := types.CommonTx{To: to, Value: *uint256.NewInt(value), GasLimit: 21000}
	dynamic := types.DynamicFeeTransaction{CommonTx: base}
	switch txType {
	case types.LegacyTxType:
		return &types.LegacyTx{CommonTx: base}
	case types.AccessListTxType:
		return &types.AccessListTx{LegacyTx: types.LegacyTx{CommonTx: base}}
	case types.DynamicFeeTxType:
		return &dynamic
	case types.BlobTxType:
		return &types.BlobTx{DynamicFeeTransaction: dynamic, BlobVersionedHashes: []common.Hash{{0x01}}}
	case types.SetCodeTxType:
		return &types.SetCodeTransaction{DynamicFeeTransaction: dynamic}
	}
	t.Fatalf("unsupported tx type %d", txType)
	return nil
}

func TestClassifyGates(t *testing.T) {
	listed := map[common.Address]struct{}{listedDest: {}, ContractAddress: {}}
	for _, txType := range []byte{types.LegacyTxType, types.AccessListTxType, types.DynamicFeeTxType, types.BlobTxType, types.SetCodeTxType} {
		for _, dest := range []struct {
			name   string
			addr   *common.Address
			listed bool
		}{
			{"nil", nil, false},
			{"listed-system", &ContractAddress, true},
			{"listed", &listedDest, true},
			{"plain", &plainDest, false},
		} {
			for _, value := range []uint64{0, 1} {
				for _, hasCode := range []bool{false, true} {
					typeOK := txType == types.LegacyTxType || txType == types.AccessListTxType || txType == types.DynamicFeeTxType
					want := GeneralLane
					switch {
					case dest.addr == nil, !typeOK:
					case dest.listed:
						want = PaymentLane
					case value > 0 && !hasCode:
						want = PaymentLane
					}
					got := Classify(newTx(t, txType, dest.addr, value), false, listed, hasCode)
					require.Equalf(t, want, got, "type=%d dest=%s value=%d hasCode=%v", txType, dest.name, value, hasCode)
				}
			}
		}
	}
}

func TestSystemTransactionIsGeneralBeforeAnythingElse(t *testing.T) {
	transfer := newTx(t, types.LegacyTxType, &plainDest, 1)
	require.Equal(t, PaymentLane, Classify(transfer, false, nil, false))
	require.Equal(t, GeneralLane, Classify(transfer, true, nil, false))

	toListed := newTx(t, types.LegacyTxType, &listedDest, 1)
	require.Equal(t, GeneralLane, Classify(toListed, true, map[common.Address]struct{}{listedDest: {}}, false))
}
