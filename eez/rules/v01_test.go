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

package rules

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var v01EEZL2 = accounts.InternAddress(protocol.RollupXEEZL2Address)

func native(t *testing.T, selector [4]byte, gas uint64) types.Transaction {
	t.Helper()
	tx, err := systemtx.NewNative(protocol.NativeTx{ChainID: 699169, GasLimit: gas, To: protocol.RollupXEEZL2Address, Input: append(selector[:], 1)})
	require.NoError(t, err)
	return tx
}

func historyUpdate(t *testing.T) types.Transaction {
	t.Helper()
	tx, err := systemtx.NewHistory(protocol.HistoryTx{ChainID: 699169, Blocks: []protocol.HistoryBlock{{Number: 1, Hash: common.Hash{1}}}})
	require.NoError(t, err)
	return tx
}

var (
	load     = protocol.LoadExecutionTableSelector
	delivery = protocol.ExecuteIncomingCrossChainCallSelector
)

// TestV01BlockEnd checks that a v0.1 block leaves EEZL2 and the native
// system address their balances at its start and burns them at its end.
func TestV01BlockEnd(t *testing.T) {
	cfg, ibs, header := block1(t, networks.Chiado699169)
	e, err := New(cfg)
	require.NoError(t, err)
	custom, syscall := syscalls(cfg, e, header, ibs)
	fund(t, ibs, 5, v01EEZL2, nativeSystemAddress, systemAddress, bystander)

	require.NoError(t, e.Initialize(cfg, nil, header, ibs, custom, log.New(), nil))
	requireBalance(t, ibs, 5, v01EEZL2, nativeSystemAddress)
	_, err = e.Finalize(cfg, header, ibs, nil, nil, nil, nil, syscall, false, log.New())
	require.NoError(t, err)
	requireBalance(t, ibs, 0, v01EEZL2, nativeSystemAddress)
	requireBalance(t, ibs, 5, systemAddress, bystander)
	require.False(t, e.IsServiceTransaction(systemAddress, nil))
}

// TestV01PostProcessTx checks which transactions complete a cross-chain
// group and burn the protocol balances: a delivery, and the user
// transaction after a load.
func TestV01PostProcessTx(t *testing.T) {
	user := &types.DynamicFeeTransaction{}
	for _, tc := range []struct {
		name  string
		txs   types.Transactions
		burns []bool
	}{
		{"history then ordinary", types.Transactions{historyUpdate(t), user}, []bool{false, false}},
		{"outbound pair", types.Transactions{native(t, load, 1), user}, []bool{false, true}},
		{"deliveries", types.Transactions{native(t, delivery, 1), native(t, delivery, 1)}, []bool{true, true}},
		{"ordinary after a delivery", types.Transactions{user, native(t, delivery, 1), user}, []bool{false, true, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, ibs, _ := block1(t, networks.Chiado699169)
			e, err := New(cfg)
			require.NoError(t, err)
			for i, burns := range tc.burns {
				fund(t, ibs, 5, v01EEZL2, nativeSystemAddress)
				require.NoError(t, e.PostProcessTx(cfg, ibs, tc.txs, i))
				if burns {
					requireBalance(t, ibs, 0, v01EEZL2, nativeSystemAddress)
				} else {
					requireBalance(t, ibs, 5, v01EEZL2, nativeSystemAddress)
					require.NoError(t, e.burnProtocolBalances(ibs))
				}
			}
		})
	}

	// Rollup0 and rollupX v0.0.1 have no group rule.
	cfg, ibs, _ := block1(t, networks.Chiado699069)
	e, err := New(cfg)
	require.NoError(t, err)
	fund(t, ibs, 5, v01EEZL2, nativeSystemAddress)
	require.NoError(t, e.PostProcessTx(cfg, ibs, types.Transactions{native(t, delivery, 1)}, 0))
	requireBalance(t, ibs, 5, v01EEZL2, nativeSystemAddress)
}

// TestV01Validate checks the positions of system transactions in a v0.1
// block.
func TestV01Validate(t *testing.T) {
	cfg, _, _ := block1(t, networks.Chiado699169)
	e, err := New(cfg)
	require.NoError(t, err)
	user := &types.DynamicFeeTransaction{}
	header := func(n uint64) *types.Header {
		return &types.Header{Number: *uint256.NewInt(n), GasLimit: 30_000_000}
	}
	ok := func(cumulative ...uint64) types.Receipts {
		rs := make(types.Receipts, len(cumulative))
		for i, c := range cumulative {
			rs[i] = &types.Receipt{Status: types.ReceiptStatusSuccessful, CumulativeGasUsed: c}
		}
		return rs
	}
	gas := protocol.NativeTxGasCap

	for _, tc := range []struct {
		name     string
		number   uint64
		txs      types.Transactions
		receipts types.Receipts
		valid    bool
	}{
		{"history update", 3, types.Transactions{historyUpdate(t), user}, ok(90_000, 111_000), true},
		{"history off schedule", 4, types.Transactions{historyUpdate(t)}, ok(90_000), false},
		{"history not first", 3, types.Transactions{user, historyUpdate(t)}, ok(21_000, 111_000), false},
		{"history failed", 3, types.Transactions{historyUpdate(t)}, types.Receipts{{Status: types.ReceiptStatusFailed}}, false},
		{"history logged", 3, types.Transactions{historyUpdate(t)}, types.Receipts{{Status: types.ReceiptStatusSuccessful, Logs: []*types.Log{{}}}}, false},
		{"outbound pair and delivery", 5, types.Transactions{native(t, load, gas), user, native(t, delivery, gas)}, ok(100_000, 200_000, 300_000), true},
		{"delivery in a full block", 5, types.Transactions{user, native(t, delivery, 1_000_000)}, ok(29_000_000, 29_500_000), true},
		{"native gas below its position", 5, types.Transactions{native(t, delivery, gas-1)}, ok(1), false},
		{"native gas above its position", 5, types.Transactions{user, native(t, delivery, 1_000_001)}, ok(29_000_000, 29_500_000), false},
		{"load last", 5, types.Transactions{native(t, load, gas)}, ok(1), false},
		{"load then delivery", 5, types.Transactions{native(t, load, gas), native(t, delivery, gas)}, ok(1, 2), false},
		{"other selector", 5, types.Transactions{native(t, [4]byte{1, 2, 3, 4}, gas)}, ok(1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := e.validateV01(header(tc.number), tc.receipts, tc.txs)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
