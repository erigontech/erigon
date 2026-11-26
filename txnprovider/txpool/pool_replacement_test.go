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

package txpool

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestFrameReplacementFee(t *testing.T) {
	maximum := *new(uint256.Int).SetAllOne()
	halfRange := uint256.Int{0, 0, 0, 1 << 63}
	// A 20% bump on this fee requires MaxUint256 + 0.6.
	roundingOverflow := *uint256.MustFromHex("0xd555555555555555555555555555555555555555555555555555555555555555")

	for _, tc := range []struct {
		name        string
		previous    uint256.Int
		replacement uint256.Int
		bump        uint64
		want        bool
	}{
		{"zero unchanged", uint256.Int{}, uint256.Int{}, 10, false},
		{"zero increased", uint256.Int{}, uint256.Int{1}, math.MaxUint64, true},
		{"fee decreased", uint256.Int{100}, uint256.Int{99}, 0, false},
		{"unchanged with zero bump", uint256.Int{100}, uint256.Int{100}, 0, false},
		{"increased with zero bump", uint256.Int{100}, uint256.Int{101}, 0, true},
		{"small fee unchanged", uint256.Int{1}, uint256.Int{1}, 10, false},
		{"small fee increased", uint256.Int{1}, uint256.Int{2}, 10, true},
		{"below exact threshold", uint256.Int{100}, uint256.Int{109}, 10, false},
		{"exact threshold", uint256.Int{100}, uint256.Int{110}, 10, true},
		{"rounded down", uint256.Int{101}, uint256.Int{111}, 10, false},
		{"rounded up", uint256.Int{101}, uint256.Int{112}, 10, true},
		{"wide product underpriced", halfRange, *new(uint256.Int).AddUint64(&halfRange, 1), 50, false},
		{"wide product exact threshold", halfRange, uint256.Int{0, 0, 0, 3 << 62}, 50, true},
		{"quotient overflow", *new(uint256.Int).SubUint64(&maximum, 1), maximum, 10, false},
		{"rounding overflow", roundingOverflow, maximum, 20, false},
		{"maximum bump underpriced", uint256.Int{100}, uint256.Int{98, 1}, math.MaxUint64, false},
		{"maximum bump exact threshold", uint256.Int{100}, uint256.Int{99, 1}, math.MaxUint64, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous, replacement := tc.previous, tc.replacement
			require.Equal(t, tc.want, frameReplacementFee(&previous, &replacement, tc.bump))
			require.Equal(t, tc.previous, previous)
			require.Equal(t, tc.replacement, replacement)
		})
	}
}

const (
	fundedTxnGas    = 21_000
	fundedTxnFeeCap = 10_000
	bumpedFeeCap    = fundedTxnFeeCap * 11 / 10
	fundedTxnCost   = fundedTxnGas * fundedTxnFeeCap
	bumpCost        = fundedTxnGas * (bumpedFeeCap - fundedTxnFeeCap)
	// The sender's balance covers four pooled transactions at fundedTxnFeeCap and leaves spareFunds.
	spareFunds uint64 = common.Ether - 4*fundedTxnCost
)

func addFundedTxns(t *testing.T, ctx context.Context, pool *TxPool, sender common.Address, tip uint64) []*TxnSlot {
	t.Helper()
	var txns TxnSlots
	for nonce := range uint64(4) {
		txn := newTestTxnSlot(nonce, 0, tip, fundedTxnFeeCap, fundedTxnGas)
		txn.IDHash[0] = byte(nonce + 1)
		txns.Append(txn, sender[:], true)
	}
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, slices.Repeat([]txpoolcfg.DiscardReason{txpoolcfg.Success}, 4), reasons)
	return txns.Txns
}

func replaceFundedTxns(t *testing.T, ctx context.Context, pool *TxPool, sender common.Address, replacements ...*TxnSlot) []txpoolcfg.DiscardReason {
	t.Helper()
	var txns TxnSlots
	for _, txn := range replacements {
		txns.Append(txn, sender[:], true)
	}
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	return reasons
}

func TestReplacementKeepsSenderTxnsFunded(t *testing.T) {
	for _, tip := range []uint64{0, 1_000} {
		for _, tc := range []struct {
			name          string
			feeCap, value uint64
			want          txpoolcfg.DiscardReason
		}{
			{"value spends spare funds", bumpedFeeCap, spareFunds - bumpCost, txpoolcfg.Success},
			{"value overdraws", bumpedFeeCap, spareFunds - bumpCost + 1, txpoolcfg.InsufficientFunds},
			{"fee cap overdraws", fundedTxnFeeCap + spareFunds/fundedTxnGas + 1, 0, txpoolcfg.InsufficientFunds},
		} {
			t.Run(fmt.Sprintf("tip %d/%s", tip, tc.name), func(t *testing.T) {
				ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
				original := addFundedTxns(t, ctx, pool, sender, tip)[0]

				replacement := newTestTxnSlot(0, 0, tip*11/10, tc.feeCap, fundedTxnGas)
				replacement.Txn.GetValue().SetUint64(tc.value)
				replacement.IDHash[0] = 9
				require.Equal(t, []txpoolcfg.DiscardReason{tc.want}, replaceFundedTxns(t, ctx, pool, sender, replacement))

				kept := original
				if tc.want == txpoolcfg.Success {
					kept = replacement
				}
				require.Same(t, kept, pool.all.get(kept.SenderID, 0).TxnSlot)
				pending, baseFee, queued := pool.CountContent()
				require.Equal(t, []int{4, 0, 0}, []int{pending, baseFee, queued})
			})
		}
	}
}

// Each replacement fits the spare funds alone; together they would unfund nonce 3.
func TestReplacementBatchSharesSpareFunds(t *testing.T) {
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	addFundedTxns(t, ctx, pool, sender, 0)

	var replacements []*TxnSlot
	for nonce := range uint64(2) {
		replacement := newTestTxnSlot(nonce, 0, 0, bumpedFeeCap, fundedTxnGas)
		replacement.Txn.GetValue().SetUint64(spareFunds / 2)
		replacement.IDHash[0] = byte(nonce + 9)
		replacements = append(replacements, replacement)
	}
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.InsufficientFunds},
		replaceFundedTxns(t, ctx, pool, sender, replacements...))
	pending, baseFee, queued := pool.CountContent()
	require.Equal(t, []int{4, 0, 0}, []int{pending, baseFee, queued})
}

// A replacement that costs less frees funds for a later replacement in the same batch.
func TestReplacementBatchReusesFreedFunds(t *testing.T) {
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	// Nonce 1 spends the spare funds but what one fee bump needs.
	var txns TxnSlots
	for nonce := range uint64(4) {
		txn := newTestTxnSlot(nonce, 0, 0, fundedTxnFeeCap, fundedTxnGas)
		if nonce == 1 {
			txn.Txn.GetValue().SetUint64(spareFunds - bumpCost)
		}
		txn.IDHash[0] = byte(nonce + 1)
		txns.Append(txn, sender[:], true)
	}
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, slices.Repeat([]txpoolcfg.DiscardReason{txpoolcfg.Success}, 4), reasons)

	var replacements []*TxnSlot
	for nonce, value := range []uint64{0, 0, spareFunds - 3*bumpCost} {
		replacement := newTestTxnSlot(uint64(nonce), 0, 0, bumpedFeeCap, fundedTxnGas)
		replacement.Txn.GetValue().SetUint64(value)
		replacement.IDHash[0] = byte(nonce + 9)
		replacements = append(replacements, replacement)
	}
	require.Equal(t, slices.Repeat([]txpoolcfg.DiscardReason{txpoolcfg.Success}, 3),
		replaceFundedTxns(t, ctx, pool, sender, replacements...))
	pending, baseFee, queued := pool.CountContent()
	require.Equal(t, []int{4, 0, 0}, []int{pending, baseFee, queued})
}

// Neither a transaction the balance cannot cover nor one after a nonce gap has funds to lose.
func TestReplacementIgnoresUnfundedTxns(t *testing.T) {
	for _, tc := range []struct {
		name         string
		nonces       []uint64
		values       []uint64
		replaced     uint64
		replaceValue uint64
		wantPending  int
	}{
		// Nonce 2 spends the funds that nonce 3 needs.
		{"unfunded nonce", []uint64{0, 1, 2, 3}, []uint64{0, 0, spareFunds + fundedTxnCost, 0}, 3, 0, 3},
		{"after nonce gap", []uint64{0, 1, 3}, []uint64{0, 0, common.Ether / 2}, 0, common.Ether / 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
			var txns TxnSlots
			for i, nonce := range tc.nonces {
				txn := newTestTxnSlot(nonce, 0, 0, fundedTxnFeeCap, fundedTxnGas)
				txn.Txn.GetValue().SetUint64(tc.values[i])
				txn.IDHash[0] = byte(i + 1)
				txns.Append(txn, sender[:], true)
			}
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, slices.Repeat([]txpoolcfg.DiscardReason{txpoolcfg.Success}, len(tc.nonces)), reasons)
			pending, _, _ := pool.CountContent()
			require.Equal(t, tc.wantPending, pending)

			replacement := newTestTxnSlot(tc.replaced, 0, 0, bumpedFeeCap, fundedTxnGas)
			replacement.Txn.GetValue().SetUint64(tc.replaceValue)
			replacement.IDHash[0] = 9
			replaced := txns.Txns[slices.Index(tc.nonces, tc.replaced)]
			require.True(t, requiredBalance(replacement).Gt(requiredBalance(replaced)))
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, replaceFundedTxns(t, ctx, pool, sender, replacement))
			pending, _, _ = pool.CountContent()
			require.Equal(t, tc.wantPending, pending)
		})
	}
}
