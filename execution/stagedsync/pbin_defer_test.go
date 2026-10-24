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

package stagedsync

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
)

func pbinOnlyStepTest(t *testing.T) (kv.TemporalRwTx, *execctx.SharedDomains) {
	t.Helper()
	commitmentflags.Restore(t)
	statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment = true, false
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	_, tx, doms := setupStepTest(t)
	doms.SetDisableInlineTouchKey(false)
	require.Equal(t, commitment.VariantBinPatriciaTrie, doms.GetCommitmentCtx().Trie().Variant())
	return tx, doms
}

func putPBinDeferAccounts(t *testing.T, tx kv.TemporalRwTx, doms *execctx.SharedDomains, from, to byte, nonce, txNum uint64) {
	t.Helper()
	for b := from; b <= to; b++ {
		account := accounts.Account{Nonce: nonce + uint64(b)}
		require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, bytes.Repeat([]byte{b}, 20), accounts.SerialiseV3(&account), txNum, nil))
	}
}

func pbinDeferRecords(t *testing.T, tx kv.TemporalRwTx, doms *execctx.SharedDomains) map[string][]byte {
	t.Helper()
	require.NoError(t, doms.Flush(t.Context(), tx))
	records := make(map[string][]byte)
	it, err := tx.Debug().RangeLatest(doms.GetCommitmentCtx().CommitmentDomain(), nil, nil, kv.Unlim)
	require.NoError(t, err)
	defer it.Close()
	for it.HasNext() {
		key, value, err := it.Next()
		require.NoError(t, err)
		records[string(key)] = bytes.Clone(value)
	}
	require.NotEmpty(t, records)
	return records
}

func requirePBinDeferRecords(t *testing.T, tx kv.TemporalRwTx, doms *execctx.SharedDomains, keys map[string]struct{}, want map[string][]byte, label string) {
	t.Helper()
	for key := range keys {
		got, _, err := doms.GetLatest(doms.GetCommitmentCtx().CommitmentDomain(), tx, []byte(key))
		require.NoError(t, err)
		require.Equal(t, len(want[key]) == 0, len(got) == 0, "%s: presence of record %x", label, key)
		if len(got) != 0 {
			require.Equal(t, want[key], got, "%s: record %x", label, key)
		}
	}
}

func TestPBinDeferCommitmentUpdatesIncludesBin(t *testing.T) {
	for _, tc := range []struct {
		name             string
		isForkValidation bool
		parallel         bool
		isApplyingBlocks bool
		want             bool
	}{
		{name: "fork validation", isForkValidation: true, want: true},
		{name: "parallel apply", parallel: true, isApplyingBlocks: true, want: true},
		{name: "parallel not applying", parallel: true},
		{name: "serial apply", isApplyingBlocks: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, doms := pbinOnlyStepTest(t)
			setDeferCommitmentUpdates(doms, tc.isForkValidation, tc.parallel, tc.isApplyingBlocks)
			putPBinDeferAccounts(t, tx, doms, 1, 8, 0, 1)
			_, err := doms.ComputeCommitment(t.Context(), tx, true, 1, 1, "defer", nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, doms.GetCommitmentCtx().HasPendingUpdate())
		})
	}
}

func TestPBinForkValidationDeferredBlockFlushesAndUnwinds(t *testing.T) {
	inlineTx, inline := pbinOnlyStepTest(t)
	putPBinDeferAccounts(t, inlineTx, inline, 1, 40, 0, 1)
	wantRoot1, err := inline.ComputeCommitment(t.Context(), inlineTx, true, 1, 1, "inline", nil)
	require.NoError(t, err)
	want1 := pbinDeferRecords(t, inlineTx, inline)
	putPBinDeferAccounts(t, inlineTx, inline, 20, 60, 100, 2)
	wantRoot2, err := inline.ComputeCommitment(t.Context(), inlineTx, true, 2, 2, "inline", nil)
	require.NoError(t, err)
	want2 := pbinDeferRecords(t, inlineTx, inline)

	tx, doms := pbinOnlyStepTest(t)
	setDeferCommitmentUpdates(doms, true, false, false)
	defer doms.SetDeferCommitmentUpdates(false)
	putPBinDeferAccounts(t, tx, doms, 1, 40, 0, 1)
	root1, err := doms.ComputeCommitment(t.Context(), tx, true, 1, 1, "deferred", nil)
	require.NoError(t, err)
	require.Equal(t, wantRoot1, root1)
	require.True(t, doms.GetCommitmentCtx().HasPendingUpdate(), "block 1 must leave its branch updates pending")
	putPBinDeferAccounts(t, tx, doms, 20, 60, 100, 2)
	root2, err := doms.ComputeCommitment(t.Context(), tx, true, 2, 2, "deferred", nil)
	require.NoError(t, err)
	require.Equal(t, wantRoot2, root2)
	require.True(t, doms.GetCommitmentCtx().HasPendingUpdate(), "block 2 must leave its branch updates pending")
	require.NoError(t, doms.FlushPendingUpdates(tx))

	keys := make(map[string]struct{}, len(want2))
	for key := range want1 {
		keys[key] = struct{}{}
	}
	for key := range want2 {
		keys[key] = struct{}{}
	}
	requirePBinDeferRecords(t, tx, doms, keys, want2, "after flush")
	doms.Unwind(2, nil)
	requirePBinDeferRecords(t, tx, doms, keys, want1, "after unwinding block 2")
}
