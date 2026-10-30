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

package state_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestPBinResumeLeavesMatchesSourceState(t *testing.T) {
	selectPBinLeafStreamHash(t)
	db, agg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	addressA := pbinLeafStreamAddress(0x11)
	addressB := pbinLeafStreamAddress(0x22)
	addressC := pbinLeafStreamAddress(0x33)
	addressD := pbinLeafStreamAddress(0x44)
	slotA := append(make([]byte, 31), 1)
	slotB := append(make([]byte, 31), 2)
	slotC := append(make([]byte, 31), 3)
	slotD := append(make([]byte, 31), 4)
	code := []byte{0x60, 0x01, 0x60, 0x00, 0x52}
	delegation := append(append([]byte(nil), eip8297.DelegationMarker[:]...), bytes.Repeat([]byte{0x42}, 20)...)

	writePBinLeafStreamRange(t, db, 0, pbinLeafStreamStepSize, []pbinLeafStreamAccount{
		{address: addressA, nonce: 1, balance: 1, slots: map[string][]byte{string(slotA): {1}}},
		{address: addressB, nonce: 2, balance: 2, slots: map[string][]byte{string(slotB): {2}}},
		{address: addressD, nonce: 4, balance: 4, slots: map[string][]byte{string(slotD): {4}}},
	})
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()

	writePBinLeafStreamRange(t, db, pbinLeafStreamStepSize, 2*pbinLeafStreamStepSize, []pbinLeafStreamAccount{
		{address: addressA, code: code, codeWritten: true, nonce: 5, balance: 6, slots: map[string][]byte{string(slotA): {9}, string(slotC): {3}}},
		{address: addressC, code: code, codeWritten: true, nonce: 3, balance: 3, slots: map[string][]byte{string(slotC): {4}}},
		{address: addressD, code: delegation, codeWritten: true, nonce: 4, balance: 4},
	})
	deletePBinResumeKey(t, db, kv.AccountsDomain, addressB, 2*pbinLeafStreamStepSize)
	deletePBinResumeKey(t, db, kv.StorageDomain, append(bytes.Clone(addressD), slotD...), 2*pbinLeafStreamStepSize)
	writePBinLeafStreamRange(t, db, 2*pbinLeafStreamStepSize, 3*pbinLeafStreamStepSize, []pbinLeafStreamAccount{
		{address: addressD, nonce: 4, balance: 4},
	})
	deletePBinResumeKey(t, db, kv.CodeDomain, addressD, 2*pbinLeafStreamStepSize)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 1, 2, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	base := pbinResumeLeafMap(t, agg)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 2, 3, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	agg.SetErigondbDomainStepsInFrozenFile(1)
	require.NoError(t, agg.MergeLoop(t.Context()))
	want := pbinResumeLeafMap(t, agg)

	at := agg.BeginFilesRo()
	t.Cleanup(at.Close)
	var hasSpanningSourceFile bool
	for _, file := range at.Files(kv.AccountsDomain) {
		if strings.HasSuffix(file.Fullpath(), ".kv") && file.StartRootNum() < 2 && file.EndRootNum() > 2 {
			hasSpanningSourceFile = true
		}
	}
	require.True(t, hasSpanningSourceFile)
	var got []state.PBinLeaf
	require.NoError(t, state.PBinResumeLeaves(t.Context(), at, state.PBinResumeOptions{
		BaseTxNum:   2 * pbinLeafStreamStepSize,
		SourceTxNum: 3 * pbinLeafStreamStepSize,
		Workers:     4,
		PostCancun:  true,
	}, func(leaf state.PBinLeaf) error {
		got = append(got, state.PBinLeaf{Key: bytes.Clone(leaf.Key), Value: bytes.Clone(leaf.Value), Stamp: leaf.Stamp})
		return nil
	}))
	for i := 1; i < len(got); i++ {
		require.Less(t, bytes.Compare(got[i-1].Key, got[i].Key), 0)
	}
	resumed := make(map[string][]byte, len(base))
	for key, leaf := range base {
		resumed[key] = bytes.Clone(leaf)
	}
	for _, leaf := range got {
		if allZero(leaf.Value) {
			delete(resumed, string(leaf.Key))
		} else {
			resumed[string(leaf.Key)] = bytes.Clone(leaf.Value)
		}
	}
	require.NotContains(t, resumed, string(eip8297.TreeKeyAccount(addressB, eip8297.BasicDataLeafKey)))
	require.NotContains(t, resumed, string(eip8297.TreeKeyStorage(addressD, slotD)))
	require.Equal(t, want, resumed)
	for _, leaf := range got {
		require.Greater(t, leaf.Stamp, 2*pbinLeafStreamStepSize)
	}
	require.Contains(t, resumed, string(eip8297.TreeKeyAccount(addressA, eip8297.BasicDataLeafKey)))
	require.NotContains(t, resumed, string(eip8297.TreeKeyAccount(addressB, eip8297.BasicDataLeafKey)))
	require.Contains(t, resumed, string(eip8297.TreeKeyCodeChunk(crypto.Keccak256Hash(code), 0)))
}

func TestPBinResumeLeavesSkipsFileEndingAtBase(t *testing.T) {
	selectPBinLeafStreamHash(t)
	db, agg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	converted := pbinLeafStreamAddress(0x11)
	gap := pbinLeafStreamAddress(0x22)
	writePBinLeafStreamRange(t, db, 0, pbinLeafStreamStepSize, []pbinLeafStreamAccount{{address: converted, nonce: 1, balance: 1}})
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	writePBinLeafStreamRange(t, db, pbinLeafStreamStepSize, 2*pbinLeafStreamStepSize, []pbinLeafStreamAccount{{address: gap, nonce: 2, balance: 2}})
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 1, 2, unboundedFinalityCtx, false))
	agg.WaitForFiles()

	at := agg.BeginFilesRo()
	t.Cleanup(at.Close)
	emitted := make(map[string]struct{})
	require.NoError(t, state.PBinResumeLeaves(t.Context(), at, state.PBinResumeOptions{
		BaseTxNum:   pbinLeafStreamStepSize - 1,
		SourceTxNum: 2*pbinLeafStreamStepSize - 1,
		Workers:     2,
		PostCancun:  true,
	}, func(leaf state.PBinLeaf) error {
		emitted[string(leaf.Key)] = struct{}{}
		return nil
	}))
	require.Contains(t, emitted, string(eip8297.TreeKeyAccount(gap, eip8297.BasicDataLeafKey)))
	require.NotContains(t, emitted, string(eip8297.TreeKeyAccount(converted, eip8297.BasicDataLeafKey)), "a file ending at the inclusive base txNum is already converted")
}

func TestPBinResumeLeavesRefusesUnsafeSource(t *testing.T) {
	selectPBinLeafStreamHash(t)
	db, agg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	writePBinLeafStreamRange(t, db, 0, 2*pbinLeafStreamStepSize, []pbinLeafStreamAccount{{address: pbinLeafStreamAddress(0x11), nonce: 1, balance: 1}})
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 2, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	for _, test := range []struct {
		name string
		opts state.PBinResumeOptions
		want string
	}{
		{name: "zero start", opts: state.PBinResumeOptions{BaseTxNum: 1, SourceTxNum: 2 * pbinLeafStreamStepSize, Workers: 2, PostCancun: true}, want: "starts at txNum 0"},
		{name: "pre Cancun", opts: state.PBinResumeOptions{BaseTxNum: pbinLeafStreamStepSize, SourceTxNum: 2 * pbinLeafStreamStepSize, Workers: 2}, want: "not entirely post-Cancun"},
		{name: "bytecode rewrite", opts: state.PBinResumeOptions{BaseTxNum: pbinLeafStreamStepSize, SourceTxNum: 2 * pbinLeafStreamStepSize, Workers: 2, PostCancun: true, BytecodeRewrite: true}, want: "bytecode rewrite"},
	} {
		t.Run(test.name, func(t *testing.T) {
			at := agg.BeginFilesRo()
			t.Cleanup(at.Close)
			err := state.PBinResumeLeaves(t.Context(), at, test.opts, func(state.PBinLeaf) error { return nil })
			require.ErrorContains(t, err, test.want)
		})
	}
}

func pbinResumeLeafMap(t *testing.T, agg *state.Aggregator) map[string][]byte {
	t.Helper()
	at := agg.BeginFilesRo()
	t.Cleanup(at.Close)
	leaves := make(map[string][]byte)
	require.NoError(t, state.ForEachPBinLeaf(at, nil, true, func(leaf state.PBinLeaf) error {
		leaves[string(leaf.Key)] = bytes.Clone(leaf.Value)
		return nil
	}))
	return leaves
}

func deletePBinResumeKey(t *testing.T, db kv.TemporalRwDB, domain kv.Domain, key []byte, txNum uint64) {
	t.Helper()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	defer sd.Close()
	previous, _, err := sd.GetLatest(domain, tx, key)
	require.NoError(t, err)
	require.NoError(t, sd.DomainDel(domain, tx, key, txNum, previous))
	require.NoError(t, sd.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
}

func allZero(value []byte) bool {
	return bytes.Equal(value, make([]byte, len(value)))
}
