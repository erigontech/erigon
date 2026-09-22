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

package v4

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestTrieProcessStartsWarmuperWhenEnabled(t *testing.T) {
	trie, updates := NewTrie(t.TempDir(), commitment.TrieConfig{})
	processCtx := newMockContext()
	warmupCtx := newMockContext()
	trie.ResetContext(processCtx)

	address := make([]byte, 20)
	address[0] = 1
	update := fullAccountUpdate(1, 2, common.Hash{})
	updates.TouchPlainKeyDirect(string(address), &update)

	var factoryCalls atomic.Int32
	_, err := trie.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{
		Enabled: true,
		CtxFactory: func(context.Context) (commitment.PatriciaContext, func()) {
			factoryCalls.Add(1)
			return warmupCtx, nil
		},
		NumWorkers: 1,
		MaxDepth:   commitment.WarmupMaxDepth,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), factoryCalls.Load())
}

func TestWarmupV4KeyAndStepAccountDescent(t *testing.T) {
	hashedKey := make([]byte, 128)
	hashedKey[0] = 2
	hashedKey[1] = 3
	rootRecord := recordFixture(0, 0, 1<<2, 0, 0, 0, nil, nil, nil, nil)
	childRecord := recordFixture(0, 1, 1<<3, 1<<3, 0, 0, nil, nil, nil, nil)
	addrHash := hashAddressPath(hashedKey[:64])
	records := map[string][]byte{
		string(AccountNodeKey(nil, nil)):           rootRecord,
		string(AccountNodeKey([]byte{2}, nil)):     childRecord,
		string(StorageNodeKey(addrHash, nil, nil)): nil,
	}
	wantKeys := [][]byte{AccountNodeKey(nil, nil), AccountNodeKey([]byte{2}, nil), StorageNodeKey(addrHash, nil, nil)}
	var scratch [66]byte
	var gotKeys [][]byte
	for depth := 0; ; {
		key, ok := warmupKeyV4(hashedKey, depth, scratch[:])
		require.True(t, ok)
		gotKeys = append(gotKeys, bytes.Clone(key))
		nextDepth, stop := warmupStepV4(records[string(key)], hashedKey, depth)
		if stop {
			break
		}
		depth = nextDepth
	}
	require.Equal(t, wantKeys, gotKeys)
}

func TestWarmupV4KeyPlaneCrossing(t *testing.T) {
	accountPath := make([]byte, 64)
	storagePath := make([]byte, 64)
	for i := range accountPath {
		accountPath[i] = byte(i & 0x0f)
		storagePath[i] = byte((i + 7) & 0x0f)
	}
	hashedKey := append(bytes.Clone(accountPath), storagePath...)
	addrHash := hashAddressPath(accountPath)
	var scratch [66]byte

	for depth := range 64 {
		got, ok := warmupKeyV4(hashedKey, depth, scratch[:])
		require.True(t, ok)
		require.Equal(t, AccountNodeKey(accountPath[:depth], nil), got)
	}
	got, ok := warmupKeyV4(hashedKey, 64, scratch[:])
	require.True(t, ok)
	require.Equal(t, StorageNodeKey(addrHash, nil, nil), got)
	got, ok = warmupKeyV4(hashedKey, 65, scratch[:])
	require.True(t, ok)
	require.Equal(t, StorageNodeKey(addrHash, storagePath[:1], nil), got)

	accountOnly := accountPath[:64]
	got, ok = warmupKeyV4(accountOnly, 64, scratch[:])
	require.True(t, ok)
	require.Equal(t, AccountNodeKey(accountOnly, nil), got)

	got, ok = warmupKeyV4(hashedKey, 64, scratch[:])
	require.True(t, ok)
	require.Equal(t, tagStorageNode, got[0])
}

func TestWarmupV4StepSelfExtensionAndTruncated(t *testing.T) {
	record := recordFixture(hdrHasSelfExt, 0, 1<<4, 0, 0, 0, extFixture([]byte{1, 2}), nil, nil, nil)
	hashedKey := make([]byte, 64)
	copy(hashedKey, []byte{1, 2, 4})
	nextDepth, stop := warmupStepV4(record, hashedKey, 0)
	require.False(t, stop)
	require.Equal(t, 3, nextDepth)

	hashedKey[2] = 5
	nextDepth, stop = warmupStepV4(record, hashedKey, 0)
	require.True(t, stop)
	require.Zero(t, nextDepth)

	require.NotPanics(t, func() {
		nextDepth, stop = warmupStepV4([]byte{hdrHasSelfExt}, hashedKey, 0)
	})
	require.True(t, stop)
	require.Zero(t, nextDepth)
}

func TestWarmupV4StepUsesPlaneDepthAndExtensionLength(t *testing.T) {
	record := recordFixture(hdrHasChildExt, 0, 1<<4, 0, 1<<4, 0, nil, map[int][]byte{4: extFixture([]byte{1, 2})}, nil, nil)
	hashedKey := make([]byte, 128)
	hashedKey[64] = 4
	nextDepth, stop := warmupStepV4(record, hashedKey, 64)
	require.False(t, stop)
	require.Equal(t, 67, nextDepth)
}

func TestWarmupV4KeyScratchReuse(t *testing.T) {
	hashedKey := bytes.Repeat([]byte{0x0b}, 128)
	var scratch [66]byte
	for _, depth := range []int{1, 2, 63, 64, 65, 128} {
		got, ok := warmupKeyV4(hashedKey, depth, scratch[:])
		require.True(t, ok)
		var want []byte
		if depth < 64 {
			want = AccountNodeKey(hashedKey[:depth], nil)
		} else {
			want = StorageNodeKey(hashAddressPath(hashedKey[:64]), hashedKey[64:depth], nil)
		}
		require.Equal(t, want, got)
		if depth&1 != 0 {
			require.Zero(t, got[len(got)-2]&0x0f)
		}
	}

	allocs := testing.AllocsPerRun(100, func() {
		_, _ = warmupKeyV4(hashedKey, 65, scratch[:])
	})
	require.Zero(t, allocs)
}
