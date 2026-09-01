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

package commitment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

func edgeTrieConfig() TrieConfig {
	cfg := DefaultTrieConfig()
	cfg.EdgeRecords = true
	return cfg
}

// edgeBatch runs one batch against a v3 record store, mirroring processModeBatchState.
func edgeBatch(t *testing.T, ms *MockState, mode runMode, workers int, keys [][]byte, upds []Update, blob []byte) ([]byte, []byte, uint64) {
	t.Helper()
	require.NoError(t, ms.applyPlainUpdates(keys, upds))
	ctx := &edgeRecordContext{MockState: ms}
	cfg := edgeTrieConfig()

	if mode == modeParallel {
		tr := NewParallelPatriciaHashed(func(context.Context) (PatriciaContext, func()) { return ctx, func() {} }, length.Addr, cfg)
		defer tr.Release()
		tr.SetNumWorkers(workers)
		tr.ResetContext(ctx)
		require.NoError(t, tr.RootTrie().SetState(blob))
		ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
		defer ut.Close()
		for _, k := range keys {
			ut.TouchPlainKey(string(k), nil, nil)
		}
		root := processRoot(t, tr, ut)
		encoded, err := tr.RootTrie().EncodeCurrentState(nil)
		require.NoError(t, err)
		return root, encoded, tr.DeepLocalFolds()
	}

	tr := NewHexPatriciaHashed(length.Addr, ctx, cfg)
	defer tr.Release()
	require.NoError(t, tr.SetState(blob))
	ut := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, keys, upds)
	defer ut.Close()
	root := processRoot(t, tr, ut)
	encoded, err := tr.EncodeCurrentState(nil)
	require.NoError(t, err)
	return root, encoded, 0
}

func edgeIncrementalRoot(t *testing.T, mode runMode, workers int, k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update) ([]byte, uint64) {
	t.Helper()
	ms := NewMockState(t)
	if mode != modeSeq {
		ms.SetConcurrentCommitment(true)
	}
	_, blob, _ := edgeBatch(t, ms, mode, workers, k1, u1, nil)
	root, _, deepFolds := edgeBatch(t, ms, mode, workers, k2, u2, blob)
	return root, deepFolds
}

// A pre-existing whale's storage folds in a mounted storage worker, whose grid starts
// below the account boundary. Under v3 the storage-leaf records that worker reads carry
// no account address of their own, so it has to be told which account it is folding.
func TestDeepFold_EdgeRecordsPreExistingWhaleParity(t *testing.T) {
	wide := nibs(0, 1, 2, 3, 4, 5, 6, 7)
	touch := nibs(0, 1, 2)
	k1, u1, k2, u2 := buildSubsetTouchedWhale(20260622, wide, touch, 60, 420)
	fk, fu := buildMixedCorpus(7777, 200)
	k1 = append(append([][]byte{}, fk...), k1...)
	u1 = append(append([]Update{}, fu...), u1...)

	seqRoot, _ := edgeIncrementalRoot(t, modeSeq, 0, k1, u1, k2, u2)
	parRoot, deepFolds := edgeIncrementalRoot(t, modeParallel, 4, k1, u1, k2, u2)
	require.Positive(t, deepFolds, "the whale must take the concurrent deep fold, or this covers nothing")
	require.Equal(t, seqRoot, parRoot, "parallel and sequential roots must match under v3 edge records")
}
