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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildNibbleSpread returns a corpus whose accounts land under distinct root
// nibbles, so processMounted actually fans out across workers.
func buildNibbleSpread(t *testing.T, nibbles, slots int) ([][]byte, []Update) {
	t.Helper()
	rnd := rand.New(rand.NewSource(9931))
	ub := NewUpdateBuilder()
	for n := range nibbles {
		addNibbleAccount(ub, rnd, n, n, slots)
	}
	return ub.Build()
}

func TestParallelPatriciaHashedReportsProgress(t *testing.T) {
	ms := NewMockState(t)
	keys, upds := buildNibbleSpread(t, 16, 4)
	require.NoError(t, ms.applyPlainUpdates(keys, upds))

	tr := newParTrie(t, ms, 4)
	defer tr.Release()
	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()
	for _, k := range keys {
		ut.TouchPlainKey(string(k), nil, nil)
	}

	var got []*CommitProgress
	_, err := tr.Process(context.Background(), ut, "", func(p *CommitProgress) {
		got = append(got, p)
	}, WarmupConfig{})
	require.NoError(t, err)

	require.Len(t, got, 1, "parallel Process must report the round exactly once")
	p := got[0]
	assert.Equal(t, p.UpdateCount, p.KeyIndex, "terminal callback reports a finished round")

	// The counters must describe the whole round, not one worker's slice.
	m := p.Metrics
	assert.Positive(t, m.AddressKeys)
	assert.Positive(t, m.StorageKeys)
	assert.Positive(t, m.Folds, "fold count is recorded")
	assert.Positive(t, m.UpdateBranch, "deferred branch writes are counted")

	// The parallel engine re-traverses some subtrees (mount+replay), so its key
	// counts are traversals, not distinct keys, and legitimately exceed the
	// sequential engine's. Guard the direction: under-counting would mean a
	// worker's Metrics never got merged.
	seqMS := NewMockState(t)
	require.NoError(t, seqMS.applyPlainUpdates(keys, upds))
	seq := newSeqTrie(t, seqMS)
	defer seq.Release()
	sut := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, keys, upds)
	defer sut.Close()
	_, err = seq.Process(context.Background(), sut, "", nil, WarmupConfig{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, m.AddressKeys, seq.metrics.addressKeys.Load(),
		"every worker's address keys are merged in")
	assert.GreaterOrEqual(t, m.StorageKeys, seq.metrics.storageKeys.Load(),
		"every worker's storage keys are merged in")
}

func TestMetricsResetClearsEveryCounter(t *testing.T) {
	m := NewMetrics("")
	m.cacheBranch.Add(3)
	m.cacheAccount.Add(4)
	m.cacheStorage.Add(5)
	m.folds.Add(6)
	m.unfolds.Add(7)
	m.spentFolding.Add(8)

	m.Reset()

	v := m.AsValues()
	assert.Zero(t, v.CacheBranch)
	assert.Zero(t, v.CacheAccount)
	assert.Zero(t, v.CacheStorage)
	assert.Zero(t, v.Folds)
	assert.Zero(t, v.Unfolds)
	assert.Zero(t, v.SpentFolding)
}
