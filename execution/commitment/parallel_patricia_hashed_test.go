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
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

func TestParallelPatriciaHashedSkeletonConstruction(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	require.NotNil(t, p)
	require.NotNil(t, p.template, "template HexPatriciaHashed allocated")
	assert.Equal(t, int16(length.Addr), p.accountKeyLen)
	assert.Equal(t, runtime.NumCPU(), p.numWorkers)
	assert.Equal(t, MinSplitKeys, p.minSplitKeys)
	assert.Nil(t, p.rootHash.Load())
}

func TestParallelPatriciaHashedSkeletonRootTrie(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	require.Same(t, p.template, p.RootTrie())
}

func TestParallelPatriciaHashedSkeletonVariant(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	assert.Equal(t, VariantParallelHexPatricia, p.Variant())
	assert.Equal(t, TrieVariant("hex-parallel-patricia-hashed"), p.Variant())
}

func TestParallelPatriciaHashedSkeletonParseTrieVariant(t *testing.T) {
	assert.Equal(t, VariantParallelHexPatricia, ParseTrieVariant("parallel"))
	// Existing variants still parse to the expected values.
	assert.Equal(t, VariantHexPatriciaTrie, ParseTrieVariant("hex"))
	assert.Equal(t, VariantConcurrentHexPatricia, ParseTrieVariant("hex-parallel"))
	assert.Equal(t, VariantBinPatriciaTrie, ParseTrieVariant("bin"))
	// Unknown falls back to the default hex variant.
	assert.Equal(t, VariantHexPatriciaTrie, ParseTrieVariant("nonsense"))
}

func TestParallelPatriciaHashedSkeletonSetNumWorkers(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)

	p.SetNumWorkers(4)
	assert.Equal(t, 4, p.numWorkers)

	// Non-positive values fall back to runtime.NumCPU.
	p.SetNumWorkers(0)
	assert.Equal(t, runtime.NumCPU(), p.numWorkers)
	p.SetNumWorkers(-3)
	assert.Equal(t, runtime.NumCPU(), p.numWorkers)
}

func TestParallelPatriciaHashedSkeletonSetMinSplitKeys(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)

	p.SetMinSplitKeys(7)
	assert.Equal(t, uint32(7), p.minSplitKeys)

	// Zero falls back to the package default.
	p.SetMinSplitKeys(0)
	assert.Equal(t, MinSplitKeys, p.minSplitKeys)
}

func TestParallelPatriciaHashedSkeletonReset(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	stashed := []byte{0xde, 0xad}
	p.rootHash.Store(&stashed)
	require.NotNil(t, p.rootHash.Load())

	p.Reset()
	assert.Nil(t, p.rootHash.Load(), "Reset clears rootHash")
	require.NotNil(t, p.template, "Reset preserves the template")
}

func TestParallelPatriciaHashedSkeletonResetContextPropagates(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	ms := NewMockState(t)

	p.ResetContext(ms)
	assert.Same(t, ms, PatriciaContext(p.template.ctx), "context propagated to template")
}

func TestParallelPatriciaHashedSkeletonSetTrieContextFactory(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	assert.Nil(t, p.trieCtxFactory)

	ms := NewMockState(t)
	called := 0
	f := func() (PatriciaContext, func()) {
		called++
		return ms, func() {}
	}
	p.SetTrieContextFactory(f)
	require.NotNil(t, p.trieCtxFactory)

	got, cleanup := p.trieCtxFactory()
	assert.Same(t, ms, got)
	assert.NotNil(t, cleanup)
	assert.Equal(t, 1, called)
}

func TestParallelPatriciaHashedSkeletonSetTraceFlags(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)

	p.SetTrace(true)
	assert.True(t, p.template.trace)
	p.SetTrace(false)
	assert.False(t, p.template.trace)

	p.SetTraceDomain(true)
	assert.True(t, p.template.traceDomain)
	p.SetTraceDomain(false)
	assert.False(t, p.template.traceDomain)
}

func TestParallelPatriciaHashedSkeletonEnableWarmupCache(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)

	p.EnableWarmupCache(true)
	assert.True(t, p.template.enableWarmupCache)
	p.EnableWarmupCache(false)
	assert.False(t, p.template.enableWarmupCache)
}

func TestParallelPatriciaHashedSkeletonCaptureRoundTrip(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)

	capture := []string{"alpha", "beta"}
	p.SetCapture(capture)
	assert.Equal(t, capture, p.GetCapture(false), "GetCapture returns the set capture without truncation")
	assert.Equal(t, capture, p.GetCapture(true), "truncating GetCapture returns the previous capture")
	assert.Nil(t, p.GetCapture(false), "capture cleared after truncate")
}

func TestParallelPatriciaHashedSkeletonEnableCsvMetricsNoPanic(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	// Empty prefix is a valid no-op in HexPatriciaHashed.EnableCsvMetrics —
	// we only verify the delegation does not panic.
	require.NotPanics(t, func() { p.EnableCsvMetrics("") })
}

func TestParallelPatriciaHashedSkeletonReleaseNilSafe(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	require.NotNil(t, p.template)

	p.Release()
	assert.Nil(t, p.template, "Release drops the template")

	// Subsequent Release is a no-op.
	require.NotPanics(t, func() { p.Release() })

	// Plumbing methods stay safe after Release.
	require.NotPanics(t, func() {
		p.SetTrace(true)
		p.SetTraceDomain(true)
		p.EnableWarmupCache(true)
		p.SetCapture(nil)
		_ = p.GetCapture(false)
		p.EnableCsvMetrics("")
		p.ResetContext(nil)
	})
}

func TestParallelPatriciaHashedSkeletonRootHashStashed(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	defer p.Release()

	stored := []byte{0xde, 0xad, 0xbe, 0xef}
	p.rootHash.Store(&stored)

	got, err := p.RootHash()
	require.NoError(t, err)
	assert.Equal(t, stored, got)

	// Returned slice must be a copy so callers cannot mutate the published
	// value.
	got[0] = 0xff
	regot, err := p.RootHash()
	require.NoError(t, err)
	assert.Equal(t, byte(0xde), regot[0], "stashed root not mutated by caller")
}

func TestParallelPatriciaHashedSkeletonRootHashFallsBackToTemplate(t *testing.T) {
	ms := NewMockState(t)

	p := NewParallelPatriciaHashed(nil, length.Addr)
	defer p.Release()
	p.ResetContext(ms)

	got, err := p.RootHash()
	require.NoError(t, err)

	// Reference: a freshly constructed sequential trie returns the same hash
	// for the no-updates path.
	seq := NewHexPatriciaHashed(length.Addr, ms)
	defer seq.Release()
	expected, err := seq.RootHash()
	require.NoError(t, err)

	assert.Equal(t, expected, got, "RootHash falls back to template for the no-updates path")
}

func TestParallelPatriciaHashedSkeletonRootHashAfterRelease(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	p.Release()

	got, err := p.RootHash()
	require.NoError(t, err)
	assert.Nil(t, got, "RootHash on released instance returns nil")
}
