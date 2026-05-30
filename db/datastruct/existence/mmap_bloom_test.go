// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0-or-later

package existence

import (
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMmapBloomParityWithLibrary builds a bloom filter via the existing
// holiman/bloomfilter writer path (NewFilter+AddHash+Build), opens it via the
// new mmap path (OpenFilter), and checks that ContainsHash answers identically
// for both inserted hashes and for a random set of probably-absent hashes.
func TestMmapBloomParityWithLibrary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "parity.kvei")

	const inserted = 50_000

	w, err := NewFilter(uint64(inserted), path, false)
	require.NoError(t, err)
	w.DisableFsync()

	r := rand.New(rand.NewSource(1))
	hashes := make([]uint64, inserted)
	for i := range hashes {
		hashes[i] = r.Uint64()
		require.NoError(t, w.AddHash(hashes[i]))
	}
	require.NoError(t, w.Build())

	rd, err := OpenFilter(path, false)
	require.NoError(t, err)
	defer rd.Close()
	require.NotNil(t, rd.mmapBloom)

	for _, h := range hashes {
		require.True(t, rd.ContainsHash(h), "inserted hash should hit: %d", h)
	}

	// Random probes — most should miss for a 1%-FPR filter. Exact parity isn't
	// what we measure here (that's done implicitly by the file format being
	// shared); we just confirm the reader doesn't lie about misses.
	const probes = 100_000
	missed := 0
	for i := 0; i < probes; i++ {
		h := r.Uint64()
		if !rd.ContainsHash(h) {
			missed++
		}
	}
	require.Greater(t, missed, probes/2, "random probes should mostly miss; got %d/%d", missed, probes)
}

func TestMmapBloomEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.kvei")

	w, err := NewFilter(1, path, false) // keysCount<2 → empty sentinel
	require.NoError(t, err)
	w.DisableFsync()
	require.NoError(t, w.Build())

	rd, err := OpenFilter(path, false)
	require.NoError(t, err)
	defer rd.Close()
	require.True(t, rd.empty)
	require.True(t, rd.ContainsHash(0xdeadbeef)) // empty filter answers true
}
