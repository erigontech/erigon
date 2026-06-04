// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0-or-later

package existence

import (
	"bufio"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	bloomfilter "github.com/holiman/bloomfilter/v2"
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

// TestMmapBloomDifferentialParity opens the same on-disk .kvei file with both
// the bloomfilter library reader and the mmap reader, then requires identical
// ContainsHash answers across every inserted key and a large random probe set.
// This is the exact byte-for-byte parity that TestMmapBloomParityWithLibrary
// only checks implicitly, and guards against drift in either reader.
func TestMmapBloomDifferentialParity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "parity-diff.kvei")

	const inserted = 50_000

	w, err := NewFilter(uint64(inserted), path, false)
	require.NoError(t, err)
	w.DisableFsync()

	r := rand.New(rand.NewSource(7))
	hashes := make([]uint64, inserted)
	for i := range hashes {
		hashes[i] = r.Uint64()
		require.NoError(t, w.AddHash(hashes[i]))
	}
	require.NoError(t, w.Build())

	f, err := os.Open(path)
	require.NoError(t, err)
	lib := new(bloomfilter.Filter)
	_, err = lib.UnmarshalFromReaderNoVerify(bufio.NewReaderSize(f, 1<<20))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	mb, err := openMmapBloom(path)
	require.NoError(t, err)
	defer func() { _ = mb.Close() }()

	require.Equal(t, lib.K(), uint64(len(mb.keys)), "key count")
	require.Equal(t, lib.M(), mb.m, "bit count")

	for i, h := range hashes {
		if got, want := mb.ContainsHash(h), lib.ContainsHash(h); got != want {
			t.Fatalf("inserted hash #%d (%d): mmap=%v library=%v", i, h, got, want)
		}
	}

	const probes = 1_000_000
	for i := 0; i < probes; i++ {
		h := r.Uint64()
		if got, want := mb.ContainsHash(h), lib.ContainsHash(h); got != want {
			t.Fatalf("probe %d (hash %d): mmap=%v library=%v", i, h, got, want)
		}
	}
}
