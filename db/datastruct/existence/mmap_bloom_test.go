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

// TestMmapBloomForceInMem verifies that pinning the bits to the heap preserves
// ContainsHash answers, releases the mapping, and that the madvise hints and a
// repeated ForceInMem are safe no-ops.
func TestMmapBloomForceInMem(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "forceinmem.kvei")

	const inserted = 20_000
	w, err := NewFilter(uint64(inserted), path, false)
	require.NoError(t, err)
	w.DisableFsync()
	r := rand.New(rand.NewSource(3))
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
	require.False(t, rd.mmapBloom.keepInMem)
	require.NotNil(t, rd.mmapBloom.mmap)

	rd.MadvWillNeed() // safe on the live mapping
	rd.MadvNormal()

	probes := append([]uint64{}, hashes...)
	for i := 0; i < inserted; i++ {
		probes = append(probes, r.Uint64())
	}
	before := make([]bool, len(probes))
	for i, h := range probes {
		before[i] = rd.ContainsHash(h)
	}

	moved := rd.ForceInMem()
	require.Positive(t, uint64(moved))
	require.True(t, rd.mmapBloom.keepInMem)
	require.Nil(t, rd.mmapBloom.mmap, "mapping released after ForceInMem")

	for i, h := range probes {
		require.Equalf(t, before[i], rd.ContainsHash(h), "ContainsHash diverged after ForceInMem at probe %d", i)
	}

	rd.MadvWillNeed() // safe no-ops once pinned
	rd.MadvNormal()
	require.Zero(t, uint64(rd.ForceInMem()), "ForceInMem should be idempotent")
}

func TestMmapBloomPolicyOnEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty-policy.kvei")
	w, err := NewFilter(1, path, false) // keysCount<2 → empty sentinel
	require.NoError(t, err)
	w.DisableFsync()
	require.NoError(t, w.Build())

	rd, err := OpenFilter(path, false)
	require.NoError(t, err)
	defer rd.Close()
	require.True(t, rd.empty)
	rd.MadvWillNeed() // no-ops on an empty filter
	rd.MadvNormal()
	require.Zero(t, uint64(rd.ForceInMem()))
}

// u64Hasher is a hash.Hash64 whose Sum64 returns a fixed value, to exercise the
// Filter.Contains(hash.Hash64) entry point.
type u64Hasher uint64

func (u u64Hasher) Sum64() uint64               { return uint64(u) }
func (u u64Hasher) Write(p []byte) (int, error) { return len(p), nil }
func (u u64Hasher) Sum(b []byte) []byte         { return b }
func (u u64Hasher) Reset()                      {}
func (u u64Hasher) Size() int                   { return 8 }
func (u u64Hasher) BlockSize() int              { return 8 }

// TestFuseFilterContains: Contains(hash.Hash64) on a fuse-backed reader must
// delegate to the fuse reader, not fall through to the nil writer-path filter.
func TestFuseFilterContains(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fuse-contains.kvei")

	const inserted = 10_000
	w, err := NewFilter(uint64(inserted), path, true)
	require.NoError(t, err)
	w.DisableFsync()
	r := rand.New(rand.NewSource(9))
	hashes := make([]uint64, inserted)
	for i := range hashes {
		hashes[i] = r.Uint64()
		require.NoError(t, w.AddHash(hashes[i]))
	}
	require.NoError(t, w.Build())
	w.Close()

	rd, err := OpenFilter(path, true)
	require.NoError(t, err)
	defer rd.Close()
	require.True(t, rd.useFuse)

	for _, h := range hashes {
		require.True(t, rd.ContainsHash(h), "inserted hash should hit")
		require.Equal(t, rd.ContainsHash(h), rd.Contains(u64Hasher(h)),
			"Contains must mirror ContainsHash on the fuse path")
	}
}

// TestFuseFilterCloseReleasesWriter: Close must release the fuse writer's
// off-heap resources, not just the reader.
func TestFuseFilterCloseReleasesWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fuse-close.kvei")

	w, err := NewFilter(10_000, path, true)
	require.NoError(t, err)
	w.DisableFsync()
	require.NotNil(t, w.fuseWriter)
	for i := uint64(1); i <= 1000; i++ {
		require.NoError(t, w.AddHash(i))
	}
	require.NoError(t, w.Build())

	w.Close()
	require.Nil(t, w.fuseWriter, "Close must release the fuse writer")
}
