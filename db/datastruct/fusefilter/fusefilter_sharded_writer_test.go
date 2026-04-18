package fusefilter

import (
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSharded_BasicRoundTrip(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_basic.tmp")
	testKeys := []uint64{1, 2, 3, 4, 5, 100, 1000, 10000, 100000}

	w, err := NewShardedWriter(filePath)
	require.NoError(err)
	for _, k := range testKeys {
		require.NoError(w.AddHash(k))
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewShardedReader(filePath)
	require.NoError(err)
	defer r.Close()
	for _, k := range testKeys {
		require.True(r.ContainsHash(k), "key %d should be present", k)
	}
}

// Route all keys to a single shard: byte(k)==7.
func TestSharded_AdversarialSingleShard(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_adversarial.tmp")

	const keyCount = 100_000
	keys := make([]uint64, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = uint64(i)<<8 | 7
	}

	w, err := NewShardedWriter(filePath)
	require.NoError(err)
	for _, k := range keys {
		require.NoError(w.AddHash(k))
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewShardedReader(filePath)
	require.NoError(err)
	defer r.Close()

	for _, k := range keys {
		require.True(r.ContainsHash(k))
	}

	// 255 shards are empty → any probe whose low byte differs must return false.
	for b := 0; b < 256; b++ {
		if b == 7 {
			continue
		}
		require.False(r.ContainsHash(uint64(b)<<8|uint64(b)), "empty shard %d unexpectedly matched", b)
	}
}

func TestSharded_UniformDistribution(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_uniform.tmp")

	rng := rand.New(rand.NewPCG(1, 2))
	const keyCount = 200_000
	keys := make(map[uint64]struct{}, keyCount)
	for len(keys) < keyCount {
		keys[rng.Uint64()] = struct{}{}
	}

	w, err := NewShardedWriter(filePath)
	require.NoError(err)
	for k := range keys {
		require.NoError(w.AddHash(k))
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewShardedReader(filePath)
	require.NoError(err)
	defer r.Close()

	for k := range keys {
		require.True(r.ContainsHash(k))
	}

	// Probe with 200K fresh random keys that were not inserted. Expect ~0.3%
	// false-positive rate; allow generous slack for test stability.
	falsePositives := 0
	const probes = 200_000
	for i := 0; i < probes; i++ {
		var k uint64
		for {
			k = rng.Uint64()
			if _, ok := keys[k]; !ok {
				break
			}
		}
		if r.ContainsHash(k) {
			falsePositives++
		}
	}
	fpRate := float64(falsePositives) / float64(probes)
	t.Logf("false positive rate: %d/%d = %.4f%%", falsePositives, probes, fpRate*100)
	require.Less(fpRate, 0.01, "false positive rate unexpectedly high")
}

// Exercise the per-shard partial-flush branch: a mix of shards with fill
// levels just below, at, and just above the 512-key page boundary.
func TestSharded_PageBoundary(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_pageboundary.tmp")

	// Plan per shard key count: 511 for shard 1, 512 for shard 2, 513 for shard 3,
	// 500 for shard 4, 1024 for shard 5, and a scattering for others.
	plan := map[byte]int{
		1: 511,
		2: 512,
		3: 513,
		4: 500,
		5: 1024,
		7: 100,
	}
	var allKeys []uint64
	for b, n := range plan {
		for i := 0; i < n; i++ {
			allKeys = append(allKeys, uint64(i)<<8|uint64(b))
		}
	}

	w, err := NewShardedWriter(filePath)
	require.NoError(err)
	for _, k := range allKeys {
		require.NoError(w.AddHash(k))
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewShardedReader(filePath)
	require.NoError(err)
	defer r.Close()
	for _, k := range allKeys {
		require.True(r.ContainsHash(k))
	}
}

// Zero keys: all 256 shards empty; any probe returns false.
func TestSharded_EmptyAllShards(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_empty.tmp")

	w, err := NewShardedWriter(filePath)
	require.NoError(err)
	require.NoError(w.Build())
	w.Close()

	r, err := NewShardedReader(filePath)
	require.NoError(err)
	defer r.Close()

	for i := uint64(0); i < 10_000; i++ {
		require.False(r.ContainsHash(i))
	}
}

// Feed intentionally-duplicated hashes to one shard to exercise xorfilter's
// in-place pruneDuplicates pass against the RDWR mmap.
func TestSharded_MmapMutationOnDuplicates(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_dedup.tmp")

	w, err := NewShardedWriter(filePath)
	require.NoError(err)
	// 1000 unique keys mixed with 100 duplicates, all routed to shard 0.
	const unique = 1000
	for i := 0; i < unique; i++ {
		require.NoError(w.AddHash(uint64(i) << 8))
	}
	for i := 0; i < 100; i++ {
		require.NoError(w.AddHash(uint64(i) << 8)) // duplicates
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewShardedReader(filePath)
	require.NoError(err)
	defer r.Close()
	for i := 0; i < unique; i++ {
		require.True(r.ContainsHash(uint64(i) << 8))
	}
}

// Force-in-mem detaches the reader from mmap; queries must still work.
func TestSharded_ForceInMem(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_forceinmem.tmp")

	rng := rand.New(rand.NewPCG(42, 43))
	const keyCount = 10_000
	keys := make([]uint64, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = rng.Uint64()
	}
	w, err := NewShardedWriter(filePath)
	require.NoError(err)
	for _, k := range keys {
		require.NoError(w.AddHash(k))
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewShardedReader(filePath)
	require.NoError(err)
	defer r.Close()
	r.ForceInMem()
	for _, k := range keys {
		require.True(r.ContainsHash(k))
	}
}
