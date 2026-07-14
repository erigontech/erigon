package commitment

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type sortedPair struct {
	hk []byte
	pk []byte
}

func collectHashSortPairs(t *testing.T, ut *Updates) []sortedPair {
	t.Helper()
	var got []sortedPair
	err := ut.HashSort(context.Background(), nil, func(hk, pk []byte, upd *Update) error {
		require.Nil(t, upd)
		got = append(got, sortedPair{hk: slices.Clone(hk), pk: slices.Clone(pk)})
		return nil
	})
	require.NoError(t, err)
	return got
}

// forEachDirectPath runs fn once on the in-memory path and once with collection
// forced through the etl collector, so both ModeDirect backends stay pinned.
func forEachDirectPath(t *testing.T, fn func(t *testing.T, newUpdates func() *Updates)) {
	t.Helper()
	t.Run("in-memory", func(t *testing.T) {
		fn(t, func() *Updates { return NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash) })
	})
	t.Run("etl", func(t *testing.T) {
		fn(t, func() *Updates {
			ut := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
			forceDirectSpill(ut)
			return ut
		})
	})
}

// TestHashSortModeDirect_DuplicateHashedKey pins that a TouchHashedKey path equal to
// hashKey(plainKey) of a touched plain key yields TWO deliveries under the same hashed
// key, ordered by touch order (the witness flows depend on seeing the plainKey-bearing
// one; see eth_call getWitness / debug_executionWitness sibling paths).
func TestHashSortModeDirect_DuplicateHashedKey(t *testing.T) {
	t.Parallel()

	addr := []byte("\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14")
	hashed := KeyToHexNibbleHash(addr)

	forEachDirectPath(t, func(t *testing.T, newUpdates func() *Updates) {
		t.Run("plain first", func(t *testing.T) {
			ut := newUpdates()
			ut.TouchPlainKey(string(addr), []byte("v"), ut.TouchStorage)
			ut.TouchHashedKey(hashed)
			require.EqualValues(t, 2, ut.Size())

			got := collectHashSortPairs(t, ut)
			require.Len(t, got, 2)
			require.Equal(t, hashed, got[0].hk)
			require.Equal(t, hashed, got[1].hk)
			require.Equal(t, addr, got[0].pk)
			require.Empty(t, got[1].pk)
		})

		t.Run("hashed first", func(t *testing.T) {
			ut := newUpdates()
			ut.TouchHashedKey(hashed)
			ut.TouchPlainKey(string(addr), []byte("v"), ut.TouchStorage)

			got := collectHashSortPairs(t, ut)
			require.Len(t, got, 2)
			require.Empty(t, got[0].pk)
			require.Equal(t, addr, got[1].pk)
		})
	})
}

// TestHashSortModeDirect_MixedLengthHashedKeys pins bytewise ordering across
// variable-length TouchHashedKey paths (1..128 nibbles, witness sibling paths)
// interleaved with full-length hashed plain keys: a prefix sorts before its extensions.
func TestHashSortModeDirect_MixedLengthHashedKeys(t *testing.T) {
	t.Parallel()

	forEachDirectPath(t, func(t *testing.T, newUpdates func() *Updates) {
		ut := newUpdates()

		addrs := make([][]byte, 8)
		for i := range addrs {
			a := make([]byte, 20)
			a[0] = byte(i * 0x11)
			a[19] = byte(i)
			addrs[i] = a
			ut.TouchPlainKey(string(a), []byte("v"), ut.TouchStorage)
		}
		prefixes := [][]byte{
			KeyToHexNibbleHash(addrs[2])[:1],
			KeyToHexNibbleHash(addrs[2])[:7],
			KeyToHexNibbleHash(addrs[5])[:3],
		}
		for _, p := range prefixes {
			ut.TouchHashedKey(p)
		}

		got := collectHashSortPairs(t, ut)
		require.Len(t, got, len(addrs)+len(prefixes))
		require.True(t, slices.IsSortedFunc(got, func(a, b sortedPair) int {
			return bytes.Compare(a.hk, b.hk)
		}), "delivery must be in ascending hashedKey byte order")
		for _, p := range prefixes {
			idx := slices.IndexFunc(got, func(s sortedPair) bool { return bytes.Equal(s.hk, p) })
			require.GreaterOrEqual(t, idx, 0)
			require.Empty(t, got[idx].pk)
			if idx+1 < len(got) {
				require.True(t, bytes.HasPrefix(got[idx+1].hk, p) || bytes.Compare(got[idx+1].hk, p) > 0)
			}
		}
	})
}

// TestHashSortModeDirect_MultiBatchOrder pins sorted delivery and exact key count
// across multiple 10k batches with random keys, including duplicated hashedKeys.
func TestHashSortModeDirect_MultiBatchOrder(t *testing.T) {
	t.Parallel()

	forEachDirectPath(t, func(t *testing.T, newUpdates func() *Updates) {
		const numKeys = 25_000
		rnd := rand.New(rand.NewSource(42))
		ut := newUpdates()

		addrs := make([][]byte, numKeys)
		for i := range addrs {
			a := make([]byte, 20)
			binary.BigEndian.PutUint64(a, rnd.Uint64())
			binary.BigEndian.PutUint64(a[8:], uint64(i))
			addrs[i] = a
			ut.TouchPlainKey(string(a), []byte("v"), ut.TouchStorage)
		}
		// Duplicate a handful of hashedKeys via the hashed-touch path.
		for i := 0; i < 5; i++ {
			ut.TouchHashedKey(KeyToHexNibbleHash(addrs[i*1000]))
		}

		got := collectHashSortPairs(t, ut)
		require.Len(t, got, numKeys+5)
		require.True(t, slices.IsSortedFunc(got, func(a, b sortedPair) int {
			return bytes.Compare(a.hk, b.hk)
		}))
		require.EqualValues(t, 0, ut.Size(), "HashSort consumes the batch")

		// Duplicated hashedKeys: plain-touched delivery precedes hashed-touched (touch order).
		for i := 0; i < 5; i++ {
			h := KeyToHexNibbleHash(addrs[i*1000])
			idx := slices.IndexFunc(got, func(s sortedPair) bool { return bytes.Equal(s.hk, h) })
			require.GreaterOrEqual(t, idx, 0)
			require.Equal(t, addrs[i*1000], got[idx].pk)
			require.Empty(t, got[idx+1].pk)
			require.Equal(t, h, got[idx+1].hk)
		}
	})
}

// touchRandomCorpus applies an identical touch sequence (plain keys, storage keys,
// and hashed-only sibling paths, with hashed duplicates of some plain keys) to ut.
func touchRandomCorpus(ut *Updates, numKeys int) {
	rnd := rand.New(rand.NewSource(7))
	for i := 0; i < numKeys; i++ {
		a := make([]byte, 20)
		binary.BigEndian.PutUint64(a, rnd.Uint64())
		binary.BigEndian.PutUint64(a[8:], uint64(i))
		switch i % 3 {
		case 0:
			ut.TouchPlainKey(string(a), []byte("v"), ut.TouchStorage)
		case 1:
			sk := append(slices.Clone(a), make([]byte, 32)...)
			binary.BigEndian.PutUint64(sk[20:], rnd.Uint64())
			ut.TouchPlainKey(string(sk), []byte("v"), ut.TouchStorage)
		case 2:
			ut.TouchPlainKey(string(a), []byte("v"), ut.TouchStorage)
			h := KeyToHexNibbleHash(a)
			if i%9 == 2 {
				ut.TouchHashedKey(h) // duplicate of the plain touch
			} else {
				ut.TouchHashedKey(h[:1+i%63]) // sibling-path prefix
			}
		}
	}
}

// TestHashSortModeDirect_PathParity drives the identical touch sequence through the
// in-memory and etl backends and requires byte-identical delivery sequences.
func TestHashSortModeDirect_PathParity(t *testing.T) {
	t.Parallel()

	const numKeys = 15_000
	inMem := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	spilled := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	forceDirectSpill(spilled)

	touchRandomCorpus(inMem, numKeys)
	touchRandomCorpus(spilled, numKeys)
	require.Equal(t, spilled.Size(), inMem.Size())

	gotInMem := collectHashSortPairs(t, inMem)
	gotSpilled := collectHashSortPairs(t, spilled)
	require.Equal(t, len(gotSpilled), len(gotInMem))
	for i := range gotSpilled {
		require.Equal(t, gotSpilled[i].hk, gotInMem[i].hk, "hashedKey diverges at %d", i)
		require.Equal(t, gotSpilled[i].pk, gotInMem[i].pk, "plainKey diverges at %d", i)
	}
}

// TestHashSortInMem_SpillMidCollection crosses a tiny directMemLimit mid-collection so
// early touches are replayed into the collector and later ones collect there directly;
// delivery must match the pure in-memory backend.
func TestHashSortInMem_SpillMidCollection(t *testing.T) {
	t.Parallel()

	const numKeys = 3_000
	pure := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	crossing := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	crossing.directMemLimit = 64 << 10 // crossed after a few hundred keys

	touchRandomCorpus(pure, numKeys)
	touchRandomCorpus(crossing, numKeys)
	require.Nil(t, pure.etl)
	require.NotNil(t, crossing.etl, "limit crossing must have spilled to the collector")
	require.Empty(t, crossing.direct)

	gotPure := collectHashSortPairs(t, pure)
	gotCrossing := collectHashSortPairs(t, crossing)
	require.Equal(t, gotPure, gotCrossing)
}

// TestHashSortModeDirect_FnError pins that an fn error aborts HashSort on both backends
// and that Reset recovers the Updates for a subsequent touch+HashSort cycle.
func TestHashSortModeDirect_FnError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("fn failed")
	forEachDirectPath(t, func(t *testing.T, newUpdates func() *Updates) {
		ut := newUpdates()
		touchRandomCorpus(ut, 100)

		calls := 0
		err := ut.HashSort(context.Background(), nil, func(hk, pk []byte, _ *Update) error {
			calls++
			if calls == 10 {
				return sentinel
			}
			return nil
		})
		require.ErrorIs(t, err, sentinel)
		require.Equal(t, 10, calls)

		ut.Reset()
		ut.TouchPlainKey(string(make([]byte, 20)), []byte("v"), ut.TouchStorage)
		got := collectHashSortPairs(t, ut)
		require.Len(t, got, 1)
	})
}

// TestHashSortInMem_CtxCancel pins per-key context checks on the in-memory drain loop.
func TestHashSortInMem_CtxCancel(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	touchRandomCorpus(ut, 100)

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := ut.HashSort(ctx, nil, func(hk, pk []byte, _ *Update) error {
		calls++
		if calls == 5 {
			cancel()
		}
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 5, calls)
}

// TestHashSortInMem_WarmupNoRace runs the in-memory path against live warmup workers;
// entry memory is stable so no reuse race must exist. -race is the signal.
func TestHashSortInMem_WarmupNoRace(t *testing.T) {
	t.Parallel()

	const numKeys = 30_000
	ut := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)
	for _, k := range genNibbleKeys(numKeys, 64) {
		ut.TouchPlainKey(string(k), []byte("v"), ut.TouchStorage)
	}
	require.Nil(t, ut.etl)

	ctx := context.Background()
	warmuper := testWarmuper(ctx, slowCtxFactory(2*time.Millisecond), 4)
	warmuper.Start()

	visited := 0
	err := ut.HashSort(ctx, warmuper, func(hk, pk []byte, _ *Update) error {
		visited++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, numKeys, visited)
	require.NoError(t, warmuper.Wait())
}

// TestHashSortModeDirect_RetouchBetweenSorts pins the eth_getProof pattern: HashSort,
// touch more keys on the same Updates, HashSort again — the second call must deliver
// exactly the new touches.
func TestHashSortModeDirect_RetouchBetweenSorts(t *testing.T) {
	t.Parallel()

	forEachDirectPath(t, func(t *testing.T, newUpdates func() *Updates) {
		ut := newUpdates()
		touchRandomCorpus(ut, 90)
		first := collectHashSortPairs(t, ut)
		require.NotEmpty(t, first)
		require.EqualValues(t, 0, ut.Size())

		addr := bytes.Repeat([]byte{0xab}, 20)
		ut.TouchPlainKey(string(addr), []byte("v"), ut.TouchStorage)
		second := collectHashSortPairs(t, ut)
		require.Len(t, second, 1)
		require.Equal(t, addr, second[0].pk)

		third := collectHashSortPairs(t, ut)
		require.Empty(t, third)
	})
}
