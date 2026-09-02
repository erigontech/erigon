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
		for i := range 5 {
			ut.TouchHashedKey(KeyToHexNibbleHash(addrs[i*1000]))
		}

		got := collectHashSortPairs(t, ut)
		require.Len(t, got, numKeys+5)
		require.True(t, slices.IsSortedFunc(got, func(a, b sortedPair) int {
			return bytes.Compare(a.hk, b.hk)
		}))
		require.EqualValues(t, 0, ut.Size(), "HashSort consumes the batch")

		for i := range 5 {
			h := KeyToHexNibbleHash(addrs[i*1000])
			idx := slices.IndexFunc(got, func(s sortedPair) bool { return bytes.Equal(s.hk, h) })
			require.GreaterOrEqual(t, idx, 0)
			require.Equal(t, addrs[i*1000], got[idx].pk)
			require.Empty(t, got[idx+1].pk)
			require.Equal(t, h, got[idx+1].hk)
		}
	})
}

func touchRandomCorpus(ut *Updates, numKeys int) {
	rnd := rand.New(rand.NewSource(7))
	for i := range numKeys {
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

func TestHashSortInMem_SpillMidCollection(t *testing.T) {
	t.Parallel()

	const numKeys = 3_000
	pure := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	crossing := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	crossing.directMemLimit = 64 << 10

	touchRandomCorpus(pure, numKeys)
	touchRandomCorpus(crossing, numKeys)
	require.Nil(t, pure.etl)
	require.NotNil(t, crossing.etl, "limit crossing must have spilled to the collector")
	require.Empty(t, crossing.direct)

	gotPure := collectHashSortPairs(t, pure)
	gotCrossing := collectHashSortPairs(t, crossing)
	require.Equal(t, gotPure, gotCrossing)
}

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
