package v4

import (
	"context"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/erigontech/erigon/execution/commitment"
)

type feedItem struct {
	plain  string
	hashed []byte
	update *commitment.Update
}

type mapFeed struct {
	byKey map[string]*commitment.Update
}

func newMapFeed() *mapFeed { return &mapFeed{byKey: make(map[string]*commitment.Update)} }

func (m *mapFeed) touch(key string, u *commitment.Update) {
	if e, ok := m.byKey[key]; ok {
		*e = *u
		return
	}
	c := new(commitment.Update)
	*c = *u
	m.byKey[key] = c
}

func (m *mapFeed) items() []feedItem {
	items := make([]feedItem, 0, len(m.byKey))
	for k, u := range m.byKey {
		items = append(items, feedItem{plain: k, update: u})
	}
	return items
}

func sortItems(items []feedItem) {
	slices.SortFunc(items, func(a, b feedItem) int {
		return slices.Compare(a.hashed, b.hashed)
	})
}

func (m *mapFeed) hashSortSerial(fn func(hk, pk []byte, u *commitment.Update) error) error {
	items := m.items()
	for i := range items {
		items[i].hashed = commitment.KeyToHexNibbleHash([]byte(items[i].plain))
	}
	sortItems(items)
	for i := range items {
		if err := fn(items[i].hashed, []byte(items[i].plain), items[i].update); err != nil {
			return err
		}
	}
	return nil
}

func (m *mapFeed) hashSortParallel(fn func(hk, pk []byte, u *commitment.Update) error) error {
	items := m.items()
	workers := runtime.NumCPU()
	var wg sync.WaitGroup
	chunk := (len(items) + workers - 1) / workers
	for w := range workers {
		lo := w * chunk
		hi := min(lo+chunk, len(items))
		if lo >= hi {
			continue
		}
		wg.Go(func() {
			for i := lo; i < hi; i++ {
				items[i].hashed = commitment.KeyToHexNibbleHash([]byte(items[i].plain))
			}
		})
	}
	wg.Wait()
	sortItems(items)
	for i := range items {
		if err := fn(items[i].hashed, []byte(items[i].plain), items[i].update); err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkZZFeed(b *testing.B) {
	entries := benchEntries("storage", 100000)
	noop := func(hk, pk []byte, u *commitment.Update) error { return nil }

	b.Run("btree/touch+hashsort", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			u := commitment.NewUpdates(commitment.ModeUpdate, b.TempDir(), commitment.KeyToHexNibbleHash)
			for _, e := range entries {
				u.TouchPlainKeyDirect(string(e.key), e.update)
			}
			if err := u.HashSort(context.Background(), nil, noop); err != nil {
				b.Fatal(err)
			}
			u.Close()
		}
	})
	b.Run("map/touch+hashsort", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			m := newMapFeed()
			for _, e := range entries {
				m.touch(string(e.key), e.update)
			}
			if err := m.hashSortSerial(noop); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("map/touch+hashsort_parallel", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			m := newMapFeed()
			for _, e := range entries {
				m.touch(string(e.key), e.update)
			}
			if err := m.hashSortParallel(noop); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestSortFeedMatchesSerialSort(t *testing.T) {
	items := make([]feedEntry, 0, 3*hashParallelMin)
	for i := range 3 * hashParallelMin {
		key := string(benchAddr(i % 997))
		if i%3 != 0 {
			key += string(benchSlot(i))
		}
		items = append(items, feedEntry{plainKey: key})
	}
	hashFeed(items, 1)
	want := slices.Clone(items)
	slices.SortFunc(want, compareFeed)
	got := sortFeed(slices.Clone(items), 8)
	if len(got) != len(want) {
		t.Fatalf("sortFeed returned %d items, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].plainKey != want[i].plainKey {
			t.Fatalf("item %d: got %x, want %x", i, got[i].plainKey, want[i].plainKey)
		}
	}
}
