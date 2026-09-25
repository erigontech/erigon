package v3

import (
	"bytes"
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

func TestPartitionFeedMatchesSerialPartition(t *testing.T) {
	var items []feedEntry
	for i := range hashParallelMin {
		addr := string(benchAddr(i))
		update := &commitment.Update{Flags: commitment.BalanceUpdate}
		if i%7 == 0 {
			update = &commitment.Update{Flags: commitment.DeleteUpdate}
		}
		items = append(items, feedEntry{plainKey: addr, update: update})
		for j := range i % 4 {
			items = append(items, feedEntry{plainKey: addr + string(benchSlot(i*8+j)), update: &commitment.Update{Flags: commitment.StorageUpdate}})
		}
	}
	hashFeed(items, 1)

	sorted := slices.Clone(items)
	slices.SortFunc(sorted, compareFeed)
	serial := &partitioner{}
	for _, e := range sorted {
		if err := serial.add(e.hashedKey, e.update); err != nil {
			t.Fatal(err)
		}
	}
	wantStorage, wantAccounts := serial.done()

	gotStorage, gotAccounts, seen, err := partitionFeed(slices.Clone(items), 8, nil)
	if err != nil {
		t.Fatal(err)
	}
	if seen != len(items) {
		t.Fatalf("seen %d, want %d", seen, len(items))
	}
	if len(gotAccounts) != len(wantAccounts) {
		t.Fatalf("accounts %d, want %d", len(gotAccounts), len(wantAccounts))
	}
	for i := range wantAccounts {
		if !bytes.Equal(gotAccounts[i].hashedKey, wantAccounts[i].hashedKey) || gotAccounts[i].storageDirty != wantAccounts[i].storageDirty || gotAccounts[i].update != wantAccounts[i].update {
			t.Fatalf("account %d differs", i)
		}
	}
	byAddr := func(tasks []storageTask) map[[32]byte]storageTask {
		out := make(map[[32]byte]storageTask, len(tasks))
		for _, task := range tasks {
			out[task.addrHash] = task
		}
		return out
	}
	want, got := byAddr(wantStorage), byAddr(gotStorage)
	if len(got) != len(want) || len(gotStorage) != len(wantStorage) {
		t.Fatalf("storage tasks %d, want %d", len(gotStorage), len(wantStorage))
	}
	for addr, w := range want {
		g := got[addr]
		if g.wipe != w.wipe || len(g.entries) != len(w.entries) {
			t.Fatalf("storage task %x differs", addr)
		}
		for i := range w.entries {
			if !bytes.Equal(g.entries[i].path, w.entries[i].path) {
				t.Fatalf("storage task %x entry %d differs", addr, i)
			}
		}
	}
}
