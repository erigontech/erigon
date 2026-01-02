package maphash

import (
	"sync"
	"testing"
)

func TestSetSeed(t *testing.T) {
	SetSeed(12345)
	key := []byte("test")
	h1 := Hash(key)

	// Same seed should produce same hash
	SetSeed(12345)
	h2 := Hash(key)
	if h1 != h2 {
		t.Errorf("same seed should produce same hash: got %d and %d", h1, h2)
	}

	// Different seed should produce different hash
	SetSeed(54321)
	h3 := Hash(key)
	if h1 == h3 {
		t.Errorf("different seed should produce different hash: both got %d", h1)
	}
}

func TestMapBasicOperations(t *testing.T) {
	SetSeed(42)
	m := NewMap[int]()

	// Test Set and Get
	m.Set([]byte("key1"), 100)
	m.Set([]byte("key2"), 200)

	v, ok := m.Get([]byte("key1"))
	if !ok || v != 100 {
		t.Errorf("expected (100, true), got (%d, %v)", v, ok)
	}

	v, ok = m.Get([]byte("key2"))
	if !ok || v != 200 {
		t.Errorf("expected (200, true), got (%d, %v)", v, ok)
	}

	// Test non-existent key
	v, ok = m.Get([]byte("nonexistent"))
	if ok || v != 0 {
		t.Errorf("expected (0, false), got (%d, %v)", v, ok)
	}

	// Test Len
	if m.Len() != 2 {
		t.Errorf("expected len 2, got %d", m.Len())
	}

	// Test Delete
	m.Delete([]byte("key1"))
	v, ok = m.Get([]byte("key1"))
	if ok {
		t.Errorf("expected key1 to be deleted, got (%d, %v)", v, ok)
	}

	if m.Len() != 1 {
		t.Errorf("expected len 1 after delete, got %d", m.Len())
	}
}

func TestMapOverwrite(t *testing.T) {
	SetSeed(42)
	m := NewMap[string]()

	m.Set([]byte("key"), "first")
	m.Set([]byte("key"), "second")

	v, ok := m.Get([]byte("key"))
	if !ok || v != "second" {
		t.Errorf("expected (second, true), got (%s, %v)", v, ok)
	}

	if m.Len() != 1 {
		t.Errorf("expected len 1, got %d", m.Len())
	}
}

func TestMapEmptyKey(t *testing.T) {
	SetSeed(42)
	m := NewMap[int]()

	m.Set([]byte{}, 999)
	v, ok := m.Get([]byte{})
	if !ok || v != 999 {
		t.Errorf("expected (999, true), got (%d, %v)", v, ok)
	}
}

func TestMapConcurrentAccess(t *testing.T) {
	SetSeed(42)
	m := NewMap[int]()

	var wg sync.WaitGroup
	n := 100

	// Concurrent writes
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte{byte(i)}
			m.Set(key, i)
		}(i)
	}
	wg.Wait()

	// Concurrent reads
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte{byte(i)}
			m.Get(key)
		}(i)
	}
	wg.Wait()

	// Concurrent mixed operations
	for i := 0; i < n; i++ {
		wg.Add(3)
		go func(i int) {
			defer wg.Done()
			key := []byte{byte(i)}
			m.Set(key, i*2)
		}(i)
		go func(i int) {
			defer wg.Done()
			key := []byte{byte(i)}
			m.Get(key)
		}(i)
		go func(i int) {
			defer wg.Done()
			m.Len()
		}(i)
	}
	wg.Wait()
}

func TestMapWithStructValue(t *testing.T) {
	SetSeed(42)
	type Person struct {
		Name string
		Age  int
	}

	m := NewMap[Person]()
	m.Set([]byte("alice"), Person{Name: "Alice", Age: 30})
	m.Set([]byte("bob"), Person{Name: "Bob", Age: 25})

	v, ok := m.Get([]byte("alice"))
	if !ok || v.Name != "Alice" || v.Age != 30 {
		t.Errorf("expected Alice/30, got %+v", v)
	}

	v, ok = m.Get([]byte("bob"))
	if !ok || v.Name != "Bob" || v.Age != 25 {
		t.Errorf("expected Bob/25, got %+v", v)
	}
}

func TestMapWithPointerValue(t *testing.T) {
	SetSeed(42)
	m := NewMap[*int]()

	val := 42
	m.Set([]byte("ptr"), &val)

	v, ok := m.Get([]byte("ptr"))
	if !ok || v == nil || *v != 42 {
		t.Errorf("expected pointer to 42, got %v", v)
	}

	// Modify through pointer
	*v = 100
	v2, _ := m.Get([]byte("ptr"))
	if *v2 != 100 {
		t.Errorf("expected 100 after modification, got %d", *v2)
	}
}

func TestDeleteNonExistent(t *testing.T) {
	SetSeed(42)
	m := NewMap[int]()

	// Should not panic
	m.Delete([]byte("nonexistent"))

	if m.Len() != 0 {
		t.Errorf("expected len 0, got %d", m.Len())
	}
}

func TestHashDeterminism(t *testing.T) {
	// Test that the same seed + key always produces the same hash
	// across multiple invocations
	testCases := []struct {
		seed uint64
		key  []byte
	}{
		{1, []byte("hello")},
		{2, []byte("world")},
		{12345, []byte("test key")},
		{0xDEADBEEF, []byte{0x00, 0x01, 0x02, 0x03}},
		{0xFFFFFFFFFFFFFFFF, []byte("")},
		{42, []byte("the quick brown fox jumps over the lazy dog")},
	}

	for _, tc := range testCases {
		SetSeed(tc.seed)

		// Hash the same key multiple times
		hashes := make([]uint64, 100)
		for i := range hashes {
			hashes[i] = Hash(tc.key)
		}

		// All hashes must be identical
		for i, h := range hashes {
			if h != hashes[0] {
				t.Errorf("seed=%d key=%q: Hash[%d]=%d != Hash[0]=%d",
					tc.seed, tc.key, i, h, hashes[0])
			}
		}

		// Reset seed and hash again - should still match
		SetSeed(tc.seed)
		h := Hash(tc.key)
		if h != hashes[0] {
			t.Errorf("seed=%d key=%q: Hash after reset=%d != original=%d",
				tc.seed, tc.key, h, hashes[0])
		}
	}
}

func TestMapDeterminism(t *testing.T) {
	// Test that map operations are deterministic given the same seed
	seed := uint64(999)

	// Run the same sequence of operations multiple times
	for run := 0; run < 10; run++ {
		SetSeed(seed)
		m := NewMap[int]()

		keys := [][]byte{
			[]byte("alpha"),
			[]byte("beta"),
			[]byte("gamma"),
			[]byte("delta"),
		}

		// Insert in order
		for i, k := range keys {
			m.Set(k, i*100)
		}

		// Verify all values are retrievable
		for i, k := range keys {
			v, ok := m.Get(k)
			if !ok {
				t.Errorf("run %d: key %q not found", run, k)
			}
			if v != i*100 {
				t.Errorf("run %d: key %q expected %d, got %d", run, k, i*100, v)
			}
		}

		// Delete and re-add
		m.Delete(keys[1])
		m.Set(keys[1], 9999)

		v, _ := m.Get(keys[1])
		if v != 9999 {
			t.Errorf("run %d: after re-add expected 9999, got %d", run, v)
		}
	}
}

func BenchmarkMapSet(b *testing.B) {
	SetSeed(42)
	m := NewMap[int]()
	key := []byte("benchmark-key")

	b.ResetTimer()
	for b.Loop() {
		m.Set(key, 123)
	}
}

func BenchmarkMapGet(b *testing.B) {
	SetSeed(42)
	m := NewMap[int]()
	key := []byte("benchmark-key")
	m.Set(key, 123)

	b.ResetTimer()
	for b.Loop() {
		m.Get(key)
	}
}

func BenchmarkMapConcurrentReadWrite(b *testing.B) {
	SetSeed(42)
	m := NewMap[int]()
	key := []byte("benchmark-key")
	m.Set(key, 123)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Get(key)
			m.Set(key, 456)
		}
	})
}
