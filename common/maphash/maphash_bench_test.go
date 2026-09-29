package maphash

import (
	"testing"
)

func BenchmarkMapSet(b *testing.B) {
	SetSeed(42)
	m := NewMap[int]()
	key := []byte("benchmark-key")

	for b.Loop() {
		m.Set(key, 123)
	}
}

func BenchmarkMapGet(b *testing.B) {
	SetSeed(42)
	m := NewMap[int]()
	key := []byte("benchmark-key")
	m.Set(key, 123)

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

func BenchmarkLRUSet(b *testing.B) {
	SetSeed(42)
	l, _ := NewLRU[int](10000)
	key := []byte("benchmark-key")

	for b.Loop() {
		l.Set(key, 123)
	}
}

func BenchmarkLRUGet(b *testing.B) {
	SetSeed(42)
	l, _ := NewLRU[int](10000)
	key := []byte("benchmark-key")
	l.Set(key, 123)

	for b.Loop() {
		l.Get(key)
	}
}

func BenchmarkMaphashMapSet(b *testing.B) {
	SetSeed(42)
	m := NewMap[int]()
	key := []byte("benchmark-key-that-is-48-bytes-long-like-pubkey!")

	for b.Loop() {
		m.Set(key, 123)
	}
}

func BenchmarkStringMapSet(b *testing.B) {
	m := NewStringMap[int]()
	key := []byte("benchmark-key-that-is-48-bytes-long-like-pubkey!")

	for b.Loop() {
		m.Set(key, 123)
	}
}

func BenchmarkMaphashMapGet(b *testing.B) {
	SetSeed(42)
	m := NewMap[int]()
	key := []byte("benchmark-key-that-is-48-bytes-long-like-pubkey!")
	m.Set(key, 123)

	for b.Loop() {
		m.Get(key)
	}
}

func BenchmarkStringMapGet(b *testing.B) {
	m := NewStringMap[int]()
	key := []byte("benchmark-key-that-is-48-bytes-long-like-pubkey!")
	m.Set(key, 123)

	for b.Loop() {
		m.Get(key)
	}
}

func BenchmarkMaphashMapSetManyKeys(b *testing.B) {
	SetSeed(42)

	// Pre-generate 10000 keys
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = make([]byte, 48)
		keys[i][0] = byte(i >> 24)
		keys[i][1] = byte(i >> 16)
		keys[i][2] = byte(i >> 8)
		keys[i][3] = byte(i)
	}

	i := 0
	for b.Loop() {
		m := NewMap[int]()
		for _, key := range keys {
			m.Set(key, i)
			i++
		}
	}
}

func BenchmarkStringMapSetManyKeys(b *testing.B) {
	// Pre-generate 10000 keys
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = make([]byte, 48)
		keys[i][0] = byte(i >> 24)
		keys[i][1] = byte(i >> 16)
		keys[i][2] = byte(i >> 8)
		keys[i][3] = byte(i)
	}

	i := 0
	for b.Loop() {
		m := NewStringMap[int]()
		for _, key := range keys {
			m.Set(key, i)
			i++
		}
	}
}

func BenchmarkUniqueHandleMapSetManyKeys(b *testing.B) {
	// Pre-generate 10000 keys
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = make([]byte, 48)
		keys[i][0] = byte(i >> 24)
		keys[i][1] = byte(i >> 16)
		keys[i][2] = byte(i >> 8)
		keys[i][3] = byte(i)
	}

	i := 0
	for b.Loop() {
		m := NewUniqueHandleMap[int]()
		for _, key := range keys {
			m.Set(key, i)
			i++
		}
	}
}

func BenchmarkMaphashMapGetManyKeys(b *testing.B) {
	SetSeed(42)
	m := NewMap[int]()

	// Pre-populate with 10000 keys
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = make([]byte, 48)
		keys[i][0] = byte(i >> 24)
		keys[i][1] = byte(i >> 16)
		keys[i][2] = byte(i >> 8)
		keys[i][3] = byte(i)
		m.Set(keys[i], i)
	}

	i := 0
	for b.Loop() {
		m.Get(keys[i%len(keys)])
		i++
	}
}

func BenchmarkStringMapGetManyKeys(b *testing.B) {
	m := NewStringMap[int]()

	// Pre-populate with 10000 keys
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = make([]byte, 48)
		keys[i][0] = byte(i >> 24)
		keys[i][1] = byte(i >> 16)
		keys[i][2] = byte(i >> 8)
		keys[i][3] = byte(i)
		m.Set(keys[i], i)
	}

	i := 0
	for b.Loop() {
		m.Get(keys[i%len(keys)])
		i++
	}
}

func BenchmarkMaphashMapConcurrent(b *testing.B) {
	SetSeed(42)
	m := NewMap[int]()
	key := []byte("benchmark-key-that-is-48-bytes-long-like-pubkey!")
	m.Set(key, 123)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Get(key)
		}
	})
}

func BenchmarkStringMapConcurrent(b *testing.B) {
	m := NewStringMap[int]()
	key := []byte("benchmark-key-that-is-48-bytes-long-like-pubkey!")
	m.Set(key, 123)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Get(key)
		}
	})
}

func BenchmarkUniqueHandleMapSet(b *testing.B) {
	m := NewUniqueHandleMap[int]()
	key := []byte("benchmark-key-that-is-48-bytes-long-like-pubkey!")

	for b.Loop() {
		m.Set(key, 123)
	}
}

func BenchmarkUniqueHandleMapGet(b *testing.B) {
	m := NewUniqueHandleMap[int]()
	key := []byte("benchmark-key-that-is-48-bytes-long-like-pubkey!")
	m.Set(key, 123)

	for b.Loop() {
		m.Get(key)
	}
}

func BenchmarkUniqueHandleMapGetManyKeys(b *testing.B) {
	m := NewUniqueHandleMap[int]()

	// Pre-populate with 10000 keys
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = make([]byte, 48)
		keys[i][0] = byte(i >> 24)
		keys[i][1] = byte(i >> 16)
		keys[i][2] = byte(i >> 8)
		keys[i][3] = byte(i)
		m.Set(keys[i], i)
	}

	i := 0
	for b.Loop() {
		m.Get(keys[i%len(keys)])
		i++
	}
}

func BenchmarkUniqueHandleMapConcurrent(b *testing.B) {
	m := NewUniqueHandleMap[int]()
	key := []byte("benchmark-key-that-is-48-bytes-long-like-pubkey!")
	m.Set(key, 123)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Get(key)
		}
	})
}
