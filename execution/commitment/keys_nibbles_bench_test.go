package commitment

import (
	"testing"
)

func BenchmarkKeyToHexNibbleHash(b *testing.B) {
	key := make([]byte, 20)
	for i := range key {
		key[i] = byte(i)
	}
	for b.Loop() {
		KeyToHexNibbleHash(key)
	}
}

func Benchmark_KeyNibbleHash_NoCache(b *testing.B) {
	for _, w := range benchWorkloads {
		keys := benchKeys(w.numAddr, w.slots)
		b.Run(w.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, k := range keys {
					_ = KeyToHexNibbleHash(k)
				}
			}
		})
	}
}

func Benchmark_KeyNibbleHash_Cached(b *testing.B) {
	for _, w := range benchWorkloads {
		keys := benchKeys(w.numAddr, w.slots)
		b.Run(w.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var c addrHashCache
				for _, k := range keys {
					_ = keyToHexNibbleHashCached(k, &c)
				}
			}
		})
	}
}

func benchKeys(numAddr, slotsPer int) [][]byte {
	keys := make([][]byte, 0, numAddr*slotsPer)
	for a := range numAddr {
		for s := range slotsPer {
			k := make([]byte, 52)
			k[0] = byte(a)
			k[1] = byte(a >> 8)
			k[19] = byte(a * 7)
			k[20] = byte(s >> 8)
			k[51] = byte(s)
			keys = append(keys, k)
		}
	}
	return keys
}

var benchWorkloads = []struct {
	name    string
	numAddr int
	slots   int
}{
	{"whale_1x1000", 1, 1000},
	{"spread5_5x200", 5, 200},
	{"spread100_100x10", 100, 10},
	{"scatter1000_1000x1", 1000, 1},
}
