package vtree

import (
	"crypto/sha256"
	"math/big"
	"math/rand"
	"testing"
)

func BenchmarkPedersenHash(b *testing.B) {
	var addr, v [32]byte

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rand.Read(v[:])
		rand.Read(addr[:])
		GetTreeKeyCodeSize(addr[:])
	}
}

func sha256GetTreeKeyCodeSize(addr []byte) []byte {
	digest := sha256.New()
	digest.Write(addr)
	treeIndexBytes := new(big.Int).Bytes()
	var payload [32]byte
	copy(payload[:len(treeIndexBytes)], treeIndexBytes)
	digest.Write(payload[:])
	h := digest.Sum(nil)
	h[31] = CodeKeccakLeafKey
	return h
}

func BenchmarkSha256Hash(b *testing.B) {
	var addr, v [32]byte

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rand.Read(v[:])
		rand.Read(addr[:])
		sha256GetTreeKeyCodeSize(addr[:])
	}
}
