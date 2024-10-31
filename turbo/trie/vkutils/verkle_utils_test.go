package vkutils

import (
	"bytes"
	"crypto/sha256"
	"math/big"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
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
	h[31] = CodeHashLeafKey
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


func TestCompareGetTreeKeyWithEvaluated(t *testing.T) {
	var addr [32]byte
	rand.Read(addr[:])
	addrpoint := EvaluateAddressPoint(addr[:])
	for i := 0; i < 100; i++ {
		var val [32]byte
		rand.Read(val[:])
		n := uint256.NewInt(0).SetBytes(val[:])
		n.Lsh(n, 8)
		subindex := byte(val[0])
		tk1 := GetTreeKey(addr[:], n, subindex)
		tk2 := GetTreeKeyWithEvaluatedAddess(addrpoint, n, subindex)

		if !bytes.Equal(tk1, tk2) {
			t.Fatalf("differing key: slot=%x, addr=%x", val, addr)
		}
	}
}