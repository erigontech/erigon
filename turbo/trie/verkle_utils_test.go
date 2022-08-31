package trie

import (
	"crypto/sha256"
	"math/big"
	"math/rand"
	"testing"

	"github.com/gballet/go-verkle"
	"github.com/holiman/uint256"
)

func TestGetTreeKey(t *testing.T) {
	var addr [32]byte
	for i := 0; i < 16; i++ {
		addr[1+2*i] = 0xff
	}
	n := uint256.NewInt(1)
	n = n.Lsh(n, 129)
	n.Add(n, uint256.NewInt(3))
	GetTreeKey(addr[:], n, 1)
}

func TestConstantPoint(t *testing.T) {
	var expectedPoly [1]verkle.Fr

	cfg, _ := verkle.GetConfig()
	verkle.FromLEBytes(&expectedPoly[0], []byte{2, 64})
	expected := cfg.CommitToPoly(expectedPoly[:], 1)

	if !verkle.Equal(expected, getTreePolyIndex0Point) {
		t.Fatalf("Marshalled constant value is incorrect: %x != %x", expected.Bytes(), getTreePolyIndex0Point.Bytes())
	}
}

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
