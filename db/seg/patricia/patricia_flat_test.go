package patricia

import (
	"math/rand"
	"reflect"
	"testing"
)

// buildTestTree creates a patricia tree with realistic Ethereum-like patterns
func buildTestTree() *PatriciaTree {
	var pt PatriciaTree
	v := []byte{1}
	// Patterns from TestFindMatches5 - realistic Ethereum address/hash patterns
	pt.Insert(decodeHex("0434e37673a8e0aaa536828f0d5b0ddba12fece1"), v)
	pt.Insert(decodeHex("e28e72fcf78647adce1f1252f240bbfaebd63bcc"), v)
	pt.Insert(decodeHex("34e28e72fcf78647adce1f1252f240bbfaebd63b"), v)
	pt.Insert(decodeHex("0434e28e72fcf78647adce1f1252f240bbfaebd6"), v)
	pt.Insert(decodeHex("090bdc64a7e3632cde8f4689f47acfc0760e35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("00090bdc64a7e3632cde8f4689f47acfc0760e35bce43af50d4b1f5973463bde"), v)
	pt.Insert(decodeHex("0000000000"), v)
	pt.Insert(decodeHex("00000000000000000000"), v)
	pt.Insert(decodeHex("000000000000000000000000000000"), v)
	pt.Insert(decodeHex("0000000000000000000000000000"), v)
	pt.Insert(decodeHex("000000000000000000"), v)
	pt.Insert(decodeHex("0000000000000000"), v)
	pt.Insert(decodeHex("00000000000000000000000000"), v)
	pt.Insert(decodeHex("000000000000000000000000"), v)
	pt.Insert(decodeHex("f47acfc0760e35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("e3632cde8f4689f47acfc0760e35bce43af50d4b"), v)
	pt.Insert(decodeHex("de8f4689f47acfc0760e35bce43af50d4b1f5973"), v)
	pt.Insert(decodeHex("dc64a7e3632cde8f4689f47acfc0760e35bce43a"), v)
	pt.Insert(decodeHex("a7e3632cde8f4689f47acfc0760e35bce43af50d"), v)
	pt.Insert(decodeHex("8f4689f47acfc0760e35bce43af50d4b1f597346"), v)
	pt.Insert(decodeHex("89f47acfc0760e35bce43af50d4b1f5973463bde"), v)
	pt.Insert(decodeHex("64a7e3632cde8f4689f47acfc0760e35bce43af5"), v)
	pt.Insert(decodeHex("632cde8f4689f47acfc0760e35bce43af50d4b1f"), v)
	pt.Insert(decodeHex("4689f47acfc0760e35bce43af50d4b1f5973463b"), v)
	pt.Insert(decodeHex("2cde8f4689f47acfc0760e35bce43af50d4b1f59"), v)
	pt.Insert(decodeHex("0bdc64a7e3632cde8f4689f47acfc0760e35bce4"), v)
	pt.Insert(decodeHex("7acfc0760e35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("0000000000000000000000"), v)
	pt.Insert(decodeHex("cfc0760e35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("00000000000000000000000000000000"), v)
	pt.Insert(decodeHex("c0760e35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("0000000000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("00000000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("760e35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("000000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("0e35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("0000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("35bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("bce43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("e43af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("1090bdc64a7e3632cde8f4689f47acfc0760e35bce43af50d4b1f5973463bde6"), v)
	pt.Insert(decodeHex("3af50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("f50d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("0d4b1f5973463bde62"), v)
	pt.Insert(decodeHex("0000000000000000000000000000000000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("4b1f5973463bde62"), v)
	pt.Insert(decodeHex("0000000000000000000000000000000000000001"), v)
	pt.Insert(decodeHex("0760e35bce43af50d4b1f5973463bde6"), v)
	return &pt
}

func buildTestData() []byte {
	return decodeHex("9d7d9d7d082073e2920896915d0e0239a7e852d86b26e03a188bc5b947972aeec206d63b6744043493d38e72c5281e78f6b364eacac6fa907ecba1640000000000000000000000000000000000000000000000000000000007bfa482043493d38e72c5281e78f6b364eacac6fa907ecba1640000000000000000000000000000000000000000000000000000000000000011043493d38e72c5281e78f6b364eacac6fa907ecba1640000000000000000000000000000000000000000000000000000000000000002043493d38e72c5281e78f6b364eacac6fa907ecba164000000000000000000000000000000000000000000000000000000000000001e0820a516e4eeef0852f3c4ee0f11237e5e5127ed67a64e43a2f2ebef2d6bc26bb384082073404b8fb6bb42e5a0c9bb7d6253d9d72084bed3991df1efd25512e7f713e796043493d38e72c5281e78f6b364eacac6fa907ecba164000000000000000000000000000000000000000000000000000000000000001f043493d38e72c5281e78f6b364eacac6fa907ecba1640000000000000000000000000000000000000000000000000000000000000012082010db8a472df5096168436e756dbf37edce306a01f4fa7a889f7ad8195e1154a9043493d38e72c5281e78f6b364eacac6fa907ecba1640000000000000000000000000000000000000000000000000000000000000006")
}

// TestFlatTreeCorrectness verifies that FlatTree + MatchFinder3 produces
// the same results as PatriciaTree + MatchFinder2 for all existing test cases.
func TestFlatTreeCorrectness_FindMatches1(t *testing.T) {
	var pt PatriciaTree
	pt.Insert([]byte("wolf"), []byte{1})
	pt.Insert([]byte("winter"), []byte{2})
	pt.Insert([]byte("wolfs"), []byte{3})

	ft := pt.Flatten()
	data := []byte("Who lives here in winter, wolfs")

	mf2 := NewMatchFinder2(&pt)
	m2 := mf2.FindLongestMatches(data)
	mf3 := NewMatchFinder3(ft)
	m3 := mf3.FindLongestMatches(data)

	assertMatchesEqual(t, m2, m3)
}

func TestFlatTreeCorrectness_FindMatches2(t *testing.T) {
	var pt PatriciaTree
	pt.Insert([]byte("wolf"), []byte{1})
	pt.Insert([]byte("winter"), []byte{2})
	pt.Insert([]byte("wolfs?"), []byte{3})

	ft := pt.Flatten()
	data := []byte("Who lives here in winter, wolfs?")

	mf2 := NewMatchFinder2(&pt)
	m2 := mf2.FindLongestMatches(data)
	mf3 := NewMatchFinder3(ft)
	m3 := mf3.FindLongestMatches(data)

	assertMatchesEqual(t, m2, m3)
}

func TestFlatTreeCorrectness_FindMatches3(t *testing.T) {
	var pt PatriciaTree
	v := []byte{1}
	pt.Insert(decodeHex("00000000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("000000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("0000000000000000000000000000000000"), v)
	pt.Insert(decodeHex("00000000000000000000000000000000"), v)
	pt.Insert(decodeHex("000000000000000000000000000000"), v)
	pt.Insert(decodeHex("0000000000000000000000000000"), v)
	pt.Insert(decodeHex("0100000000000000000000003b30000001000003"), v)
	pt.Insert(decodeHex("0000000000000000003b30000001000003000100"), v)
	pt.Insert(decodeHex("000000000000000000003b300000010000030001"), v)
	pt.Insert(decodeHex("00000000000000000000003b3000000100000300"), v)
	pt.Insert(decodeHex("00000000000000000000000000"), v)
	pt.Insert(decodeHex("00000000000000003b30000001000003000100"), v)
	pt.Insert(decodeHex("000000000000000000000000"), v)
	pt.Insert(decodeHex("000000000000003b30000001000003000100"), v)
	pt.Insert(decodeHex("0000000000003b30000001000003000100"), v)
	pt.Insert(decodeHex("00000000003b30000001000003000100"), v)
	pt.Insert(decodeHex("000000003b30000001000003000100"), v)
	pt.Insert(decodeHex("0000003b30000001000003000100"), v)
	pt.Insert(decodeHex("00003b30000001000003000100"), v)
	pt.Insert(decodeHex("0100000000000000"), v)
	pt.Insert(decodeHex("003b30000001000003000100"), v)
	pt.Insert(decodeHex("3b30000001000003000100"), v)
	pt.Insert(decodeHex("00000000000000003b3000000100000300010000"), v)
	pt.Insert(decodeHex("0100000000000000000000003a30000001000000"), v)
	pt.Insert(decodeHex("000000003a300000010000000000010010000000"), v)
	pt.Insert(decodeHex("00000000003a3000000100000000000100100000"), v)
	pt.Insert(decodeHex("0000000000003a30000001000000000001001000"), v)
	pt.Insert(decodeHex("000000000000003a300000010000000000010010"), v)
	pt.Insert(decodeHex("00000000000000003a3000000100000000000100"), v)
	pt.Insert(decodeHex("0000000000000000003a30000001000000000001"), v)
	pt.Insert(decodeHex("000000000000000000003a300000010000000000"), v)
	pt.Insert(decodeHex("00000000000000000000003a3000000100000000"), v)

	ft := pt.Flatten()
	data := decodeHex("0100000000000000000000003a30000001000000000001001000000044004500")

	mf2 := NewMatchFinder2(&pt)
	m2 := mf2.FindLongestMatches(data)
	mf3 := NewMatchFinder3(ft)
	m3 := mf3.FindLongestMatches(data)

	assertMatchesEqual(t, m2, m3)
}

func TestFlatTreeCorrectness_FindMatches4(t *testing.T) {
	var pt PatriciaTree
	v := []byte{1}
	pt.Insert(decodeHex("00000000000000000000000000000000000000"), v)

	ft := pt.Flatten()
	data := decodeHex("01")

	mf2 := NewMatchFinder2(&pt)
	m2 := mf2.FindLongestMatches(data)
	mf3 := NewMatchFinder3(ft)
	m3 := mf3.FindLongestMatches(data)

	assertMatchesEqual(t, m2, m3)
}

func TestFlatTreeCorrectness_FindMatches5(t *testing.T) {
	pt := buildTestTree()
	ft := pt.Flatten()
	data := buildTestData()

	mf2 := NewMatchFinder2(pt)
	m2 := mf2.FindLongestMatches(data)
	mf3 := NewMatchFinder3(ft)
	m3 := mf3.FindLongestMatches(data)

	assertMatchesEqual(t, m2, m3)
}

func TestFlatTreeCorrectness_ShortData(t *testing.T) {
	pt := buildTestTree()
	ft := pt.Flatten()

	// data < 2 bytes
	mf2 := NewMatchFinder2(pt)
	m2 := mf2.FindLongestMatches([]byte{0x01})
	mf3 := NewMatchFinder3(ft)
	m3 := mf3.FindLongestMatches([]byte{0x01})
	assertMatchesEqual(t, m2, m3)

	// empty
	m2 = mf2.FindLongestMatches(nil)
	m3 = mf3.FindLongestMatches(nil)
	assertMatchesEqual(t, m2, m3)
}

func TestFlatTreeCorrectness_RandomData(t *testing.T) {
	pt := buildTestTree()
	ft := pt.Flatten()
	mf2 := NewMatchFinder2(pt)
	mf3 := NewMatchFinder3(ft)

	// Run several random inputs with a fixed seed for reproducibility
	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 100; trial++ {
		size := 32 + trial*8
		data := make([]byte, size)
		rng.Read(data)

		m2 := mf2.FindLongestMatches(data)
		m3 := mf3.FindLongestMatches(data)
		assertMatchesEqual(t, m2, m3)
	}
}

func assertMatchesEqual(t *testing.T, expected, got []Match) {
	t.Helper()
	if len(expected) != len(got) {
		t.Fatalf("match count mismatch: expected %d, got %d", len(expected), len(got))
	}
	for i, m := range expected {
		g := got[i]
		if m.Start != g.Start || m.End != g.End || !reflect.DeepEqual(m.Val, g.Val) {
			t.Errorf("match[%d] mismatch: expected {Start:%d End:%d Val:%v}, got {Start:%d End:%d Val:%v}",
				i, m.Start, m.End, m.Val, g.Start, g.End, g.Val)
		}
	}
}

// Benchmarks

// buildLargeTestTree creates a large patricia tree (~64K patterns, matching real
// MaxDictPatterns=64*1024) to simulate real compression dictionary sizes.
func buildLargeTestTree() (*PatriciaTree, []byte) {
	const numPatterns = 64 * 1024
	var pt PatriciaTree
	seed := [32]byte{0x42}
	patterns := make([][]byte, 0, numPatterns)

	// Generate patterns of varying lengths (5-64 bytes), mimicking Ethereum data patterns
	for i := 0; i < numPatterns; i++ {
		pLen := 5 + (i % 60)
		pattern := make([]byte, pLen)
		for j := range pattern {
			seed[j%32] ^= byte(i*7 + j*13)
			pattern[j] = seed[j%32]
		}
		pt.Insert(pattern, []byte{byte(i & 0xff), byte(i >> 8)})
		patterns = append(patterns, pattern)
	}

	// Build test data (~32KB): concatenate subsets of patterns with filler
	data := make([]byte, 0, 32*1024)
	for i := 0; len(data) < 32*1024; i++ {
		p := patterns[i%len(patterns)]
		if i%3 == 0 {
			data = append(data, p...)
		} else {
			half := len(p) / 2
			data = append(data, p[:half]...)
		}
		for j := 0; j < 8; j++ {
			data = append(data, byte(i*3+j))
		}
	}
	return &pt, data
}

func BenchmarkMatchFinder2_Small(b *testing.B) {
	pt := buildTestTree()
	data := buildTestData()
	mf2 := NewMatchFinder2(pt)
	mf2.FindLongestMatches(data) // warm up

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mf2.FindLongestMatches(data)
	}
}

func BenchmarkMatchFinder3_Small(b *testing.B) {
	pt := buildTestTree()
	ft := pt.Flatten()
	data := buildTestData()
	mf3 := NewMatchFinder3(ft)
	mf3.FindLongestMatches(data) // warm up

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mf3.FindLongestMatches(data)
	}
}

func BenchmarkMatchFinder2_Large(b *testing.B) {
	pt, data := buildLargeTestTree()
	mf2 := NewMatchFinder2(pt)
	mf2.FindLongestMatches(data) // warm up

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mf2.FindLongestMatches(data)
	}
}

func BenchmarkMatchFinder3_Large(b *testing.B) {
	pt, data := buildLargeTestTree()
	ft := pt.Flatten()
	mf3 := NewMatchFinder3(ft)
	mf3.FindLongestMatches(data) // warm up

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mf3.FindLongestMatches(data)
	}
}

func BenchmarkMatchFinder2_LargeParallel(b *testing.B) {
	pt, data := buildLargeTestTree()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		mf2 := NewMatchFinder2(pt)
		for pb.Next() {
			mf2.FindLongestMatches(data)
		}
	})
}

func BenchmarkMatchFinder3_LargeParallel(b *testing.B) {
	pt, data := buildLargeTestTree()
	ft := pt.Flatten()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		mf3 := NewMatchFinder3(ft)
		for pb.Next() {
			mf3.FindLongestMatches(data)
		}
	})
}

func TestFlatTreeCorrectness_Large(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large correctness test in short mode")
	}
	pt, data := buildLargeTestTree()
	ft := pt.Flatten()
	mf2 := NewMatchFinder2(pt)
	mf3 := NewMatchFinder3(ft)
	m2 := mf2.FindLongestMatches(data)
	m3 := mf3.FindLongestMatches(data)
	assertMatchesEqual(t, m2, m3)
}
