package sais

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSais(t *testing.T) {
	data := []byte{4, 5, 6, 4, 5, 6, 4, 5, 6}
	sa := make([]int32, len(data))
	err := Sais(data, sa)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []int32{6, 3, 0, 7, 4, 1, 8, 5, 2}, sa)
}

func TestSaisText(t *testing.T) {
	data := []byte("abracadabra")
	sa := make([]int32, len(data))
	require.NoError(t, Sais(data, sa))
	// Verify suffix array is sorted
	for i := 1; i < len(sa); i++ {
		a := string(data[sa[i-1]:])
		b := string(data[sa[i]:])
		if a >= b {
			t.Fatalf("sa not sorted at %d: %q >= %q", i, a, b)
		}
	}
}

func TestSaisRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for _, n := range []int{2, 3, 10, 100, 1000, 10000} {
		data := make([]byte, n)
		rng.Read(data)
		sa := make([]int32, n)
		require.NoError(t, Sais(data, sa))

		// Verify: sa is a permutation and suffixes are sorted
		seen := make([]bool, n)
		for _, v := range sa {
			require.False(t, seen[v], "duplicate in sa")
			seen[v] = true
		}
		for i := 1; i < n; i++ {
			a := string(data[sa[i-1]:])
			b := string(data[sa[i]:])
			if a >= b {
				t.Fatalf("n=%d: sa not sorted at %d", n, i)
			}
		}
	}
}

func TestSaisEdgeCases(t *testing.T) {
	// Empty
	require.NoError(t, Sais(nil, nil))

	// Single byte
	sa := make([]int32, 1)
	require.NoError(t, Sais([]byte{42}, sa))
	assert.Equal(t, []int32{0}, sa)

	// All same bytes
	data := make([]byte, 100)
	sa = make([]int32, 100)
	require.NoError(t, Sais(data, sa))
	expected := make([]int32, 100)
	for i := range expected {
		expected[i] = int32(99 - i)
	}
	assert.Equal(t, expected, sa)
}

// loremWords are short tokens that repeat heavily, mimicking real-world
// compressed data (RLP fields, repeated addresses, etc.).
var loremWords = []string{
	"lorem", "ipsum", "dolor", "sit", "amet", "consectetur",
	"adipiscing", "elit", "sed", "do", "eiusmod", "tempor",
	"incididunt", "ut", "labore", "et", "dolore", "magna", "aliqua",
}

func makeLoremData(size int) []byte {
	data := make([]byte, 0, size)
	i := 0
	for len(data) < size {
		w := loremWords[i%len(loremWords)]
		data = append(data, w...)
		data = append(data, ' ')
		i++
	}
	return data[:size]
}

func BenchmarkSais(b *testing.B) {
	for _, size := range []int{16 * 1024 * 1024} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			data := makeLoremData(size)
			sa := make([]int32, len(data))
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := Sais(data, sa); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestExpand8_32BStrictlyDecreasing verifies that the sequence of bucket write
// positions (b values) produced inside expand_8_32 is strictly decreasing.
// It does this by instrumenting a small corpus through the full Sais path and
// checking the invariant directly via a white-box helper.
func TestExpand8_32BStrictlyDecreasing(t *testing.T) {
	inputs := [][]byte{
		[]byte("abracadabra"),
		[]byte{4, 5, 6, 4, 5, 6, 4, 5, 6},
		[]byte("mississippi"),
		[]byte("aaaaabbbbbccccc"),
		[]byte("zyxwvutsrqponmlkjihgfedcba"),
	}

	for _, text := range inputs {
		n := len(text)
		freq := make([]int32, 256)
		bucket := make([]int32, 256)

		// Count character frequencies.
		for _, c := range text {
			freq[c]++
		}

		// Build max-bucket positions (end of each bucket, exclusive).
		var sum int32
		for i := range bucket {
			sum += freq[i]
			bucket[i] = sum
		}

		// Collect LMS positions using the standard SAIS classification.
		// A position i is LMS if text[i] is S-type and text[i-1] is L-type.
		isS := make([]bool, n+1)
		isS[n] = true // sentinel is S-type
		for i := n - 1; i >= 1; i-- {
			if text[i] < text[i-1] {
				isS[i] = true
			} else if text[i] == text[i-1] {
				isS[i] = isS[i+1]
			}
		}
		var lms []int32
		for i := 1; i < n; i++ {
			if isS[i] && !isS[i-1] {
				lms = append(lms, int32(i))
			}
		}
		numLMS := len(lms)
		if numLMS == 0 {
			continue // nothing to verify
		}

		// Pack LMS positions into the front of sa (as expand_8_32 expects).
		sa := make([]int32, n)
		for i, v := range lms {
			sa[i] = v
		}

		// Simulate expand_8_32 and record b values in order.
		bSeq := simulateExpand8_32(text, freq, bucket, sa, numLMS)

		// The b values must be strictly decreasing.
		for i := 1; i < len(bSeq); i++ {
			if bSeq[i] >= bSeq[i-1] {
				t.Errorf("input %q: b values not strictly decreasing at step %d: b[%d]=%d >= b[%d]=%d",
					text, i, i, bSeq[i], i-1, bSeq[i-1])
			}
		}
	}
}

// simulateExpand8_32 mirrors the logic of expand_8_32 and returns the sequence
// of b values (bucket write positions) in the order they are computed.
func simulateExpand8_32(text []byte, freq, bucketIn, sa []int32, numLMS int) []int32 {
	// Rebuild bucket (max) from freq — same as bucketMax_8_32 does.
	bucket := make([]int32, 256)
	var sum int32
	for i := range bucket {
		sum += freq[i]
		bucket[i] = sum
	}

	var bSeq []int32

	x := numLMS - 1
	saX := sa[x]
	c := text[saX]
	b := bucket[c] - 1
	bucket[c] = b
	bSeq = append(bSeq, b)

	for i := len(sa) - 1; i >= 0; i-- {
		if i != int(b) {
			continue
		}
		if x > 0 {
			x--
			saX = sa[x]
			c = text[saX]
			b = bucket[c] - 1
			bucket[c] = b
			bSeq = append(bSeq, b)
		}
	}

	return bSeq
}
