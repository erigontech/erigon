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

// TestExpand8_32BStrictlyDecreasing verifies that within each character bucket,
// the sequence of b values (bucket write positions) produced by expand_8_32 is
// strictly decreasing. This follows from the algorithm always computing
// b = bucket[c] - 1 and then storing bucket[c] = b, so each successive write
// into the same bucket moves one position to the left.
func TestExpand8_32BStrictlyDecreasing(t *testing.T) {
	inputs := []struct {
		name string
		data []byte
	}{
		{"repeated pattern", []byte{4, 5, 6, 4, 5, 6, 4, 5, 6}},
		{"abracadabra", []byte("abracadabra")},
		{"mississippi", []byte("mississippi")},
		{"all same", []byte("aaaaabbbbbccccc")},
		{"reverse alpha", []byte("zyxwvutsrqponmlkjihgfedcba")},
		// small alphabet forces multiple LMS suffixes into the same bucket
		{"small alphabet", []byte{3, 3, 1, 1, 1, 1, 1, 0, 2, 3, 0, 3, 1, 3, 1, 2, 3, 1, 1, 3}},
	}

	for _, tc := range inputs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			text := tc.data
			n := len(text)

			// Compute LMS positions.
			isS := make([]bool, n+1)
			isS[n] = true
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
				t.Skip("no LMS suffixes")
			}

			// Pack LMS positions into front of sa, as the real algorithm does.
			sa := make([]int32, n)
			for i, v := range lms {
				sa[i] = v
			}

			// perBucketB maps character → ordered list of b values produced for it.
			perBucketB := collectExpand8_32BValues(text, sa, numLMS)

			// Within every bucket the b values must be strictly decreasing.
			for c, bs := range perBucketB {
				for i := 1; i < len(bs); i++ {
					if bs[i] >= bs[i-1] {
						t.Errorf("input %q char=%d: b values not strictly decreasing at step %d: b[%d]=%d >= b[%d]=%d",
							text, c, i, i, bs[i], i-1, bs[i-1])
					}
				}
			}
		})
	}
}

// collectExpand8_32BValues simulates the logic of expand_8_32 and returns, for
// each character, the ordered sequence of b values (bucket write positions)
// produced during the expansion.
func collectExpand8_32BValues(text []byte, sa []int32, numLMS int) map[byte][]int32 {
	// Rebuild bucket-max from scratch (mirrors bucketMax_8_32).
	freq := make([]int32, 256)
	for _, c := range text {
		freq[c]++
	}
	bucket := make([]int32, 256)
	var sum int32
	for i := range bucket {
		sum += freq[i]
		bucket[i] = sum
	}

	perBucket := make(map[byte][]int32)

	x := numLMS - 1
	saX := sa[x]
	c := text[saX]
	b := bucket[c] - 1
	bucket[c] = b
	perBucket[c] = append(perBucket[c], b)

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
			perBucket[c] = append(perBucket[c], b)
		}
	}

	return perBucket
}
