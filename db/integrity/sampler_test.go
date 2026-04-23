// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package integrity

import (
	"slices"
	"testing"
)

func TestSampler_CanSkip_FullSample(t *testing.T) {
	s := NewSampler(42, 1.0)
	for i := 0; i < 1000; i++ {
		if s.CanSkip() {
			t.Fatal("CanSkip() returned true with sampleRatio=1.0")
		}
	}
}

func TestSampler_CanSkip_ZeroSample(t *testing.T) {
	s := NewSampler(42, 0.0)
	skipped := 0
	const n = 1000
	for i := 0; i < n; i++ {
		if s.CanSkip() {
			skipped++
		}
	}
	// With sampleRatio=0.0, Float64() >= 0.0 is always true → always skip
	if skipped != n {
		t.Fatalf("expected all %d items skipped, got %d", n, skipped)
	}
}

func TestSampler_CanSkip_SampleRate(t *testing.T) {
	const seed = 123
	const ratio = 0.1
	const n = 100_000
	s := NewSampler(seed, ratio)
	kept := 0
	for i := 0; i < n; i++ {
		if !s.CanSkip() {
			kept++
		}
	}
	gotRatio := float64(kept) / n
	if gotRatio < 0.08 || gotRatio > 0.12 {
		t.Fatalf("sampleRatio=0.1 but got %.4f kept ratio over %d trials", gotRatio, n)
	}
}

func TestSampler_CanSkip_Reproducible(t *testing.T) {
	s1 := NewSampler(7, 0.5)
	s2 := NewSampler(7, 0.5)
	for i := 0; i < 20; i++ {
		a, b := s1.CanSkip(), s2.CanSkip()
		if a != b {
			t.Fatalf("same seed produced different result at index %d", i)
		}
	}
}

func TestSampler_BlockNums_FullSample(t *testing.T) {
	s := NewSampler(0, 1.0)
	const from, to uint64 = 10, 21 // exclusive: 10..20
	var got []uint64
	for n := range s.BlockNums(from, to) {
		got = append(got, n)
	}
	if uint64(len(got)) != to-from {
		t.Fatalf("expected %d blocks, got %d", to-from, len(got))
	}
	for i, n := range got {
		if n != from+uint64(i) {
			t.Fatalf("expected block %d at index %d, got %d", from+uint64(i), i, n)
		}
	}
}

func TestSampler_BlockNums_EmptyRange(t *testing.T) {
	s := NewSampler(0, 1.0)
	var count int
	for range s.BlockNums(100, 100) { // from == to → empty
		count++
	}
	if count != 0 {
		t.Fatalf("expected 0 blocks for empty range, got %d", count)
	}
}

func TestSampler_BlockNums_SingleBlock(t *testing.T) {
	s := NewSampler(0, 1.0)
	var got []uint64
	for n := range s.BlockNums(5, 6) { // [5,6) → just 5
		got = append(got, n)
	}
	if len(got) != 1 || got[0] != 5 {
		t.Fatalf("expected [5], got %v", got)
	}
}

func TestSampler_BlockNums_SampleRate(t *testing.T) {
	const ratio = 0.1
	const from, to uint64 = 0, 100_000
	s := NewSampler(42, ratio)
	var count uint64
	for range s.BlockNums(from, to) {
		count++
	}
	gotRatio := float64(count) / float64(to-from)
	if gotRatio < 0.07 || gotRatio > 0.13 {
		t.Fatalf("sampleRatio=0.1 but got %.4f kept ratio (%d/%d)", gotRatio, count, to-from)
	}
}

func TestSampler_BlockNums_Reproducible(t *testing.T) {
	const from, to uint64 = 0, 10_000
	collect := func(seed int64) []uint64 {
		s := NewSampler(seed, 0.2)
		var out []uint64
		for n := range s.BlockNums(from, to) {
			out = append(out, n)
		}
		return out
	}
	a := collect(99)
	b := collect(99)
	if !slices.Equal(a, b) {
		t.Fatalf("same seed produced different sequences (len %d vs %d)", len(a), len(b))
	}
}

func TestSampler_BlockNums_EarlyStop(t *testing.T) {
	s := NewSampler(0, 1.0)
	var got []uint64
	for n := range s.BlockNums(0, 100) {
		got = append(got, n)
		if n == 4 {
			break
		}
	}
	if len(got) != 5 || got[len(got)-1] != 4 {
		t.Fatalf("expected early stop at 4, got %v", got)
	}
}

func TestSampler_TxNums_FullSample(t *testing.T) {
	s := NewSampler(0, 1.0)
	const from, to uint64 = 0, 5 // [0,5) → 0..4
	var got []uint64
	for n := range s.TxNums(from, to) {
		got = append(got, n)
	}
	if uint64(len(got)) != to-from {
		t.Fatalf("expected %d txnums, got %d", to-from, len(got))
	}
}

func TestSampler_TxNums_SampleRate(t *testing.T) {
	const ratio = 0.05
	const from, to uint64 = 0, 200_000
	s := NewSampler(7, ratio)
	var count uint64
	for range s.TxNums(from, to) {
		count++
	}
	gotRatio := float64(count) / float64(to-from)
	if gotRatio < 0.03 || gotRatio > 0.07 {
		t.Fatalf("sampleRatio=0.05 but got %.4f kept ratio (%d/%d)", gotRatio, count, to-from)
	}
}
