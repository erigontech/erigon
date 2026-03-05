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
	"fmt"
	"iter"
	"math"
	"math/rand/v2"
)

// Sampler encapsulates pseudo-random sampling logic for integrity checks.
// It uses a seeded PCG RNG so that checks are reproducible given the same seed
// and sampleRatio.
type Sampler struct {
	rng         *rand.Rand
	SampleRatio float64
	Seed        int64
}

// ValidateSampleRatio returns an error if sampleRatio is outside the valid range (0, 1].
func ValidateSampleRatio(sampleRatio float64) error {
	if sampleRatio <= 0 || sampleRatio > 1 {
		return fmt.Errorf("--sample must be in (0, 1], got %v", sampleRatio)
	}
	return nil
}

// NewSampler creates a Sampler with the given seed and sampleRatio (0.0–1.0].
// sampleRatio=1.0 means check everything; sampleRatio=0.05 means check ~5%.
func NewSampler(seed int64, sampleRatio float64) *Sampler {
	return &Sampler{
		rng:         rand.New(rand.NewPCG(uint64(seed), 0)),
		SampleRatio: sampleRatio,
		Seed:        seed,
	}
}

// CanSkip returns true if the current item should be skipped.
// Draws one random value per call, so call it once per item in the iteration loop.
func (s *Sampler) CanSkip() bool {
	return s.SampleRatio < 1.0 && s.rng.Float64() >= s.SampleRatio
}

// BlockNums returns an iterator over sampled block numbers in [from, to) (exclusive upper bound).
// Uses geometric skipping so cost is O(sampled) rather than O(total).
//
//	for blockNum := range sampler.BlockNums(from, to) {
//	    // check blockNum
//	}
func (s *Sampler) BlockNums(from, to uint64) iter.Seq[uint64] {
	return s.nums(from, to)
}

// TxNums returns an iterator over sampled transaction numbers in [from, to) (exclusive upper bound).
// Uses geometric skipping so cost is O(sampled) rather than O(total).
//
//	for txNum := range sampler.TxNums(from, to) {
//	    // check txNum
//	}
func (s *Sampler) TxNums(from, to uint64) iter.Seq[uint64] {
	return s.nums(from, to)
}

func (s *Sampler) nums(from, to uint64) iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		for n := from + s.geometricSkip(); n < to; n += 1 + s.geometricSkip() {
			if !yield(n) {
				return
			}
		}
	}
}

// geometricSkip draws the number of items to skip before the next sampled item,
// using the inverse-CDF of the geometric distribution.
// For sampleRatio=1.0 it always returns 0 (no skipping).
func (s *Sampler) geometricSkip() uint64 {
	if s.SampleRatio >= 1.0 {
		return 0
	}
	if s.SampleRatio <= 0 {
		return math.MaxUint64
	}
	u := s.rng.Float64()
	if u <= 0 {
		return 0
	}
	return uint64(math.Log(u) / math.Log(1.0-s.SampleRatio))
}
