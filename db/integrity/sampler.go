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

// SamplerCfg is plain configuration data (seed + ratio) with no mutable RNG state.
// It is safe to share across goroutines and to pass to multiple check functions —
// each call to NewSampler produces an independent Sampler starting from block 0.
type SamplerCfg struct {
	Seed        int64
	SampleRatio float64
}

// NewSamplerCfg creates a SamplerCfg and validates sampleRatio.
func NewSamplerCfg(seed int64, sampleRatio float64) (SamplerCfg, error) {
	if err := validateSampleRatio(sampleRatio); err != nil {
		return SamplerCfg{}, err
	}
	return SamplerCfg{Seed: seed, SampleRatio: sampleRatio}, nil
}

// NewSampler creates a fresh Sampler from this config. Every call returns an
// independent instance starting at its own RNG position — safe to call from
// multiple goroutines or multiple sequential checks without interference.
func (c SamplerCfg) NewSampler() *Sampler {
	return &Sampler{
		rng:         rand.New(rand.NewPCG(uint64(c.Seed), 0)),
		SampleRatio: c.SampleRatio,
		Seed:        c.Seed,
	}
}

// NewWindowSampler returns a Sampler whose seed is derived by XORing offset into
// c.Seed.  Use this when spawning one sampler per window/shard so that each shard
// samples different relative positions rather than all selecting identical offsets.
func (c SamplerCfg) NewWindowSampler(offset uint64) *Sampler {
	return SamplerCfg{Seed: c.Seed ^ int64(offset), SampleRatio: c.SampleRatio}.NewSampler()
}

// validateSampleRatio returns an error if sampleRatio is outside the valid range (0, 1].
func validateSampleRatio(sampleRatio float64) error {
	if sampleRatio <= 0 || sampleRatio > 1 {
		return fmt.Errorf("--sample must be in (0, 1], got %v", sampleRatio)
	}
	return nil
}

// ValidateSampleRatio is the exported form for CLI validation.
func ValidateSampleRatio(sampleRatio float64) error { return validateSampleRatio(sampleRatio) }

// ExpectedN returns the expected number of sampled items from a population of n.
func (s *Sampler) ExpectedN(n uint64) uint64 {
	return uint64(math.Round(float64(n) * s.SampleRatio))
}

// NewSampler creates a Sampler with the given seed and sampleRatio (0.0–1.0].
// Prefer SamplerCfg.NewSampler when the config is shared across calls.
func NewSampler(seed int64, sampleRatio float64) *Sampler {
	return SamplerCfg{Seed: seed, SampleRatio: sampleRatio}.NewSampler()
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

// Buckets returns an iterator over sampled bucket indices in [from, to) (exclusive upper bound).
// Uses geometric skipping so cost is O(sampled) rather than O(total).
//
//	for bucket := range sampler.Buckets(0, numBuckets) {
//	    // check bucket
//	}
func (s *Sampler) Buckets(from, to int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for n := range s.nums(uint64(from), uint64(to)) {
			if !yield(int(n)) {
				return
			}
		}
	}
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
