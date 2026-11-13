// Copyright 2026 The Erigon Authors
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

package services

import "sync"

// validatorVotes records, per target epoch, the validators that already have a valid attestation on gossip. Gossip
// accepts attestations of the previous and the current epoch only, so it keeps the two newest epochs.
type validatorVotes struct {
	mu       sync.RWMutex
	byEpoch  map[uint64][]uint64 // target epoch -> bitset by validator index
	maxEpoch uint64
}

func newValidatorVotes() *validatorVotes {
	return &validatorVotes{byEpoch: make(map[uint64][]uint64)}
}

func (v *validatorVotes) seen(validatorIndex, epoch uint64) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	bits := v.byEpoch[epoch]
	word := validatorIndex / 64
	return word < uint64(len(bits)) && bits[word]&(1<<(validatorIndex%64)) != 0
}

// claim records the vote and reports whether it is the validator's first one for the epoch.
func (v *validatorVotes) claim(validatorIndex, epoch uint64) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	if epoch+1 < v.maxEpoch {
		return true
	}
	if epoch > v.maxEpoch {
		v.maxEpoch = epoch
		for e := range v.byEpoch {
			if e+1 < epoch {
				delete(v.byEpoch, e)
			}
		}
	}
	bits := v.byEpoch[epoch]
	word, mask := validatorIndex/64, uint64(1)<<(validatorIndex%64)
	if word >= uint64(len(bits)) {
		bits = append(bits, make([]uint64, word+1-uint64(len(bits)))...)
		v.byEpoch[epoch] = bits
	}
	if bits[word]&mask != 0 {
		return false
	}
	bits[word] |= mask
	return true
}
