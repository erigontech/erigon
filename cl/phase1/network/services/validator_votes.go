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

import (
	"sync"

	"github.com/erigontech/erigon/cl/clparams"
)

// validatorVotes records, per target epoch, the validators that already have a valid attestation on gossip. It keeps
// the newest epochs that the propagation range can still admit; a vote for an older epoch is never new.
type validatorVotes struct {
	mu       sync.RWMutex
	byEpoch  map[uint64][]uint64 // target epoch -> bitset by validator index
	maxEpoch uint64
	epochs   uint64
}

// validatorVoteEpochs is the number of target epochs the propagation range can span, plus one for an epoch that
// starts before the head reaches it.
func validatorVoteEpochs(beaconCfg *clparams.BeaconChainConfig, netCfg *clparams.NetworkConfig) uint64 {
	return (netCfg.AttestationPropagationSlotRange+beaconCfg.SlotsPerEpoch-1)/beaconCfg.SlotsPerEpoch + 1
}

func newValidatorVotes(epochs uint64) *validatorVotes {
	return &validatorVotes{byEpoch: make(map[uint64][]uint64), epochs: epochs}
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
	if epoch+v.epochs <= v.maxEpoch {
		return false
	}
	if epoch > v.maxEpoch {
		v.maxEpoch = epoch
		for e := range v.byEpoch {
			if e+v.epochs <= epoch {
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
