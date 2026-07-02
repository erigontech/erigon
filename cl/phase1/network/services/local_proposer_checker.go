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
	"slices"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
)

type localProposerChecker struct {
	beaconCfg *clparams.BeaconChainConfig

	// Cached proposer indices per epoch (for current epoch check)
	proposerIndicesCache *lru.Cache[uint64, []uint64]
}

func newLocalProposerChecker(beaconCfg *clparams.BeaconChainConfig) localProposerChecker {
	proposerIndicesCache, err := lru.New[uint64, []uint64]("proposerIndices", 3)
	if err != nil {
		panic(err)
	}
	return localProposerChecker{
		beaconCfg:            beaconCfg,
		proposerIndicesCache: proposerIndicesCache,
	}
}

// isLocalValidatorProposer checks if any local validator is a proposer in the current or next epoch.
// For current epoch: uses cached GetBeaconProposerIndices.
// For next epoch: uses GetProposerLookahead() (Fulu+) or always returns true (pre-Fulu).
func (c localProposerChecker) isLocalValidatorProposer(headState *state.CachingBeaconState, currentEpoch uint64, localValidators []uint64) bool {
	if headState.Version() < clparams.FuluVersion {
		return true
	}
	// Check current epoch using cached proposer indices
	currentProposers, ok := c.proposerIndicesCache.Get(currentEpoch)
	if !ok {
		var err error
		currentProposers, err = headState.GetBeaconProposerIndices(currentEpoch)
		if err == nil {
			c.proposerIndicesCache.Add(currentEpoch, currentProposers)
		}
	}
	for _, validatorIndex := range localValidators {
		if slices.Contains(currentProposers, validatorIndex) {
			return true
		}
	}

	// For Fulu+, use the efficient proposer lookahead for next epoch
	lookahead := headState.GetProposerLookahead()
	// The lookahead contains proposers for current and next epoch, skip current epoch slots
	slotsPerEpoch := int(c.beaconCfg.SlotsPerEpoch)
	for i := slotsPerEpoch; i < lookahead.Length(); i++ {
		proposerIndex := lookahead.Get(i)
		if slices.Contains(localValidators, proposerIndex) {
			return true
		}
	}
	return false
}
