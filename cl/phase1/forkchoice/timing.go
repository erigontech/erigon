// Copyright 2024 The Erigon Authors
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

package forkchoice

import "github.com/erigontech/erigon/cl/clparams"

// [New in Gloas:EIP7732] Timing helper functions.
//
// GLOAS replaces the fixed INTERVALS_PER_SLOT division with a BPS (basis points)
// system for finer-grained control of slot-internal deadlines. All functions are
// epoch-aware: pre-GLOAS epochs use the legacy IntervalsPerSlot arithmetic,
// post-GLOAS epochs use BPS constants.

// getAttestationDueMs returns the attestation deadline in milliseconds from slot start.
//
// Pre-GLOAS:  SecondsPerSlot * 1000 / IntervalsPerSlot  (1/3 of slot = 4000 ms on mainnet)
// Post-GLOAS: SecondsPerSlot * AttestationDueBpsGloas / 10  (25% of slot = 3000 ms on mainnet)
func (f *ForkChoiceStore) getAttestationDueMs(epoch uint64) uint64 {
	if epoch >= f.beaconCfg.GloasForkEpoch {
		return f.beaconCfg.SecondsPerSlot * clparams.AttestationDueBpsGloas / (clparams.BpsFactor / 1000)
	}
	return f.beaconCfg.SecondsPerSlot * 1000 / f.beaconCfg.IntervalsPerSlot
}

// getAggregateDueMs returns the aggregate deadline in milliseconds from slot start.
//
// Pre-GLOAS:  SecondsPerSlot * 1000 * 2 / IntervalsPerSlot  (2/3 of slot = 8000 ms on mainnet)
// Post-GLOAS: SecondsPerSlot * AggregateDueBpsGloas / 10    (50% of slot = 6000 ms on mainnet)
func (f *ForkChoiceStore) getAggregateDueMs(epoch uint64) uint64 {
	if epoch >= f.beaconCfg.GloasForkEpoch {
		return f.beaconCfg.SecondsPerSlot * clparams.AggregateDueBpsGloas / (clparams.BpsFactor / 1000)
	}
	return f.beaconCfg.SecondsPerSlot * 1000 * 2 / f.beaconCfg.IntervalsPerSlot
}

// getSyncMessageDueMs returns the sync committee message deadline in milliseconds.
// Same as the attestation deadline in both pre- and post-GLOAS.
func (f *ForkChoiceStore) getSyncMessageDueMs(epoch uint64) uint64 {
	return f.getAttestationDueMs(epoch)
}

// getContributionDueMs returns the sync committee contribution deadline in milliseconds.
// Same as the aggregate deadline in both pre- and post-GLOAS.
func (f *ForkChoiceStore) getContributionDueMs(epoch uint64) uint64 {
	return f.getAggregateDueMs(epoch)
}

// getPayloadAttestationDueMs returns the PTC payload attestation deadline in milliseconds.
// [New in Gloas:EIP7732] This deadline only exists post-GLOAS (75% of slot = 9000 ms on mainnet).
// Callers should only invoke this for post-GLOAS epochs; the returned value is well-defined
// regardless, but meaningless pre-GLOAS since PTC duties do not exist before GLOAS.
func (f *ForkChoiceStore) getPayloadAttestationDueMs(_ uint64) uint64 {
	return f.beaconCfg.SecondsPerSlot * clparams.PayloadAttestationDueBps / (clparams.BpsFactor / 1000)
}

// shouldApplyProposerBoost returns whether a block arriving at the current store time
// is timely enough to receive the proposer boost.
//
// Pre-GLOAS:  seconds_into_slot < SecondsPerSlot / IntervalsPerSlot
// Post-GLOAS: seconds_into_slot * 1000 < get_attestation_due_ms(epoch)
//
// This function is used by record_block_timeliness (on_block) to decide whether
// to set proposer_boost_root. It is NOT the same as WeightStore.ShouldApplyProposerBoost(),
// which simply checks whether proposer_boost_root has been set.
func (f *ForkChoiceStore) shouldApplyProposerBoost() bool {
	secondsIntoSlot := (f.time.Load() - f.genesisTime) % f.beaconCfg.SecondsPerSlot
	epoch := f.computeEpochAtSlot(f.Slot())
	attestationDueMs := f.getAttestationDueMs(epoch)
	return secondsIntoSlot*1000 < attestationDueMs
}
