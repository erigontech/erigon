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

package state

import (
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
)

func (b *CachingBeaconState) UpgradeToAltair() error {
	b.previousStateRoot = libcommon.Hash{}
	epoch := Epoch(b.BeaconState)
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.CurrentVersion = utils.Uint32ToBytes4(uint32(b.BeaconConfig().AltairForkVersion))
	b.SetFork(fork)
	// Process new fields
	b.SetPreviousEpochParticipationFlags(make(cltypes.ParticipationFlagsList, b.ValidatorLength()))
	b.SetCurrentEpochParticipationFlags(make(cltypes.ParticipationFlagsList, b.ValidatorLength()))
	b.SetInactivityScores(make([]uint64, b.ValidatorLength()))
	// Change version
	b.SetVersion(clparams.AltairVersion)
	// Fill in previous epoch participation from the pre state's pending attestations
	if err := solid.RangeErr[*solid.PendingAttestation](b.PreviousEpochAttestations(), func(i1 int, pa *solid.PendingAttestation, i2 int) error {
		attestationData := pa.Data
		flags, err := b.GetAttestationParticipationFlagIndicies(attestationData, pa.InclusionDelay, false)
		if err != nil {
			return err
		}
		attestation := &solid.Attestation{
			AggregationBits: pa.AggregationBits,
			Data:            attestationData,
			// don't care signature and committee_bits here
		}
		indices, err := b.GetAttestingIndicies(attestation, false)
		if err != nil {
			return err
		}
		for _, index := range indices {
			for _, flagIndex := range flags {
				b.AddPreviousEpochParticipationAt(int(index), flagIndex)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	b.ResetPreviousEpochAttestations()
	// Process sync committees
	var err error
	currentSyncCommittee, err := b.ComputeNextSyncCommittee()
	if err != nil {
		return err
	}
	b.SetCurrentSyncCommittee(currentSyncCommittee)
	nextSyncCommittee, err := b.ComputeNextSyncCommittee()
	if err != nil {
		return err
	}
	b.SetNextSyncCommittee(nextSyncCommittee)

	return nil
}

func (b *CachingBeaconState) UpgradeToBellatrix() error {
	b.previousStateRoot = libcommon.Hash{}
	epoch := Epoch(b.BeaconState)
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.PreviousVersion = fork.CurrentVersion
	fork.CurrentVersion = utils.Uint32ToBytes4(uint32(b.BeaconConfig().BellatrixForkVersion))
	b.SetFork(fork)
	b.SetLatestExecutionPayloadHeader(cltypes.NewEth1Header(clparams.BellatrixVersion))
	// Update the state root cache
	b.SetVersion(clparams.BellatrixVersion)
	return nil
}

func (b *CachingBeaconState) UpgradeToCapella() error {
	b.previousStateRoot = libcommon.Hash{}
	epoch := Epoch(b.BeaconState)
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.PreviousVersion = fork.CurrentVersion
	fork.CurrentVersion = utils.Uint32ToBytes4(uint32(b.BeaconConfig().CapellaForkVersion))
	b.SetFork(fork)
	// Update the payload header.
	header := b.LatestExecutionPayloadHeader()
	header.Capella()
	b.SetLatestExecutionPayloadHeader(header)
	// Set new fields
	b.SetNextWithdrawalIndex(0)
	b.SetNextWithdrawalValidatorIndex(0)
	b.ResetHistoricalSummaries()
	// Update the state root cache
	b.SetVersion(clparams.CapellaVersion)
	return nil
}

func (b *CachingBeaconState) UpgradeToDeneb() error {
	b.previousStateRoot = libcommon.Hash{}
	epoch := Epoch(b.BeaconState)
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.PreviousVersion = fork.CurrentVersion
	fork.CurrentVersion = utils.Uint32ToBytes4(uint32(b.BeaconConfig().DenebForkVersion))
	b.SetFork(fork)
	// Update the payload header.
	header := b.LatestExecutionPayloadHeader()
	header.Deneb()
	b.SetLatestExecutionPayloadHeader(header)
	// Update the state root cache
	b.SetVersion(clparams.DenebVersion)
	return nil
}

func (b *CachingBeaconState) UpgradeToElectra() error {
	b.previousStateRoot = libcommon.Hash{}
	epoch := Epoch(b.BeaconState)
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.PreviousVersion = fork.CurrentVersion
	fork.CurrentVersion = utils.Uint32ToBytes4(uint32(b.BeaconConfig().ElectraForkVersion))
	b.SetFork(fork)

	// Update the payload header.
	//header := b.LatestExecutionPayloadHeader()
	// header.Electra()
	//b.SetLatestExecutionPayloadHeader(header)

	// Update the state root cache
	b.SetVersion(clparams.ElectraVersion)
	return nil
}
