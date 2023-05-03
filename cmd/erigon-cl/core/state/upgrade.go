package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func (b *BeaconState) UpgradeToAltair() error {
	b.previousStateRoot = libcommon.Hash{}
	epoch := b.Epoch()
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.CurrentVersion = utils.Uint32ToBytes4(b.BeaconConfig().AltairForkVersion)
	b.SetFork(fork)
	// Process new fields
	b.SetPreviousEpochParticipationFlags(make(cltypes.ParticipationFlagsList, b.ValidatorLength()))
	b.SetCurrentEpochParticipationFlags(make(cltypes.ParticipationFlagsList, b.ValidatorLength()))
	b.SetInactivityScores(make([]uint64, b.ValidatorLength()))
	// Change version
	b.SetVersion(clparams.AltairVersion)
	// Fill in previous epoch participation from the pre state's pending attestations
	for _, attestation := range b.PreviousEpochAttestations() {
		flags, err := b.GetAttestationParticipationFlagIndicies(attestation.Data, attestation.InclusionDelay)
		if err != nil {
			return err
		}
		indicies, err := b.GetAttestingIndicies(attestation.Data, attestation.AggregationBits, false)
		if err != nil {
			return err
		}

		pep := b.EpochParticipation(false)
		for _, index := range indicies {
			for _, flagIndex := range flags {
				pep[index].Add(int(flagIndex))
			}
		}
		b.SetPreviousEpochParticipationFlags(pep)
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

func (b *BeaconState) UpgradeToBellatrix() error {
	b.previousStateRoot = libcommon.Hash{}
	epoch := b.Epoch()
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.PreviousVersion = fork.CurrentVersion
	fork.CurrentVersion = utils.Uint32ToBytes4(b.BeaconConfig().BellatrixForkVersion)
	b.SetFork(fork)
	b.SetLatestExecutionPayloadHeader(cltypes.NewEth1Header(clparams.BellatrixVersion))
	// Update the state root cache
	b.SetVersion(clparams.BellatrixVersion)
	return nil
}

func (b *BeaconState) UpgradeToCapella() error {
	b.previousStateRoot = libcommon.Hash{}
	epoch := b.Epoch()
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.PreviousVersion = fork.CurrentVersion
	fork.CurrentVersion = utils.Uint32ToBytes4(b.BeaconConfig().CapellaForkVersion)
	b.SetFork(fork)
	// Update the payload header.
	header := b.LatestExecutionPayloadHeader()
	header.Capella()
	b.SetLatestExecutionPayloadHeader(header)
	// Set new fields
	b.SetNextWithdrawalIndex(0)
	b.SetNextWithdrawalValidatorIndex(0)
	// Update the state root cache
	b.SetVersion(clparams.CapellaVersion)
	return nil
}
