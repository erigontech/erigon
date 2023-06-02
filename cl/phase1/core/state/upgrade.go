package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func (b *BeaconState) UpgradeToAltair() error {
	b.previousStateRoot = libcommon.Hash{}
	epoch := Epoch(b.BeaconState)
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
	if err := solid.RangeErr[*solid.PendingAttestation](b.PreviousEpochAttestations(), func(i1 int, pa *solid.PendingAttestation, i2 int) error {
		attestationData := pa.AttestantionData()
		flags, err := b.GetAttestationParticipationFlagIndicies(attestationData, pa.InclusionDelay())
		if err != nil {
			return err
		}
		indices, err := b.GetAttestingIndicies(attestationData, pa.AggregationBits(), false)
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

func (b *BeaconState) UpgradeToBellatrix() error {
	b.previousStateRoot = libcommon.Hash{}
	epoch := Epoch(b.BeaconState)
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
	epoch := Epoch(b.BeaconState)
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
	b.ResetHistoricalSummaries()
	// Update the state root cache
	b.SetVersion(clparams.CapellaVersion)
	return nil
}

func (b *BeaconState) UpgradeToDeneb() error {
	b.previousStateRoot = libcommon.Hash{}
	epoch := Epoch(b.BeaconState)
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.PreviousVersion = fork.CurrentVersion
	fork.CurrentVersion = utils.Uint32ToBytes4(b.BeaconConfig().DenebForkVersion)
	b.SetFork(fork)
	// Update the payload header.
	header := b.LatestExecutionPayloadHeader()
	header.Deneb()
	b.SetLatestExecutionPayloadHeader(header)
	// Update the state root cache
	b.SetVersion(clparams.DenebVersion)
	return nil
}
