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
	b.fork.Epoch = epoch
	b.fork.CurrentVersion = utils.Uint32ToBytes4(b.beaconConfig.AltairForkVersion)
	// Process new fields
	b.previousEpochParticipation = make(cltypes.ParticipationFlagsList, len(b.validators))
	b.currentEpochParticipation = make(cltypes.ParticipationFlagsList, len(b.validators))
	b.inactivityScores = make([]uint64, len(b.validators))
	// Change version
	b.version = clparams.AltairVersion
	// Fill in previous epoch participation from the pre state's pending attestations
	for _, attestation := range b.previousEpochAttestations {
		flags, err := b.GetAttestationParticipationFlagIndicies(attestation.Data, attestation.InclusionDelay)
		if err != nil {
			return err
		}
		indicies, err := b.GetAttestingIndicies(attestation.Data, attestation.AggregationBits, false)
		if err != nil {
			return err
		}

		for _, index := range indicies {
			for _, flagIndex := range flags {
				b.previousEpochParticipation[index] = b.previousEpochParticipation[index].Add(int(flagIndex))
			}
		}
	}
	b.previousEpochAttestations = nil
	// Process sync committees
	var err error
	if b.currentSyncCommittee, err = b.ComputeNextSyncCommittee(); err != nil {
		return err
	}
	if b.nextSyncCommittee, err = b.ComputeNextSyncCommittee(); err != nil {
		return err
	}
	// Update the state root cache
	b.touchedLeaves[ForkLeafIndex] = true
	b.touchedLeaves[PreviousEpochParticipationLeafIndex] = true
	b.touchedLeaves[CurrentEpochParticipationLeafIndex] = true
	b.touchedLeaves[InactivityScoresLeafIndex] = true
	b.touchedLeaves[CurrentSyncCommitteeLeafIndex] = true
	b.touchedLeaves[NextSyncCommitteeLeafIndex] = true

	return nil
}

func (b *BeaconState) UpgradeToBellatrix() error {
	b.previousStateRoot = libcommon.Hash{}
	epoch := b.Epoch()
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnVersionChange(b.version)
		b.reverseChangeset.OnForkChange(b.fork)
	}
	// update version
	b.fork.Epoch = epoch
	b.fork.PreviousVersion = b.fork.CurrentVersion
	b.fork.CurrentVersion = utils.Uint32ToBytes4(b.beaconConfig.BellatrixForkVersion)
	b.latestExecutionPayloadHeader = cltypes.NewEth1Header(clparams.BellatrixVersion)
	// Update the state root cache
	b.touchedLeaves[ForkLeafIndex] = true
	b.touchedLeaves[LatestExecutionPayloadHeaderLeafIndex] = true
	b.version = clparams.BellatrixVersion
	return nil
}

func (b *BeaconState) UpgradeToCapella() error {
	b.previousStateRoot = libcommon.Hash{}
	epoch := b.Epoch()
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnVersionChange(b.version)
		b.reverseChangeset.OnForkChange(b.fork)
		b.reverseChangeset.OnEth1Header(b.latestExecutionPayloadHeader)
	}
	// update version
	b.fork.Epoch = epoch
	b.fork.PreviousVersion = b.fork.CurrentVersion
	b.fork.CurrentVersion = utils.Uint32ToBytes4(b.beaconConfig.CapellaForkVersion)
	// Update the payload header.
	b.latestExecutionPayloadHeader.Capella()
	// Set new fields
	b.nextWithdrawalIndex = 0
	b.nextWithdrawalValidatorIndex = 0
	b.historicalSummaries = nil
	// Update the state root cache
	b.touchedLeaves[ForkLeafIndex] = true
	b.touchedLeaves[LatestExecutionPayloadHeaderLeafIndex] = true
	b.touchedLeaves[NextWithdrawalIndexLeafIndex] = true
	b.touchedLeaves[NextWithdrawalValidatorIndexLeafIndex] = true
	b.touchedLeaves[HistoricalSummariesLeafIndex] = true
	b.version = clparams.CapellaVersion
	return nil
}
