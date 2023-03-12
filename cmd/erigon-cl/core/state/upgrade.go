package state

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

// Upgrade process a state upgrade.
func (b *BeaconState) Upgrade() error {
	switch b.version {
	case clparams.Phase0Version:
		return b.upgradeToAltair()
	case clparams.AltairVersion:
		return b.upgradeToBellatrix()
	case clparams.BellatrixVersion:
		return b.upgradeToCapella()
	default:
		panic("not implemented")
	}

}

func (b *BeaconState) upgradeToAltair() error {
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
				b.previousEpochParticipation[index].Add(int(flagIndex))
			}
		}
	}
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

func (b *BeaconState) upgradeToBellatrix() error {
	epoch := b.Epoch()
	// update version
	b.fork.Epoch = epoch
	b.fork.CurrentVersion = utils.Uint32ToBytes4(b.beaconConfig.BellatrixForkVersion)
	b.latestExecutionPayloadHeader = cltypes.NewEth1Header(clparams.BellatrixVersion)
	// Update the state root cache
	b.touchedLeaves[ForkLeafIndex] = true
	b.touchedLeaves[LatestExecutionPayloadHeaderLeafIndex] = true
	b.version = clparams.BellatrixVersion
	return nil
}

func (b *BeaconState) upgradeToCapella() error {
	epoch := b.Epoch()
	// update version
	b.fork.Epoch = epoch
	b.fork.CurrentVersion = utils.Uint32ToBytes4(b.beaconConfig.CapellaForkVersion)
	// Update the payload header.
	latestExecutionPayloadHeader := cltypes.NewEth1Header(clparams.CapellaVersion)
	latestExecutionPayloadHeader.ParentHash = b.latestExecutionPayloadHeader.ParentHash
	latestExecutionPayloadHeader.FeeRecipient = b.latestExecutionPayloadHeader.FeeRecipient
	latestExecutionPayloadHeader.ReceiptsRoot = b.latestExecutionPayloadHeader.ReceiptsRoot
	latestExecutionPayloadHeader.LogsBloom = b.latestExecutionPayloadHeader.LogsBloom
	latestExecutionPayloadHeader.PrevRandao = b.latestExecutionPayloadHeader.PrevRandao
	latestExecutionPayloadHeader.BlockNumber = b.latestExecutionPayloadHeader.BlockNumber
	latestExecutionPayloadHeader.GasLimit = b.latestExecutionPayloadHeader.GasLimit
	latestExecutionPayloadHeader.GasUsed = b.latestExecutionPayloadHeader.GasUsed
	latestExecutionPayloadHeader.Time = b.latestExecutionPayloadHeader.Time
	latestExecutionPayloadHeader.Extra = b.latestExecutionPayloadHeader.Extra
	latestExecutionPayloadHeader.BaseFeePerGas = b.latestExecutionPayloadHeader.BaseFeePerGas
	latestExecutionPayloadHeader.BlockHash = b.latestExecutionPayloadHeader.BlockHash
	latestExecutionPayloadHeader.TransactionsRoot = b.latestExecutionPayloadHeader.TransactionsRoot
	b.latestExecutionPayloadHeader = latestExecutionPayloadHeader
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
