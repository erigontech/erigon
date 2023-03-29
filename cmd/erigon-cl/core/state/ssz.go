package state

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/clonable"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/state_encoding"
)

func (b *BeaconState) baseOffsetSSZ() uint32 {
	switch b.version {
	case clparams.Phase0Version:
		return 2687377
	case clparams.AltairVersion:
		return 2736629
	case clparams.BellatrixVersion:
		return 2736633
	case clparams.CapellaVersion:
		return 2736653
	default:
		// ?????
		panic("tf is that")
	}
}

func (b *BeaconState) EncodeSSZ(buf []byte) ([]byte, error) {
	var (
		dst = buf
		err error
	)
	// Sanity checks
	if len(b.historicalRoots) > state_encoding.HistoricalRootsLength {
		return nil, fmt.Errorf("too many historical roots")
	}

	if len(b.historicalSummaries) > state_encoding.HistoricalRootsLength {
		return nil, fmt.Errorf("too many summaries")
	}

	if len(b.eth1DataVotes) > state_encoding.Eth1DataVotesRootsLimit {
		return nil, fmt.Errorf("too many votes")
	}

	if len(b.validators) > state_encoding.ValidatorRegistryLimit {
		return nil, fmt.Errorf("too many validators")
	}

	if len(b.balances) > state_encoding.ValidatorRegistryLimit {
		return nil, fmt.Errorf("too many balances")
	}

	maxEpochAttestations := int(b.beaconConfig.SlotsPerEpoch * b.beaconConfig.MaxAttestations)
	if b.version != clparams.Phase0Version && len(b.previousEpochParticipation) > state_encoding.ValidatorRegistryLimit || len(b.currentEpochParticipation) > state_encoding.ValidatorRegistryLimit {
		return nil, fmt.Errorf("too many participations")
	}

	if b.version == clparams.Phase0Version && len(b.previousEpochAttestations) > maxEpochAttestations || len(b.currentEpochAttestations) > maxEpochAttestations {
		return nil, fmt.Errorf("too many participations")
	}

	if len(b.inactivityScores) > state_encoding.ValidatorRegistryLimit {
		return nil, fmt.Errorf("too many inactivities scores")
	}
	// Start encoding
	offset := b.baseOffsetSSZ()

	dst = append(dst, ssz.Uint64SSZ(b.genesisTime)...)
	dst = append(dst, b.genesisValidatorsRoot[:]...)
	dst = append(dst, ssz.Uint64SSZ(b.slot)...)

	if dst, err = b.fork.EncodeSSZ(dst); err != nil {
		return nil, err
	}
	if dst, err = b.latestBlockHeader.EncodeSSZ(dst); err != nil {
		return nil, err
	}

	for _, blockRoot := range &b.blockRoots {
		dst = append(dst, blockRoot[:]...)
	}

	for _, stateRoot := range &b.stateRoots {
		dst = append(dst, stateRoot[:]...)
	}

	// Historical roots offset
	dst = append(dst, ssz.OffsetSSZ(offset)...)
	offset += uint32(len(b.historicalRoots) * 32)

	if dst, err = b.eth1Data.EncodeSSZ(dst); err != nil {
		return nil, err
	}

	// votes offset
	dst = append(dst, ssz.OffsetSSZ(offset)...)
	offset += uint32(len(b.eth1DataVotes)) * 72

	dst = append(dst, ssz.Uint64SSZ(b.eth1DepositIndex)...)

	// validators offset
	dst = append(dst, ssz.OffsetSSZ(offset)...)
	offset += uint32(len(b.validators)) * 121

	// balances offset
	dst = append(dst, ssz.OffsetSSZ(offset)...)
	offset += uint32(len(b.balances)) * 8

	for _, mix := range &b.randaoMixes {
		dst = append(dst, mix[:]...)
	}

	for _, slashing := range &b.slashings {
		dst = append(dst, ssz.Uint64SSZ(slashing)...)
	}

	// prev participation offset
	dst = append(dst, ssz.OffsetSSZ(offset)...)
	// Gotta account for phase0 format (we used to store the attestations).
	if b.version == clparams.Phase0Version {
		for _, attestation := range b.previousEpochAttestations {
			offset += uint32(attestation.EncodingSizeSSZ()) + 4
		}
	} else {
		offset += uint32(len(b.previousEpochParticipation))
	}
	// current participation offset
	dst = append(dst, ssz.OffsetSSZ(offset)...)
	// Gotta account for phase0 format (we used to store the attestations).
	if b.version == clparams.Phase0Version {
		for _, attestation := range b.currentEpochAttestations {
			offset += uint32(attestation.EncodingSizeSSZ()) + 4
		}
	} else {
		offset += uint32(len(b.currentEpochParticipation))
	}

	dst = append(dst, b.justificationBits.Byte())

	// Checkpoints
	if dst, err = b.previousJustifiedCheckpoint.EncodeSSZ(dst); err != nil {
		return nil, err
	}

	if dst, err = b.currentJustifiedCheckpoint.EncodeSSZ(dst); err != nil {
		return nil, err
	}
	if dst, err = b.finalizedCheckpoint.EncodeSSZ(dst); err != nil {
		return nil, err
	}
	if b.version >= clparams.AltairVersion {

		// Inactivity scores offset
		dst = append(dst, ssz.OffsetSSZ(offset)...)
		offset += uint32(len(b.inactivityScores)) * 8

		// Sync commitees
		if dst, err = b.currentSyncCommittee.EncodeSSZ(dst); err != nil {
			return nil, err
		}
		if dst, err = b.nextSyncCommittee.EncodeSSZ(dst); err != nil {
			return nil, err
		}

		// Offset (24) 'LatestExecutionPayloadHeader'
		dst = append(dst, ssz.OffsetSSZ(offset)...)
		if b.version >= clparams.BellatrixVersion {
			offset += uint32(b.latestExecutionPayloadHeader.EncodingSizeSSZ())
		}
	}

	if b.version >= clparams.CapellaVersion {
		dst = append(dst, ssz.Uint64SSZ(b.nextWithdrawalIndex)...)
		dst = append(dst, ssz.Uint64SSZ(b.nextWithdrawalValidatorIndex)...)
		dst = append(dst, ssz.OffsetSSZ(offset)...)
	}
	// Write historical roots (offset 1)
	for _, root := range b.historicalRoots {
		dst = append(dst, root[:]...)
	}
	// Write votes (offset 2)
	for _, vote := range b.eth1DataVotes {
		if dst, err = vote.EncodeSSZ(dst); err != nil {
			return nil, err
		}
	}
	// Write validators (offset 3)
	for _, validator := range b.validators {
		if dst, err = validator.EncodeSSZ(dst); err != nil {
			return nil, err
		}
	}
	// Write balances (offset 4)
	for _, balance := range b.balances {
		dst = append(dst, ssz.Uint64SSZ(balance)...)
	}

	// Write participations (offset 4 & 5)
	if b.version == clparams.Phase0Version {
		if dst, err = ssz.EncodeDynamicList(dst, b.previousEpochAttestations); err != nil {
			return nil, err
		}
		if dst, err = ssz.EncodeDynamicList(dst, b.currentEpochAttestations); err != nil {
			return nil, err
		}
	} else {
		dst = append(dst, b.previousEpochParticipation.Bytes()...)
		dst = append(dst, b.currentEpochParticipation.Bytes()...)
	}
	if b.version >= clparams.AltairVersion {
		// write inactivity scores (offset 6)
		for _, score := range b.inactivityScores {
			dst = append(dst, ssz.Uint64SSZ(score)...)
		}
	}

	// write execution header (offset 7)
	if b.version >= clparams.BellatrixVersion {
		if dst, err = b.latestExecutionPayloadHeader.EncodeSSZ(dst); err != nil {
			return nil, err
		}
	}

	if b.version >= clparams.CapellaVersion {
		for _, summary := range b.historicalSummaries {
			if dst, err = summary.EncodeSSZ(dst); err != nil {
				return nil, err
			}
		}
	}

	return dst, nil

}

func (b *BeaconState) DecodeSSZWithVersion(buf []byte, version int) error {
	b.version = clparams.StateVersion(version)
	if len(buf) < b.EncodingSizeSSZ() {
		return ssz.ErrLowBufferSize
	}
	// Direct unmarshalling for first 3 fields
	b.genesisTime = ssz.UnmarshalUint64SSZ(buf)
	copy(b.genesisValidatorsRoot[:], buf[8:])
	b.slot = ssz.UnmarshalUint64SSZ(buf[40:])
	// Fork data
	b.fork = new(cltypes.Fork)
	if err := b.fork.DecodeSSZ(buf[48:]); err != nil {
		return err
	}
	pos := 64
	// Latest block header
	b.latestBlockHeader = new(cltypes.BeaconBlockHeader)
	if err := b.latestBlockHeader.DecodeSSZ(buf[64:]); err != nil {
		return err
	}
	pos += b.latestBlockHeader.EncodingSizeSSZ()
	// Decode block roots
	for i := range b.blockRoots {
		copy(b.blockRoots[i][:], buf[pos:])
		pos += length.Hash
	}
	// Decode state roots
	for i := range b.stateRoots {
		copy(b.stateRoots[i][:], buf[pos:])
		pos += 32
	}
	// Read historical roots offset
	historicalRootsOffset := ssz.DecodeOffset(buf[pos:])
	pos += 4
	// Decode eth1 data
	b.eth1Data = new(cltypes.Eth1Data)
	if err := b.eth1Data.DecodeSSZ(buf[pos:]); err != nil {
		return err
	}
	pos += b.eth1Data.EncodingSizeSSZ()
	// Read votes offset
	votesOffset := ssz.DecodeOffset(buf[pos:])
	pos += 4
	// Decode deposit index
	b.eth1DepositIndex = ssz.UnmarshalUint64SSZ(buf[pos:])
	pos += 8
	// Read validators offset
	validatorsOffset := ssz.DecodeOffset(buf[pos:])
	pos += 4
	// Read balances offset
	balancesOffset := ssz.DecodeOffset(buf[pos:])
	pos += 4
	// Decode randao mixes
	for i := range b.randaoMixes {
		copy(b.randaoMixes[i][:], buf[pos:])
		pos += 32
	}
	// Decode slashings
	for i := range b.slashings {
		b.slashings[i] = ssz.UnmarshalUint64SSZ(buf[pos:])
		pos += 8
	}
	// partecipation offsets
	previousEpochParticipationOffset := ssz.DecodeOffset(buf[pos:])
	pos += 4
	currentEpochParticipationOffset := ssz.DecodeOffset(buf[pos:])
	pos += 4

	// just take that one smol byte
	b.justificationBits.FromByte(buf[pos])
	pos++
	// Decode checkpoints
	b.previousJustifiedCheckpoint = new(cltypes.Checkpoint)
	b.currentJustifiedCheckpoint = new(cltypes.Checkpoint)
	b.finalizedCheckpoint = new(cltypes.Checkpoint)
	if err := b.previousJustifiedCheckpoint.DecodeSSZ(buf[pos:]); err != nil {
		return err
	}
	pos += b.previousJustifiedCheckpoint.EncodingSizeSSZ()
	if err := b.currentJustifiedCheckpoint.DecodeSSZ(buf[pos:]); err != nil {
		return err
	}
	pos += b.currentJustifiedCheckpoint.EncodingSizeSSZ()
	if err := b.finalizedCheckpoint.DecodeSSZ(buf[pos:]); err != nil {
		return err
	}
	pos += b.finalizedCheckpoint.EncodingSizeSSZ()
	// Offset for inactivity scores
	var inactivityScoresOffset uint32
	// Decode sync committees
	if b.version >= clparams.AltairVersion {
		inactivityScoresOffset = ssz.DecodeOffset(buf[pos:])
		pos += 4
		b.currentSyncCommittee = new(cltypes.SyncCommittee)
		b.nextSyncCommittee = new(cltypes.SyncCommittee)
		if err := b.currentSyncCommittee.DecodeSSZ(buf[pos:]); err != nil {
			return err
		}
		pos += b.currentSyncCommittee.EncodingSizeSSZ()
		if err := b.nextSyncCommittee.DecodeSSZ(buf[pos:]); err != nil {
			return err
		}
		pos += b.nextSyncCommittee.EncodingSizeSSZ()
	}

	var executionPayloadOffset uint32
	// Execution Payload header offset
	if b.version >= clparams.BellatrixVersion {
		executionPayloadOffset = ssz.DecodeOffset(buf[pos:])
		pos += 4
	}
	var historicalSummariesOffset uint32
	if b.version >= clparams.CapellaVersion {
		b.nextWithdrawalIndex = ssz.UnmarshalUint64SSZ(buf[pos:])
		pos += 8
		b.nextWithdrawalValidatorIndex = ssz.UnmarshalUint64SSZ(buf[pos:])
		pos += 8
		historicalSummariesOffset = ssz.DecodeOffset(buf[pos:])
		// pos += 4
	}
	// Now decode all the lists.
	var err error
	if b.historicalRoots, err = ssz.DecodeHashList(buf, historicalRootsOffset, votesOffset, state_encoding.HistoricalRootsLength); err != nil {
		return err
	}
	if b.eth1DataVotes, err = ssz.DecodeStaticList[*cltypes.Eth1Data](buf, votesOffset, validatorsOffset, 72, maxEth1Votes); err != nil {
		return err
	}
	if b.validators, err = ssz.DecodeStaticList[*cltypes.Validator](buf, validatorsOffset, balancesOffset, 121, state_encoding.ValidatorRegistryLimit); err != nil {
		return err
	}
	if b.balances, err = ssz.DecodeNumbersList(buf, balancesOffset, previousEpochParticipationOffset, state_encoding.ValidatorRegistryLimit); err != nil {
		return err
	}
	if b.version == clparams.Phase0Version {
		maxAttestations := b.beaconConfig.SlotsPerEpoch * b.beaconConfig.MaxAttestations
		if b.previousEpochAttestations, err = ssz.DecodeDynamicList[*cltypes.PendingAttestation](buf, previousEpochParticipationOffset, currentEpochParticipationOffset, maxAttestations); err != nil {
			return err
		}
		if b.currentEpochAttestations, err = ssz.DecodeDynamicList[*cltypes.PendingAttestation](buf, currentEpochParticipationOffset, uint32(len(buf)), maxAttestations); err != nil {
			return err
		}
		return b.initBeaconState()
	} else {
		var previousEpochParticipation, currentEpochParticipation []byte
		if previousEpochParticipation, err = ssz.DecodeString(buf, uint64(previousEpochParticipationOffset), uint64(currentEpochParticipationOffset), state_encoding.ValidatorRegistryLimit); err != nil {
			return err
		}
		if currentEpochParticipation, err = ssz.DecodeString(buf, uint64(currentEpochParticipationOffset), uint64(inactivityScoresOffset), state_encoding.ValidatorRegistryLimit); err != nil {
			return err
		}
		b.previousEpochParticipation, b.currentEpochParticipation = cltypes.ParticipationFlagsListFromBytes(previousEpochParticipation), cltypes.ParticipationFlagsListFromBytes(currentEpochParticipation)

	}

	endOffset := uint32(len(buf))
	if executionPayloadOffset != 0 {
		endOffset = executionPayloadOffset
	}
	if b.inactivityScores, err = ssz.DecodeNumbersList(buf, inactivityScoresOffset, endOffset, state_encoding.ValidatorRegistryLimit); err != nil {
		return err
	}
	if b.version == clparams.AltairVersion {
		return b.initBeaconState()
	}
	endOffset = uint32(len(buf))
	if historicalSummariesOffset != 0 {
		endOffset = historicalSummariesOffset
	}

	if len(buf) < int(endOffset) || executionPayloadOffset > endOffset {
		return ssz.ErrLowBufferSize
	}
	b.latestExecutionPayloadHeader = new(cltypes.Eth1Header)
	if err := b.latestExecutionPayloadHeader.DecodeSSZWithVersion(buf[executionPayloadOffset:endOffset], int(b.version)); err != nil {
		return err
	}
	if b.version == clparams.BellatrixVersion {
		return b.initBeaconState()
	}
	if b.historicalSummaries, err = ssz.DecodeStaticList[*cltypes.HistoricalSummary](buf, historicalSummariesOffset, uint32(len(buf)), 64, state_encoding.HistoricalRootsLength); err != nil {
		return err
	}
	// Capella
	return b.initBeaconState()
}

// SSZ size of the Beacon State
func (b *BeaconState) EncodingSizeSSZ() (size int) {
	size = int(b.baseOffsetSSZ()) + (len(b.historicalRoots) * 32)
	size += len(b.eth1DataVotes) * 72
	size += len(b.validators) * 121
	size += len(b.balances) * 8
	if b.version == clparams.Phase0Version {
		for _, pendingAttestation := range b.previousEpochAttestations {
			size += pendingAttestation.EncodingSizeSSZ()
		}
		for _, pendingAttestation := range b.currentEpochAttestations {
			size += pendingAttestation.EncodingSizeSSZ()
		}
	} else {
		size += len(b.previousEpochParticipation)
		size += len(b.currentEpochParticipation)
	}

	size += len(b.inactivityScores) * 8
	size += len(b.historicalSummaries) * 64
	return
}

func (b *BeaconState) Clone() clonable.Clonable {
	return &BeaconState{beaconConfig: b.beaconConfig}
}
