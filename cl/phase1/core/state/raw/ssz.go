package raw

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/phase1/core/state/state_encoding"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"

	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

// BlockRoot computes the block root for the state.
func (b *BeaconState) BlockRoot() ([32]byte, error) {
	stateRoot, err := b.HashSSZ()
	if err != nil {
		return [32]byte{}, err
	}
	return (&cltypes.BeaconBlockHeader{
		Slot:          b.latestBlockHeader.Slot,
		ProposerIndex: b.latestBlockHeader.ProposerIndex,
		BodyRoot:      b.latestBlockHeader.BodyRoot,
		ParentRoot:    b.latestBlockHeader.ParentRoot,
		Root:          stateRoot,
	}).HashSSZ()
}

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
	case clparams.DenebVersion:
		return 2736653
	default:
		// ?????
		panic("tf is that")
	}
}

func (b *BeaconState) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.Encode(buf, b.getSchema()...)
}

func (b *BeaconState) getSchema() []interface{} {
	s := []interface{}{&b.genesisTime, b.genesisValidatorsRoot[:], &b.slot, b.fork, b.latestBlockHeader, b.blockRoots, b.stateRoots, b.historicalRoots,
		b.eth1Data, b.eth1DataVotes, &b.eth1DepositIndex, b.validators, b.balances, b.randaoMixes, b.slashings}
	if b.version == clparams.Phase0Version {
		return append(s, b.previousEpochAttestations, b.currentEpochAttestations, b.justificationBits)
	}
	s = append(s, b.previousEpochParticipation, b.currentEpochParticipation, b.justificationBits, b.previousJustifiedCheckpoint, b.currentJustifiedCheckpoint,
		b.finalizedCheckpoint, b.inactivityScores, b.currentSyncCommittee, b.nextSyncCommittee)
	if b.version >= clparams.BellatrixVersion {
		s = append(s, b.latestBlockHeader)
	}
	if b.version >= clparams.CapellaVersion {
		s = append(s, &b.nextWithdrawalIndex, &b.nextWithdrawalValidatorIndex, b.historicalSummaries)
	}
	return s
}

func (b *BeaconState) DecodeSSZ(buf []byte, version int) error {
	b.version = clparams.StateVersion(version)
	if len(buf) < b.EncodingSizeSSZ() {
		return fmt.Errorf("[BeaconState] err: %s", ssz.ErrLowBufferSize)
	}
	// Direct unmarshalling for first 3 fields
	b.genesisTime = ssz.UnmarshalUint64SSZ(buf)
	copy(b.genesisValidatorsRoot[:], buf[8:])
	b.slot = ssz.UnmarshalUint64SSZ(buf[40:])
	// Fork data
	b.fork = new(cltypes.Fork)
	if err := b.fork.DecodeSSZ(buf[48:], version); err != nil {
		return err
	}
	pos := 64
	// Latest block header
	b.latestBlockHeader = new(cltypes.BeaconBlockHeader)
	if err := b.latestBlockHeader.DecodeSSZ(buf[64:], version); err != nil {
		return err
	}
	pos += b.latestBlockHeader.EncodingSizeSSZ()
	if b.blockRoots == nil {
		b.blockRoots = solid.NewHashVector(blockRootsLength)
	}
	// Decode block roots
	if err := b.blockRoots.DecodeSSZ(buf[pos:], version); err != nil {
		return err
	}
	pos += b.blockRoots.EncodingSizeSSZ()
	if b.stateRoots == nil {
		b.stateRoots = solid.NewHashVector(stateRootsLength)
	}
	// Decode state roots
	if err := b.stateRoots.DecodeSSZ(buf[pos:], version); err != nil {
		return err
	}
	pos += b.stateRoots.EncodingSizeSSZ()

	// Read historical roots offset
	historicalRootsOffset := ssz.DecodeOffset(buf[pos:])
	pos += 4
	// Decode eth1 data
	b.eth1Data = new(cltypes.Eth1Data)
	if err := b.eth1Data.DecodeSSZ(buf[pos:], version); err != nil {
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
	if b.randaoMixes == nil {
		b.randaoMixes = solid.NewHashVector(randoMixesLength)
	}
	// Decode randao mixes
	if err := b.randaoMixes.DecodeSSZ(buf[pos:], version); err != nil {
		return err
	}
	pos += b.randaoMixes.EncodingSizeSSZ()
	if b.slashings == nil {
		b.slashings = solid.NewUint64VectorSSZ(slashingsLength)
	}
	// Decode slashings
	if err := b.slashings.DecodeSSZ(buf[pos:], version); err != nil {
		return err
	}
	pos += b.slashings.EncodingSizeSSZ()
	// partecipation offsets
	previousEpochParticipationOffset := ssz.DecodeOffset(buf[pos:])
	pos += 4
	currentEpochParticipationOffset := ssz.DecodeOffset(buf[pos:])
	pos += 4

	// just take that one smol byte
	b.justificationBits.DecodeSSZ(buf[pos:], version)
	pos++
	// Decode checkpoints
	b.previousJustifiedCheckpoint = solid.NewCheckpoint()
	b.currentJustifiedCheckpoint = solid.NewCheckpoint()
	b.finalizedCheckpoint = solid.NewCheckpoint()
	if err := b.previousJustifiedCheckpoint.DecodeSSZ(buf[pos:], version); err != nil {
		return err
	}
	pos += b.previousJustifiedCheckpoint.EncodingSizeSSZ()
	if err := b.currentJustifiedCheckpoint.DecodeSSZ(buf[pos:], version); err != nil {
		return err
	}
	pos += b.currentJustifiedCheckpoint.EncodingSizeSSZ()
	if err := b.finalizedCheckpoint.DecodeSSZ(buf[pos:], version); err != nil {
		return err
	}
	pos += b.finalizedCheckpoint.EncodingSizeSSZ()
	// Offset for inactivity scores
	var inactivityScoresOffset uint32
	// Decode sync committees
	if b.version >= clparams.AltairVersion {
		inactivityScoresOffset = ssz.DecodeOffset(buf[pos:])
		pos += 4
		b.currentSyncCommittee = new(solid.SyncCommittee)
		b.nextSyncCommittee = new(solid.SyncCommittee)
		if err := b.currentSyncCommittee.DecodeSSZ(buf[pos:], version); err != nil {
			return err
		}
		pos += b.currentSyncCommittee.EncodingSizeSSZ()
		if err := b.nextSyncCommittee.DecodeSSZ(buf[pos:], version); err != nil {
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
	if b.historicalRoots == nil {
		b.historicalRoots = solid.NewHashList(int(b.beaconConfig.HistoricalRootsLimit))
	}
	var err error
	if err = b.historicalRoots.DecodeSSZ(buf[historicalRootsOffset:votesOffset], version); err != nil {
		return err
	}
	if b.eth1DataVotes == nil {
		b.eth1DataVotes = solid.NewStaticListSSZ[*cltypes.Eth1Data](int(b.beaconConfig.Eth1DataVotesLength()), 72)
	}
	if err = b.eth1DataVotes.DecodeSSZ(buf[votesOffset:validatorsOffset], version); err != nil {
		return err
	}
	if b.validators == nil {
		b.validators = cltypes.NewValidatorSet(int(b.beaconConfig.ValidatorRegistryLimit))
	}
	if b.validators.DecodeSSZ(buf[validatorsOffset:balancesOffset], version); err != nil {
		return err
	}
	if b.balances == nil {
		b.balances = solid.NewUint64ListSSZ(state_encoding.ValidatorRegistryLimit)
	}
	if err := b.balances.DecodeSSZ(buf[balancesOffset:previousEpochParticipationOffset], version); err != nil {
		return err
	}

	if b.version == clparams.Phase0Version {
		b.previousEpochAttestations = solid.NewDynamicListSSZ[*solid.PendingAttestation](int(b.BeaconConfig().PreviousEpochAttestationsLength()))
		b.currentEpochAttestations = solid.NewDynamicListSSZ[*solid.PendingAttestation](int(b.BeaconConfig().CurrentEpochAttestationsLength()))

		if err = b.previousEpochAttestations.DecodeSSZ(buf[previousEpochParticipationOffset:currentEpochParticipationOffset], version); err != nil {
			return err
		}
		if err = b.currentEpochAttestations.DecodeSSZ(buf[currentEpochParticipationOffset:uint32(len(buf))], version); err != nil {
			return err
		}
		return b.init()
	} else {
		var previousEpochParticipation, currentEpochParticipation []byte
		if previousEpochParticipation, err = ssz.DecodeString(buf, uint64(previousEpochParticipationOffset), uint64(currentEpochParticipationOffset), state_encoding.ValidatorRegistryLimit); err != nil {
			return err
		}
		if currentEpochParticipation, err = ssz.DecodeString(buf, uint64(currentEpochParticipationOffset), uint64(inactivityScoresOffset), state_encoding.ValidatorRegistryLimit); err != nil {
			return err
		}
		previousEpochParticipationCopy := make([]byte, len(previousEpochParticipation))
		copy(previousEpochParticipationCopy, previousEpochParticipation)

		currentEpochParticipationCopy := make([]byte, len(currentEpochParticipation))
		copy(currentEpochParticipationCopy, currentEpochParticipation)

		b.previousEpochParticipation = solid.BitlistFromBytes(previousEpochParticipationCopy, state_encoding.ValidatorRegistryLimit)
		b.currentEpochParticipation = solid.BitlistFromBytes(currentEpochParticipationCopy, state_encoding.ValidatorRegistryLimit)
	}

	endOffset := uint32(len(buf))
	if executionPayloadOffset != 0 {
		endOffset = executionPayloadOffset
	}
	if b.inactivityScores == nil {
		b.inactivityScores = solid.NewUint64ListSSZ(state_encoding.ValidatorRegistryLimit)
	}

	if err := b.inactivityScores.DecodeSSZ(buf[inactivityScoresOffset:endOffset], version); err != nil {
		return err
	}

	if b.version == clparams.AltairVersion {
		return b.init()
	}
	endOffset = uint32(len(buf))
	if historicalSummariesOffset != 0 {
		endOffset = historicalSummariesOffset
	}
	if len(buf) < int(endOffset) || executionPayloadOffset > endOffset {
		return fmt.Errorf("[BeaconState] err: %s", ssz.ErrLowBufferSize)
	}
	b.latestExecutionPayloadHeader = new(cltypes.Eth1Header)
	if err := b.latestExecutionPayloadHeader.DecodeSSZ(buf[executionPayloadOffset:endOffset], int(b.version)); err != nil {
		return err
	}
	if b.version == clparams.BellatrixVersion {
		return b.init()
	}
	if b.historicalSummaries == nil {
		b.historicalSummaries = solid.NewStaticListSSZ[*cltypes.HistoricalSummary](int(b.beaconConfig.HistoricalRootsLimit), 64)
	}

	if err := b.historicalSummaries.DecodeSSZ(buf[historicalSummariesOffset:], version); err != nil {
		return err
	}
	// Capella
	return b.init()
}

// SSZ size of the Beacon State
func (b *BeaconState) EncodingSizeSSZ() (size int) {
	if b.historicalRoots == nil {
		b.historicalRoots = solid.NewHashList(int(b.beaconConfig.HistoricalRootsLimit))
	}
	size = int(b.baseOffsetSSZ()) + b.historicalRoots.EncodingSizeSSZ()
	if b.eth1DataVotes == nil {
		b.eth1DataVotes = solid.NewStaticListSSZ[*cltypes.Eth1Data](int(b.beaconConfig.Eth1DataVotesLength()), 72)
	}
	size += b.eth1DataVotes.EncodingSizeSSZ()
	if b.validators == nil {
		b.validators = cltypes.NewValidatorSet(int(b.beaconConfig.ValidatorRegistryLimit))
	}
	size += b.validators.EncodingSizeSSZ()
	size += b.balances.Length() * 8
	if b.previousEpochAttestations == nil {
		b.previousEpochAttestations = solid.NewDynamicListSSZ[*solid.PendingAttestation](int(b.BeaconConfig().PreviousEpochAttestationsLength()))
	}
	if b.currentEpochAttestations == nil {
		b.currentEpochAttestations = solid.NewDynamicListSSZ[*solid.PendingAttestation](int(b.BeaconConfig().CurrentEpochAttestationsLength()))
	}
	if b.version == clparams.Phase0Version {
		size += b.previousEpochAttestations.EncodingSizeSSZ()
		size += b.currentEpochAttestations.EncodingSizeSSZ()
	} else {
		size += b.previousEpochParticipation.Length()
		size += b.currentEpochParticipation.Length()
	}

	size += b.inactivityScores.Length() * 8
	if b.historicalSummaries == nil {
		b.historicalSummaries = solid.NewStaticListSSZ[*cltypes.HistoricalSummary](int(b.beaconConfig.HistoricalRootsLimit), 64)
	}
	size += b.historicalSummaries.EncodingSizeSSZ()
	return
}

func (b *BeaconState) Clone() clonable.Clonable {
	return &BeaconState{beaconConfig: b.beaconConfig}
}
