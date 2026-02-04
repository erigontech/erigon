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

package raw

import (
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/ssz"
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
	case clparams.ElectraVersion:
		return 2736653
	case clparams.FuluVersion:
		return 2736653
	case clparams.GloasVersion:
		return 2736653
	default:
		// ?????
		panic("tf is that")
	}
}

func (b *BeaconState) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.getSchema()...)
}

// getSchema gives the schema for the current beacon state version according to ETH 2.0 specs.
func (b *BeaconState) getSchema() []any {
	s := []any{&b.genesisTime, b.genesisValidatorsRoot[:], &b.slot, b.fork, b.latestBlockHeader, b.blockRoots, b.stateRoots, b.historicalRoots,
		b.eth1Data, b.eth1DataVotes, &b.eth1DepositIndex, b.validators, b.balances, b.randaoMixes, b.slashings}
	if b.version == clparams.Phase0Version {
		return append(s, b.previousEpochAttestations, b.currentEpochAttestations, &b.justificationBits, &b.previousJustifiedCheckpoint, &b.currentJustifiedCheckpoint,
			&b.finalizedCheckpoint)
	}
	s = append(s, b.previousEpochParticipation, b.currentEpochParticipation, &b.justificationBits, &b.previousJustifiedCheckpoint, &b.currentJustifiedCheckpoint,
		&b.finalizedCheckpoint, b.inactivityScores, b.currentSyncCommittee, b.nextSyncCommittee)
	if b.version >= clparams.BellatrixVersion {
		// Note: latestExecutionPayloadHeader will be removed and replaced by latestExecutionPayloadBid after Gloas fork
		if b.version >= clparams.GloasVersion {
			s = append(s, b.latestExecutionPayloadBid)
		} else {
			s = append(s, b.latestExecutionPayloadHeader)
		}
	}
	if b.version >= clparams.CapellaVersion {
		s = append(s, &b.nextWithdrawalIndex, &b.nextWithdrawalValidatorIndex, b.historicalSummaries)
	}
	if b.version >= clparams.ElectraVersion {
		// Electra fields
		s = append(s, &b.depositRequestsStartIndex, &b.depositBalanceToConsume, &b.exitBalanceToConsume, &b.earliestExitEpoch, &b.consolidationBalanceToConsume,
			&b.earliestConsolidationEpoch, b.pendingDeposits, b.pendingPartialWithdrawals, b.pendingConsolidations)
	}
	if b.version >= clparams.FuluVersion {
		s = append(s, b.proposerLookahead)
	}
	if b.version >= clparams.GloasVersion {
		s = append(s, b.builders, &b.nextWithdrawalBuilderIndex, b.executionPayloadAvailability, b.builderPendingPayments, b.builderPendingWithdrawals, b.latestBlockHash[:], b.payloadExpectedWithdrawals)
	}
	return s
}

func (b *BeaconState) DecodeSSZ(buf []byte, version int) error {
	b.version = clparams.StateVersion(version)
	if len(buf) < int(b.baseOffsetSSZ()) {
		return fmt.Errorf("[BeaconState] err: %s", ssz.ErrLowBufferSize)
	}
	if version >= int(clparams.BellatrixVersion) {
		b.latestExecutionPayloadHeader = &cltypes.Eth1Header{}
	}
	if version >= int(clparams.ElectraVersion) {
		b.pendingDeposits = solid.NewPendingDepositList(b.beaconConfig)
		b.pendingPartialWithdrawals = solid.NewPendingWithdrawalList(b.beaconConfig)
		b.pendingConsolidations = solid.NewPendingConsolidationList(b.beaconConfig)
	}
	if version >= int(clparams.FuluVersion) {
		b.proposerLookahead = solid.NewUint64VectorSSZ(int((b.beaconConfig.MinSeedLookahead + 1) * b.beaconConfig.SlotsPerEpoch))
	}
	if version >= int(clparams.GloasVersion) {
		b.latestExecutionPayloadBid = &cltypes.ExecutionPayloadBid{}
		b.builders = solid.NewStaticListSSZ[*cltypes.Builder](int(b.beaconConfig.BuilderRegistryLimit), new(cltypes.Builder).EncodingSizeSSZ())
		b.nextWithdrawalBuilderIndex = 0
		b.executionPayloadAvailability = solid.NewBitVector(int(b.beaconConfig.SlotsPerHistoricalRoot))
		b.builderPendingPayments = solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](int(2 * b.beaconConfig.SlotsPerEpoch))
		b.builderPendingWithdrawals = solid.NewStaticListSSZ[*cltypes.BuilderPendingWithdrawal](int(b.beaconConfig.BuilderPendingWithdrawalsLimit), new(cltypes.BuilderPendingWithdrawal).EncodingSizeSSZ())
		b.latestBlockHash = common.Hash{}
		b.payloadExpectedWithdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(b.beaconConfig.MaxWithdrawalsPerPayload), new(cltypes.Withdrawal).EncodingSizeSSZ())
	}
	if err := ssz2.UnmarshalSSZ(buf, version, b.getSchema()...); err != nil {
		return err
	}
	// Capella
	return b.init()
}

// SSZ size of the Beacon State
func (b *BeaconState) EncodingSizeSSZ() (size int) {
	size = int(b.baseOffsetSSZ()) + b.historicalRoots.EncodingSizeSSZ()
	size += b.eth1DataVotes.EncodingSizeSSZ()
	size += b.validators.EncodingSizeSSZ()
	size += b.balances.Length() * 8
	if b.version == clparams.Phase0Version {
		size += b.previousEpochAttestations.EncodingSizeSSZ()
		size += b.currentEpochAttestations.EncodingSizeSSZ()
	} else {
		size += b.previousEpochParticipation.Length()
		size += b.currentEpochParticipation.Length()
	}

	size += b.inactivityScores.Length() * 8
	size += b.historicalSummaries.EncodingSizeSSZ()

	if b.version >= clparams.ElectraVersion {
		// 6 uint64 fields
		size += 6 * 8
		size += b.pendingDeposits.EncodingSizeSSZ()
		size += b.pendingPartialWithdrawals.EncodingSizeSSZ()
		size += b.pendingConsolidations.EncodingSizeSSZ()
	}
	if b.version >= clparams.FuluVersion {
		size += b.proposerLookahead.EncodingSizeSSZ()
	}
	if b.version >= clparams.GloasVersion {
		// replace latestExecutionPayloadHeader with latestExecutionPayloadBid
		size -= b.latestExecutionPayloadHeader.EncodingSizeSSZ()
		size += b.latestExecutionPayloadBid.EncodingSizeSSZ()
		// new fields
		size += b.builders.EncodingSizeSSZ()
		size += 8
		size += b.executionPayloadAvailability.EncodingSizeSSZ()
		size += b.builderPendingPayments.EncodingSizeSSZ()
		size += b.builderPendingWithdrawals.EncodingSizeSSZ()
		size += 32
		size += b.payloadExpectedWithdrawals.EncodingSizeSSZ()
	}
	return
}

func (b *BeaconState) Clone() clonable.Clonable {
	return &BeaconState{beaconConfig: b.beaconConfig}
}
