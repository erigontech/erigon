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

package mock_services

import (
	"context"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/das/mock_services"
	peerdasstatemock "github.com/erigontech/erigon/cl/das/state/mock_services"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/validator/sync_contribution_pool"
	syncpoolmock "github.com/erigontech/erigon/cl/validator/sync_contribution_pool/mock_services"
)

// Make mocks with maps and simple setters and getters, panic on methods from ForkChoiceStorageWriter

type ForkChoiceStorageMock struct {
	Ancestors              map[uint64]common.Hash
	AnchorSlotVal          uint64
	FinalizedCheckpointVal solid.Checkpoint
	FinalizedSlotVal       uint64
	HeadVal                common.Hash
	HeadSlotVal            uint64
	HighestSeenVal         uint64
	JustifiedCheckpointVal solid.Checkpoint
	JustifiedSlotVal       uint64
	ProposerBoostRootVal   common.Hash
	SlotVal                uint64
	TimeVal                uint64

	ParticipationVal *solid.ParticipationBitList

	StateAtBlockRootVal       map[common.Hash]*state.CachingBeaconState
	StateAtSlotVal            map[uint64]*state.CachingBeaconState
	GetSyncCommitteesVal      map[uint64][2]*solid.SyncCommittee
	GetFinalityCheckpointsVal map[common.Hash][3]solid.Checkpoint
	WeightsMock               []forkchoice.ForkNode
	LightClientBootstraps     map[common.Hash]*cltypes.LightClientBootstrap
	NewestLCUpdate            *cltypes.LightClientUpdate
	LCUpdates                 map[uint64]*cltypes.LightClientUpdate
	SyncContributionPool      sync_contribution_pool.SyncContributionPool
	Headers                   map[common.Hash]*cltypes.BeaconBlockHeader
	GetBeaconCommitteeMock    func(slot, committeeIndex uint64) ([]uint64, error)

	Pool pool.OperationsPool

	// Mock for PeerDas
	MockPeerDas *mock_services.MockPeerDas
}

func makeSyncContributionPoolMock(t *testing.T) sync_contribution_pool.SyncContributionPool {
	ctrl := gomock.NewController(t)
	type syncContributionKey struct {
		slot              uint64
		subcommitteeIndex uint64
		beaconBlockRoot   common.Hash
	}
	u := map[syncContributionKey]*cltypes.Contribution{}
	pool := syncpoolmock.NewMockSyncContributionPool(ctrl)
	pool.EXPECT().
		AddSyncContribution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(headState *state.CachingBeaconState, contribution *cltypes.Contribution) error {
			key := syncContributionKey{
				slot:              contribution.Slot,
				subcommitteeIndex: contribution.SubcommitteeIndex,
				beaconBlockRoot:   contribution.BeaconBlockRoot,
			}
			u[key] = contribution
			return nil
		}).
		AnyTimes()
	pool.EXPECT().
		GetSyncContribution(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(slot uint64, subcommitteeIndex uint64, beaconBlockRoot common.Hash) *cltypes.Contribution {
			key := syncContributionKey{
				slot:              slot,
				subcommitteeIndex: subcommitteeIndex,
				beaconBlockRoot:   beaconBlockRoot,
			}
			return u[key]
		}).
		AnyTimes()
	pool.EXPECT().
		AddSyncCommitteeMessage(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(headState *state.CachingBeaconState, subCommitee uint64, message *cltypes.SyncCommitteeMessage) error {
			key := syncContributionKey{
				slot:              message.Slot,
				subcommitteeIndex: subCommitee,
				beaconBlockRoot:   message.BeaconBlockRoot,
			}
			u[key] = &cltypes.Contribution{
				Slot:              message.Slot,
				SubcommitteeIndex: subCommitee,
				BeaconBlockRoot:   message.BeaconBlockRoot,
				AggregationBits:   make([]byte, cltypes.SyncCommitteeAggregationBitsSize),
			}
			return nil
		}).AnyTimes()
	return pool
}

func NewForkChoiceStorageMock(t *testing.T) *ForkChoiceStorageMock {
	ctrl := gomock.NewController(t)
	mockPeerDas := mock_services.NewMockPeerDas(ctrl)
	mockPeerDasStateReader := peerdasstatemock.NewMockPeerDasStateReader(ctrl)

	// Set up default expectations for the mock
	mockPeerDas.EXPECT().
		DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	mockPeerDas.EXPECT().
		IsDataAvailable(gomock.Any(), gomock.Any()).
		Return(true, nil).
		AnyTimes()
	mockPeerDas.EXPECT().
		Prune(gomock.Any()).
		Return(nil).
		AnyTimes()
	mockPeerDas.EXPECT().
		UpdateValidatorsCustody(gomock.Any()).
		AnyTimes()
	mockPeerDas.EXPECT().
		TryScheduleRecover(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	mockPeerDas.EXPECT().
		StateReader().
		Return(mockPeerDasStateReader).
		AnyTimes()

	// Set up default expectations for the PeerDasStateReader mock
	mockPeerDasStateReader.EXPECT().
		GetEarliestAvailableSlot().
		Return(uint64(0)).
		AnyTimes()
	mockPeerDasStateReader.EXPECT().
		GetRealCgc().
		Return(uint64(0)).
		AnyTimes()
	mockPeerDasStateReader.EXPECT().
		GetAdvertisedCgc().
		Return(uint64(0)).
		AnyTimes()

	return &ForkChoiceStorageMock{
		Ancestors:                 make(map[uint64]common.Hash),
		AnchorSlotVal:             0,
		FinalizedCheckpointVal:    solid.Checkpoint{},
		FinalizedSlotVal:          0,
		HeadVal:                   common.Hash{},
		HighestSeenVal:            0,
		JustifiedCheckpointVal:    solid.Checkpoint{},
		JustifiedSlotVal:          0,
		ProposerBoostRootVal:      common.Hash{},
		SlotVal:                   0,
		TimeVal:                   0,
		StateAtBlockRootVal:       make(map[common.Hash]*state.CachingBeaconState),
		StateAtSlotVal:            make(map[uint64]*state.CachingBeaconState),
		GetSyncCommitteesVal:      make(map[uint64][2]*solid.SyncCommittee),
		GetFinalityCheckpointsVal: make(map[common.Hash][3]solid.Checkpoint),
		LightClientBootstraps:     make(map[common.Hash]*cltypes.LightClientBootstrap),
		LCUpdates:                 make(map[uint64]*cltypes.LightClientUpdate),
		Headers:                   make(map[common.Hash]*cltypes.BeaconBlockHeader),
		GetBeaconCommitteeMock:    nil,
		SyncContributionPool:      makeSyncContributionPoolMock(t),
		MockPeerDas:               mockPeerDas,
	}
}

func (f *ForkChoiceStorageMock) GetPeerDas() das.PeerDas {
	return f.MockPeerDas
}

func (f *ForkChoiceStorageMock) Ancestor(root common.Hash, slot uint64) common.Hash {
	return f.Ancestors[slot]
}

func (f *ForkChoiceStorageMock) AnchorSlot() uint64 {
	return f.AnchorSlotVal
}

func (f *ForkChoiceStorageMock) Engine() execution_client.ExecutionEngine {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) FinalizedCheckpoint() solid.Checkpoint {
	return f.FinalizedCheckpointVal
}

func (f *ForkChoiceStorageMock) FinalizedSlot() uint64 {
	return f.FinalizedSlotVal
}

func (f *ForkChoiceStorageMock) GetEth1Hash(eth2Root common.Hash) common.Hash {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) GetHead(_ *state.CachingBeaconState) (common.Hash, uint64, error) {
	return f.HeadVal, f.HeadSlotVal, nil
}

func (f *ForkChoiceStorageMock) HighestSeen() uint64 {
	return f.HighestSeenVal
}

func (f *ForkChoiceStorageMock) JustifiedCheckpoint() solid.Checkpoint {
	return f.JustifiedCheckpointVal
}

func (f *ForkChoiceStorageMock) JustifiedSlot() uint64 {
	return f.JustifiedSlotVal
}

func (f *ForkChoiceStorageMock) ProposerBoostRoot() common.Hash {
	return f.ProposerBoostRootVal
}

func (f *ForkChoiceStorageMock) GetStateAtBlockRoot(
	blockRoot common.Hash,
	alwaysCopy bool,
) (*state.CachingBeaconState, error) {
	return f.StateAtBlockRootVal[blockRoot], nil
}

func (f *ForkChoiceStorageMock) GetFinalityCheckpoints(
	blockRoot common.Hash,
) (solid.Checkpoint, solid.Checkpoint, solid.Checkpoint, bool) {
	oneNil := f.GetFinalityCheckpointsVal[blockRoot][0] != solid.Checkpoint{} &&
		f.GetFinalityCheckpointsVal[blockRoot][1] != solid.Checkpoint{} &&
		f.GetFinalityCheckpointsVal[blockRoot][2] != solid.Checkpoint{}

	return f.GetFinalityCheckpointsVal[blockRoot][0], f.GetFinalityCheckpointsVal[blockRoot][1], f.GetFinalityCheckpointsVal[blockRoot][2], oneNil
}

func (f *ForkChoiceStorageMock) GetSyncCommittees(
	period uint64,
) (*solid.SyncCommittee, *solid.SyncCommittee, bool) {
	return f.GetSyncCommitteesVal[period][0], f.GetSyncCommitteesVal[period][1], f.GetSyncCommitteesVal[period][0] != nil &&
		f.GetSyncCommitteesVal[period][1] != nil
}

func (f *ForkChoiceStorageMock) Slot() uint64 {
	return f.SlotVal
}

func (f *ForkChoiceStorageMock) Time() uint64 {
	return f.TimeVal
}

func (f *ForkChoiceStorageMock) OnAttestation(
	attestation *solid.Attestation,
	fromBlock, insert bool,
) error {
	f.Pool.AttestationsPool.Insert(attestation.Signature, attestation)
	return nil
}

func (f *ForkChoiceStorageMock) OnAttesterSlashing(
	attesterSlashing *cltypes.AttesterSlashing,
	test bool,
) error {
	f.Pool.AttesterSlashingsPool.Insert(
		pool.ComputeKeyForAttesterSlashing(attesterSlashing),
		attesterSlashing,
	)
	return nil
}

func (f *ForkChoiceStorageMock) OnBlock(
	ctx context.Context,
	block *cltypes.SignedBeaconBlock,
	newPayload bool,
	fullValidation bool,
	checkDataAvaiability bool,
) error {
	return nil
}

func (f *ForkChoiceStorageMock) OnTick(time uint64) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) BlockRewards(root common.Hash) (*eth2.BlockRewardsCollector, bool) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) TotalActiveBalance(root common.Hash) (uint64, bool) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) RandaoMixes(blockRoot common.Hash, out solid.HashListSSZ) bool {
	return false
}

func (f *ForkChoiceStorageMock) LowestAvailableSlot() uint64 {
	return f.FinalizedSlotVal
}

func (f *ForkChoiceStorageMock) Participation(epoch uint64) (*solid.ParticipationBitList, bool) {
	return f.ParticipationVal, f.ParticipationVal != nil
}

func (f *ForkChoiceStorageMock) ForkNodes() []forkchoice.ForkNode {
	return f.WeightsMock
}

func (f *ForkChoiceStorageMock) Synced() bool {
	return true
}

func (f *ForkChoiceStorageMock) SetSynced(synced bool) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) GetLightClientBootstrap(
	blockRoot common.Hash,
) (*cltypes.LightClientBootstrap, bool) {
	return f.LightClientBootstraps[blockRoot], f.LightClientBootstraps[blockRoot] != nil
}

func (f *ForkChoiceStorageMock) NewestLightClientUpdate() *cltypes.LightClientUpdate {
	return f.NewestLCUpdate
}

func (f *ForkChoiceStorageMock) GetLightClientUpdate(
	period uint64,
) (*cltypes.LightClientUpdate, bool) {
	return f.LCUpdates[period], f.LCUpdates[period] != nil
}

func (f *ForkChoiceStorageMock) GetHeader(
	blockRoot common.Hash,
) (*cltypes.BeaconBlockHeader, bool) {
	return f.Headers[blockRoot], f.Headers[blockRoot] != nil
}

func (f *ForkChoiceStorageMock) GetBalances(blockRoot common.Hash) (solid.Uint64ListSSZ, error) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) GetInactivitiesScores(
	blockRoot common.Hash,
) (solid.Uint64ListSSZ, error) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) GetPreviousParticipationIndicies(
	blockRoot common.Hash,
) (*solid.ParticipationBitList, error) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) GetValidatorSet(
	blockRoot common.Hash,
) (*solid.ValidatorSet, error) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) GetCurrentParticipationIndicies(
	blockRoot common.Hash,
) (*solid.ParticipationBitList, error) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) GetPublicKeyForValidator(
	blockRoot common.Hash,
	idx uint64,
) (common.Bytes48, error) {
	panic("implement me")
}

// func (f *ForkChoiceStorageMock) OnSignedContributionAndProof(signedContribution *cltypes.SignedContributionAndProof, test bool) error {
// 	f.SyncContributionPool.AddSyncContribution(nil, signedContribution.Message.Contribution)
// 	return nil
// }

func (f *ForkChoiceStorageMock) AddPreverifiedBlobSidecar(msg *cltypes.BlobSidecar) error {
	return nil
}
func (f *ForkChoiceStorageMock) ValidateOnAttestation(attestation *solid.Attestation) error {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) ProcessAttestingIndicies(
	attestation *solid.Attestation,
	attestionIndicies []uint64,
) {
}

func (f *ForkChoiceStorageMock) IsRootOptimistic(root common.Hash) bool {
	return false
}

func (f *ForkChoiceStorageMock) IsHeadOptimistic() bool {
	return false
}

func (f *ForkChoiceStorageMock) GetPendingConsolidations(blockRoot common.Hash) (*solid.ListSSZ[*solid.PendingConsolidation], bool) {
	return nil, false
}

func (f *ForkChoiceStorageMock) GetPendingDeposits(blockRoot common.Hash) (*solid.ListSSZ[*solid.PendingDeposit], bool) {
	return nil, false
}

func (f *ForkChoiceStorageMock) GetPendingPartialWithdrawals(blockRoot common.Hash) (*solid.ListSSZ[*solid.PendingPartialWithdrawal], bool) {
	return nil, false
}

func (f *ForkChoiceStorageMock) GetProposerLookahead(slot uint64) (solid.Uint64VectorSSZ, bool) {
	return nil, false
}
