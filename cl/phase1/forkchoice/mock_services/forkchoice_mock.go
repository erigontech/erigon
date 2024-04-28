package mock_services

import (
	"context"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2"
	"github.com/ledgerwatch/erigon/cl/validator/sync_contribution_pool"
	syncpoolmock "github.com/ledgerwatch/erigon/cl/validator/sync_contribution_pool/mock_services"
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

	ParticipationVal *solid.BitList

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
	}
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

func (f *ForkChoiceStorageMock) GetHead() (common.Hash, uint64, error) {
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
) (bool, solid.Checkpoint, solid.Checkpoint, solid.Checkpoint) {
	oneNil := f.GetFinalityCheckpointsVal[blockRoot][0] != nil &&
		f.GetFinalityCheckpointsVal[blockRoot][1] != nil &&
		f.GetFinalityCheckpointsVal[blockRoot][2] != nil
	return oneNil, f.GetFinalityCheckpointsVal[blockRoot][0], f.GetFinalityCheckpointsVal[blockRoot][1], f.GetFinalityCheckpointsVal[blockRoot][2]
}

func (f *ForkChoiceStorageMock) GetSyncCommittees(
	period uint64,
) (*solid.SyncCommittee, *solid.SyncCommittee, bool) {
	return f.GetSyncCommitteesVal[period][0], f.GetSyncCommitteesVal[period][1], f.GetSyncCommitteesVal[period][0] != nil &&
		f.GetSyncCommitteesVal[period][1] != nil
}

func (f *ForkChoiceStorageMock) GetBeaconCommitee(slot, committeeIndex uint64) ([]uint64, error) {
	if f.GetBeaconCommitteeMock != nil {
		return f.GetBeaconCommitteeMock(slot, committeeIndex)
	}
	return []uint64{1, 2, 3, 4, 5, 6, 7, 8}, nil
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
	f.Pool.AttestationsPool.Insert(attestation.Signature(), attestation)
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

func (f *ForkChoiceStorageMock) LowestAvaiableSlot() uint64 {
	return f.FinalizedSlotVal
}

func (f *ForkChoiceStorageMock) Partecipation(epoch uint64) (*solid.BitList, bool) {
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
	blockRoot libcommon.Hash,
) (*cltypes.BeaconBlockHeader, bool) {
	return f.Headers[blockRoot], f.Headers[blockRoot] != nil
}

func (f *ForkChoiceStorageMock) GetBalances(blockRoot libcommon.Hash) (solid.Uint64ListSSZ, error) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) GetInactivitiesScores(
	blockRoot libcommon.Hash,
) (solid.Uint64ListSSZ, error) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) GetPreviousPartecipationIndicies(
	blockRoot libcommon.Hash,
) (*solid.BitList, error) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) GetValidatorSet(
	blockRoot libcommon.Hash,
) (*solid.ValidatorSet, error) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) GetCurrentPartecipationIndicies(
	blockRoot libcommon.Hash,
) (*solid.BitList, error) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) GetPublicKeyForValidator(
	blockRoot libcommon.Hash,
	idx uint64,
) (libcommon.Bytes48, error) {
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
