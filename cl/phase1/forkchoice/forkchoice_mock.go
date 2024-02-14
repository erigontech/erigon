package forkchoice

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2"
)

// type ForkChoiceStorage interface {
// 	ForkChoiceStorageWriter
// 	ForkChoiceStorageReader
// }

// type ForkChoiceStorageReader interface {
// 	Ancestor(root common.Hash, slot uint64) common.Hash
// 	AnchorSlot() uint64
// 	Engine() execution_client.ExecutionEngine
// 	FinalizedCheckpoint() solid.Checkpoint
// 	FinalizedSlot() uint64
// 	GetEth1Hash(eth2Root common.Hash) common.Hash
// 	GetHead() (common.Hash, uint64, error)
// 	HighestSeen() uint64
// 	JustifiedCheckpoint() solid.Checkpoint
// 	JustifiedSlot() uint64
// 	ProposerBoostRoot() common.Hash
// 	GetStateAtBlockRoot(blockRoot libcommon.Hash, alwaysCopy bool) (*state.CachingBeaconState, error)
// 	GetFinalityCheckpoints(blockRoot libcommon.Hash) (bool, solid.Checkpoint, solid.Checkpoint, solid.Checkpoint)
// 	GetSyncCommittees(blockRoot libcommon.Hash) (*solid.SyncCommittee, *solid.SyncCommittee, bool)
// 	Slot() uint64
// 	Time() uint64

// 	GetStateAtSlot(slot uint64, alwaysCopy bool) (*state.CachingBeaconState, error)
// 	GetStateAtStateRoot(root libcommon.Hash, alwaysCopy bool) (*state.CachingBeaconState, error)
// }

// type ForkChoiceStorageWriter interface {
// 	OnAttestation(attestation *solid.Attestation, fromBlock bool) error
// 	OnAttesterSlashing(attesterSlashing *cltypes.AttesterSlashing, test bool) error
// 	OnBlock(block *cltypes.SignedBeaconBlock, newPayload bool, fullValidation bool) error
// 	OnTick(time uint64)
// }

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
	GetSyncCommitteesVal      map[common.Hash][2]*solid.SyncCommittee
	GetFinalityCheckpointsVal map[common.Hash][3]solid.Checkpoint
	WeightsMock               []ForkNode
	LightClientBootstraps     map[common.Hash]*cltypes.LightClientBootstrap
	NewestLCUpdate            *cltypes.LightClientUpdate
	LCUpdates                 map[uint64]*cltypes.LightClientUpdate

	Pool pool.OperationsPool
}

func NewForkChoiceStorageMock() *ForkChoiceStorageMock {
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
		GetSyncCommitteesVal:      make(map[common.Hash][2]*solid.SyncCommittee),
		GetFinalityCheckpointsVal: make(map[common.Hash][3]solid.Checkpoint),
		LightClientBootstraps:     make(map[common.Hash]*cltypes.LightClientBootstrap),
		LCUpdates:                 make(map[uint64]*cltypes.LightClientUpdate),
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

func (f *ForkChoiceStorageMock) GetStateAtBlockRoot(blockRoot common.Hash, alwaysCopy bool) (*state.CachingBeaconState, error) {
	return f.StateAtBlockRootVal[blockRoot], nil
}

func (f *ForkChoiceStorageMock) GetFinalityCheckpoints(blockRoot common.Hash) (bool, solid.Checkpoint, solid.Checkpoint, solid.Checkpoint) {
	oneNil := f.GetFinalityCheckpointsVal[blockRoot][0] != nil && f.GetFinalityCheckpointsVal[blockRoot][1] != nil && f.GetFinalityCheckpointsVal[blockRoot][2] != nil
	return oneNil, f.GetFinalityCheckpointsVal[blockRoot][0], f.GetFinalityCheckpointsVal[blockRoot][1], f.GetFinalityCheckpointsVal[blockRoot][2]
}

func (f *ForkChoiceStorageMock) GetSyncCommittees(blockRoot common.Hash) (*solid.SyncCommittee, *solid.SyncCommittee, bool) {
	return f.GetSyncCommitteesVal[blockRoot][0], f.GetSyncCommitteesVal[blockRoot][1], f.GetSyncCommitteesVal[blockRoot][0] != nil && f.GetSyncCommitteesVal[blockRoot][1] != nil
}

func (f *ForkChoiceStorageMock) GetStateAtSlot(slot uint64, alwaysCopy bool) (*state.CachingBeaconState, error) {
	return f.StateAtSlotVal[slot], nil
}

func (f *ForkChoiceStorageMock) Slot() uint64 {
	return f.SlotVal
}

func (f *ForkChoiceStorageMock) Time() uint64 {
	return f.TimeVal
}

func (f *ForkChoiceStorageMock) OnAttestation(attestation *solid.Attestation, fromBlock, insert bool) error {
	f.Pool.AttestationsPool.Insert(attestation.Signature(), attestation)
	return nil
}

func (f *ForkChoiceStorageMock) OnAttesterSlashing(attesterSlashing *cltypes.AttesterSlashing, test bool) error {
	f.Pool.AttesterSlashingsPool.Insert(pool.ComputeKeyForAttesterSlashing(attesterSlashing), attesterSlashing)
	return nil
}

func (f *ForkChoiceStorageMock) OnBlock(block *cltypes.SignedBeaconBlock, newPayload bool, fullValidation bool) error {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) OnTick(time uint64) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) GetStateAtStateRoot(root common.Hash, alwaysCopy bool) (*state.CachingBeaconState, error) {
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

func (f *ForkChoiceStorageMock) OnVoluntaryExit(signedVoluntaryExit *cltypes.SignedVoluntaryExit, test bool) error {
	f.Pool.VoluntaryExistsPool.Insert(signedVoluntaryExit.VoluntaryExit.ValidatorIndex, signedVoluntaryExit)
	return nil
}

func (f *ForkChoiceStorageMock) OnProposerSlashing(proposerSlashing *cltypes.ProposerSlashing, test bool) error {
	f.Pool.ProposerSlashingsPool.Insert(pool.ComputeKeyForProposerSlashing(proposerSlashing), proposerSlashing)
	return nil
}

func (f *ForkChoiceStorageMock) OnBlsToExecutionChange(signedChange *cltypes.SignedBLSToExecutionChange, test bool) error {
	f.Pool.BLSToExecutionChangesPool.Insert(signedChange.Signature, signedChange)
	return nil
}

func (f *ForkChoiceStorageMock) ForkNodes() []ForkNode {
	return f.WeightsMock
}

func (f *ForkChoiceStorageMock) OnAggregateAndProof(aggregateAndProof *cltypes.SignedAggregateAndProof, test bool) error {
	f.Pool.AttestationsPool.Insert(aggregateAndProof.Message.Aggregate.Signature(), aggregateAndProof.Message.Aggregate)
	return nil
}

func (f *ForkChoiceStorageMock) Synced() bool {
	return true
}

func (f *ForkChoiceStorageMock) SetSynced(synced bool) {
	panic("implement me")
}

func (f *ForkChoiceStorageMock) GetLightClientBootstrap(blockRoot common.Hash) (*cltypes.LightClientBootstrap, bool) {
	return f.LightClientBootstraps[blockRoot], f.LightClientBootstraps[blockRoot] != nil
}

func (f *ForkChoiceStorageMock) NewestLightClientUpdate() *cltypes.LightClientUpdate {
	return f.NewestLCUpdate
}

func (f *ForkChoiceStorageMock) GetLightClientUpdate(period uint64) (*cltypes.LightClientUpdate, bool) {
	return f.LCUpdates[period], f.LCUpdates[period] != nil
}
