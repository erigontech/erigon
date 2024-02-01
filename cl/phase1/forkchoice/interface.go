package forkchoice

import (
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2"
)

type ForkChoiceStorage interface {
	ForkChoiceStorageWriter
	ForkChoiceStorageReader
}

type ForkChoiceStorageReader interface {
	Ancestor(root common.Hash, slot uint64) common.Hash
	AnchorSlot() uint64
	Engine() execution_client.ExecutionEngine
	FinalizedCheckpoint() solid.Checkpoint
	FinalizedSlot() uint64
	LowestAvaiableSlot() uint64
	GetEth1Hash(eth2Root common.Hash) common.Hash
	GetHead() (common.Hash, uint64, error)
	HighestSeen() uint64
	JustifiedCheckpoint() solid.Checkpoint
	JustifiedSlot() uint64
	ProposerBoostRoot() common.Hash
	GetStateAtBlockRoot(blockRoot libcommon.Hash, alwaysCopy bool) (*state.CachingBeaconState, error)
	GetFinalityCheckpoints(blockRoot libcommon.Hash) (bool, solid.Checkpoint, solid.Checkpoint, solid.Checkpoint)
	GetSyncCommittees(blockRoot libcommon.Hash) (*solid.SyncCommittee, *solid.SyncCommittee, bool)
	Slot() uint64
	Time() uint64
	Partecipation(epoch uint64) (*solid.BitList, bool)
	RandaoMixes(blockRoot libcommon.Hash, out solid.HashListSSZ) bool
	BlockRewards(root libcommon.Hash) (*eth2.BlockRewardsCollector, bool)
	TotalActiveBalance(root libcommon.Hash) (uint64, bool)

	GetStateAtSlot(slot uint64, alwaysCopy bool) (*state.CachingBeaconState, error)
	GetStateAtStateRoot(root libcommon.Hash, alwaysCopy bool) (*state.CachingBeaconState, error)
	ForkNodes() []ForkNode
	Synced() bool
	GetLightClientBootstrap(blockRoot libcommon.Hash) (*cltypes.LightClientBootstrap, bool)
	NewestLightClientUpdate() *cltypes.LightClientUpdate
	GetLightClientUpdate(period uint64) (*cltypes.LightClientUpdate, bool)
}

type ForkChoiceStorageWriter interface {
	OnAggregateAndProof(aggregateAndProof *cltypes.SignedAggregateAndProof, test bool) error
	OnAttestation(attestation *solid.Attestation, fromBlock, insert bool) error
	OnAttesterSlashing(attesterSlashing *cltypes.AttesterSlashing, test bool) error
	OnVoluntaryExit(signedVoluntaryExit *cltypes.SignedVoluntaryExit, test bool) error
	OnProposerSlashing(proposerSlashing *cltypes.ProposerSlashing, test bool) error
	OnBlsToExecutionChange(signedChange *cltypes.SignedBLSToExecutionChange, test bool) error
	OnBlock(block *cltypes.SignedBeaconBlock, newPayload bool, fullValidation bool) error
	OnTick(time uint64)
	SetSynced(synced bool)
}
