package forkchoice

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
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
	GetEth1Hash(eth2Root common.Hash) common.Hash
	GetHead() (common.Hash, uint64, error)
	HighestSeen() uint64
	JustifiedCheckpoint() solid.Checkpoint
	ProposerBoostRoot() common.Hash
	Slot() uint64
	Time() uint64
}

type ForkChoiceStorageWriter interface {
	OnAttestation(attestation *solid.Attestation, fromBlock bool) error
	OnAttesterSlashing(attesterSlashing *cltypes.AttesterSlashing, test bool) error
	OnBlock(block *cltypes.SignedBeaconBlock, newPayload bool, fullValidation bool) error
	OnTick(time uint64)
}
