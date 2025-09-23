package beaconevents

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

type EventStream struct {
	Event EventTopic  `json:"event"`
	Data  interface{} `json:"data"`
}

type EventTopic string

// Operation event topics
const (
	OpAttestation       EventTopic = "attestation"
	OpVoluntaryExit     EventTopic = "voluntary_exit"
	OpProposerSlashing  EventTopic = "proposer_slashing"
	OpAttesterSlashing  EventTopic = "attester_slashing"
	OpBlsToExecution    EventTopic = "bls_to_execution_change"
	OpContributionProof EventTopic = "contribution_and_proof"
	OpBlobSidecar       EventTopic = "blob_sidecar"
	OpDataColumnSidecar EventTopic = "data_column_sidecar"
)

type (
	// Operation event data types
	AttestationData           = solid.Attestation
	SingleAttestationData     = solid.SingleAttestation
	VoluntaryExitData         = cltypes.SignedVoluntaryExit
	ProposerSlashingData      = cltypes.ProposerSlashing
	AttesterSlashingData      = cltypes.AttesterSlashing
	BlsToExecutionChangesData = cltypes.SignedBLSToExecutionChange
	ContributionAndProofData  = cltypes.SignedContributionAndProof
	BlobSidecarData           = cltypes.BlobSidecar
	DataColumnSidecarData     = cltypes.DataColumnSidecar
)

// State event topics
const (
	StateHead                        EventTopic = "head"
	StateBlock                       EventTopic = "block"
	StateBlockGossip                 EventTopic = "block_gossip"
	StateFinalizedCheckpoint         EventTopic = "finalized_checkpoint"
	StateChainReorg                  EventTopic = "chain_reorg"
	StateLightClientFinalityUpdate   EventTopic = "light_client_finality_update"
	StateLightClientOptimisticUpdate EventTopic = "light_client_optimistic_update"
	StatePayloadAttributes           EventTopic = "payload_attributes"
)

// State event data types
type HeadData struct {
	Slot                      uint64      `json:"slot,string"`
	Block                     common.Hash `json:"block"`
	State                     common.Hash `json:"state"`
	EpochTransition           bool        `json:"epoch_transition"`
	PreviousDutyDependentRoot common.Hash `json:"previous_duty_dependent_root"`
	CurrentDutyDependentRoot  common.Hash `json:"current_duty_dependent_root"`
	ExecutionOptimistic       bool        `json:"execution_optimistic"`
}

type BlockData struct {
	Slot                uint64      `json:"slot,string"`
	Block               common.Hash `json:"block"`
	ExecutionOptimistic bool        `json:"execution_optimistic"`
}

type BlockGossipData struct {
	Slot  uint64      `json:"slot,string"`
	Block common.Hash `json:"block"`
}

type FinalizedCheckpointData struct {
	Block               common.Hash `json:"block"`
	State               common.Hash `json:"state"`
	Epoch               uint64      `json:"epoch,string"`
	ExecutionOptimistic bool        `json:"execution_optimistic"`
}

type ChainReorgData struct {
	Slot                uint64      `json:"slot,string"`
	Depth               uint64      `json:"depth,string"`
	OldHeadBlock        common.Hash `json:"old_head_block"`
	NewHeadBlock        common.Hash `json:"new_head_block"`
	OldHeadState        common.Hash `json:"old_head_state"`
	NewHeadState        common.Hash `json:"new_head_state"`
	Epoch               uint64      `json:"epoch,string"`
	ExecutionOptimistic bool        `json:"execution_optimistic"`
}

type LightClientFinalityUpdateData struct {
	Version string                            `json:"version"`
	Data    cltypes.LightClientFinalityUpdate `json:"data"`
}

type LightClientOptimisticUpdateData struct {
	Version string                              `json:"version"`
	Data    cltypes.LightClientOptimisticUpdate `json:"data"`
}

type PayloadAttributesData struct {
	Version string                   `json:"version"`
	Data    PayloadAttributesContent `json:"data"`
}

type PayloadAttributesContent struct {
	/*
		proposal_slot: the slot at which a block using these payload attributes may be built.
		parent_block_root: the beacon block root of the parent block to be built upon.
		parent_block_number: the execution block number of the parent block.
		parent_block_hash: the execution block hash of the parent block.
		proposer_index: the validator index of the proposer at proposal_slot on the chain identified by parent_block_root.
	*/
	ProposerIndex     uint64                         `json:"proposer_index,string"`
	ProposalSlot      uint64                         `json:"proposal_slot,string"`
	ParentBlockNumber uint64                         `json:"parent_block_number,string"`
	ParentBlockRoot   common.Hash                    `json:"parent_block_root"`
	ParentBlockHash   common.Hash                    `json:"parent_block_hash"`
	PayloadAttributes engine_types.PayloadAttributes `json:"payload_attributes"`
}
