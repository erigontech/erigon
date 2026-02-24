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

package forkchoice

import (
	"context"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/common"
)

type ForkChoiceStorage interface {
	ForkChoiceStorageWriter
	ForkChoiceStorageReader
}

type ForkChoiceStorageReader interface {
	// [Modified in Gloas:EIP7732] Returns ForkChoiceNode with payload status.
	Ancestor(root common.Hash, slot uint64) ForkChoiceNode
	AnchorSlot() uint64
	Engine() execution_client.ExecutionEngine
	FinalizedCheckpoint() solid.Checkpoint
	FinalizedSlot() uint64
	LowestAvailableSlot() uint64
	GetEth1Hash(eth2Root common.Hash) common.Hash
	GetHead(auxilliaryState *state.CachingBeaconState) (common.Hash, uint64, error)
	HighestSeen() uint64
	JustifiedCheckpoint() solid.Checkpoint
	JustifiedSlot() uint64
	ProposerBoostRoot() common.Hash
	GetStateAtBlockRoot(
		blockRoot common.Hash,
		alwaysCopy bool,
	) (*state.CachingBeaconState, error)
	GetFinalityCheckpoints(
		blockRoot common.Hash,
	) (solid.Checkpoint, solid.Checkpoint, solid.Checkpoint, bool)
	GetSyncCommittees(period uint64) (*solid.SyncCommittee, *solid.SyncCommittee, bool)
	Slot() uint64
	Time() uint64
	Participation(epoch uint64) (*solid.ParticipationBitList, bool)
	RandaoMixes(blockRoot common.Hash, out solid.HashListSSZ) bool
	BlockRewards(root common.Hash) (*eth2.BlockRewardsCollector, bool)
	TotalActiveBalance(root common.Hash) (uint64, bool)

	ForkNodes() []ForkNode
	Synced() bool
	GetLightClientBootstrap(blockRoot common.Hash) (*cltypes.LightClientBootstrap, bool)
	NewestLightClientUpdate() *cltypes.LightClientUpdate
	GetLightClientUpdate(period uint64) (*cltypes.LightClientUpdate, bool)
	GetHeader(blockRoot common.Hash) (*cltypes.BeaconBlockHeader, bool)
	// [New in Gloas:EIP7732] GetBlock returns the full block for a given block root.
	GetBlock(blockRoot common.Hash) (*cltypes.SignedBeaconBlock, bool)

	GetBalances(blockRoot common.Hash) (solid.Uint64ListSSZ, error)
	GetInactivitiesScores(blockRoot common.Hash) (solid.Uint64ListSSZ, error)
	GetPreviousParticipationIndicies(blockRoot common.Hash) (*solid.ParticipationBitList, error)
	GetValidatorSet(blockRoot common.Hash) (*solid.ValidatorSet, error)
	GetCurrentParticipationIndicies(blockRoot common.Hash) (*solid.ParticipationBitList, error)

	// New stuff added for ssz queues in the beacon state.
	GetPendingConsolidations(blockRoot common.Hash) (*solid.ListSSZ[*solid.PendingConsolidation], bool)
	GetPendingDeposits(blockRoot common.Hash) (*solid.ListSSZ[*solid.PendingDeposit], bool)
	GetPendingPartialWithdrawals(blockRoot common.Hash) (*solid.ListSSZ[*solid.PendingPartialWithdrawal], bool)
	GetProposerLookahead(slot uint64) (solid.Uint64VectorSSZ, bool)

	ValidateOnAttestation(attestation *solid.Attestation) error
	IsRootOptimistic(root common.Hash) bool
	IsHeadOptimistic() bool
	GetPeerDas() das.PeerDas
	// [New in Gloas:EIP7732] GetRecentExecutionPayloadStatus returns the validation status of a recently
	// validated execution payload by its execution block hash. Used for parent payload validation in gossip.
	// Note: This is an LRU cache lookup; older payloads may not be found.
	GetRecentExecutionPayloadStatus(executionBlockHash common.Hash) (execution_client.PayloadStatus, bool)
}

type ForkChoiceStorageWriter interface {
	OnAttestation(attestation *solid.Attestation, fromBlock, insert bool) error
	OnAttesterSlashing(attesterSlashing *cltypes.AttesterSlashing, test bool) error
	OnBlock(
		ctx context.Context,
		block *cltypes.SignedBeaconBlock,
		newPayload bool,
		fullValidation bool,
		checkDataAvaibility bool,
	) error
	// [New in Gloas:EIP7732] OnExecutionPayload processes an execution payload envelope from the builder.
	// checkBlobData: verify blob data availability via PeerDAS
	// validatePayload: call engine.NewPayload() to validate with EL
	OnExecutionPayload(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, checkBlobData, validatePayload bool) error
	// [New in Gloas:EIP7732] OnPayloadAttestationMessage processes a PTC attestation message from gossip.
	// Returns error if validation fails (REJECT), nil if accepted or ignored.
	OnPayloadAttestationMessage(msg *cltypes.PayloadAttestationMessage, isFromBlock bool) error
	AddPreverifiedBlobSidecar(blobSidecar *cltypes.BlobSidecar) error
	OnTick(time uint64)
	SetSynced(synced bool)
	ProcessAttestingIndicies(attestation *solid.Attestation, attestionIndicies []uint64)
}
