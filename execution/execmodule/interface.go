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

package execmodule

import (
	"context"
	"fmt"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/types"
)

// ExecutionStatus is a native equivalent of the proto ExecutionStatus enum.
// The numeric values intentionally match the proto constants for easy conversion.
type ExecutionStatus int32

const (
	ExecutionStatusSuccess           ExecutionStatus = 0
	ExecutionStatusBadBlock          ExecutionStatus = 1
	ExecutionStatusTooFarAway        ExecutionStatus = 2
	ExecutionStatusMissingSegment    ExecutionStatus = 3
	ExecutionStatusInvalidForkchoice ExecutionStatus = 4
	ExecutionStatusBusy              ExecutionStatus = 5
	ExecutionStatusReorgTooDeep      ExecutionStatus = 6
)

func (s ExecutionStatus) String() string {
	switch s {
	case ExecutionStatusSuccess:
		return "Success"
	case ExecutionStatusBadBlock:
		return "BadBlock"
	case ExecutionStatusTooFarAway:
		return "TooFarAway"
	case ExecutionStatusMissingSegment:
		return "MissingSegment"
	case ExecutionStatusInvalidForkchoice:
		return "InvalidForkchoice"
	case ExecutionStatusBusy:
		return "Busy"
	case ExecutionStatusReorgTooDeep:
		return "ReorgTooDeep"
	default:
		return fmt.Sprintf("ExecutionStatus(%d)", int32(s))
	}
}

// ValidationResult is the native return type for ValidateChain.
type ValidationResult struct {
	ValidationStatus ExecutionStatus
	LatestValidHash  common.Hash
	ValidationError  string
	// ComputedRoot is the state root the executor computed over the (accumulated) state during
	// this call. On the flashblock PreExecute path this is the per-round root over the body
	// executed so far (Root is otherwise deferred to seal). Zero when no commitment ran.
	ComputedRoot common.Hash
	// FlashblockReceiptCount is the number of receipts accumulated across the in-progress
	// flashblock's rounds so far (== the number of body txs executed). Seal input; 0 off the
	// flashblock path.
	FlashblockReceiptCount int
	// GasUsed, ReceiptHash and Bloom are the SEALED output-side header fields the CLOSE derives
	// over the accumulated flashblock receipts (doms.FlashblockReceipts()) with ZERO re-execution —
	// the read-and-package seal. Set only at the close (FlashblockAccumulating unset, receipts present);
	// combined with ComputedRoot they are exactly the computed fields of the sealed header H1.
	GasUsed     uint64
	ReceiptHash common.Hash
	Bloom       types.Bloom
}

// ForkChoiceResult is the native return type for UpdateForkChoice.
type ForkChoiceResult struct {
	Status          ExecutionStatus
	LatestValidHash common.Hash
	ValidationError string
}

// ForkChoiceState is the native return type for GetForkChoice.
type ForkChoiceState struct {
	HeadHash      common.Hash
	SafeHash      common.Hash
	FinalizedHash common.Hash
}

// AssembleBlockResult is the native return type for AssembleBlock.
type AssembleBlockResult struct {
	// Busy is true when the execution module is already processing a request
	// and cannot start a new block build immediately.
	Busy      bool
	PayloadID uint64
}

// AssembledBlockResult is the native return type for GetAssembledBlock.
type AssembledBlockResult struct {
	// Busy is true when the builder has not finished yet.
	Busy bool
	// Block holds the assembled block with receipts and requests.
	// Nil when Busy is true or when no builder was found for the payload ID.
	Block      *types.BlockWithReceipts
	BlockValue *uint256.Int
}

// PayloadBody is a block body in engine-API format.
// Unlike types.RawBody it contains no uncle headers and includes the encoded
// block access list (for Amsterdam+ chains).
type PayloadBody struct {
	Transactions    [][]byte
	Withdrawals     []*types.Withdrawal
	BlockAccessList []byte // RLP-encoded block access list, nil for pre-Amsterdam blocks
}

// ExecutionModule is a plain Go interface for the in-process execution module.
// It replaces the gRPC ExecutionClient/ExecutionServer interfaces for callers
// that communicate with the execution module within the same process.
//
// All methods use native Erigon types (common.Hash, *types.Header, …) rather
// than protobuf-generated types, eliminating the serialisation round-trip and
// the proto↔native conversion layer that previously lived in
// execution/execmodule/moduleutil/grpc.go and node/direct/execution_client.go.
type ExecutionModule interface {
	// --- Block insertion --------------------------------------------------

	// InsertBlocks stores one or more blocks in the execution layer.
	// Returns ExecutionStatusSuccess on success or a non-success status on
	// rejection (e.g. ExecutionStatusTooFarAway).
	InsertBlocks(ctx context.Context, blocks []*types.RawBlock) (ExecutionStatus, error)

	// --- Chain validation -------------------------------------------------

	// ValidateChain validates the chain ending at the block identified by
	// blockHash and blockNumber.
	ValidateChain(ctx context.Context, blockHash common.Hash, blockNumber uint64) (ValidationResult, error)

	// PreExecute incrementally executes a flashblock's NEW transactions into the ONE
	// maintained SharedDomains (carry-forward, OnTx once per tx, no finished-block checks),
	// leaving a pre-executed block for a subsequent ValidateChain to finalise with zero
	// re-execution. See execmodule.PreExecute.
	PreExecute(ctx context.Context, blockHash common.Hash, blockNumber uint64) (ValidationResult, error)

	// PreExecuteFlashblock is the encapsulated flashblock pre-exec: the consensus half streams the UNFILTERED
	// committed txs for the in-progress block plus its fixed header inputs; the execution half filters the
	// stream against its own SD, maintains the (filtered) block body, builds+inserts it, and pre-executes.
	// Returns the current filtered body and its hash. See execmodule.PreExecuteFlashblock.
	PreExecuteFlashblock(ctx context.Context, inputs FlashblockInputs, newTxRLPs [][]byte) (*types.RawBody, common.Hash, ValidationResult, error)

	// GetPreExecutedBody returns the node's locally pre-executed in-progress flashblock body
	// (accumulated from the DAG across PreExecute rounds) plus the deferred in-progress hash and
	// number. Lets the newPayload for a DAG-preconfirmed flashblock be body-LESS — each node
	// supplies the body locally rather than from the wire. See execmodule.GetPreExecutedBody.
	GetPreExecutedBody(ctx context.Context) (*types.RawBody, common.Hash, uint64, error)

	// IngestSealedFlashblock is the newPayload step for a sealed flashblock: given only the sealed
	// HEADER (the payload message is body-less), it materialises the block by pairing the header with
	// the node's OWN pre-executed body and re-points the extending fork to it — NO re-execution — so a
	// subsequent normal UpdateForkChoice canonicalises it. See execmodule.IngestSealedFlashblock.
	IngestSealedFlashblock(ctx context.Context, sealed *types.Header) error

	// --- Fork choice ------------------------------------------------------

	// UpdateForkChoice updates the canonical head, safe, and finalized block
	// hashes.  The caller may set a deadline on ctx to bound how long to wait
	// synchronously: a DeadlineExceeded returns ForkChoiceResult{Status:Busy}
	// so the caller can poll with GetForkChoice.
	UpdateForkChoice(ctx context.Context, headHash, safeHash, finalizedHash common.Hash) (ForkChoiceResult, error)

	// GetForkChoice returns the current fork choice state (head, safe,
	// finalized).
	GetForkChoice(ctx context.Context) (ForkChoiceState, error)

	// --- Block building ---------------------------------------------------

	// AssembleBlock initiates building a new block with the supplied
	// parameters.  Returns the payload ID assigned to the build job.
	AssembleBlock(ctx context.Context, params *builder.Parameters) (AssembleBlockResult, error)

	// GetAssembledBlock retrieves the block that was assembled under the
	// given payloadID.  The result is Busy when the builder has not finished.
	GetAssembledBlock(ctx context.Context, payloadID uint64) (AssembledBlockResult, error)

	// SealBoundary is the marker-driven CLOSE: the boundary assembler calls it when a block-end marker
	// commits in consensus to seal the pre-executed in-progress flashblock (zero re-execution) and store
	// it by parent hash, so GetAssembledBlock (proposer) / newPayload (follower) retrieve it without
	// re-sealing. Runs on every node at the marker. See execmodule.SealBoundary.
	SealBoundary(ctx context.Context, params *builder.Parameters, forceEmpty bool) (*types.BlockWithReceipts, error)

	// FrontierHeader returns the exec-owned run-ahead FRONTIER head — the last sealed block a newly-opening
	// flashblock chains onto — or nil before the first seal. This is a LOCK-FREE ATOMIC READ (not a semaphore
	// operation), the one legitimate cross-boundary read the driver needs for its consensus open-decision — like
	// CurrentHeader. Exec sets it internally (SealBoundary advances it; AssembleBlock re-anchors it).
	FrontierHeader() *types.Header

	// ExecCostUpperQuartile returns the running window's 75th-percentile per-tx execution TIME and GAS — the
	// driver sizes the next batch it feeds into exec from it (time bound; gas cross-check for the txpool estimate).
	ExecCostUpperQuartile() (time.Duration, uint64)

	// AssembleInProgress seals the CURRENT in-progress flashblock (block-end over its maintained SD, zero body
	// re-execution) and returns the sealed header WITHOUT advancing the frontier or opening a successor — the
	// non-mutating seal a follower uses to verify a delivered newPayload. (nil,false) ⇒ no valid in-progress
	// flashblock. This is the verb that replaced the driver reading in-progress exec state to seal it itself.
	AssembleInProgress(ctx context.Context) (*types.Header, bool, error)

	// NOTE: the in-progress flashblock INSPECTION hooks (InProgressRoot / InProgressReceiptCount /
	// InProgressBlock) are deliberately NOT on this interface. They do not take the exec semaphore (they read
	// flash.mu-guarded state), so they are not real exec operations — they are concrete-only test/inspection
	// methods on *ExecModule. Tests reach them by asserting the concrete type; production code never needs them.

	// NOTE: discarding a stale in-progress flashblock (the former AbandonExtendingFork) is exec-INTERNAL now —
	// its only triggers, the SealBoundary reconcile and the AssembleBlock boundary re-anchor, run under the exec
	// semaphore and call abandonExtendingForkLocked directly. There is no public wrapper (it would be a
	// semaphore-free mutation on this interface — the anti-pattern this refactor removed).

	// --- Header / body queries --------------------------------------------

	// CurrentHeader returns the canonical head block header.
	CurrentHeader(ctx context.Context) (*types.Header, error)

	// GetHeader returns the header for the block identified by blockHash
	// and/or blockNumber.  Pass nil for either argument to let the
	// implementation resolve the missing value from the database.
	// Returns nil (no error) when the block is not found.
	GetHeader(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*types.Header, error)

	// GetBody returns the raw body for the block identified by blockHash
	// and/or blockNumber.  Pass nil for an unknown argument.
	// Returns nil (no error) when the block is not found.
	GetBody(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*types.RawBody, error)

	// HasBlock reports whether a block with the given hash and/or number is
	// stored locally.
	HasBlock(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (bool, error)

	// GetBodiesByRange returns the raw bodies for the canonical blocks in
	// [start, start+count).
	GetBodiesByRange(ctx context.Context, start, count uint64) ([]*types.RawBody, error)

	// GetBodiesByHashes returns the raw bodies for the given block hashes in
	// the same order.  A nil entry is returned for unknown hashes.
	GetBodiesByHashes(ctx context.Context, hashes []common.Hash) ([]*types.RawBody, error)

	// GetPayloadBodiesByHash returns bodies in engine-API format for the
	// given hashes.  A nil entry is returned for unknown hashes.
	GetPayloadBodiesByHash(ctx context.Context, hashes []common.Hash) ([]*PayloadBody, error)

	// GetPayloadBodiesByRange returns bodies in engine-API format for the
	// canonical blocks in [start, start+count).
	GetPayloadBodiesByRange(ctx context.Context, start, count uint64) ([]*PayloadBody, error)

	// --- Hash / number queries --------------------------------------------

	// IsCanonicalHash reports whether blockHash belongs to the canonical
	// chain.
	IsCanonicalHash(ctx context.Context, blockHash common.Hash) (bool, error)

	// GetHeaderHashNumber returns the block number for blockHash, or nil
	// when the hash is unknown.
	GetHeaderHashNumber(ctx context.Context, blockHash common.Hash) (*uint64, error)

	// GetTD returns the total difficulty for the block identified by
	// blockHash and/or blockNumber.  Pass nil for an unknown argument.
	// Returns nil (no error) when the block is not found.
	GetTD(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*uint256.Int, error)

	// --- Module state -----------------------------------------------------

	// Ready reports whether the execution module has finished its startup
	// sequence and is ready to serve requests.
	Ready(ctx context.Context) (bool, error)

	// FrozenBlocks returns the number of blocks stored in read-only snapshots
	// and whether there is a gap between the snapshot tip and the live
	// database.
	FrozenBlocks(ctx context.Context) (frozenBlocks uint64, hasGap bool, err error)
}
