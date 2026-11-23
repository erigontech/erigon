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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/exec"
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
	// AppliedTxs is how many of the block's body transactions this call left executed. It equals the body
	// it was given unless the round was STOPPED part way, in which case the body was cut to it.
	AppliedTxs int
	// RoundKept is how many of the transactions THIS CALL was offered survived the stream filter and were
	// appended to the body. It is NOT len(newTxRLPs): the filter drops transactions whose nonce is behind
	// the frontier (stale) and defers ones ahead of it (future), before anything executes.
	//
	// ⚠ THE CALLER USED TO ASSUME THE BATCH LENGTH. Measured on live352, 455,798 transactions were offered
	// across 3,841 rounds and 46 were kept — while the driver recorded every one of them as fed, marked
	// them in flight, and its round-end line read applied=128 offered=128 while the chain held ONE
	// transaction per block. It cannot be reconstructed from the returned body either: that is handed back
	// BEFORE a round that does not commit is rolled back out of it.
	RoundKept int
	// RoundKeptHashes names them, in the order they went into the body; the filter keeps a SUBSET, and unless
	// commit-order execution is on it restores each sender's nonce order, so the caller cannot take a prefix.
	RoundKeptHashes []common.Hash
	// RoundDroppedHashes names the ones DECIDED AGAINST — already sealed on the frontier, or already judged
	// in this block, and under commit-order execution also a nonce ahead of the sender's next one. Anything
	// offered that is in neither list was deferred or not reached and must be offered again.
	RoundDroppedHashes []common.Hash
	// GasFull reports a round that ended because the BLOCK ran out of gas room, not because its
	// caller stopped it. The transaction that did not fit is excluded and is the caller's to re-offer.
	GasFull bool
	// GasUsed, ReceiptHash and Bloom are the SEALED output-side header fields the CLOSE derives
	// over the accumulated flashblock receipts (doms.FlashblockReceipts()) with ZERO re-execution —
	// the read-and-package seal. Set only at the close (FlashblockAccumulating unset, receipts present);
	// combined with ComputedRoot they are exactly the computed fields of the sealed header H1.
	GasUsed     uint64
	ReceiptHash common.Hash
	Bloom       types.Bloom
}

// BlockStateView reads a block: blockTx holds the block and its executed state, parentState the latest state of its parent.
type BlockStateView func(blockTx kv.TemporalTx, parentState kv.TemporalGetter) error

// ForkChoiceResult is the native return type for UpdateForkChoice.
type ForkChoiceResult struct {
	Status          ExecutionStatus
	LatestValidHash common.Hash
	ValidationError string
	MissingHash     common.Hash
	// UpdateInProgress distinguishes admitted asynchronous work from a Busy rejection.
	UpdateInProgress bool
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
	// Busy is true when the module was already occupied, not when the builder is still working:
	// otherwise the call waits for the builder to finish.
	Busy bool
	// Unknown is true when no builder is held for the payload id, so nothing will ever arrive for
	// it. A caller polling that id cannot otherwise tell it from a build still running.
	Unknown bool
	// Block holds the assembled block with receipts and requests.
	// Nil when Busy or Unknown is true, or when the builder produced nothing.
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
	InsertBlocks(ctx context.Context, blocks []*types.Block) (ExecutionStatus, error)

	AddSendersRecovery(blockHash common.Hash, recovery *exec.SendersRecovery)

	// --- Chain validation -------------------------------------------------

	// ValidateChain validates the chain ending at block.
	ValidateChain(ctx context.Context, block *types.Block) (ValidationResult, error)

	// ValidateChainAndView is ValidateChain that, for a VALID block, also returns a view that calls fn with the
	// block's state. The view holds no module lock and must be called once; the caller may first release its own
	// locks. A nil view means fn already ran, for a tip on a side fork whose state the module drops once released,
	// or that there is no state: a committed block whose changesets are pruned. It reads a committed block without
	// the module, waits up to wait for a busy module, and executes again a block whose state is no longer held.
	ValidateChainAndView(ctx context.Context, block *types.Block, wait time.Duration, fn BlockStateView) (ValidationResult, func() error, error)

	// PreExecuteFlashblock is the encapsulated flashblock pre-exec: the consensus half streams the UNFILTERED
	// committed txs for the in-progress block plus its fixed header inputs; the execution half filters the
	// stream against its own SD, maintains the (filtered) block body, builds+inserts it, and pre-executes.
	// Returns the current filtered body and its hash. See execmodule.PreExecuteFlashblock.
	PreExecuteFlashblock(ctx context.Context, inputs FlashblockInputs, newTxRLPs [][]byte) (*types.RawBody, common.Hash, ValidationResult, error)
	// RebuildFlashblock re-executes the in-progress block from body alone, for a build that ran past the block's end.
	RebuildFlashblock(ctx context.Context, inputs FlashblockInputs, body [][]byte) (ValidationResult, error)

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
	// synchronously: an admitted update that exceeds the deadline returns Busy
	// with UpdateInProgress set so the caller can poll with GetForkChoice.
	// TooFarAway reports state ahead of the blocks; the canonical chain up to
	// headHash is already committed when it is returned.
	UpdateForkChoice(ctx context.Context, headHash, safeHash, finalizedHash common.Hash) (ForkChoiceResult, error)

	// GetForkChoice returns the current fork choice state (head, safe,
	// finalized).
	GetForkChoice(ctx context.Context) (ForkChoiceState, error)

	// --- Block building ---------------------------------------------------

	// NewPayloadAttrs delivers the next block's payload attributes (built by the CL per slot from the head
	// beacon state, on every client) BEFORE AssembleBlock, so the DAG boundary assembler can open that block
	// and start executing it under the correct attrs. Non-blocking; no-op when no boundary assembler is set.
	NewPayloadAttrs(ctx context.Context, params *builder.Parameters) error

	// AssembleBlock initiates building a block. Busy reports that the execution module is occupied;
	// otherwise PayloadID identifies the new build.
	// Unfinished explicit-list builds (RequireAllTransactions) are discarded when ctx ends.
	// Ordinary payload builds remain available for later retrieval after the requesting call ends.
	AssembleBlock(ctx context.Context, params *builder.Parameters) (AssembleBlockResult, error)

	// GetAssembledBlock stops and waits for payloadID's builder. Busy reports that the execution
	// module is occupied; Unknown reports that no builder is held for payloadID.
	GetAssembledBlock(ctx context.Context, payloadID uint64) (AssembledBlockResult, error)

	// ReconcileAttrs brings the open block onto attributes delivered after it was opened, so the close never
	// has to. See execmodule.ReconcileAttrs.
	ReconcileAttrs(ctx context.Context, params *builder.Parameters) error

	// SealBlock is the marker-driven CLOSE: the boundary assembler calls it when a block-end marker
	// commits in consensus to seal the pre-executed in-progress flashblock (zero re-execution) and store
	// it by parent hash, so GetAssembledBlock (proposer) / newPayload (follower) retrieve it without
	// re-sealing. Runs on every node at the marker. See execmodule.SealBlock.
	SealBlock(ctx context.Context, params *builder.Parameters, forceEmpty bool) (*types.BlockWithReceipts, error)

	// FrontierHeader returns the exec-owned run-ahead FRONTIER head — the last sealed block a newly-opening
	// flashblock chains onto — or nil before the first seal. This is a LOCK-FREE ATOMIC READ (not a semaphore
	// operation), the one legitimate cross-boundary read the driver needs for its consensus open-decision — like
	// CurrentHeader. Exec sets it internally (SealBlock advances it; AssembleBlock re-anchors it).
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
	// its only triggers, the SealBlock reconcile and the AssembleBlock boundary re-anchor, run under the exec
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

	// GetHeadersByRange returns canonical headers from one read view,
	// truncated at the latest known block. Unavailable headers within
	// the range are returned as nil entries.
	GetHeadersByRange(ctx context.Context, start, count uint64) ([]*types.Header, error)

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
	// chain and Execution progress has reached its height.
	IsCanonicalHash(ctx context.Context, blockHash common.Hash) (bool, error)

	// IsValidated reports whether a payload with blockHash already passed
	// validation.
	IsValidated(blockHash common.Hash) bool

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

	// InclusionList builds an inclusion list for EIP-7805 from its view of the mempool.
	InclusionList(ctx context.Context) (types.Transactions, error)
}
