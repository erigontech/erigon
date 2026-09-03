// Copyright 2022 The Erigon Authors
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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/shards"
)

type BlockTimings [2]time.Duration

const (
	BlockTimingsValidationIndex    = 0
	BlockTimingsFlushExtendingFork = 1
)

const timingsCacheSize = 16

type ForkValidator struct {
	// current memory batch containing chain head that extend canonical fork.
	sharedDom *execctx.SharedDomains
	// notifications accumulated for the extending fork
	extendingForkNotifications *shards.Notifications
	// hash of chain head that extend canonical fork.
	extendingForkHeadHash common.Hash
	extendingForkNumber   uint64
	maxReorgDepth         uint64
	// preExec is the PRODUCER's pre-executed block chain — a separate space with its own lifecycle (see
	// preexec_frontier.go). The validator only ever READS it, for the two things validation legitimately
	// needs to know: that an ancestor is already pre-executed and must not be re-executed as a side fork,
	// and that the block newPayload is validating is one we already built, so it can be PROMOTED into
	// validation space instead of re-executed. Nothing here writes it; the dependency is one-directional.
	preExec *preExecFrontier
	// pipeline executor used for fork validation (ValidateBlock).
	executor    *PipelineExecutor
	blockReader services.FullBlockReader
	// this is the current point where we processed the chain so far.
	currentHeight uint64
	// block hashes that are deemed valid
	validHashes *lru.Cache[common.Hash, bool]

	ctx context.Context

	// we want fork validator to be thread safe so let
	lock sync.Mutex

	timingsCache *lru.Cache[common.Hash, BlockTimings]

	// promotedHash/promotedNumber record the LAST block canonicalised by PromoteBlock (the prefetch
	// "promoted" lifecycle state). A prefetched gen is kept live after promotion (it stays the read-through
	// parent of its successor until the successor canonicalises), so WITHOUT this record a re-FCU of the
	// same head — routine for an L2 CL that re-sends FCU every slot — would re-pass HasLiveGen and
	// re-promote (re-merge) the same block, republishing notifications and tearing down the eager-opened
	// successor frontier. IsPromotedHead lets updateForkChoice treat a re-FCU of the already-promoted head
	// as a true no-op. Promote-ONCE is a prefetch-lifecycle invariant (prefetched → promoted → retired).
	promotedHash   common.Hash
	promotedNumber uint64
}

func newForkValidator(ctx context.Context, currentHeight uint64, executor *PipelineExecutor, blockReader services.FullBlockReader, maxReorgDepth uint64, preExec *preExecFrontier) *ForkValidator {
	validHashes, err := lru.New[common.Hash, bool]("validHashes", int(maxReorgDepth)*8)
	if err != nil {
		panic(err)
	}

	timingsCache, err := lru.New[common.Hash, BlockTimings]("timingsCache", timingsCacheSize)
	if err != nil {
		panic(err)
	}
	return &ForkValidator{
		preExec:       preExec,
		executor:      executor,
		currentHeight: currentHeight,
		blockReader:   blockReader,
		ctx:           ctx,
		validHashes:   validHashes,
		timingsCache:  timingsCache,
		maxReorgDepth: maxReorgDepth,
	}
}

// ExtendingForkHeadHash return the fork head hash of the fork that extends the canonical chain.
func (fv *ForkValidator) ExtendingForkHeadHash() common.Hash {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	return fv.extendingForkHeadHash
}

// NotifyCurrentHeight is to be called at the end of the stage cycle and represent the last processed block.
// The head advancing invalidates a VALIDATION candidate fork built on the old head, so it is dropped here —
// this no longer needs a frontier exemption, because the producer's run-ahead blocks are not in this slot.
func (fv *ForkValidator) NotifyCurrentHeight(currentHeight uint64) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if fv.currentHeight == currentHeight {
		return
	}
	fv.currentHeight = currentHeight
	// If the head changed, previous assumptions on head are incorrect now.
	if fv.sharedDom != nil {
		fv.sharedDom.Close()
	}
	fv.sharedDom = nil
	fv.extendingForkNotifications = nil
	fv.extendingForkNumber = 0
	fv.extendingForkHeadHash = common.Hash{}
}

// MergeExtendingFork merges the shared domains of the current extending fork into the current shared domains if fcu chooses its head hash as the fork choice.
func (fv *ForkValidator) MergeExtendingFork(ctx context.Context, tx kv.TemporalTx, sd *execctx.SharedDomains, target *Accumulation) error {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	start := time.Now()
	if fv.sharedDom != nil {
		if err := fv.sharedDom.FlushPendingUpdates(ctx, tx); err != nil {
			return err
		}
		sdTxNum, _, err := sd.SeekCommitment(ctx, tx)
		if err != nil {
			return err
		}
		otherTxNum, _, err := fv.sharedDom.SeekCommitment(ctx, tx)
		if err != nil {
			return err
		}
		// Close the merged SD only if it is OURS. When it is a promoted pre-exec generation it is still the
		// read-through parent of the block above it, and the frontier releases it in order once its state is
		// durable — closing it here would break that chain past the first hop.
		err = sd.Merge(ctx, sdTxNum, fv.sharedDom, otherTxNum, !fv.preExec.Owns(fv.sharedDom))
		if err != nil {
			return err
		}
	}
	timings, _ := fv.timingsCache.Get(fv.extendingForkHeadHash)
	timings[BlockTimingsFlushExtendingFork] = time.Since(start)
	fv.timingsCache.Add(fv.extendingForkHeadHash, timings)
	fv.extendingForkNotifications.Accumulator.CopyAndReset(target.Accumulator)
	fv.extendingForkNotifications.RecentReceipts.CopyAndReset(target.RecentReceipts)
	// Release pre-exec generations strictly below the block just canonicalised: their state is now durable,
	// while this block's own generation stays live as its successor's read-through parent (the committed DB
	// can be incomplete for that read until the block is fully durable → "empty branch data read during
	// unfold"). A no-op when the block did not come from the frontier.
	fv.preExec.RetireBelow(ctx, tx, fv.extendingForkNumber)
	// Clear the validation slot — its state was transferred into the canonical SD by the merge above (and
	// the SD itself closed, unless the frontier still owns it).
	fv.sharedDom = nil
	fv.extendingForkHeadHash = common.Hash{}
	fv.extendingForkNumber = 0
	fv.extendingForkNotifications = nil
	return nil
}

type HasDiff interface {
	Diff() (*membatchwithdb.MemoryDiff, error)
}

// ValidatePayload returns whether a payload is valid or invalid, or if cannot be determined, it will be accepted.
// if the payload extends the canonical chain, then we stack it in extendingFork without any unwind.
// if the payload is a fork then we unwind to the point where the fork meets the canonical chain, and there we check whether it is valid.
// if for any reason none of the actions above can be performed due to lack of information, we accept the payload and avoid validation.
func (fv *ForkValidator) ValidatePayload(ctx context.Context, sd *execctx.SharedDomains, tx kv.TemporalRwTx, header *types.Header, body *types.RawBody, logger log.Logger) (status engine_types.EngineStatus, latestValidHash common.Hash, validationError error, criticalError error) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if fv.executor == nil {
		status = engine_types.AcceptedStatus
		return
	}
	hash := header.Hash()
	number := header.Number.Uint64()

	// THE BRIDGE. This is the ONLY action that moves a SharedDomains from pre-exec space into validation
	// space. When the payload is a block WE pre-executed — the producer's own block arriving back from the
	// consensus layer — its post-state is already live in the frontier generation, so adopt that generation
	// as the validation candidate instead of re-executing the body against its own already-applied state
	// (which would fail "nonce too low"). The FCU then canonicalises it through the ordinary merge path, so
	// producer and follower share one canonicalisation route.
	//
	// The generation is LENT, not handed over: the frontier still owns it and is still building the block
	// above it, so every place that would close fv.sharedDom asks preExec.Owns first.
	if fv.adoptPreExecutedLocked(hash, number) {
		logger.Debug("[execmodule] newPayload promoted a pre-executed block", "number", number, "hash", hash)
		return engine_types.ValidStatus, hash, nil, nil
	}

	// A flashblock CLOSE reuses the maintained accumulating SD (sd == fv.sharedDom) with accumulation
	// now off. The accumulation rounds already ran the block's body through this same ValidatePayload
	// path, so the block hash is BOTH in validHashes and marked canonical — but block-end (systemEnd/
	// Finalize) has NOT run yet (it belongs to the close). The "already validated / already canonical"
	// short-circuits below would skip the close's block-end seal entirely. Force the close to execute.
	flashblockClose := sd != nil && sd == fv.sharedDom && !sd.FlashblockAccumulating()

	// If the block is stored within the side fork it means it was already validated.
	if _, ok := fv.validHashes.Get(hash); ok && !flashblockClose {
		status = engine_types.ValidStatus
		latestValidHash = hash
		return
	}

	// if the block is not in range of maxReorgDepth from head then we do not validate it.
	if math.AbsoluteDifference(fv.currentHeight, header.Number.Uint64()) > fv.maxReorgDepth {
		status = engine_types.AcceptedStatus
		return
	}
	var foundCanonical bool
	foundCanonical, criticalError = fv.blockReader.IsCanonical(fv.ctx, tx, hash, number)
	if criticalError != nil {
		return
	}
	if foundCanonical && !flashblockClose {
		status = engine_types.ValidStatus
		latestValidHash = header.Hash()
		return
	}
	// Let's assemble the side fork backwards
	currentHash := header.ParentHash
	unwindPoint := number - 1
	baseIsPreExecLive := false // true when the side-fork base is a live in-memory frontier block, not canonical
	foundCanonical, criticalError = fv.blockReader.IsCanonical(fv.ctx, tx, currentHash, unwindPoint)
	if criticalError != nil {
		return
	}
	// A live in-memory frontier ancestor takes PRECEDENCE over a canonical marking. The seal re-keys a
	// sealed frontier block as canonical so it can be FOUND, but its STATE still lives only in the frontier
	// SD (FCU lags). Chain to that live SD rather than treating it as a committed-canonical base — the
	// latter reads a lagging/absent DB base and makes the commitment root diverge. A genuinely committed
	// (non-live) canonical ancestor is unaffected: isPreExecutedLive is false there.
	if fv.preExec.Live(currentHash, unwindPoint) {
		foundCanonical = true
		baseIsPreExecLive = true
	}

	logger.Debug("Execution ForkValidator.ValidatePayload", "foundCanonical", foundCanonical, "currentHash", currentHash, "unwindPoint", unwindPoint)

	var bodiesChain []*types.RawBody
	var headersChain []*types.Header
	for !foundCanonical {
		var (
			header *types.Header
			body   *types.Body
		)
		header, criticalError = fv.blockReader.Header(fv.ctx, tx, currentHash, unwindPoint)
		if criticalError != nil {
			return
		}
		if header == nil {
			// We miss some components so we did not check validity.
			status = engine_types.AcceptedStatus
			return
		}
		body, criticalError = fv.blockReader.BodyWithTransactions(fv.ctx, tx, currentHash, unwindPoint)
		if criticalError != nil {
			return
		}
		if body == nil {
			criticalError = fmt.Errorf("found chain gap in block body at hash %s, number %d", currentHash, unwindPoint)
			return
		}

		headersChain = append([]*types.Header{header}, headersChain...)
		bodiesChain = append([]*types.RawBody{body.RawBody()}, bodiesChain...)

		currentHash = header.ParentHash
		unwindPoint = header.Number.Uint64() - 1
		foundCanonical, criticalError = fv.blockReader.IsCanonical(fv.ctx, tx, currentHash, unwindPoint)
		if criticalError != nil {
			return
		}
		// Stop at any pre-executed LIVE ancestor on the extending chain (FCU may canonicalise several
		// blocks at once, so a run of extending blocks can still be un-canonicalised here). Its state is
		// already in memory on the frontier stack — do not re-execute it. Takes PRECEDENCE over a canonical
		// marking (a re-keyed-canonical-but-still-live frontier block must chain to its live SD, not the DB).
		if fv.preExec.Live(currentHash, unwindPoint) {
			foundCanonical = true
			baseIsPreExecLive = true
		}
		logger.Debug("Execution ForkValidator.ValidatePayload", "foundCanonical", foundCanonical, "currentHash", currentHash, "unwindPoint", unwindPoint)
	}
	// A pre-executed LIVE base is chained in memory (SetParent) — it is NOT a canonical ancestor to unwind
	// to, and trying would read its (non-existent) canonical hash. There is nothing to unwind: the SD
	// already carries the base's state via the parent chain. Skip the unwind.
	if baseIsPreExecLive {
		unwindPoint = 0
	}
	// Do not set an unwind point if we are already there.
	if unwindPoint == fv.currentHeight {
		unwindPoint = 0
	}
	// Displace the previous validation candidate. Never close a SharedDomains the frontier owns — a promoted
	// generation is on loan to validation, and the producer is still building on it.
	if fv.sharedDom != nil && fv.sharedDom != sd && !fv.preExec.Owns(fv.sharedDom) {
		fv.sharedDom.Close()
	}
	fv.sharedDom = sd
	// Use the validation pipeline's own notifications object so that state
	// changes accumulated by exec3 during ValidateBlock are visible here.
	// Reset accumulator and receipts to match main's behaviour of creating
	// a fresh Sync (and thus fresh notifications) per validation call.
	fv.extendingForkNotifications = fv.executor.ValidationNotifications()
	fv.extendingForkNotifications.Accumulator.Reset(0)
	fv.extendingForkNotifications.RecentReceipts.Clear()
	status, latestValidHash, validationError, criticalError =
		fv.validateAndStorePayload(fv.ctx, fv.sharedDom, tx, header, body, unwindPoint, headersChain, bodiesChain)

	if fv.sharedDom != nil &&
		(criticalError != nil || status == engine_types.InvalidStatus) {
		fv.sharedDom.Close()
		fv.sharedDom = nil
	}

	return
}

// clear wipes out current extending fork data, this method is called after fcu is called,
// because fcu decides what the head is and after the call is done all the non-chosen forks are
// to be considered obsolete.
// Only a SharedDomains that validation OWNS is closed here — a generation lent by the frontier
// (an adopted pre-executed block) belongs to the producer, which is still building on top of it.
func (fv *ForkValidator) clear() {
	fv.extendingForkHeadHash = common.Hash{}
	fv.extendingForkNumber = 0
	if fv.sharedDom != nil && !fv.preExec.Owns(fv.sharedDom) {
		fv.sharedDom.Close()
	}
	fv.sharedDom = nil
	fv.extendingForkNotifications = nil
}

// ClearWithUnwind wipes out current extending fork data. Unconditional again: with pre-exec state held
// separately, this can no longer reach the producer's run-ahead blocks.
func (fv *ForkValidator) ClearWithUnwind() {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	fv.clear()
}

// FlashblockUpdate describes the result of checking whether an incoming
// block is a flashblock update to the in-progress block.
type FlashblockUpdate struct {
	// IsUpdate is true when the incoming block extends the in-progress flashblock.
	IsUpdate bool
	// PrefixLen is the number of already-executed transactions that match.
	// The caller should execute only transactions from PrefixLen onward.
	PrefixLen int
	// SD is the SharedDomains from the previous flashblock, carrying
	// accumulated VersionedIO state. Nil when IsUpdate is false.
	SD *execctx.SharedDomains
}

// AdoptPreExecuted moves the pre-executed generation for (hash, number) into validation space, making it the
// candidate the next FCU canonicalises. It reports whether there was one.
//
// This is THE BRIDGE, and newPayload is its only caller — either ValidatePayload for a block that reaches
// execution, or ValidateChain's locally-sealed fast path, which is what the producer's own block takes when
// the consensus layer hands it back. Nothing else may move a SharedDomains from pre-exec into validation.
func (fv *ForkValidator) AdoptPreExecuted(hash common.Hash, number uint64) bool {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	return fv.adoptPreExecutedLocked(hash, number)
}

// adoptPreExecutedLocked is AdoptPreExecuted with fv.lock held. The generation is LENT, not transferred: the
// frontier still owns the SharedDomains and is still building the block above it, so every place that would
// close fv.sharedDom asks preExec.Owns first.
func (fv *ForkValidator) adoptPreExecutedLocked(hash common.Hash, number uint64) bool {
	sd, notifications := fv.preExec.Gen(hash, number)
	if sd == nil {
		return false
	}
	if fv.sharedDom != nil && fv.sharedDom != sd && !fv.preExec.Owns(fv.sharedDom) {
		fv.sharedDom.Close()
	}
	fv.sharedDom = sd
	fv.extendingForkHeadHash = hash
	fv.extendingForkNumber = number
	fv.extendingForkNotifications = notifications
	fv.validHashes.Add(hash, true)
	return true
}

// ExecuteInto runs header+body into the CALLER's SharedDomains and reports the outcome, touching no
// validation state whatsoever — no extending fork, no validHashes, no candidate slot. It is the
// PRODUCER's execution primitive: pre-exec needs execution, not validation, and routing it through
// ValidatePayload (as it used to) is precisely what deposited pre-exec SharedDomains in the validation
// slot and made block production inherit the validator's teardown rules.
//
// No unwind point: the block extends the pre-exec frontier in memory, whose state the SD already carries
// through its parent chain — there is no canonical ancestor to unwind to.
//
// Returns the notifications accumulated for this block so the frontier can hold them; the FCU publishes
// them if and when the block canonicalises.
func (fv *ForkValidator) ExecuteInto(ctx context.Context, sd *execctx.SharedDomains, tx kv.TemporalRwTx, header *types.Header, body *types.RawBody,
) (status engine_types.EngineStatus, notifications *shards.Notifications, validationError error, criticalError error) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if fv.executor == nil {
		return engine_types.AcceptedStatus, nil, nil, nil
	}
	hash := header.Hash()
	number := header.Number.Uint64()

	// Use the pipeline's own notifications object so the state changes exec3 accumulates are visible here,
	// reset per round to match a fresh Sync's behaviour.
	notifications = fv.executor.ValidationNotifications()
	notifications.Accumulator.Reset(0)
	notifications.RecentReceipts.Clear()

	start := time.Now()
	if err := fv.executor.ValidateBlock(ctx, sd, tx, 0, []*types.Header{header}, []*types.RawBody{body}); err != nil {
		if errors.Is(err, rules.ErrInvalidBlock) {
			validationError = err
		} else {
			return status, nil, nil, fmt.Errorf("ExecuteInto: %w", err)
		}
	}
	fv.timingsCache.Add(hash, BlockTimings{time.Since(start), 0})
	if validationError != nil {
		return engine_types.InvalidStatus, notifications, validationError, nil
	}
	if _, err := rawdb.WriteRawBodyIfNotExists(tx, hash, number, body); err != nil {
		return status, nil, nil, err
	}
	return engine_types.ValidStatus, notifications, nil, nil
}

// MarkValid records a block hash as fully validated. The producer calls it once a block is SEALED (its
// block-end has run), so a later newPayload of that hash short-circuits.
func (fv *ForkValidator) MarkValid(hash common.Hash) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	fv.validHashes.Add(hash, true)
}

// validateAndStorePayload validate and store a payload fork chain if such chain results valid.
func (fv *ForkValidator) validateAndStorePayload(ctx context.Context, sd *execctx.SharedDomains, tx kv.TemporalRwTx, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
) (status engine_types.EngineStatus, latestValidHash common.Hash, validationError error, criticalError error) {
	start := time.Now()
	headersChain = append(headersChain, header)
	bodiesChain = append(bodiesChain, body)
	hash := header.Hash()
	number := header.Number.Uint64()
	if err := fv.executor.ValidateBlock(ctx, sd, tx, unwindPoint, headersChain, bodiesChain); err != nil {
		if errors.Is(err, rules.ErrInvalidBlock) {
			validationError = err
		} else {
			criticalError = fmt.Errorf("validateAndStorePayload: %w", err)
			return
		}
	}
	fv.timingsCache.Add(hash, BlockTimings{time.Since(start), 0})

	latestValidHash = hash
	fv.extendingForkHeadHash = hash
	fv.extendingForkNumber = number
	if validationError != nil {
		var latestValidNumber uint64
		latestValidNumber, criticalError = stages.GetStageProgress(tx, stages.Execution)

		if criticalError != nil {
			return
		}
		var ok bool
		latestValidHash, ok, criticalError = fv.blockReader.CanonicalHash(fv.ctx, tx, latestValidNumber)
		if criticalError != nil {
			return
		}
		if !ok {
			criticalError = fmt.Errorf("canonical hash not found: %d", latestValidNumber)
			return
		}
		status = engine_types.InvalidStatus
		fv.extendingForkHeadHash = common.Hash{}
		fv.extendingForkNumber = 0
		return
	}
	// A flashblock PRE-EXECUTE accumulation round must NOT mark the block hash validated: block-end
	// (systemEnd/Finalize) has not run yet, so the block is not fully sealed. The final round's hash is
	// identical to the CLOSE's hash (same body), so adding it here would make the close's ValidatePayload
	// short-circuit on the validHashes cache and skip the block-end seal entirely. Only the CLOSE
	// (FlashblockAccumulating unset) records the fully-validated hash.
	if !sd.FlashblockAccumulating() {
		fv.validHashes.Add(hash, true)
	}

	_, criticalError = rawdb.WriteRawBodyIfNotExists(tx, hash, number, body)
	if criticalError != nil {
		return //nolint:nilnesserr
	}

	status = engine_types.ValidStatus
	return
}

// GetTimings returns the timings of the last block validation.
func (fv *ForkValidator) GetTimings(hash common.Hash) BlockTimings {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if timings, ok := fv.timingsCache.Get(hash); ok {
		return timings
	}
	return BlockTimings{}
}

func (fv *ForkValidator) ExtendingFork() (common.Hash, uint64, *execctx.SharedDomains) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	return fv.extendingForkHeadHash, fv.extendingForkNumber, fv.sharedDom
}
