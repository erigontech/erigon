// Copyright 2026 The Erigon Authors
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

package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// CommitmentDomainValidator is the Stage-2 batch validator: when a
// commitment-domain step batch reaches LifecycleIndexed, this opens
// the commitment.kv via the existing aggregator read path, decodes
// KeyCommitmentState, runs the consistency checks, and registers the
// derived (step, block) binding into the inventory.
//
// Three responsibilities in one pass:
//
//  1. **State decoded successfully.** The KeyCommitmentState entry
//     for the step's range exists and parses. Catches torrent-piece-
//     hash-pass but format-corrupt files.
//
//  2. **Recorded blockNum is consistent with recorded txNum.** The
//     block whose txnum range includes the commitment's recorded
//     txNum must equal the recorded blockNum. Catches files where
//     the (txNum, blockNum) pair was clobbered or written against
//     the wrong block. Note: commitment files are written at STEP
//     boundaries (txNum = step's last txnum), and step boundaries
//     normally cut mid-block — so an "end-of-block" check would
//     reject every well-formed file. The block-membership check
//     captures the real invariant.
//
//  3. **Bind step → block.** On success, register the
//     (toStep, blockNum) binding in the inventory so block-snapshot
//     callers can map block ranges into step units.
//
// Runs identically on publisher and consumer — both call this from
// their lifecycle's BuildOnBatchValidation chain. Failure on either
// side prevents the step from advancing to Advertisable.
//
// Non-commitment-domain steps short-circuit (no-op): there's no
// commitment.kv to read, and the validator has nothing to check.
type CommitmentDomainValidator struct {
	DB          kv.TemporalRoDB
	BlockReader services.FullBlockReader
	Inventory   *snapshot.Inventory

	// Logger may be nil. When set, ValidateStep emits a positive
	// "binding registered" Info line on every successful validation
	// — used by multi-day fleet tests to distinguish lifecycle-path
	// bindings (commitment files going through the per-step batch
	// hook on publisher OR consumer) from the bootstrap-seeder
	// path (which has its own existing log line). Bootstrap-path
	// callers leave this nil to suppress duplicate logs.
	Logger log.Logger

	// PausedCache is optional. When set, ValidateStep memoizes the
	// LoadCommitmentRoot result for files that hit the partial-block
	// pause path. On the publisher's race window (commitment lands
	// before the matching block .seg reaches Advertisable), each
	// sweep retries; with the cache, retries skip the heavyweight
	// read+decode+invariants chain and re-check only the
	// blockSegAdvertisableForBlock gate. Cleared on successful
	// validation. Nil → cache disabled (behaviour unchanged from
	// before this field existed).
	PausedCache *PausedCommitmentCache
}

// PausedCommitmentCache memoizes the read+decode+invariants result of
// commitment validation across paused retries. Concurrency-safe.
// Production wiring (storage Provider) constructs one and injects via
// CommitmentDomainValidator.PausedCache; tests can leave it nil.
type PausedCommitmentCache struct {
	mu sync.Mutex
	m  map[string]integrity.CommitmentRootInfo
}

// NewPausedCommitmentCache returns an empty cache.
func NewPausedCommitmentCache() *PausedCommitmentCache {
	return &PausedCommitmentCache{m: make(map[string]integrity.CommitmentRootInfo)}
}

// Get returns the cached info and whether the entry exists.
func (c *PausedCommitmentCache) Get(name string) (integrity.CommitmentRootInfo, bool) {
	if c == nil {
		return integrity.CommitmentRootInfo{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	info, ok := c.m[name]
	return info, ok
}

// Put records info for name (overwriting any prior entry).
func (c *PausedCommitmentCache) Put(name string, info integrity.CommitmentRootInfo) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[name] = info
}

// Forget clears the entry for name. Called when validation passes
// (no longer paused) or when external state changes invalidate the
// cached info.
func (c *PausedCommitmentCache) Forget(name string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, name)
}

// Name implements validation.StepValidator.
func (CommitmentDomainValidator) Name() string { return "commitment_domain_state_at_end" }

// ValidateStep implements validation.StepValidator. Short-circuits
// for non-commitment-domain groups (introspects files[0] for the
// domain — within a group all files share the relevant axis).
func (v CommitmentDomainValidator) ValidateStep(ctx context.Context, files []*snapshot.FileEntry) error {
	if len(files) == 0 || files[0].Domain != snapshot.DomainCommitment {
		return nil
	}
	fromStep := files[0].FromStep
	toStep := files[0].ToStep
	if v.DB == nil {
		return fmt.Errorf("CommitmentDomainValidator: nil DB")
	}
	if v.BlockReader == nil {
		return fmt.Errorf("CommitmentDomainValidator: nil BlockReader")
	}

	// At fresh bootstrap, the snapshot files are on disk but the EL
	// hasn't OpenSegments'd yet — BlockReader.FrozenBlocks() returns
	// 0 and any header lookup will fail. Return ErrPause so the
	// lifecycle retries on next sweep instead of accumulating
	// failures toward quarantine. The block-aligned validators
	// (HeaderChain / TxRoot / Receipt) detect this via per-block
	// nil-header checks; commitment's deeper LoadCommitmentRoot path
	// makes per-block detection awkward, so an early gate is cleaner.
	if v.BlockReader.FrozenBlocks() == 0 {
		return fmt.Errorf("BlockReader frozen tip = 0 (EL hasn't opened segments yet): %w", validation.ErrPause)
	}

	tx, err := v.DB.BeginTemporalRo(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Cache hit on a previously-paused file: skip the heavyweight
	// read+decode+invariants and re-check only the gate that's
	// expected to change between sweeps (block .seg reaching
	// Advertisable). Cache stays valid for the file's lifetime at
	// LifecycleIndexed — once it advances, the lifecycle dispatch
	// won't call us again on this name.
	info, cached := v.PausedCache.Get(files[0].Name)
	if !cached {
		stepSize := tx.Debug().StepSize()
		endTxNum := toStep * stepSize
		startTxNum := fromStep * stepSize
		var err error
		info, err = integrity.LoadCommitmentRoot(ctx, tx, v.BlockReader, startTxNum, endTxNum)
		if err != nil {
			return fmt.Errorf("commitment step [%d, %d): %w", fromStep, toStep, err)
		}
	}

	if !info.PartialBlock() {
		if err := integrity.VerifyCommitmentMatchesHeader(ctx, tx, v.BlockReader, info); err != nil {
			return fmt.Errorf("commitment step [%d, %d): %w", fromStep, toStep, err)
		}
	} else if !blockSegAdvertisableForBlock(v.Inventory, info.BlockNum) {
		// Block-aligned commitments cross-check against header.stateRoot;
		// partial-block commitments pause until the matching block .seg
		// is Advertisable for replay-verify. See
		// docs/plans/20260510-partial-block-validation-model.md.
		// validation.ErrPause is treated as transient by the lifecycle
		// dispatch — no quarantine tick.
		v.PausedCache.Put(files[0].Name, info)
		return fmt.Errorf("partial-block commitment for block %d (txNum=%d, blockMaxTxNum=%d) — block .seg not yet Advertisable; pausing (step [%d, %d)): %w",
			info.BlockNum, info.TxNum, info.BlockMaxTxNum, fromStep, toStep, validation.ErrPause)
	}
	// Validation passed — drop any prior pause-cache entry.
	v.PausedCache.Forget(files[0].Name)

	if v.Inventory != nil {
		v.Inventory.RegisterStepBlockBoundary(toStep, info.BlockNum)
		v.Inventory.SetAnchors(files[0].Name, snapshot.Anchors{
			Root:           info.RootHash,
			AtBlock:        info.BlockNum,
			AtTxNum:        info.TxNum,
			IsPartialBlock: info.PartialBlock(),
		})
	}
	logBindingRegistered(v.Logger, "lifecycle", files[0].Name, toStep, &info.BlockNum)
	return nil
}

// logBindingRegistered emits the canonical (step, block) binding log
// with a source tag. Used by both the lifecycle-path validator and
// the bootstrap-seeder; multi-day fleet tests grep one signal for
// both. block is a pointer because the bootstrap caller doesn't
// always know it (it's resolved lazily via the validator's side
// effect on Inventory).
func logBindingRegistered(logger log.Logger, source, name string, toStep uint64, block *uint64) {
	if logger == nil {
		return
	}
	if block != nil {
		logger.Info("[storage] binding registered",
			"source", source, "name", name, "toStep", toStep, "block", *block)
		return
	}
	logger.Info("[storage] binding registered",
		"source", source, "name", name, "toStep", toStep)
}

// blockSegAdvertisableForBlock reports whether some block-domain
// FileEntry in inv covers blockNum and is at LifecycleAdvertisable.
// Uses the FromBlock/ToBlock fields already populated by
// PopulateFromName at AddFile time — no re-parse. Iterates in reverse
// because partial-block validations are tip-relative and matching
// block .seg entries are appended last.
func blockSegAdvertisableForBlock(inv *snapshot.Inventory, blockNum uint64) bool {
	if inv == nil {
		return false
	}
	files := inv.BlockFiles()
	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]
		if !f.Advertisable || f.ToBlock <= f.FromBlock {
			continue
		}
		if blockNum >= f.FromBlock && blockNum < f.ToBlock {
			return true
		}
	}
	return false
}

// seedLatestCommitmentBinding finds the highest commitment-domain
// step in the inventory and runs CommitmentDomainValidator on it
// directly to register a (step, block) binding. Called once after
// bootstrap populates the inventory.
//
// Bootstrap files start at LifecycleAdvertisable directly (back-
// compat: visible-in-aggregator implies fully validated by previous
// runs), so they bypass the lifecycle's per-step batch validation
// and CommitmentDomainValidator never fires on them. Without a
// binding registered, the block-step wait gate in
// BuildOnBatchValidation blocks ALL block files indefinitely. This
// helper closes that loop by registering the latest bootstrap
// commitment binding directly.
//
// On error or empty inventory, no binding is registered — block
// files keep waiting until a commitment file goes through the
// lifecycle (e.g. post-tip retire produces one).
// stepValidator is the narrow interface seedLatestCommitmentBinding
// needs from CommitmentDomainValidator — exposed to make the
// candidate-iteration logic testable without a real DB / BlockReader.
type stepValidator interface {
	ValidateStep(ctx context.Context, files []*snapshot.FileEntry) error
}

func seedLatestCommitmentBinding(ctx context.Context, inv *snapshot.Inventory, v stepValidator, logger log.Logger) {
	if inv == nil {
		return
	}
	commitmentFiles := inv.AllDomainFiles(snapshot.DomainCommitment)
	if len(commitmentFiles) == 0 {
		if logger != nil {
			logger.Debug("[storage] no bootstrap commitment files; block-files will wait until lifecycle produces one")
		}
		return
	}

	// Sort by ToStep descending — try the most recent first, fall
	// back to earlier steps if the latest fails (e.g. mid-block
	// commitment files written by an interrupted retire). We need
	// any binding to release the block-step wait gate; an older
	// binding still covers all blocks at or below its step boundary.
	candidates := make([]*snapshot.FileEntry, 0, len(commitmentFiles))
	for _, e := range commitmentFiles {
		if e == nil || e.ToStep == 0 {
			continue
		}
		candidates = append(candidates, e)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ToStep > candidates[j].ToStep
	})
	if len(candidates) == 0 {
		return
	}

	var lastErr error
	var lastFailed *snapshot.FileEntry
	for _, candidate := range candidates {
		if err := v.ValidateStep(ctx, []*snapshot.FileEntry{candidate}); err != nil {
			lastErr = err
			lastFailed = candidate
			continue
		}
		logBindingRegistered(logger, "bootstrap", candidate.Name, candidate.ToStep, nil)
		return
	}

	// All candidates failed. Logged at Warn so operators can
	// investigate persistent failures; the most recent failure
	// carries enough context to diagnose.
	if logger != nil && lastFailed != nil {
		logger.Warn("[storage] failed to seed any bootstrap commitment binding (all candidates rejected)",
			"candidates", len(candidates),
			"lastTried", lastFailed.Name, "lastStep", lastFailed.ToStep, "err", lastErr)
	}
}
