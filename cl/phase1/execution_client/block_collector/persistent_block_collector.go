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

package block_collector

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/execution/types"
)

// PersistentBlockCollector stores downloaded blocks to an MDBX database
// so they survive restarts. The database is cleared after successful loading.
type PersistentBlockCollector struct {
	db             kv.RwDB
	persistDir     string
	beaconChainCfg *clparams.BeaconChainConfig
	logger         log.Logger
	engine         execution_client.ExecutionEngine

	mu sync.Mutex
}

// openMaxRetries / openRetryDelay tolerate the brief window after a
// CaplinService.Restart during which the prior goroutine's async
// db.Close has not yet released the MDBX env lock. mdbx returns
// "resource temporarily unavailable" while the lock is held; retrying
// covers the common case without requiring cross-goroutine lifecycle
// synchronization at the call site. 30s is a generous upper bound:
// in practice the prior close completes in under 5s once any
// in-flight cursors / transactions wind down. If we exhaust the
// budget, the old goroutine is genuinely stuck — log loudly so the
// operator can investigate rather than silently retrying forever.
const (
	openMaxRetries = 30
	openRetryDelay = 1 * time.Second
)

func openPersistentDB(ctx context.Context, logger log.Logger, persistDir string) (kv.RwDB, error) {
	var lastErr error
	for attempt := 0; attempt < openMaxRetries; attempt++ {
		db, err := mdbx.New(kv.Label(dbcfg.CaplinDB), logger).
			Path(persistDir).
			WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
				return kv.TableCfg{
					kv.Headers: kv.TableCfgItem{},
				}
			}).
			GrowthStep(16 * datasize.MB).
			MapSize(1 * datasize.TB).
			Open(ctx)
		if err == nil {
			return db, nil
		}
		lastErr = err
		if !strings.Contains(err.Error(), "resource temporarily unavailable") {
			return nil, err
		}
		logger.Warn("[PersistentBlockCollector] MDBX env locked by prior instance; retrying", "attempt", attempt+1, "path", persistDir)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(openRetryDelay):
		}
	}
	return nil, lastErr
}

// NewPersistentBlockCollector creates a new persistent block collector
// that stores blocks in an MDBX database at the given directory. The
// MDBX handle is released ONLY by the explicit Close() method; the
// caller is responsible for invoking Close before its ctx-bound
// goroutine returns. This is load-bearing for CaplinService.Restart:
// the prior goroutine MUST release the env lock before the new
// goroutine reopens the same path, and a synchronous Close on the
// teardown path is the only way to guarantee that ordering (the
// previous async <-ctx.Done() pattern raced the relaunch's open).
func NewPersistentBlockCollector(
	ctx context.Context,
	logger log.Logger,
	engine execution_client.ExecutionEngine,
	beaconChainCfg *clparams.BeaconChainConfig,
	persistDir string,
) *PersistentBlockCollector {
	db, err := openPersistentDB(ctx, logger, persistDir)
	if err != nil {
		logger.Error("[PersistentBlockCollector] Failed to open database", "err", err, "path", persistDir)
		return nil
	}
	return &PersistentBlockCollector{
		db:             db,
		persistDir:     persistDir,
		beaconChainCfg: beaconChainCfg,
		logger:         logger,
		engine:         engine,
	}
}

// AddBlock adds a block to the collector, persisting it to the database
func (p *PersistentBlockCollector) AddBlock(block *cltypes.BeaconBlock) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db == nil {
		return fmt.Errorf("database not initialized")
	}

	// Encode the block
	payload := block.Body.ExecutionPayload
	encodedBlock, err := encodeBlock(payload, block.ParentRoot, block.Body.GetExecutionRequestsList())
	if err != nil {
		return fmt.Errorf("failed to encode block: %w", err)
	}

	// Create key for sorting (block number + hash)
	key, err := payloadKey(payload)
	if err != nil {
		return fmt.Errorf("failed to create payload key: %w", err)
	}

	// Store in database (skip if already exists)
	return p.db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.Headers, key, encodedBlock)
	})
}

// AddGloasBlock adds a GLOAS (EIP-7732) FULL block with its execution payload envelope to the collector.
// The execution payload is extracted from the envelope, not the beacon block body.
func (p *PersistentBlockCollector) AddGloasBlock(block *cltypes.BeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db == nil {
		return fmt.Errorf("database not initialized")
	}

	payload := envelope.Message.Payload
	executionRequestsList := cltypes.GetExecutionRequestsList(p.beaconChainCfg, envelope.Message.ExecutionRequests)
	encodedBlock, err := encodeBlock(payload, block.ParentRoot, executionRequestsList)
	if err != nil {
		return fmt.Errorf("failed to encode gloas block: %w", err)
	}

	key, err := payloadKey(payload)
	if err != nil {
		return fmt.Errorf("failed to create payload key: %w", err)
	}

	return p.db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.Headers, key, encodedBlock)
	})
}

// Flush loads all collected blocks into the execution engine and clears the database.
// Keys are block-number + payload SSZ root. Identical execution payloads therefore
// collide on payloadKey and tx.Put overwrites the existing row, so multiple rows at
// the same block number only exist when the execution payload itself differs (that
// is, competing execution forks at the same height). The variant chosen is the one
// whose BlockHash matches the ParentHash of the next row — a single-row look-ahead.
// If a real gap is detected, rows past the gap are kept so the next Flush can retry
// once the missing range is re-downloaded.
func (p *PersistentBlockCollector) Flush(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db == nil {
		return fmt.Errorf("database not initialized")
	}

	// Pre-Flush: drop cached entries that are unreachable from the EL's
	// current head. An admin SetHead (mode B) may have unwound the EL chain
	// past entries we already buffered, leaving them parented to a wiped TD
	// row — InsertBlocks would loop forever on "parent's total difficulty
	// not found". Only prune when a gap exists between elHead and the
	// lowest cached entry; a contiguous queue starting at elHead+1 is
	// insertable and preserved. Reproduced live on hoodi 2026-06-08 —
	// see docs/caplin-componentization-requirements.md for the architectural
	// follow-up.
	//
	// TODO(componentization): the right fix is for Caplin to write through
	// the storage component and subscribe to its bus, where Provider.Unwind
	// already publishes the event we'd want to consume. While Caplin is not
	// a component, this prune-on-Flush stop-gap stands in for that
	// subscription. See docs/caplin-componentization-requirements.md.
	if err := p.pruneStaleCachedBlocks(ctx); err != nil {
		p.logger.Warn("[BlockCollector] failed to prune stale cached blocks past EL head", "err", err)
	}

	blocksBatch := []*types.Block{}
	inserted := uint64(0)
	var lastInsertedBlock *types.Block

	// minInsertableBlockNumber is the floor below which Flush skips
	// cached entries — blocks already absorbed into EL state. Use
	// elHead+1, not FrozenBlocks(): after a Mode-B unwind, EL's
	// chaindata head sits well below the snapshot tip
	// (FrozenBlocks returns the snapshot tip, untouched by mode-B).
	// Keying the skip on FrozenBlocks would discard exactly the
	// blocks we need to push into EL for it to catch up across the
	// gap — observed live on hoodi soak v19 iter 4 (depth 60k,
	// elHead=2,986,464, FrozenBlocks=3,042,999): every cached
	// gap-block was skipped, Flush surfaced no work to do, and the
	// chain wedged at the unwind target for 1802s until the soak
	// driver gave up. elHead+1 makes the floor track EL's actual
	// progress, including the post-unwind regression.
	var minInsertableBlockNumber uint64
	if currentHeader, err := p.engine.CurrentHeader(ctx); err == nil && currentHeader != nil {
		minInsertableBlockNumber = currentHeader.Number.Uint64() + 1
	} else {
		// CurrentHeader fetch failed (rare — engine RPC down). Fall
		// back to FrozenBlocks as the old behaviour did; the prune-stale
		// step above already runs CurrentHeader once and gates on its
		// success, so reaching here means we're in degraded state and
		// the safer move is to skip more (avoid trying to insert blocks
		// EL probably can't accept right now).
		minInsertableBlockNumber = p.engine.FrozenBlocks(ctx)
	}
	var pending []*types.Block // variants at pendingHeight, awaiting resolution
	var pendingHeight uint64
	var lastCommittedHeight uint64
	gapDetected := false

	// resolvePending picks the variant from `pending` whose BlockHash matches
	// next.ParentHash. With one variant (no ambiguity) or next == nil (end of
	// cursor, nothing to match against), the first variant is returned. Returns
	// nil only when pending has multiple variants and none chains onto next.
	resolvePending := func(next *types.Block) *types.Block {
		if len(pending) == 0 {
			return nil
		}
		if len(pending) == 1 || next == nil {
			return pending[0]
		}
		for _, c := range pending {
			if c.Hash() == next.ParentHash() {
				return c
			}
		}
		return nil
	}

	if err := p.db.View(ctx, func(tx kv.Tx) error {
		cursor, err := tx.Cursor(kv.Headers)
		if err != nil {
			return err
		}
		defer cursor.Close()

		for k, v, err := cursor.First(); k != nil; k, v, err = cursor.Next() {
			if err != nil {
				return err
			}

			block, err := p.decodeBlock(v)
			if err != nil {
				p.logger.Warn("[BlockCollector] Failed to decode block", "key", common.Bytes2Hex(k), "err", err)
				continue
			}
			if block == nil {
				continue
			}
			if block.NumberU64() < minInsertableBlockNumber {
				continue
			}

			// Another variant at the current height: buffer it for look-ahead resolution.
			if pendingHeight > 0 && block.NumberU64() == pendingHeight {
				pending = append(pending, block)
				continue
			}

			// Different height. If not the immediate successor, it's a real gap —
			// we can't use this block to disambiguate competing variants at pendingHeight.
			if pendingHeight > 0 && block.NumberU64() != pendingHeight+1 {
				// Commit the pending group only if it's unambiguous. With multiple
				// variants and no successor to match against, leave rows for retry.
				if len(pending) == 1 {
					blocksBatch = append(blocksBatch, pending[0])
					lastCommittedHeight = pendingHeight
				}
				p.logger.Warn("[BlockCollector] Gap detected in collected blocks, will re-download missing range",
					"lastBlock", pendingHeight, "nextBlock", block.NumberU64(),
					"gap", block.NumberU64()-pendingHeight-1)
				gapDetected = true
				break
			}

			// Immediate successor: resolve the pending group against this block's parent.
			if pendingHeight > 0 {
				resolved := resolvePending(block)
				if resolved == nil {
					p.logger.Warn("[BlockCollector] Fork detected: no stored variant matches next block's parent, leaving rows for retry",
						"height", pendingHeight, "nextBlock", block.NumberU64(), "variants", len(pending))
					gapDetected = true
					break
				}
				blocksBatch = append(blocksBatch, resolved)
				lastCommittedHeight = pendingHeight
				if len(blocksBatch) >= batchSize {
					if err := p.insertBatch(ctx, blocksBatch, &inserted, &lastInsertedBlock); err != nil {
						return err
					}
					// Drive FCU after each batch so execution + prune can drain
					// BlockTransaction as InsertBlocks proceeds. Without this,
					// the entire backfill (potentially 100k+ blocks → 20+ GB
					// of tx data) accumulates in chaindata before any drain
					// can occur.
					if lastInsertedBlock != nil {
						p.doForkChoiceUpdate(ctx, lastInsertedBlock)
					}
					blocksBatch = []*types.Block{}
				}
			}

			pending = []*types.Block{block}
			pendingHeight = block.NumberU64()
		}

		// End of cursor: resolve the final pending group with no successor to match
		// against. Single variants are unambiguous. With multiple variants we can't
		// disambiguate, so leave them for a future Flush (same policy as the
		// mid-cursor gap branch) rather than guessing a pick the clean-path DB wipe
		// could permanently discard.
		if !gapDetected && pendingHeight > 0 {
			if len(pending) == 1 {
				blocksBatch = append(blocksBatch, pending[0])
				lastCommittedHeight = pendingHeight
			} else {
				p.logger.Warn("[BlockCollector] Fork at final height with no successor, leaving rows for retry",
					"height", pendingHeight, "variants", len(pending))
				gapDetected = true
			}
		}

		return nil
	}); err != nil {
		return fmt.Errorf("failed to flush blocks from database: %w", err)
	}

	// Insert remaining blocks
	if len(blocksBatch) > 0 {
		if err := p.insertBatch(ctx, blocksBatch, &inserted, &lastInsertedBlock); err != nil {
			return err
		}
	}

	if lastInsertedBlock != nil {
		p.doForkChoiceUpdate(ctx, lastInsertedBlock)
	}

	if gapDetected {
		// Prune only rows the caller is done with; rows past the gap stay so a
		// future re-download of the missing range unblocks the next Flush.
		// Use a non-cancelable context: if ctx was cancelled the caller cares
		// about stopping, but skipping cleanup would leave already-inserted
		// rows in place and the next Flush would re-read and re-insert them.
		cutoff := minInsertableBlockNumber
		if lastCommittedHeight+1 > cutoff {
			cutoff = lastCommittedHeight + 1
		}
		if err := p.db.Update(context.Background(), func(tx kv.RwTx) error {
			cursor, err := tx.RwCursor(kv.Headers)
			if err != nil {
				return err
			}
			defer cursor.Close()
			for k, _, err := cursor.First(); k != nil; k, _, err = cursor.Next() {
				if err != nil {
					return err
				}
				if len(k) < 8 {
					// Defensive: payloadKey always produces 40-byte keys.
					continue
				}
				if binary.BigEndian.Uint64(k[:8]) >= cutoff {
					break
				}
				if err := cursor.DeleteCurrent(); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			p.logger.Warn("[BlockCollector] Failed to prune consumed blocks", "err", err)
		}
		return nil
	}

	// No gap: drop the whole DB — cheaper than walking keys.
	p.db.Close()

	if err := dir.RemoveAll(p.persistDir); err != nil {
		p.logger.Warn("[BlockCollector] Failed to remove database directory", "err", err)
	}

	db, err := openPersistentDB(ctx, p.logger, p.persistDir)
	if err != nil {
		p.logger.Error("[BlockCollector] Failed to reopen database", "err", err)
		p.db = nil
		return fmt.Errorf("failed to reopen database: %w", err)
	}
	p.db = db

	return nil
}

func (p *PersistentBlockCollector) decodeBlock(v []byte) (*types.Block, error) {
	if len(v) == 0 {
		return nil, nil
	}

	v, err := utils.DecompressSnappy(v, false)
	if err != nil {
		return nil, err
	}
	if len(v) < 33 {
		return nil, fmt.Errorf("persistent block value too short: have %d, want at least 33", len(v))
	}

	version := clparams.StateVersion(v[0])
	parentRoot := common.BytesToHash(v[1:33])
	requestsHash := common.Hash{}

	if version >= clparams.ElectraVersion {
		if len(v) < 65 {
			return nil, fmt.Errorf("persistent block value too short for execution requests: have %d, want at least 65", len(v))
		}
		requestsHash = common.BytesToHash(v[33:65])
		v = v[65:]
	} else {
		v = v[33:]
	}

	executionPayload := cltypes.NewEth1Block(version, p.beaconChainCfg)
	if err := executionPayload.DecodeSSZ(v, int(version)); err != nil {
		return nil, err
	}

	body := executionPayload.Body()
	txs, err := types.DecodeTransactions(body.Transactions)
	if err != nil {
		return nil, err
	}

	// Skip genesis block
	if executionPayload.BlockNumber == 0 {
		return nil, nil
	}

	header, err := executionPayload.RlpHeader(&parentRoot, requestsHash)
	if err != nil {
		return nil, err
	}

	return types.NewBlockFromStorageWithBinaryTxs(executionPayload.BlockHash, header, txs, body.Transactions, nil, body.Withdrawals), nil
}

// caseCMaxCachedAhead bounds how many blocks past the lowest cached
// entry the gap-prune keeps in the cache while waiting for EL to catch
// up via the snapshot-backed Execution stage. Anything beyond is
// dropped to prevent unbounded growth if EL never catches up. Sized to
// comfortably cover typical mode-B unwind soak depths (≤ 90k blocks
// gap) without exhausting memory (~1 GB at ~100 KB/block). Declared as
// a var so tests can override.
var caseCMaxCachedAhead uint64 = 16384

// pruneStaleCachedBlocks reconciles the cached beacon-block queue against
// the EL's current head. Three cases on the entry seeked to (elHead+1):
//
//	A. No cached row past elHead → nothing to do.
//	B. Lowest cached row is exactly elHead+1 → contiguous; insertable.
//	C. Lowest cached row is > elHead+1 → gap.
//
// Case C fires a single ForkChoiceUpdate at the lowest cached block's
// hash and KEEPS the cache. Erigon's engineapi HandleForkChoice resolves
// the hash via the snapshot-backed BlockReader (the blocks above the
// post-unwind EL head live in snapshot files after a mode-B unwind)
// and, when headNum > finishProgressBefore by more than the
// smallBlockJumpThreshold, runs the Execution stage forward from elHead
// through the snapshot-backed blocks. On the next Flush the gap has
// closed (or shrunk past Case-B) and InsertBlocks proceeds normally.
//
// An upper-bound trim deletes cached entries past firstPast +
// caseCMaxCachedAhead so the queue can't grow without bound if EL never
// catches up.
//
// Idempotent and cheap on the steady-state path (Case A returns
// immediately after one cursor Seek; Case B does one comparison).
func (p *PersistentBlockCollector) pruneStaleCachedBlocks(ctx context.Context) error {
	currentHeader, err := p.engine.CurrentHeader(ctx)
	if err != nil {
		return fmt.Errorf("CurrentHeader: %w", err)
	}
	if currentHeader == nil {
		// No head yet (e.g. genesis before any execution). Nothing to prune.
		return nil
	}
	elHead := currentHeader.Number.Uint64()
	if elHead == 0 {
		// EL is reporting its head at genesis (block 0). This happens
		// during preverified bootstrap before the first FCU has fired:
		// CurrentHeader returns the genesis header even though the EL's
		// snapshot files cover blocks up to a far higher tip. Pruning
		// here would treat every cached beacon block (typically at
		// blocks 2.9M+) as "past elHead with a gap" and wipe the
		// entire historical-download payload before InsertBlocks gets
		// a chance to feed it to the EL. Skip; let the first FCU
		// advance the head before we make any prune decisions.
		return nil
	}
	cutoff := make([]byte, 8)
	binary.BigEndian.PutUint64(cutoff, elHead+1)

	var (
		firstPast    uint64
		trimmedTail  int
		lowestCached *types.Block
	)
	if err := p.db.Update(ctx, func(tx kv.RwTx) error {
		cursor, err := tx.RwCursor(kv.Headers)
		if err != nil {
			return err
		}
		defer cursor.Close()

		k, v, err := cursor.Seek(cutoff)
		if err != nil {
			return err
		}
		if k == nil {
			return nil // case A: nothing past elHead
		}
		if len(k) < 8 {
			return fmt.Errorf("pruneStaleCachedBlocks: malformed key length=%d", len(k))
		}
		firstPast = binary.BigEndian.Uint64(k)
		if firstPast == elHead+1 {
			return nil // case B: contiguous from elHead, insertable
		}
		// case C: gap detected.
		// Decode the lowest cached block — its hash is the FCU target.
		block, decErr := p.decodeBlock(v)
		if decErr != nil {
			return fmt.Errorf("pruneStaleCachedBlocks: decode lowest cached block: %w", decErr)
		}
		lowestCached = block
		// Trim the tail past firstPast + caseCMaxCachedAhead to bound
		// memory growth while EL catches up.
		trimAt := firstPast + caseCMaxCachedAhead
		trimCutoff := make([]byte, 8)
		binary.BigEndian.PutUint64(trimCutoff, trimAt)
		k2, _, err := cursor.Seek(trimCutoff)
		if err != nil {
			return err
		}
		for ; k2 != nil; k2, _, err = cursor.Next() {
			if err != nil {
				return err
			}
			if err := cursor.DeleteCurrent(); err != nil {
				return err
			}
			trimmedTail++
		}
		return nil
	}); err != nil {
		return err
	}
	if lowestCached != nil {
		gap := firstPast - elHead
		p.logger.Info("[BlockCollector] gap-prune Case C: FCU nudge",
			"elHead", elHead, "firstPast", firstPast, "gap", gap,
			"trimmedTail", trimmedTail, "trimAt", firstPast+caseCMaxCachedAhead)
		// Fire FCU at the lowest cached block's hash so EL's
		// engineapi initialCycle path runs the Execution stage
		// forward through snapshot-backed blocks until the gap closes.
		p.doForkChoiceUpdate(ctx, lowestCached)
	}
	return nil
}

func (p *PersistentBlockCollector) insertBatch(ctx context.Context, blocksBatch []*types.Block, inserted *uint64, lastInserted **types.Block) error {
	p.logger.Info("[BlockCollector] Inserting blocks",
		"from", blocksBatch[0].NumberU64(),
		"to", blocksBatch[len(blocksBatch)-1].NumberU64())

	if err := p.engine.InsertBlocks(ctx, blocksBatch, true); err != nil {
		p.logger.Warn("[BlockCollector] Failed to insert blocks", "err", err)
		return err
	}

	*inserted += uint64(len(blocksBatch))
	*lastInserted = blocksBatch[len(blocksBatch)-1]
	p.logger.Info("[BlockCollector] Inserted blocks", "progress", blocksBatch[len(blocksBatch)-1].NumberU64())

	return nil
}

// doForkChoiceUpdate sends a ForkChoiceUpdate to the EL for the given block.
func (p *PersistentBlockCollector) doForkChoiceUpdate(ctx context.Context, lastBlock *types.Block) {
	lastBlockHash := lastBlock.Hash()
	currentHeader, err := p.engine.CurrentHeader(ctx)
	if err != nil {
		p.logger.Warn("[BlockCollector] Failed to get current header", "err", err)
	}

	isForkchoiceNeeded := currentHeader == nil || lastBlock.NumberU64() > currentHeader.Number.Uint64()
	if !isForkchoiceNeeded {
		return
	}

	fcuVersion := clparams.DenebVersion
	if lastBlock.HeaderNoCopy().SlotNumber != nil {
		fcuVersion = clparams.GloasVersion
	}
	if _, err := p.engine.ForkChoiceUpdate(ctx, lastBlockHash, lastBlockHash, lastBlockHash, nil, fcuVersion); err != nil {
		p.logger.Warn("[BlockCollector] Failed to update fork choice", "err", err)
	}
}

// HasBlock checks if a block with the given number is already in the collector
func (p *PersistentBlockCollector) HasBlock(blockNumber uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db == nil {
		return false
	}

	var hasBlock bool
	if err := p.db.View(context.Background(), func(tx kv.Tx) error {
		cursor, err := tx.Cursor(kv.Headers)
		if err != nil {
			return err
		}
		defer cursor.Close()
		// Keys are prefixed with block number (8 bytes big-endian)
		prefix := make([]byte, 8)
		binary.BigEndian.PutUint64(prefix, blockNumber)
		k, _, err := cursor.Seek(prefix)
		if err != nil {
			return err
		}
		// Check if the key starts with our block number
		hasBlock = len(k) >= 8 && binary.BigEndian.Uint64(k[:8]) == blockNumber
		return nil
	}); err != nil {
		p.logger.Warn("[BlockCollector] Failed to check for block", "err", err)
	}

	return hasBlock
}

// Close closes the database
func (p *PersistentBlockCollector) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db != nil {
		p.db.Close()
		p.db = nil
	}
	return nil
}
