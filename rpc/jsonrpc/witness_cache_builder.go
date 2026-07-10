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

package jsonrpc

import (
	"context"
	"errors"
	"time"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

const (
	witnessBuildInitialBackoff = 10 * time.Millisecond
	witnessBuildMaxBackoff     = 500 * time.Millisecond
)

// headState is the per-attempt verdict of waitCommittedHead's decision logic.
type headState int

const (
	headWait    headState = iota // committed head below num; retry after backoff
	headReorged                  // committed head reached num but the hash there differs; block gone
	headBuild                    // committed head reached num and the hash matches; build now
)

// decideCommittedHead is the pure gate waitCommittedHead loops on: build once the
// committed head has reached num and the canonical hash there still matches the
// requested hash; a mismatch means a reorg dropped the block; otherwise keep waiting.
func decideCommittedHead(committedHead, num uint64, canonicalHash, wantHash common.Hash) headState {
	if committedHead < num {
		return headWait
	}
	if canonicalHash != wantHash {
		return headReorged
	}
	return headBuild
}

// shouldBuild is the tip-gate: build only a clean single-block advance to the newest
// header seen that has not already been built. Multi-header (reorg/catch-up) batches,
// a superseded tip, and an already-built number all fall through to on-demand.
func shouldBuild(num uint64, singleHeaderBatch bool, lastSeen, frozen uint64) bool {
	return singleHeaderBatch && num > frozen && num == lastSeen
}

// waitCommittedHead blocks until the committed head reaches num, then returns the open
// temporal RO tx to build against. It opens a fresh tx per attempt because an MDBX RO
// tx pins a fixed snapshot, so committed progress only advances on a re-opened tx. It
// returns (nil,false,nil) if the block reorged away before committing, or the ctx error
// on shutdown; the returned tx is owned by the caller.
func waitCommittedHead(ctx context.Context, db kv.TemporalRoDB, num uint64, hash common.Hash) (kv.TemporalTx, bool, error) {
	backoff := witnessBuildInitialBackoff
	for {
		// The tx is handed to the caller on a match and rolled back explicitly on every
		// other branch, so a deferred rollback is inapplicable (and wrong inside the loop).
		tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic
		if err != nil {
			return nil, false, err
		}
		committedHead, err := stages.GetStageProgress(tx, stages.Finish)
		if err != nil {
			tx.Rollback()
			return nil, false, err
		}
		var canonicalHash common.Hash
		if committedHead >= num {
			if canonicalHash, err = rawdb.ReadCanonicalHash(tx, num); err != nil {
				tx.Rollback()
				return nil, false, err
			}
		}
		switch decideCommittedHead(committedHead, num, canonicalHash, hash) {
		case headBuild:
			return tx, true, nil
		case headReorged:
			tx.Rollback()
			return nil, false, nil
		default: // headWait
			tx.Rollback()
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > witnessBuildMaxBackoff {
				backoff = witnessBuildMaxBackoff
			}
		}
	}
}

// WitnessCacheShouldEnable is the embedded-wiring gate: the eager cache runs only
// when a positive block count is requested and the datadir was built with commitment
// history (without it every build errors, so the cache would stay empty).
func WitnessCacheShouldEnable(blocks uint, commitmentHistoryEnabled bool) bool {
	return blocks > 0 && commitmentHistoryEnabled
}

// NewWitnessCacheBuilderAPI builds the shared witness cache plus a builder-owned
// DebugAPIImpl wired to it, mirroring the debug impl APIList constructs for serving
// so both paths run the identical build seam. The returned cache is threaded into
// APIList so the serve-side impl shares it; the returned impl drives
// RunWitnessCacheBuilder. When enable is false it returns (nil, nil) and the caller
// leaves the cache disabled.
func NewWitnessCacheBuilderAPI(
	enable bool,
	db kv.TemporalRoDB, eth rpchelper.ApiBackend,
	filters *rpchelper.Filters, stateCache kvcache.Cache,
	blockReader services.FullBlockReader, cfg *httpcfg.HttpCfg,
	engine rules.EngineReader, bridgeReader bridgeReader,
) (*witnessCache, *DebugAPIImpl) {
	if !enable {
		return nil, nil
	}
	cache := newWitnessCache(cfg.WitnessCacheBlocks, cfg.WitnessCacheMaxMB)
	base := NewBaseApi(filters, stateCache, blockReader, cfg.WithDatadir, cfg.EvmCallTimeout, engine, cfg.Dirs, bridgeReader, cfg.BlockRangeLimit, cfg.GetLogsMaxResults)
	impl := NewPrivateDebugAPI(base, db, eth, cfg.Gascap, cfg.GethCompatibility)
	impl.witnessCache = cache
	return cache, impl
}

// RunWitnessCacheBuilder consumes canonical-header batches and eagerly builds the
// legacy-mode witness for the freshest tip into the shared cache, reconciling the cache
// against every batch so reorgs evict stale entries. It runs until ctx is done or the
// header channel closes. A nil cache is a no-op.
func RunWitnessCacheBuilder(ctx context.Context, dbg *DebugAPIImpl, headerCh <-chan [][]byte) {
	cache := dbg.witnessCache
	if cache == nil {
		return
	}
	var frozen, lastSeen uint64
	for {
		select {
		case <-ctx.Done():
			return
		case batch, ok := <-headerCh:
			if !ok {
				return
			}
			newest, single, valid := processHeaderBatch(cache, batch)
			if !valid {
				continue
			}
			lastSeen = max(lastSeen, newest.num)
			// Coalesce: drain queued batches to the freshest tip, reconciling each so a
			// missed eviction self-heals, then build only that newest header.
			for draining := true; draining; {
				select {
				case b2, open := <-headerCh:
					if !open {
						draining = false
						break
					}
					if n2, s2, v2 := processHeaderBatch(cache, b2); v2 {
						witnessCacheCoalesceDropCounter.Inc()
						newest, single = n2, s2
						lastSeen = max(lastSeen, n2.num)
					}
				default:
					draining = false
				}
			}
			if !shouldBuild(newest.num, single, lastSeen, frozen) {
				continue
			}
			if dbg.buildAndCache(ctx, newest.num, newest.hash) {
				frozen = newest.num
			} else if ctx.Err() != nil {
				return
			}
		}
	}
}

// processHeaderBatch decodes a canonical-header batch, reconciles the cache against it,
// and returns the newest header, whether the batch was a single-header advance, and
// whether the batch was usable (valid=false on a decode error or empty batch).
func processHeaderBatch(cache *witnessCache, batch [][]byte) (newest headerRef, singleHeaderBatch, valid bool) {
	refs, err := decodeHeaderRefs(batch)
	if err != nil {
		log.Warn("[witness-cache] decode headers", "err", err)
		return headerRef{}, false, false
	}
	if len(refs) == 0 {
		return headerRef{}, false, false
	}
	cache.reconcile(refs)
	newest = refs[0]
	for _, r := range refs[1:] {
		if r.num > newest.num {
			newest = r
		}
	}
	return newest, len(refs) == 1, true
}

// decodeHeaderRefs decodes each RLP-encoded header in a batch to its (num, hash),
// mirroring the header-subscription decode in rpchelper. Empty payloads are skipped.
func decodeHeaderRefs(batch [][]byte) ([]headerRef, error) {
	refs := make([]headerRef, 0, len(batch))
	for _, payload := range batch {
		if len(payload) == 0 {
			continue
		}
		var header types.Header
		if err := rlp.DecodeBytes(payload, &header); err != nil {
			return nil, err
		}
		refs = append(refs, headerRef{num: header.Number.Uint64(), hash: header.Hash()})
	}
	return refs, nil
}

// buildAndCache waits for num to commit, builds its legacy witness against the committed
// tx via the shared buildWitnessResult seam, and stores it. It returns false without
// caching on a reorg-away, a build error (so the block falls through to on-demand), or
// shutdown; the caller checks ctx to distinguish shutdown.
func (api *DebugAPIImpl) buildAndCache(ctx context.Context, num uint64, hash common.Hash) bool {
	tx, ok, err := waitCommittedHead(ctx, api.db, num, hash)
	if err != nil {
		if ctx.Err() == nil {
			log.Warn("[witness-cache] wait committed head", "block", num, "err", err)
			witnessCacheBuildFailOtherCounter.Inc()
		}
		return false
	}
	if !ok {
		return false
	}
	defer tx.Rollback()

	// Resolve by the hash waitCommittedHead already validated as canonical at num, not
	// by number: the built block is then provably the one stored under (num, hash), with
	// no dependence on how block-number resolution treats the pending-commit overlay.
	info, err := api.resolveWitnessBlock(ctx, tx, rpc.BlockNumberOrHashWithHash(hash, false))
	if err != nil {
		log.Warn("[witness-cache] resolve block", "block", num, "err", err)
		witnessCacheBuildFailOtherCounter.Inc()
		return false
	}
	start := time.Now()
	result, err := api.buildWitnessResult(ctx, tx, info, witnessModeLegacy)
	witnessCacheBuildDuration.ObserveDuration(start)
	if err != nil {
		if errors.Is(err, errWitnessVerifyFailed) {
			witnessCacheBuildFailVerifyCounter.Inc()
		} else {
			witnessCacheBuildFailOtherCounter.Inc()
		}
		log.Warn("[witness-cache] build witness", "block", num, "err", err)
		return false
	}
	api.witnessCache.put(num, hash, result)
	witnessCacheBuildOKCounter.Inc()
	return true
}
