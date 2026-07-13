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

// shouldBuild gates eager building: a single-header advance to the freshest header in the
// batch whose hash isn't cached yet. Keying on the cached hash rather than a high-water
// block number lets a reorged head — a new hash at an already-built height — still be
// rebuilt. Multi-header (catch-up) batches and a superseded tip fall through to on-demand.
func shouldBuild(num uint64, singleHeaderBatch bool, freshest uint64, alreadyCached bool) bool {
	return singleHeaderBatch && num == freshest && !alreadyCached
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

// rollingPin is the one-block-lag parent snapshot the head-capture builder holds: an
// open RO temporal tx whose commitment-latest is the parent(B) commitment plane when it
// is pinned at B-1, tagged with the (num, hash) it was opened at. Owned solely by the
// builder goroutine; released and re-opened as the tip rolls forward.
type rollingPin struct {
	tx   kv.TemporalTx
	num  uint64
	hash common.Hash
}

// close rolls back the pinned tx and clears it, so a second call is a no-op.
func (p *rollingPin) close() {
	if p != nil && p.tx != nil {
		p.tx.Rollback()
		p.tx = nil
	}
}

// openRollingPin opens a fresh RO temporal tx pinned at the current committed head and
// tags it with that head's (num, canonical hash); the tx's commitment-latest is that
// head's commitment plane. It returns (nil,nil) before any block has committed (num 0)
// or when the head has no canonical hash. The caller owns the returned tx.
func openRollingPin(ctx context.Context, db kv.TemporalRoDB) (*rollingPin, error) {
	// The tx is handed to the caller on success and rolled back on every other branch,
	// so a deferred rollback is inapplicable.
	tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	num, err := stages.GetStageProgress(tx, stages.Finish)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	if num == 0 {
		tx.Rollback()
		return nil, nil
	}
	hash, err := rawdb.ReadCanonicalHash(tx, num)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	if hash == (common.Hash{}) {
		tx.Rollback()
		return nil, nil
	}
	return &rollingPin{tx: tx, num: num, hash: hash}, nil
}

// pinVerdict is decidePin's ruling on whether the held rolling pin can serve as the
// commitment parent of block B.
type pinVerdict int

const (
	pinUsable pinVerdict = iota // pin sits at B-1 with a still-canonical hash; build B with it
	pinStale                    // pin missing, off by more than one block, or reorged; drop and skip B
)

// decidePin reports whether a rolling pin held at (pinNum, pinHash) is a valid commitment
// parent for block B: it must sit exactly one block back (pinNum == B-1) and its hash must
// still be canonical at B-1 (canonicalParentHash). No pin, a pin off by more than one block
// (tip jumped past a coalesced gap), or a reorged parent all read as stale — B is skipped to
// out-of-window and the pin is re-established at the committed head.
func decidePin(havePin bool, pinNum uint64, pinHash common.Hash, blockNum uint64, canonicalParentHash common.Hash) pinVerdict {
	if !havePin || blockNum == 0 || pinNum != blockNum-1 || pinHash != canonicalParentHash {
		return pinStale
	}
	return pinUsable
}

// WitnessCacheShouldEnable is the embedded-wiring gate: the eager cache runs only
// when a positive block count is requested and the node can build witnesses at all —
// either the datadir carries commitment history (durable recompute) or head-capture is
// enabled (pinned-parent build on a minimal node). Without either, every build errors
// and the cache would stay empty.
func WitnessCacheShouldEnable(blocks uint, commitmentHistoryEnabled, headCapture bool) bool {
	return blocks > 0 && (commitmentHistoryEnabled || headCapture)
}

// WitnessCacheMode resolves the eager-cache wiring decision: whether to enable it and,
// if so, whether it runs in head-capture (cache-only, pinned-parent) mode. Head-capture
// engages only when commitment history is absent and the flag is set; a datadir with
// commitment history keeps the durable recompute path even if the flag is on.
func WitnessCacheMode(blocks uint, commitmentHistoryEnabled, headCaptureFlag bool) (enable, headCapture bool) {
	enable = WitnessCacheShouldEnable(blocks, commitmentHistoryEnabled, headCaptureFlag)
	headCapture = enable && !commitmentHistoryEnabled && headCaptureFlag
	return enable, headCapture
}

// NewWitnessCacheBuilderAPI builds the shared witness cache plus a builder-owned
// DebugAPIImpl wired to it, mirroring the debug impl APIList constructs for serving
// so both paths run the identical build seam. The returned cache is threaded into
// APIList so the serve-side impl shares it; the returned impl drives
// RunWitnessCacheBuilder. When enable is false it returns (nil, nil) and the caller
// leaves the cache disabled.
func NewWitnessCacheBuilderAPI(
	enable, headCapture bool,
	db kv.TemporalRoDB, eth rpchelper.ApiBackend,
	filters *rpchelper.Filters, stateCache kvcache.Cache,
	blockReader services.FullBlockReader, cfg *httpcfg.HttpCfg,
	engine rules.EngineReader, bridgeReader bridgeReader,
) (*witnessResultCache, *DebugAPIImpl) {
	if !enable {
		return nil, nil
	}
	cache := newWitnessResultCache(cfg.WitnessCacheBlocks, int(cfg.WitnessCacheMaxMB)*bytesPerMB, headCapture, headCapture)
	base := NewBaseApi(filters, stateCache, blockReader, cfg.WithDatadir, cfg.EvmCallTimeout, engine, cfg.Dirs, bridgeReader, cfg.BlockRangeLimit, cfg.GetLogsMaxResults)
	impl := NewPrivateDebugAPI(base, db, eth, cfg.Gascap, cfg.GethCompatibility)
	impl.witnessCache = cache
	return cache, impl
}

// RunWitnessCacheBuilder consumes canonical-header batches and eagerly builds the
// legacy-mode witness for the freshest tip into the shared cache. It runs until ctx is
// done or the header channel closes. A nil cache is a no-op.
func RunWitnessCacheBuilder(ctx context.Context, dbg *DebugAPIImpl, headerCh <-chan [][]byte) {
	if dbg.witnessCache == nil {
		return
	}
	headCapture := dbg.witnessCache.HeadCapture()
	// pin is the one-block-lag parent snapshot for head-capture builds; nil in the
	// durable path. The deferred close releases whichever pin is held at shutdown.
	var pin *rollingPin
	defer func() { pin.close() }()
	for {
		select {
		case <-ctx.Done():
			return
		case batch, ok := <-headerCh:
			if !ok {
				return
			}
			newest, single, valid := processHeaderBatch(batch)
			if !valid {
				continue
			}
			// Coalesce: drain queued batches to the freshest tip, then build only that
			// newest header. freshest is per-burst so a reorg to a lower tip is not masked.
			freshest := newest.num
			for draining := true; draining; {
				select {
				case b2, open := <-headerCh:
					if !open {
						draining = false
						break
					}
					if n2, s2, v2 := processHeaderBatch(b2); v2 {
						witnessCacheCoalesceDropCounter.Inc()
						newest, single = n2, s2
						freshest = max(freshest, n2.num)
					}
				default:
					draining = false
				}
			}
			if !shouldBuild(newest.num, single, freshest, dbg.witnessCache.Contains(newest.hash)) {
				continue
			}
			if headCapture {
				pin = dbg.buildAndCacheHeadCapture(ctx, pin, newest.num, newest.hash)
				if ctx.Err() != nil {
					return
				}
				continue
			}
			if !dbg.buildAndCache(ctx, newest.num, newest.hash) && ctx.Err() != nil {
				return
			}
		}
	}
}

// headerRef is a canonical block's number and hash, decoded from a header batch.
type headerRef struct {
	num  uint64
	hash common.Hash
}

// processHeaderBatch decodes a canonical-header batch and returns the newest header,
// whether the batch was a single-header advance, and whether it was usable
// (valid=false on a decode error or empty batch).
func processHeaderBatch(batch [][]byte) (newest headerRef, singleHeaderBatch, valid bool) {
	refs, err := decodeHeaderRefs(batch)
	if err != nil {
		log.Warn("[witness-cache] decode headers", "err", err)
		return headerRef{}, false, false
	}
	if len(refs) == 0 {
		return headerRef{}, false, false
	}
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
	result, err := api.buildWitnessResult(ctx, tx, nil, info, witnessModeLegacy)
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
	enc, err := result.MarshalFastJSON()
	if err != nil {
		witnessCacheBuildFailOtherCounter.Inc()
		log.Warn("[witness-cache] marshal witness", "block", num, "err", err)
		return false
	}
	api.witnessCache.Add(hash, &ExecutionWitnessResult{cachedJSON: enc})
	witnessCacheEntriesResidentGauge.SetInt(api.witnessCache.Len())
	witnessCacheBuildOKCounter.Inc()
	return true
}

// buildAndCacheHeadCapture builds block B's witness on a minimal (no commitment-history)
// node: the held rolling pin (at B-1) supplies parent commitment-latest and waitCommittedHead
// supplies the committed >=B tx for plain history. It validates the pin is a canonical parent
// of B; a stale pin (tip jumped, reorg, or none yet) skips B to out-of-window and caches
// nothing. It returns the pin to hold next: rolled forward to the committed head after a build
// or a skip, the unchanged pin on a transient wait error / reorg-before-commit, or nil if
// re-pinning failed.
func (api *DebugAPIImpl) buildAndCacheHeadCapture(ctx context.Context, pin *rollingPin, num uint64, hash common.Hash) *rollingPin {
	committedTx, ok, err := waitCommittedHead(ctx, api.db, num, hash)
	if err != nil {
		if ctx.Err() == nil {
			log.Warn("[witness-cache] wait committed head", "block", num, "err", err)
			witnessCacheBuildFailOtherCounter.Inc()
		}
		return pin
	}
	if !ok {
		return pin
	}
	defer committedTx.Rollback()

	api.tryHeadCaptureBuild(ctx, committedTx, pin, num, hash)

	// Release the consumed pin and re-establish it at the committed head for B+1, whether
	// the build succeeded, the pin was stale, or a gate failed.
	pin.close()
	next, err := openRollingPin(ctx, api.db)
	if err != nil {
		if ctx.Err() == nil {
			log.Warn("[witness-cache] roll pin forward", "block", num, "err", err)
		}
		return nil
	}
	return next
}

// tryHeadCaptureBuild validates the pin as canonical parent(B) and, when valid, builds B's
// head-capture witness against the committed tx and caches it by hash. It returns true only
// when a witness was built and cached; a stale pin or any gate failure returns false and
// caches nothing (fail-closed → out-of-window).
func (api *DebugAPIImpl) tryHeadCaptureBuild(ctx context.Context, committedTx kv.TemporalTx, pin *rollingPin, num uint64, hash common.Hash) bool {
	if num == 0 {
		return false
	}
	parentHash, err := rawdb.ReadCanonicalHash(committedTx, num-1)
	if err != nil {
		witnessCacheBuildFailOtherCounter.Inc()
		return false
	}
	havePin := pin != nil
	var pinNum uint64
	var pinHash common.Hash
	if havePin {
		pinNum, pinHash = pin.num, pin.hash
	}
	if decidePin(havePin, pinNum, pinHash, num, parentHash) != pinUsable {
		witnessCacheBuildFailOtherCounter.Inc()
		return false
	}

	info, err := api.resolveWitnessBlock(ctx, committedTx, rpc.BlockNumberOrHashWithHash(hash, false))
	if err != nil {
		log.Warn("[witness-cache] resolve block", "block", num, "err", err)
		witnessCacheBuildFailOtherCounter.Inc()
		return false
	}
	start := time.Now()
	result, err := api.buildWitnessResultHeadCapture(ctx, committedTx, pin.tx, info, witnessModeLegacy)
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
	enc, err := result.MarshalFastJSON()
	if err != nil {
		witnessCacheBuildFailOtherCounter.Inc()
		log.Warn("[witness-cache] marshal witness", "block", num, "err", err)
		return false
	}
	api.witnessCache.Add(hash, &ExecutionWitnessResult{cachedJSON: enc})
	witnessCacheEntriesResidentGauge.SetInt(api.witnessCache.Len())
	witnessCacheBuildOKCounter.Inc()
	return true
}
