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

package execmodule

import (
	"context"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/shards"
)

// preExecFrontier owns the PRODUCER's pre-executed block chain — the blocks the DAG-L2 producer has
// executed ahead of consensus, each held as a live SharedDomains so its successor can read its state
// before it canonicalises.
//
// It is deliberately SEPARATE from ForkValidator. ForkValidator.sharedDom is VALIDATION space: a
// candidate fork built by newPayload that the FCU either merges or discards, cleared by ClearWithUnwind
// and rebuilt on reorg. Pre-exec state has a different lifecycle entirely — it advances with the DAG,
// is retired against committedHeight, and must survive every FCU. Holding both in one slot is what made
// the producer inherit the validator's teardown rules ([[preexec_validation_space_separation]]).
//
// The rule this type exists to enforce, in one line: the ONLY action that moves a SharedDomains from
// pre-exec space into validation space is newPayload, which PROMOTES a generation out of here. Nothing
// in the producer path reads or writes the validation slot.
//
// It owns BOTH halves of the in-progress block — the SharedDomains chain AND the accumulated body's tx
// hashes. They were previously split across ForkValidator (SD) and ExecModule.flash (body), and a clear
// touched one and not the other; the resulting disagreement silently wedged block production.
type preExecFrontier struct {
	mu sync.Mutex
	// gens is the chain of pre-executed, not-yet-canonical generations, ordered oldest→newest. The LAST
	// entry is the ACTIVE in-progress block (the one accumulating txs); the earlier entries are its
	// ancestors, kept alive as read-through parents because the just-committed DB can still be incomplete
	// for a successor's commitment read. Empty in steady state only before the first block opens.
	gens []*preExecGen
	// txHashes are the tx hashes already executed into the ACTIVE generation, for prefix detection on the
	// next accumulation round.
	txHashes []common.Hash
	// committedHeight is the last block whose state is flushed+committed to the durable DB. A generation
	// is retired only once committed — until then its commitment branches live only in its SD, so closing
	// it early makes a successor's commitment read them empty ("empty branch data read during unfold").
	committedHeight uint64
}

// preExecGen is one pre-executed block held live: its SharedDomains and the block it belongs to.
type preExecGen struct {
	sd       *execctx.SharedDomains
	headHash common.Hash
	number   uint64
	// sealed marks that the block's block-end has run and SealActive has re-keyed the generation to its
	// sealed hash. ONLY a sealed generation may cross into validation space: an in-progress one has no
	// output side yet, so promoting it would canonicalise a block whose state root was never computed.
	sealed bool
	// notifications are the state changes accumulated while executing this block. The FCU publishes them
	// when the block canonicalises, so they belong to the generation rather than to the validator — pre-exec
	// never writes validation state, and a promoted generation must still be able to announce itself.
	notifications *shards.Notifications
}

func newPreExecFrontier() *preExecFrontier { return &preExecFrontier{} }

// Open records sd as the ACTIVE generation for the given block, pushing it onto the chain. The previous
// active generation stays live beneath it as the read-through parent. Re-opening the SAME block number
// REPLACES the active generation (a re-open after an abandon), closing the SD it displaces.
func (f *preExecFrontier) Open(sd *execctx.SharedDomains, headHash common.Hash, number uint64) {
	if sd == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if n := len(f.gens); n > 0 && f.gens[n-1].number == number {
		if old := f.gens[n-1]; old.sd != nil && old.sd != sd {
			old.sd.Close()
		}
		f.gens[n-1] = &preExecGen{sd: sd, headHash: headHash, number: number}
		f.txHashes = nil
		return
	}
	f.gens = append(f.gens, &preExecGen{sd: sd, headHash: headHash, number: number})
	f.txHashes = nil
}

// SetActiveHead updates the ACTIVE generation's block hash. The in-progress header re-hashes every
// accumulation round as its body grows, so the hash recorded at open goes stale immediately; ancestor
// lookups (Live, Promote) match on it, so it must track each round.
func (f *preExecFrontier) SetActiveHead(headHash common.Hash, number uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n := len(f.gens); n > 0 && f.gens[n-1].number == number {
		f.gens[n-1].headHash = headHash
	}
}

// Active returns the in-progress generation — the block the producer is accumulating into.
func (f *preExecFrontier) Active() (common.Hash, uint64, *execctx.SharedDomains) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n := len(f.gens); n > 0 {
		g := f.gens[n-1]
		return g.headHash, g.number, g.sd
	}
	return common.Hash{}, 0, nil
}

// ParentFor returns the live SharedDomains a block opening at `number` should chain to: its IMMEDIATE
// predecessor (number-1) when that is live, else the newest generation below it. Picking the immediate
// predecessor matters — chaining to a grandparent copies a txNum index missing the parent's entry, so
// AppendCanonicalTxNums for the new block fails with an append gap and the block cannot open.
// Returns nil when there is no live ancestor (the caller falls back to the canonical context).
func (f *preExecFrontier) ParentFor(number uint64) *execctx.SharedDomains {
	f.mu.Lock()
	defer f.mu.Unlock()
	var best *preExecGen
	for _, g := range f.gens {
		if g.sd == nil || g.number >= number {
			continue
		}
		if number > 0 && g.number == number-1 {
			return g.sd
		}
		if best == nil || g.number > best.number {
			best = g
		}
	}
	if best == nil {
		return nil
	}
	return best.sd
}

// Abandon closes and drops the ACTIVE generation, leaving its ancestors intact. Used to DISCARD a
// provisionally pre-executed in-progress block so the next open re-executes it from a fresh SD: the
// atomic open stamps placeholder attrs before the block's real CL attributes are known, and correcting
// them needs a fresh SD (a carry-forward round would reuse the maintained SD and skip the block-start
// system tx, leaving the old ParentBeaconBlockRoot/PrevRandao baked into state). Safe off the FCU path:
// the abandoned block was never canonicalised.
func (f *preExecFrontier) Abandon() {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := len(f.gens)
	if n == 0 {
		return
	}
	if g := f.gens[n-1]; g.sd != nil {
		g.sd.Close()
	}
	f.gens = f.gens[:n-1]
	f.txHashes = nil
}

// Live reports whether (hash, number) is a block held live here. Such a block is a valid BASE for its
// successor — its full post-execution state is in the SharedDomains the successor chains to — so
// newPayload must not re-assemble and re-execute it as an unvalidated side fork (which would replay its
// txs against its own already-applied state → "nonce too low").
func (f *preExecFrontier) Live(hash common.Hash, number uint64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.findLocked(hash, number) != nil
}

// Find returns the live generation's SharedDomains for (hash, number), or nil. This is the read side of
// the ONE bridge into validation space: newPayload calls it to promote an already-pre-executed block
// instead of re-executing it.
func (f *preExecFrontier) Find(hash common.Hash, number uint64) *execctx.SharedDomains {
	f.mu.Lock()
	defer f.mu.Unlock()
	if g := f.findLocked(hash, number); g != nil {
		return g.sd
	}
	return nil
}

func (f *preExecFrontier) findLocked(hash common.Hash, number uint64) *preExecGen {
	for _, g := range f.gens {
		if g.sd != nil && g.headHash == hash && g.number == number {
			return g
		}
	}
	return nil
}

// Gen returns the SEALED generation for (hash, number) — its SharedDomains and the notifications to publish
// when it canonicalises — or (nil, nil) when there is no such sealed block here.
//
// This is the read side of the ONE bridge into validation space, so it deliberately refuses an in-progress
// generation even when the hash matches: mid-accumulation the block carries a placeholder header whose
// output side (state root, receipts, gas) has not been computed, and adopting it would let the FCU
// canonicalise a block that was never sealed. The producer's own close runs against the in-progress hash,
// and must fall through to real execution rather than be short-circuited here.
func (f *preExecFrontier) Gen(hash common.Hash, number uint64) (*execctx.SharedDomains, *shards.Notifications) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if g := f.findLocked(hash, number); g != nil && g.sealed {
		return g.sd, g.notifications
	}
	return nil, nil
}

// Owns reports whether sd is one of the live generations. The FCU merge asks this to decide whether it
// may CLOSE the SharedDomains it merged: a generation is still the read-through parent of the block
// above it, so closing it there would break the chain — the frontier releases it later, in order, once
// its state is durable. This replaces the former frontierMode flag, which had to be set globally to say
// the same thing far less precisely.
func (f *preExecFrontier) Owns(sd *execctx.SharedDomains) bool {
	if sd == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, g := range f.gens {
		if g.sd == sd {
			return true
		}
	}
	return false
}

// SetActiveNotifications records the notifications object for the ACTIVE generation.
func (f *preExecFrontier) SetActiveNotifications(n *shards.Notifications) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.gens) > 0 {
		f.gens[len(f.gens)-1].notifications = n
	}
}

// NotifyCommitted records the last block whose state is flushed+committed to the durable DB.
func (f *preExecFrontier) NotifyCommitted(height uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if height > f.committedHeight {
		f.committedHeight = height
	}
}

// CommittedHeight is the last block whose state is durable.
func (f *preExecFrontier) CommittedHeight() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.committedHeight
}

// RetireBelow flushes and releases generations STRICTLY BELOW upTo, oldest-first, so the live-SD count
// stays flat. The just-canonicalised block itself is KEPT: its successor reads its commitment through it
// (the DB copy can still be incomplete), and it retires one step later when the successor canonicalises.
// Best-effort on flush error — a generation that fails to flush is still released rather than leaked,
// since its state is either already durable or will be re-derived on the next open.
func (f *preExecFrontier) RetireBelow(ctx context.Context, tx kv.TemporalTx, upTo uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	kept := f.gens[:0]
	for _, g := range f.gens {
		if g.number < upTo {
			if g.sd != nil {
				_ = g.sd.FlushPendingUpdates(ctx, tx)
				g.sd.Close()
			}
			continue
		}
		kept = append(kept, g)
	}
	f.gens = kept
}

// Depth is the number of live generations — 1 when the producer is caught up with consensus, higher
// when it runs ahead. Diagnostic only.
func (f *preExecFrontier) Depth() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.gens)
}

// SealActive re-keys the ACTIVE generation from its deferred-output in-progress hash to the SEALED header
// hash. Pre-exec built the block with a placeholder (zero-output) header so exec could locate it; the seal
// has now materialised the real-root header, and the generation must answer to it — that hash is what the
// consensus layer sends back in newPayload, and what the promote bridge matches on.
//
// The overlay carries two rows keyed to the placeholder that would otherwise ride the merge into durable
// state: HeadHeaderKey (which stage_finish copies into the head block hash, so a stale value yields a
// head/blockHash mismatch) and canonical[number] (which copyFrontierChainTables propagates FORWARD into
// every successor's overlay, so a later read resolves a header-less canonical hash and rebuilds a
// SharedDomains). Rewrite both to the sealed hash here.
func (f *preExecFrontier) SealActive(oldHash, newHash common.Hash, number uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := len(f.gens)
	if n == 0 {
		return nil
	}
	g := f.gens[n-1]
	if g.number != number || (g.headHash != oldHash && g.headHash != newHash) {
		return nil
	}
	g.headHash = newHash
	g.sealed = true
	if g.sd == nil {
		return nil
	}
	ov := g.sd.BlockOverlay()
	if ov == nil {
		return nil
	}
	if err := rawdb.WriteHeadHeaderHash(ov, newHash); err != nil {
		return err
	}
	return rawdb.WriteCanonicalHash(ov, newHash, number)
}

// RecordTxHashes records the tx hashes executed into the ACTIVE generation so the next round can detect
// the already-executed prefix.
func (f *preExecFrontier) RecordTxHashes(txs []types.Transaction) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.txHashes = make([]common.Hash, len(txs))
	for i, tx := range txs {
		f.txHashes[i] = tx.Hash()
	}
}

// CheckUpdate reports whether txs is a prefix-extension of the ACTIVE generation's accumulated body, so
// the round can carry the maintained SD forward and execute only the new suffix. An EMPTY in-progress
// block IS reusable: the atomic open creates the successor's SD as an empty block at the close, and the
// first content round carries forward into it with PrefixLen=0. A non-matching prefix (reordered txs)
// returns IsUpdate=false — the caller restarts the block on a fresh SD.
func (f *preExecFrontier) CheckUpdate(blockNumber uint64, txs []types.Transaction) FlashblockUpdate {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := len(f.gens)
	if n == 0 {
		return FlashblockUpdate{}
	}
	active := f.gens[n-1]
	if active.number != blockNumber || active.sd == nil {
		return FlashblockUpdate{}
	}
	if len(txs) < len(f.txHashes) {
		return FlashblockUpdate{}
	}
	for i, h := range f.txHashes {
		if txs[i].Hash() != h {
			return FlashblockUpdate{}
		}
	}
	return FlashblockUpdate{IsUpdate: true, PrefixLen: len(f.txHashes), SD: active.sd}
}
