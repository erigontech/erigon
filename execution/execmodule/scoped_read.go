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
	"errors"
	"sync/atomic"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
)

// errHeadMismatch means the current head is not the block the caller asked to
// read/build on — the published head has not (yet) reached that block, or a
// reorg moved it. The caller signals Busy rather than reading the wrong block.
var errHeadMismatch = errors.New("head is not at the requested block")

// ScopedReadView is a by-block-consistent read snapshot pinned to a specific
// block under background commit: a base MDBX roTx captured at a settled instant
// (while the caller holds the foreground semaphore, so no commit is mid-flight)
// plus the head SharedDomains whose in-flight mem the snapshot layers on (nil
// when the committed DB is already current). getLatest reads sd.mem → the head
// chain → the roTx, and mem shadows the DB, so the two are one coherent view of
// the block regardless of whether its commit has landed.
//
// Single-owner: exactly one consumer holds it and MUST call Release exactly
// once. Release is idempotent; failing to call it leaks an open MDBX reader and
// wedges DB close.
type ScopedReadView struct {
	roTx      kv.TemporalTx
	headSD    *execctx.SharedDomains
	blockHash common.Hash
	blockNum  uint64
	released  atomic.Bool
}

// Tx returns the pinned base roTx. Readers pass it to sd.AsGetter /
// BlockOverlay().NewReadView / BlockOverlayTemporalTx.
func (v *ScopedReadView) Tx() kv.TemporalTx { return v.roTx }

// HeadSD returns the head SharedDomains this view is pinned to, or nil when the
// committed DB is already current (all generations committed).
func (v *ScopedReadView) HeadSD() *execctx.SharedDomains { return v.headSD }

// BlockHash returns the block this view reads.
func (v *ScopedReadView) BlockHash() common.Hash { return v.blockHash }

// BlockNum returns the number of the block this view reads.
func (v *ScopedReadView) BlockNum() uint64 { return v.blockNum }

// Release rolls back the pinned roTx. Idempotent; safe on nil.
func (v *ScopedReadView) Release() {
	if v == nil || v.released.Swap(true) {
		return
	}
	if v.roTx != nil {
		v.roTx.Rollback()
	}
}

// captureScopedReadView pins a consistent read snapshot for wantHash. The caller
// MUST hold the foreground semaphore (mutually exclusive with the background
// commit worker) so the snapshot is settled. The head is the newest
// not-yet-committed generation's SharedDomains when its block is wantHash, else
// the committed DB. Returns errHeadMismatch when the head is not wantHash so the
// caller can return Busy instead of reading the wrong block. The returned view
// owns the roTx — the caller must Release it.
func (e *ExecModule) captureScopedReadView(ctx context.Context, wantHash common.Hash) (*ScopedReadView, error) {
	e.fgMu.Lock()
	var head *execctx.SharedDomains
	var headHash common.Hash
	var headNum uint64
	if n := len(e.gens); n > 0 {
		if last := e.gens[n-1]; !last.committed {
			head, headHash, headNum = last.sd, last.blockHash, last.blockNum
		}
	}
	e.fgMu.Unlock()

	roTx, err := e.db.BeginTemporalRo(ctx) //nolint:gocritic // handed to the returned view, which owns Rollback; guard below covers all error/panic paths before handoff
	if err != nil {
		return nil, err
	}
	handedOff := false
	defer func() {
		if !handedOff {
			roTx.Rollback()
		}
	}()

	if head != nil {
		if headHash != wantHash {
			return nil, errHeadMismatch
		}
		handedOff = true
		return &ScopedReadView{roTx: roTx, headSD: head, blockHash: wantHash, blockNum: headNum}, nil
	}

	// No uncommitted generation: the committed DB is the head. Confirm wantHash
	// is canonical at its number in this snapshot.
	num, err := e.blockReader.HeaderNumber(ctx, roTx, wantHash)
	if err != nil {
		return nil, err
	}
	if num == nil {
		return nil, errHeadMismatch
	}
	canon, err := e.canonicalHash(ctx, roTx, *num)
	if err != nil {
		return nil, err
	}
	if canon != wantHash {
		return nil, errHeadMismatch
	}
	handedOff = true
	return &ScopedReadView{roTx: roTx, blockHash: wantHash, blockNum: *num}, nil
}
