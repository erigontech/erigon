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

	"github.com/erigontech/erigon/db/kv"
	dbexecctx "github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// Compile-time assertion: the production *SharedDomains satisfies the
// narrow CommitmentStateWriter interface Provider.Unwind expects in
// UnwindOpts.Domains. Guarantees SetHead-side wiring compiles without
// surprise the moment that callsite lands.
var _ commitmentdb.CommitmentStateWriter = (*dbexecctx.SharedDomains)(nil)

// UnwindOpts holds the per-call inputs Provider.Unwind needs that it
// cannot derive from its own state.
//
// All fields are caller-supplied so Provider.Unwind stays free of any
// SharedDomains construction or aggregator-positioning concerns — the
// caller (production: execution/execmodule.SetHead; future: fork-from
// CLI) drives the db state machinery and hands the resulting handles
// plus the encoded trie state in here.
type UnwindOpts struct {
	// BlockAligned signals that the caller is operating on a chain
	// configured with --snap.block-aligned-boundaries (i.e.
	// snapcfg.Cfg.BlockAlignedBoundaries == true). When false,
	// Provider.Unwind refuses; see the method docstring for why.
	BlockAligned bool

	// TxNum is the last txNum at toBlock — typically computed by the
	// caller via rawdbv3.TxNums.Max(ctx, tx, toBlock).
	TxNum uint64

	// TrieState is the encoded patricia trie state at toBlock — the
	// bytes from commitment.HexPatriciaHashed.EncodeCurrentState(nil)
	// or ConcurrentPatriciaHashed.RootTrie().EncodeCurrentState(nil).
	// The caller is responsible for positioning the trie at toBlock
	// before encoding (production: the existing
	// pipelineExecutor.RunUnwind pass does this).
	TrieState []byte

	// Domains is the SharedDomains handle the commitment-entry write
	// is performed through. *db/state/execctx.SharedDomains satisfies
	// commitmentdb.CommitmentStateWriter structurally.
	Domains commitmentdb.CommitmentStateWriter

	// Tx is the temporal transaction the commitment-entry write is
	// performed inside. Caller owns its lifecycle (Begin / Commit /
	// Rollback).
	Tx kv.TemporalTx
}

// Unwind is the storage-layer entry point for an *administrative*
// unwind to an arbitrary block (debug_setHead, fork-from CLI). The
// exec-stage unwind path is unrelated: it stays on the existing
// AggregatorRoTx.Unwind / unwindExec3 chain and remains bounded by
// rawtemporaldb.CanUnwindBeforeBlockNum. The two paths share no code
// below this method.
//
// Sub-op contract:
//
//  1. DB unwind — *caller's responsibility*. Production caller SetHead
//     does this via its existing pipelineExecutor.RunUnwind pass
//     BEFORE invoking Provider.Unwind, leaving the temporal db, the
//     aggregator state, and the patricia trie all positioned at
//     toBlock.
//  2. Commitment entry write — WriteCommitmentEntryAtBlock anchors a
//     commitment state entry at (toBlock, opts.TxNum) using
//     opts.TrieState so subsequent reads find a valid coordinate.
//  3. Snapshot file trim past toBlock — deletes / truncates segments
//     whose To > toBlock, drops their torrents, republishes
//     chain.toml. Not yet implemented; until it is, callers must
//     keep toBlock within the unfrozen (db-side) range.
//
// Why this method can lift CLAUDE.md's "Unwind beyond data in
// snapshots not allowed" for aligned chains: that rule was a
// placeholder for the code in (2) + (3). It stands for non-aligned
// rounded-boundary chains because trimming an arbitrary block out of
// the middle of a 1k-rounded file would corrupt the file. Aligned
// mode lifts it because the unit of cutting *is* the block — every
// toBlock is a real file boundary by construction.
//
// Concurrency: Provider.Unwind does not synchronise. SetHead already
// holds the ExecModule semaphore for the duration of the unwind, and
// the CLI fork-from caller is offline at invocation.
func (p *Provider) Unwind(ctx context.Context, toBlock uint64, opts UnwindOpts) error {
	if p == nil {
		return fmt.Errorf("storage.Provider.Unwind: nil provider")
	}
	if !opts.BlockAligned {
		return fmt.Errorf("storage.Provider.Unwind: BlockAligned=false; administrative arbitrary-block unwind requires --snap.block-aligned-boundaries (the existing exec-stage CanUnwindBeforeBlockNum guard governs non-aligned chains)")
	}
	if opts.Domains == nil {
		return fmt.Errorf("storage.Provider.Unwind: opts.Domains is nil")
	}
	if opts.Tx == nil {
		return fmt.Errorf("storage.Provider.Unwind: opts.Tx is nil")
	}

	if err := commitmentdb.WriteCommitmentEntryAtBlock(opts.Domains, opts.Tx, toBlock, opts.TxNum, opts.TrieState); err != nil {
		return fmt.Errorf("storage.Provider.Unwind: commitment-entry write at block %d: %w", toBlock, err)
	}

	// TODO: sub-op #3 (snapshot file trim past toBlock). ctx is
	// accepted now so the signature is stable when the trim path
	// (which IS context-aware) is filled in.
	_ = ctx

	return nil
}
