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
	"path/filepath"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// regenPair captures the on-disk paths a single domain's boundary-step
// regenerate-and-swap touches. FinalizeUnwind atomically promotes
// regenPath → finalPath; if oldBroadPath differs from finalPath, the
// old broad file gets removed (it's a wider step range whose content
// the regen has now replaced with a narrower truncated file).
//
// Two shapes:
//
//   - Aligned: stepBoundary == boundary.ToStep — the boundary file is
//     already at exactly the unwind-target step. No name change, the
//     regen replaces the file in place. finalPath == oldBroadPath.
//
//   - Truncated: stepBoundary < boundary.ToStep — the boundary file is
//     a wider (merged) range that straddles the unwind target. The
//     regen output covers only [FromStep, stepBoundary) of state, so
//     it MUST live under a filename naming that narrower range. The
//     wider pre-merge file at oldBroadPath co-existing with the
//     narrower regen output is the 2026-06-25 union-cover wedge.
type regenPair struct {
	regenPath    string // <snapDir>/domain/<truncatedName>.kv.regen
	finalPath    string // <snapDir>/domain/<truncatedName>.kv  (truncated name when ToStep narrowed)
	oldBroadPath string // <snapDir>/domain/<originalName>.kv   (removed in FinalizeUnwind when != finalPath)
	domain       kv.Domain
}

// pendingRegenState is the deferred set of boundary-step regeneration
// ops mode-B staged for post-commit execution. FinalizeUnwind
// atomically swaps each .regen → .kv + rebuilds accessors;
// AbortUnwind unlinks each .regen on rollback.
type pendingRegenState struct {
	pairs []regenPair
}

// regenerateBoundaryStepFiles walks every state domain
// (snapshot.AllDomains) and, for each domain's boundary-step file —
// the file whose step coverage straddles the unwind target —
// regenerates it via RegenerateBoundaryStepFile so its entries
// reflect chain state at lastTxNum rather than the file's previous
// max txNum. The new .kv files are written to <originalPath>.regen
// and staged via Provider.pendingRegen for FinalizeUnwind to swap +
// rebuild accessors atomically post-commit. AbortUnwind unlinks them
// if mode-B's tx rolls back.
//
// For the commitment domain, the KeyCommitmentState entry is replaced
// with an anchor blob encoding (blockNum=toBlock, txNum=lastTxNum,
// trieState=encodedTrieState). encodedTrieState comes from
// ensureCommitmentAtBlockCompute's recompute result — the same trie
// state ensureCommitmentAtBlockApply writes to the writable shadow.
//
// AsOfLookup is wired to tx.GetAsOf at ts=lastTxNum+1, so a key whose
// last write predates lastTxNum but has no history entry at/after
// lastTxNum falls through to GetLatest and is kept (not dropped).
//
// Returns a *pendingRegenState (or nil if no boundary-step files
// existed for any domain — defensive; in practice every domain has
// one once the chain has progressed past the first step). The caller
// stages it via Provider.pendingRegen for FinalizeUnwind /
// AbortUnwind.
func (p *Provider) regenerateBoundaryStepFiles(
	ctx context.Context,
	tx kv.TemporalRwTx,
	toBlock, lastTxNum uint64,
	encodedTrieState []byte,
) (*pendingRegenState, error) {
	if p.Aggregator == nil {
		return nil, nil // tests / tools without an Aggregator skip cleanly
	}
	if p.Inventory == nil {
		return nil, nil
	}
	stepSize := p.Aggregator.StepSize()
	if stepSize == 0 {
		return nil, fmt.Errorf("aggregator StepSize() == 0")
	}
	stepBoundary := (lastTxNum / stepSize) + 1

	// Encode the commitment anchor once — every regen of the
	// commitment domain plants the same blob.
	commitmentState := commitmentdb.NewCommitmentState(lastTxNum, toBlock, encodedTrieState)
	commitmentAnchor, err := commitmentState.Encode()
	if err != nil {
		return nil, fmt.Errorf("encode commitment anchor: %w", err)
	}

	// tx.GetAsOf (not tx.HistorySeek) so the lookup falls through to
	// GetLatest when history has no change at or after ts. For a key
	// last written before the unwind target and never modified since,
	// HistorySeek returns NOT FOUND (no entry at ts) — the regen would
	// drop the key from the rebuilt boundary file and downstream reads
	// at lastTxNum would return zero, surfacing as wrong-state gas-
	// mismatch in catchup. GetAsOf treats history NOT FOUND as "no
	// change since some past write" and reads the surviving current
	// value via GetLatest. ts is lastTxNum+1 so a change at exactly
	// lastTxNum is included in the answer (matching the wipe path's
	// GetAsOf(_, _, lastTxNum+1) convention).
	lookup := func(domain kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
		return tx.GetAsOf(domain, key, ts+1)
	}

	pairs := make([]regenPair, 0, len(snapshot.AllDomains))
	for _, sd := range snapshot.AllDomains {
		kvDomain, ok := snapshotDomainToKVDomain(sd)
		if !ok {
			return nil, fmt.Errorf("unknown storage domain %q: no kv.Domain mapping", sd)
		}
		boundary := p.boundaryStepFileForDomain(sd, stepBoundary)
		if boundary == nil {
			// Domain has no boundary-step file — possible early in
			// chain history before the step has retired. Nothing to
			// regenerate for this domain.
			continue
		}
		// Inventory FileEntry.Name is the bare basename; resolve
		// against the kind subdir (domain/) where the downloader writes
		// the file in production. Plain filepath.Join(snapDir, name)
		// only finds files in the legacy flat-layout case and is the
		// reason a fresh hoodi datadir wedges mid-life mode-B setHead
		// with "open old <snapDir>/v2.0-accounts.272-273.kv: no such
		// file or directory" — see [[flow/orchestrator.go:70]] for the
		// same fix in the validation-chain entrypoint.
		oldPath := snapshot.ResolveExistingPath(p.snapDir, boundary.Name)
		compression := p.Aggregator.DomainCompression(kvDomain)

		var anchor []byte
		if kvDomain == kv.CommitmentDomain {
			anchor = commitmentAnchor
		}

		// Truncated rename: when the boundary file's ToStep extends
		// past the unwind-target step boundary, the regen output
		// covers only [FromStep, stepBoundary) of state — a narrower
		// range than the original file's name advertises. The new
		// file MUST be named to match its actual coverage, otherwise
		// the wider-named regen output co-exists with any pre-existing
		// narrower files in the same range and rule-driven cull picks
		// the wider one (M-A default direction), serving stale state
		// for the truncated portion. This is the 2026-06-25 v2.0-
		// accounts.272-280-co-exists-with-272-276 union-cover wedge
		// reproduced live in iter-4 mode-B at depth 60k on hoodi.
		// stepBoundary == boundary.ToStep is the aligned case: regen
		// replaces the file in place (finalPath == oldBroadPath).
		//
		// Derive the new basename from filepath.Base(oldPath), NOT
		// from boundary.Name — Inventory's Name field carries the
		// kind-subdir prefix (e.g. "domain/v2.0-accounts.280-284.kv")
		// matching the chain.toml entry shape, and joining that under
		// filepath.Dir(oldPath) (already inside the kind subdir) would
		// double up the prefix, producing "<snapDir>/domain/domain/...".
		// Live-caught 2026-06-30 iter-1 mode_a2 right after the
		// truncated-rename change landed.
		oldBaseName := filepath.Base(oldPath)
		truncatedBaseName := oldBaseName
		if stepBoundary < boundary.ToStep {
			truncatedBaseName = renameStepRange(oldBaseName, boundary.FromStep, boundary.ToStep, stepBoundary)
		}
		finalPath := filepath.Join(filepath.Dir(oldPath), truncatedBaseName)
		regenPath := finalPath + ".regen"

		if err := RegenerateBoundaryStepFile(
			ctx, kvDomain, oldPath, regenPath, lookup, lastTxNum,
			compression, anchor, p.snapTmpDir, p.logger,
		); err != nil {
			return nil, fmt.Errorf("regen %s boundary-step file %s: %w", sd, boundary.Name, err)
		}
		pairs = append(pairs, regenPair{
			regenPath:    regenPath,
			finalPath:    finalPath,
			oldBroadPath: oldPath,
			domain:       kvDomain,
		})
	}

	if len(pairs) == 0 {
		return nil, nil
	}
	return &pendingRegenState{pairs: pairs}, nil
}

// boundaryStepFileForDomain returns the FileEntry for the .kv file
// whose [FromStep, ToStep) range contains stepBoundary — the file
// covering the txNum range that straddles the unwind target. When
// the aggregator has merged step files into wider chunks, the
// boundary is reached strictly inside a merged file; the aligned
// case (ToStep == stepBoundary) is the strict sub-case where the
// boundary lands exactly on a file edge. Returns nil when no file
// straddles stepBoundary (early history before the step has retired,
// or stepBoundary is past every retired file).
func (p *Provider) boundaryStepFileForDomain(domain snapshot.Domain, stepBoundary uint64) *snapshot.FileEntry {
	for _, e := range p.Inventory.AllDomainFiles(domain) {
		if e.Kind != snapshot.KindKV {
			continue
		}
		if e.FromStep >= stepBoundary {
			continue
		}
		if e.ToStep < stepBoundary {
			continue
		}
		return e
	}
	return nil
}

// snapshotDomainToKVDomain maps storage's string-typed Domain enum to
// the kv package's integer-typed Domain enum. The two enumerations are
// kept in lockstep by docs/plans/...; this mapping should be updated
// whenever snapshot.AllDomains grows.
func snapshotDomainToKVDomain(d snapshot.Domain) (kv.Domain, bool) {
	switch d {
	case snapshot.DomainAccounts:
		return kv.AccountsDomain, true
	case snapshot.DomainStorage:
		return kv.StorageDomain, true
	case snapshot.DomainCode:
		return kv.CodeDomain, true
	case snapshot.DomainCommitment:
		return kv.CommitmentDomain, true
	case snapshot.DomainReceipt:
		return kv.ReceiptDomain, true
	}
	return 0, false
}

// Compile-time assertion that seg is imported — referenced indirectly
// via RegenerateBoundaryStepFile's seg.FileCompression parameter.
var _ = seg.CompressNone
