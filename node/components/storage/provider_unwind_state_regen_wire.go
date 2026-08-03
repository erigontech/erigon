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

// removalEntry captures a state-domain .kv file that the mode-B
// unwind staged for removal. The file's [FromStep, ToStep) range is
// entirely past the unwind boundary (FromStep >= stepBoundary), so
// its content reflects state at steps that haven't happened post-
// unwind — stale. FinalizeUnwind unlinks the .kv + its accessors
// (.bt/.kvi/.kvei) + the .torrent sidecar + drops the Inventory
// entry. The post-unwind forward-exec re-creates this step range
// from MDBX live state; a future retire produces a fresh canonical
// file under the same name.
type removalEntry struct {
	path   string // absolute path of the .kv file on disk
	name   string // basename + kind-subdir prefix (matches Inventory FileEntry.Name)
	domain kv.Domain
}

// pendingRegenState is the deferred set of boundary-step regeneration
// + entirely-past-boundary removal ops mode-B staged for post-commit
// execution. FinalizeUnwind atomically swaps each .regen → .kv,
// rebuilds accessors, and unlinks the removals; AbortUnwind unlinks
// each .regen on rollback and leaves the removals untouched (their
// files were never mutated during Provider.Unwind).
type pendingRegenState struct {
	pairs    []regenPair
	removals []removalEntry
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
	recompute *commitmentRecomputeResult,
) (*pendingRegenState, error) {
	if p.Aggregator == nil {
		return nil, nil // tests / tools without an Aggregator skip cleanly
	}
	if p.Inventory == nil {
		return nil, nil
	}
	if recompute == nil {
		return nil, fmt.Errorf("regenerateBoundaryStepFiles: nil recompute")
	}
	stepSize := p.Aggregator.StepSize()
	if stepSize == 0 {
		return nil, fmt.Errorf("aggregator StepSize() == 0")
	}
	stepBoundary := (lastTxNum / stepSize) + 1

	// Encode the commitment anchor once — every regen of the
	// commitment domain plants the same blob.
	commitmentState := commitmentdb.NewCommitmentState(lastTxNum, toBlock, recompute.encodedTrieState)
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
	removals := make([]removalEntry, 0)
	for _, sd := range snapshot.AllDomains {
		kvDomain, ok := snapshotDomainToKVDomain(sd)
		if !ok {
			return nil, fmt.Errorf("unknown storage domain %q: no kv.Domain mapping", sd)
		}

		// planStateFileActions handles ALL local .kv files per domain
		// uniformly — regen straddlers, remove entirely-past.
		domainFiles, ranges := localKVRanges(p.Inventory.AllDomainFiles(sd))
		if len(ranges) == 0 {
			// Domain has no .kv files yet (early chain). Skip.
			continue
		}

		compression := p.Aggregator.DomainCompression(kvDomain)
		var anchor []byte
		if kvDomain == kv.CommitmentDomain {
			anchor = commitmentAnchor
		}

		// Probe once per domain: does this domain's IX cover the AsOf
		// lookup ts (lastTxNum+1)? Under --prune.mode=minimal the IX
		// only covers the last ~100k blocks; a deep unwind can target
		// a txN below that horizon. Regen's per-key AsOf would then
		// raise "data before txNum=<horizon> not available".
		// overrideActionForDomain resolves this per-domain plus the
		// commitment-straddler-preserves-stale-branches override.
		ixStart := tx.Debug().HistoryStartFrom(kvDomain)
		ixCoversTarget := ixStart <= lastTxNum+1
		if !ixCoversTarget {
			p.logger.Warn("mode-B unwind: domain IX pruned past target",
				"domain", sd, "ixStart", ixStart, "target", lastTxNum+1)
		}

		// Walk every file; map each to an action via classifyStateFileForUnwind.
		for i, fileEntry := range domainFiles {
			action := classifyStateFileForUnwind(ranges[i], stepBoundary)
			action, err = overrideActionForDomain(action, kvDomain, ixCoversTarget)
			if err != nil {
				return nil, fmt.Errorf("classify %s file %s: %w", sd, fileEntry.Name, err)
			}
			switch action {
			case actionKeep:
				// content valid post-unwind; nothing to do.
				continue
			case actionRemove:
				// File entirely past boundary. Stage for removal at
				// FinalizeUnwind time; the file itself isn't touched
				// during Provider.Unwind so AbortUnwind doesn't need
				// to undo anything for it.
				removals = append(removals, removalEntry{
					path:   snapshot.ResolveExistingPath(p.snapDir, fileEntry.Name),
					name:   fileEntry.Name,
					domain: kvDomain,
				})
				continue
			case actionRegenInPlace, actionRegenTruncate:
				// Regen straddler. Continue below.
			default:
				return nil, fmt.Errorf("unhandled stateFileAction %d for %s", action, fileEntry.Name)
			}

			// Resolve oldPath against the live disk via Inventory's
			// Name (which carries the kind-subdir prefix). Plain
			// filepath.Join(snapDir, name) only finds files in the
			// legacy flat-layout case.
			oldPath := snapshot.ResolveExistingPath(p.snapDir, fileEntry.Name)

			// Two emit shapes:
			//   - actionRegenInPlace: file's endStep already equals the
			//     unwind target's step boundary. Rewrite in place under
			//     its own step-aligned name.
			//   - actionRegenTruncate: file straddles a mid-step unwind
			//     target. Emit under a v4.0 raw-txnum-named path with
			//     endTxN = lastTxN+1 so the file's advertised horizon
			//     matches its as-of-lastTxN content.
			var finalPath string
			if action == actionRegenTruncate {
				fromTxN := uint64(fileEntry.FromStep) * stepSize
				finalPath = p.Aggregator.DomainKVFilePathV4(kvDomain, fromTxN, lastTxNum+1)
			} else {
				finalPath = oldPath
			}
			regenPath := finalPath + ".regen"

			// Commitment mode-C: emit the v4 file directly from the
			// mode-C compute's captured branches rather than iterating
			// the OLD file. Iterating the OLD file replays its stale
			// post-lastTxN branch set (the 2026-06-03 subversion); the
			// compute's branches are the authoritative trie state at
			// lastTxNum, freshly folded from accounts/storage/code.
			if kvDomain == kv.CommitmentDomain && action == actionRegenTruncate {
				if recompute.regenBranches == nil {
					return nil, fmt.Errorf("regenerateBoundaryStepFiles: commitment truncate needs recompute.regenBranches (Apply must run first)")
				}
				if err := WriteCommitmentBoundaryFileV4(
					ctx, recompute.regenBranches, anchor, regenPath,
					p.snapTmpDir, compression, p.logger,
				); err != nil {
					return nil, fmt.Errorf("emit v4 commitment file %s: %w", regenPath, err)
				}
			} else {
				if err := RegenerateBoundaryStepFile(
					ctx, kvDomain, oldPath, regenPath, lookup, lastTxNum,
					compression, anchor, p.snapTmpDir, p.logger,
				); err != nil {
					return nil, fmt.Errorf("regen %s boundary-step file %s: %w", sd, fileEntry.Name, err)
				}
			}
			pairs = append(pairs, regenPair{
				regenPath:    regenPath,
				finalPath:    finalPath,
				oldBroadPath: oldPath,
				domain:       kvDomain,
			})
		}
	}

	if len(pairs) == 0 && len(removals) == 0 {
		return nil, nil
	}
	return &pendingRegenState{pairs: pairs, removals: removals}, nil
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
