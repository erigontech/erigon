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

// regenPair is a (regenPath, finalPath) pair that FinalizeUnwind
// atomically swaps post-commit.
type regenPair struct {
	regenPath string // <snapDir>/domain/v2.0-DOMAIN.FROM-TO.kv.regen
	finalPath string // <snapDir>/domain/v2.0-DOMAIN.FROM-TO.kv
	domain    kv.Domain
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
// AsOfLookup is wired to tx.HistorySeek, which returns the value at
// the most recent history entry at or before lastTxNum. Phase 4
// pins the precise semantic against forward-exec read behavior.
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

	lookup := func(domain kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
		return tx.HistorySeek(domain, key, ts)
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
		oldPath := filepath.Join(p.snapDir, boundary.Name)
		compression := p.Aggregator.DomainCompression(kvDomain)

		var anchor []byte
		if kvDomain == kv.CommitmentDomain {
			anchor = commitmentAnchor
		}

		newPath, err := RegenerateBoundaryStepFile(
			ctx, kvDomain, oldPath, lookup, lastTxNum,
			compression, anchor, p.snapTmpDir, p.logger,
		)
		if err != nil {
			return nil, fmt.Errorf("regen %s boundary-step file %s: %w", sd, boundary.Name, err)
		}
		pairs = append(pairs, regenPair{
			regenPath: newPath,
			finalPath: oldPath,
			domain:    kvDomain,
		})
	}

	if len(pairs) == 0 {
		return nil, nil
	}
	return &pendingRegenState{pairs: pairs}, nil
}

// boundaryStepFileForDomain returns the FileEntry for the .kv file
// whose step coverage is [FromStep, stepBoundary) — the file
// containing the txNum range that straddles the unwind target.
// Returns nil if no such file is in the inventory (e.g. domain has no
// files at this step yet).
//
// The boundary-step file is the unique entry per domain whose
// ToStep == stepBoundary AND Kind == KindKV (the .kv primary, not the
// accessor variants .kvi/.bt/.kvei — those get rebuilt against the
// regenerated .kv by FinalizeUnwind via Aggregator.BuildMissedAccessors).
func (p *Provider) boundaryStepFileForDomain(domain snapshot.Domain, stepBoundary uint64) *snapshot.FileEntry {
	for _, e := range p.Inventory.AllDomainFiles(domain) {
		if e.Kind != snapshot.KindKV {
			continue
		}
		if e.ToStep != stepBoundary {
			continue
		}
		if e.FromStep >= stepBoundary {
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
