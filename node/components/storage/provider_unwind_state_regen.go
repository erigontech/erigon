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
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/erigontech/erigon/common/log/v3"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// AsOfLookup resolves the value of (domain, key) valid at lastTxNum.
// Returns (val=nil, found=false) for keys that didn't exist at
// lastTxNum (created strictly after). Returns (val, true) for keys
// whose value at lastTxNum can be reconstructed from history +
// current state.
//
// Phase 3 wires this to kv.TemporalTx.HistorySeek (or GetAsOf — TBD
// based on the precise "AT txNum" semantic the boundary-step regen
// needs; Phase 4 will pin the choice via end-to-end verify). For
// unit tests, callers supply a synthetic lookup.
type AsOfLookup func(domain kv.Domain, key []byte, lastTxNum uint64) (val []byte, found bool, err error)

// RegenerateBoundaryStepFile rewrites the given boundary-step state
// snapshot file so every (k, v) pair reflects the value of k valid at
// lastTxNum, replacing post-anchor stale entries with their as-of-
// lastTxNum value via the supplied AsOfLookup. Keys that didn't exist
// at lastTxNum are dropped. For the commitment domain, the
// KeyCommitmentState entry is replaced with the supplied
// commitmentAnchor blob unconditionally — that's the mode-B
// post-state anchor encoding (blockNum=toBlock, txNum=lastTxNum,
// trieState).
//
// The new file is written to <originalPath>.regen — NOT to the
// original path. Phase 3's FinalizeUnwind handles the atomic rename
// (and AbortUnwind handles the .regen cleanup on rollback) so a
// mid-call crash never leaves the original file in a partially-
// written state.
//
// Caller is responsible for:
//   - supplying compression matching the domain's DomainCfg.Compression
//     (so the new file's wire format matches what readers expect),
//   - rebuilding accessor files (.kvi / .bt / .kvei) against the
//     regenerated .kv after a successful return (NOT done here so
//     callers can batch / parallelise the accessor builds),
//   - staging the new + old file paths for FinalizeUnwind /
//     AbortUnwind.
//
// commitmentAnchor MUST be non-nil when domain == kv.CommitmentDomain
// (the whole point of the regen is to plant a fresh anchor) and MUST
// be nil otherwise (defensive — callers shouldn't be plumbing
// commitment-specific blobs for non-commitment domains).
// CommitmentBranchProvider yields the branches the recompute emitted
// (via trie.Process → PutBranch) in sorted-by-key order — the merge
// stream regen consumes against the baseline .kv to produce a
// self-coherent at-lastTxNum commitment boundary file. All branches
// carry values at lastTxNum: any branch whose sub-tree covers a
// state key touched in (baseline, lastTxNum] is refolded by Process
// and lands here.
type CommitmentBranchProvider interface {
	Len() int
	KeyAt(i int) []byte
	ValueAt(i int) []byte
}

// SortedBranchPairs is the concrete CommitmentBranchProvider backed
// by a caller-supplied sorted-by-K slice. Ownership: callers must
// keep the slices alive for the duration of the regen call.
type SortedBranchPairs struct {
	Keys [][]byte
	Vals [][]byte
}

func (s *SortedBranchPairs) Len() int             { return len(s.Keys) }
func (s *SortedBranchPairs) KeyAt(i int) []byte   { return s.Keys[i] }
func (s *SortedBranchPairs) ValueAt(i int) []byte { return s.Vals[i] }

func RegenerateBoundaryStepFile(
	ctx context.Context,
	domain kv.Domain,
	oldKVPath string,
	baselineKVPath string,
	newKVPath string,
	lookup AsOfLookup,
	branches CommitmentBranchProvider,
	expander CommitmentBranchExpander,
	lastTxNum uint64,
	compression seg.FileCompression,
	commitmentAnchor []byte,
	tmpDir string,
	logger log.Logger,
) error {
	if domain == kv.CommitmentDomain && commitmentAnchor == nil {
		return fmt.Errorf("RegenerateBoundaryStepFile(commitment): commitmentAnchor required")
	}
	if domain != kv.CommitmentDomain && commitmentAnchor != nil {
		return fmt.Errorf("RegenerateBoundaryStepFile(%s): commitmentAnchor must be nil for non-commitment domains", domain)
	}
	if domain == kv.CommitmentDomain && baselineKVPath == "" {
		return fmt.Errorf("RegenerateBoundaryStepFile(commitment): baselineKVPath required")
	}
	if domain != kv.CommitmentDomain && lookup == nil {
		return fmt.Errorf("RegenerateBoundaryStepFile(%s): lookup is required for non-commitment domains", domain)
	}
	if newKVPath == "" {
		return fmt.Errorf("RegenerateBoundaryStepFile: newKVPath is required")
	}

	compressCfg := seg.DefaultCfg
	comp, err := seg.NewCompressor(ctx, "mode-B boundary-step regen", newKVPath, tmpDir, compressCfg, log.LvlInfo, logger)
	if err != nil {
		return fmt.Errorf("create new %s: %w", newKVPath, err)
	}
	defer comp.Close()
	writer := seg.NewWriter(comp, compression)

	if domain == kv.CommitmentDomain {
		if expander == nil {
			return fmt.Errorf("RegenerateBoundaryStepFile(commitment): CommitmentBranchExpander required")
		}
		kept, err := regenerateCommitmentBoundary(baselineKVPath, writer, compression, branches, expander, commitmentAnchor)
		if err != nil {
			return err
		}
		if err := comp.Compress(); err != nil {
			return fmt.Errorf("compress %s: %w", newKVPath, err)
		}
		if logger != nil {
			branchCount := 0
			if branches != nil {
				branchCount = branches.Len()
			}
			logger.Info("[storage] mode-B boundary-step regen (commitment)",
				"baseline", baselineKVPath, "new", newKVPath,
				"kept", kept, "touched_branches", branchCount)
		}
		return nil
	}

	// Value-domains: iterate the straddler oldKVPath, use as-of lookup
	// per key. `!found` from AsOfLookup collapses two cases: (a) key
	// was tombstoned at some txN ≤ lastTxNum, (b) key was created
	// strictly after lastTxNum. Writing (K, empty) in the regen
	// shadows any stale pre-tombstone value in older .kv files (files
	// have no concept of deletion — see the exec-stage unwind
	// tombstone comment in db/state/domain.go). Case (b) is
	// harmless-with-empty.
	oldDecomp, err := seg.NewDecompressor(oldKVPath)
	if err != nil {
		return fmt.Errorf("open old %s: %w", oldKVPath, err)
	}
	defer oldDecomp.Close()
	reader := seg.NewReader(oldDecomp.MakeGetter(), compression)

	var (
		keyBuf, valBuf []byte
		kept           uint64
	)
	for reader.HasNext() {
		keyBuf, _ = reader.Next(keyBuf[:0])
		if !reader.HasNext() {
			return fmt.Errorf("malformed %s: trailing key with no value", oldKVPath)
		}
		valBuf, _ = reader.Next(valBuf[:0])

		newVal, found, err := lookup(domain, keyBuf, lastTxNum)
		if err != nil {
			return fmt.Errorf("AsOfLookup(%s, key, %d): %w", domain, lastTxNum, err)
		}
		if !found {
			newVal = nil
		}

		if _, err := writer.Write(keyBuf); err != nil {
			return fmt.Errorf("write key: %w", err)
		}
		if _, err := writer.Write(newVal); err != nil {
			return fmt.Errorf("write value: %w", err)
		}
		kept++
	}

	if err := comp.Compress(); err != nil {
		return fmt.Errorf("compress %s: %w", newKVPath, err)
	}

	if logger != nil {
		logger.Info("[storage] mode-B boundary-step regen",
			"domain", domain, "old", oldKVPath, "new", newKVPath, "kept", kept)
	}

	return nil
}

// regenerateCommitmentBoundary merge-walks the baseline commitment
// .kv against the recompute's touched-branches stream to produce a
// self-coherent at-lastTxNum boundary file. Post-unwind, MDBX is
// empty at stepContaining(lastTxNum) for commitment (wipe wholeStep);
// this file is the authoritative source of at-lastTxNum branches.
//
// Rules:
//   - KeyCommitmentState: the supplied anchor blob is written, regardless
//     of what the baseline held. Baseline MUST contain a KeyCommitmentState
//     record (all commitment files do); we return an error otherwise so a
//     silently-anchor-less file can't slip through.
//   - K present in both baseline and touched-branches: use touched-branches'
//     V (Apply's at-lastTxNum, correct by construction — the sub-tree changed
//     in the range, so baseline's stored hash is stale).
//   - K only in baseline: use baseline's V (unchanged in (baseline, lastTxNum],
//     so baseline's V == at-lastTxNum V by inductive equality).
//   - K only in touched-branches: use its V (new branch created in the
//     range that didn't exist at baseline).
//
// Both streams are sorted by K, so a two-pointer walk emits sorted output.
func regenerateCommitmentBoundary(
	baselinePath string,
	writer *seg.Writer,
	compression seg.FileCompression,
	branches CommitmentBranchProvider,
	expander CommitmentBranchExpander,
	anchor []byte,
) (uint64, error) {
	baselineDecomp, err := seg.NewDecompressor(baselinePath)
	if err != nil {
		return 0, fmt.Errorf("open baseline %s: %w", baselinePath, err)
	}
	defer baselineDecomp.Close()
	baselineReader := seg.NewReader(baselineDecomp.MakeGetter(), compression)

	var (
		baseKey, baseVal []byte
		baseHas          bool
	)
	advance := func() error {
		if !baselineReader.HasNext() {
			baseHas = false
			return nil
		}
		baseKey, _ = baselineReader.Next(baseKey[:0])
		if !baselineReader.HasNext() {
			return fmt.Errorf("malformed baseline %s: trailing key with no value", baselinePath)
		}
		baseVal, _ = baselineReader.Next(baseVal[:0])
		baseHas = true
		return nil
	}
	if err := advance(); err != nil {
		return 0, err
	}

	branchLen := 0
	if branches != nil {
		branchLen = branches.Len()
	}
	branchIdx := 0
	branchHas := branchIdx < branchLen

	var (
		kept          uint64
		anchorPlanted bool
	)
	// emitBaseline writes a (K, V) pair sourced from the baseline .kv,
	// expanding any shortened key refs in V to full plain keys via the
	// expander so the regen output is uniformly full-plain-key. Fails
	// hard on an unresolvable ref — a partial file would silently
	// break future reads.
	emitBaseline := func(k, v []byte) error {
		if bytes.Equal(k, commitmentdb.KeyCommitmentState) {
			v = anchor
			anchorPlanted = true
		} else if len(v) > 0 {
			expanded, err := expander.Expand(commitment.BranchData(v))
			if err != nil {
				return fmt.Errorf("expand baseline branch key=%x: %w", k, err)
			}
			v = expanded
		}
		if _, err := writer.Write(k); err != nil {
			return fmt.Errorf("write key: %w", err)
		}
		if _, err := writer.Write(v); err != nil {
			return fmt.Errorf("write value: %w", err)
		}
		kept++
		return nil
	}
	// emitBranch writes a (K, V) pair sourced from the recompute's
	// PutBranch collector. Process emits full-plain-key form, so no
	// expansion is needed. KeyCommitmentState never appears in the
	// collector (Process emits branches, not the anchor record).
	emitBranch := func(k, v []byte) error {
		if _, err := writer.Write(k); err != nil {
			return fmt.Errorf("write key: %w", err)
		}
		if _, err := writer.Write(v); err != nil {
			return fmt.Errorf("write value: %w", err)
		}
		kept++
		return nil
	}

	for baseHas || branchHas {
		switch {
		case !branchHas:
			if err := emitBaseline(baseKey, baseVal); err != nil {
				return 0, err
			}
			if err := advance(); err != nil {
				return 0, err
			}
		case !baseHas:
			if err := emitBranch(branches.KeyAt(branchIdx), branches.ValueAt(branchIdx)); err != nil {
				return 0, err
			}
			branchIdx++
			branchHas = branchIdx < branchLen
		default:
			cmp := bytes.Compare(baseKey, branches.KeyAt(branchIdx))
			switch {
			case cmp < 0:
				if err := emitBaseline(baseKey, baseVal); err != nil {
					return 0, err
				}
				if err := advance(); err != nil {
					return 0, err
				}
			case cmp > 0:
				if err := emitBranch(branches.KeyAt(branchIdx), branches.ValueAt(branchIdx)); err != nil {
					return 0, err
				}
				branchIdx++
				branchHas = branchIdx < branchLen
			default:
				// Both have the key: touched-branch V wins (recompute
				// refolded the sub-tree). No expansion needed on branch V.
				if err := emitBranch(branches.KeyAt(branchIdx), branches.ValueAt(branchIdx)); err != nil {
					return 0, err
				}
				branchIdx++
				branchHas = branchIdx < branchLen
				if err := advance(); err != nil {
					return 0, err
				}
			}
		}
	}

	if !anchorPlanted {
		return 0, fmt.Errorf("regenerateCommitmentBoundary: baseline %s had no KeyCommitmentState entry", baselinePath)
	}
	return kept, nil
}

// renameStepRange returns a copy of basename with the embedded
// "<from>-<oldTo>" step segment replaced by "<from>-<newTo>". The step
// segment is the first "<digits>-<digits>" occurrence in the name
// matching the provided values, so callers passing FromStep/ToStep
// from a parsed FileEntry get a deterministic, single-shot rewrite.
//
// Returns the input unchanged if the expected segment is not present
// (defensive; the caller should only invoke this with a matching
// boundary file).
func renameStepRange(basename string, fromStep, oldToStep, newToStep uint64) string {
	oldSeg := fmt.Sprintf("%d-%d", fromStep, oldToStep)
	newSeg := fmt.Sprintf("%d-%d", fromStep, newToStep)
	if !strings.Contains(basename, oldSeg) {
		return basename
	}
	return strings.Replace(basename, oldSeg, newSeg, 1)
}
