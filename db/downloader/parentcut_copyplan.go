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

package downloader

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/erigontech/erigon/db/snaptype"
)

// CopyClassification labels each parent snap-dir entry's relationship
// to the fork's cut. Used by fork-from to decide which files to copy
// into the fork's fresh datadir.
type CopyClassification int

const (
	// CopyPreCut: file's coverage is entirely strictly before the cut.
	// Copy into the fork's snap dir as-is; fork inherits.
	CopyPreCut CopyClassification = iota

	// CopyPostCut: file's coverage is entirely strictly after the cut.
	// Skip; not relevant to a fork that diverges from the cut.
	CopyPostCut

	// CopyStraddle: file's coverage spans the cut block. EXCLUDED from
	// the copy plan per the converged design decision 6 (straddle step
	// = full self-contained): the fork's own first retire of this step
	// produces a fresh full step file with the fork's lineage. Logged
	// so the operator sees what the fork will recompute.
	CopyStraddle

	// CopyUnknown: the file couldn't be classified — usually a name
	// that ParseFileName doesn't understand (config metadata, salt
	// files, foreign artefacts). Conservative default: copy. The
	// operator sees the list of unknowns so they can audit.
	CopyUnknown
)

func (c CopyClassification) String() string {
	switch c {
	case CopyPreCut:
		return "pre-cut"
	case CopyPostCut:
		return "post-cut"
	case CopyStraddle:
		return "straddle"
	case CopyUnknown:
		return "unknown"
	}
	return fmt.Sprintf("unknown(%d)", int(c))
}

// CopyPlanEntry is one parent snap-dir file's classification, with the
// resolved [from, to) range used for the decision (in blocks for block
// files, in steps for state files). RelPath is the file's path
// relative to the parent's snap dir, suitable for joining onto the
// fork's snap dir for the actual copy.
type CopyPlanEntry struct {
	RelPath        string
	Classification CopyClassification
	IsStateFile    bool
	From           uint64 // blocks (block files) or steps (state files)
	To             uint64
	Reason         string // human-readable explanation; populated for Straddle / Unknown
}

// CopyPlan is the result of BuildCopyPlan: every file in the parent's
// snap dir, classified. The Copy list is what fork-from should
// physically copy; Straddle + PostCut + Unknown are tracked for
// operator visibility (logged in the CLI's --dry-run / verbose output).
type CopyPlan struct {
	Copy     []CopyPlanEntry // PreCut + Unknown — to be copied
	Straddle []CopyPlanEntry // span the cut; fork retires fresh
	PostCut  []CopyPlanEntry // entirely after the cut; skipped
	Errors   []string        // non-fatal classification errors (logged + skipped)
}

// StepToBlock maps a state-file step boundary (FromStep / ToStep) to
// the EL block number whose end-state was anchored at that step.
// Populated from the parent's V2 manifest's commitment-domain
// AtBlock anchors (DomainFileEntry.AtBlock at the file's ToStep).
// Empty mapping is acceptable: state-file classification then
// degrades to "if ToStep is unmappable, classify as Straddle and let
// the fork retire fresh". Conservative + matches the spirit of
// decision 6.
type StepToBlock map[uint64]uint64

// BuildCopyPlan walks parentSnapDir and classifies every file against
// cutBlock. stepToBlock maps state-file step boundaries to EL blocks;
// supply an empty map to fall back to conservative "unknown → copy" /
// "ambiguous-step → straddle" behaviour.
//
// File classification rules:
//   - Block files (parsed by snaptype.ParseFileName, IsStateFile=false):
//     compare [From, To) directly against cutBlock.
//   - State files (IsStateFile=true): use stepToBlock to resolve ToStep
//     to a block; classify by that block. If ToStep isn't in
//     stepToBlock, classify as Straddle (conservative — fork retires).
//   - Names that don't parse (salt files, config files, foreign):
//     classified Unknown; placed in Copy with Reason="unparsed".
func BuildCopyPlan(parentSnapDir string, cutBlock uint64, stepToBlock StepToBlock) (*CopyPlan, error) {
	if parentSnapDir == "" {
		return nil, fmt.Errorf("build copy plan: empty parent snap dir")
	}
	info, err := os.Stat(parentSnapDir)
	if err != nil {
		return nil, fmt.Errorf("build copy plan: stat parent snap dir %s: %w", parentSnapDir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("build copy plan: %s is not a directory", parentSnapDir)
	}

	plan := &CopyPlan{}
	walkErr := filepath.Walk(parentSnapDir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			plan.Errors = append(plan.Errors, fmt.Sprintf("walk %s: %v", path, err))
			return nil // continue
		}
		if fi.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(parentSnapDir, path)
		if err != nil {
			plan.Errors = append(plan.Errors, fmt.Sprintf("relpath %s: %v", path, err))
			return nil
		}
		// Skip torrent metadata + transient files — they get
		// regenerated for the fork.
		base := filepath.Base(rel)
		if strings.HasSuffix(base, ".torrent") || strings.HasSuffix(base, ".tmp") {
			return nil
		}

		entry := classify(rel, cutBlock, stepToBlock)
		switch entry.Classification {
		case CopyPreCut, CopyUnknown:
			plan.Copy = append(plan.Copy, entry)
		case CopyStraddle:
			plan.Straddle = append(plan.Straddle, entry)
		case CopyPostCut:
			plan.PostCut = append(plan.PostCut, entry)
		}
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("build copy plan: walk: %w", walkErr)
	}
	// Sort each list by RelPath so the result is deterministic +
	// human-friendly.
	for _, list := range []*[]CopyPlanEntry{&plan.Copy, &plan.Straddle, &plan.PostCut} {
		sort.Slice(*list, func(i, j int) bool { return (*list)[i].RelPath < (*list)[j].RelPath })
	}
	return plan, nil
}

// classify is the per-file classifier. Exported for tests; not part of
// the package's public API.
//
// Uses snaptype.ParseFileName to get the structural fields (isStateFile,
// From, To, TypeString) — ParseFileName's `ok` return depends on the
// type-enum being registered in the running binary's snaptype set,
// which is not load-bearing here; we follow the same "look at TypeString
// + From/To regardless of ok" pattern as db/snapshotsync/ParseRange.
func classify(relPath string, cutBlock uint64, stepToBlock StepToBlock) CopyPlanEntry {
	base := filepath.Base(relPath)
	parsed, isState, _ := snaptype.ParseFileName("", base)
	if parsed.TypeString == "" {
		return CopyPlanEntry{
			RelPath:        relPath,
			Classification: CopyUnknown,
			Reason:         "unparseable filename — copying conservatively",
		}
	}
	entry := CopyPlanEntry{
		RelPath:     relPath,
		IsStateFile: isState,
		From:        parsed.From,
		To:          parsed.To,
	}
	// Non-range files (salt-*.txt, erigondb.toml-equivalents that parse
	// with a TypeString but no From/To range): not block- or
	// step-bound, so they always apply regardless of cut. Route to
	// Unknown (which copies conservatively).
	if !isState && parsed.From == 0 && parsed.To == 0 {
		entry.Classification = CopyUnknown
		entry.Reason = fmt.Sprintf("non-range file (typeString=%q) — copying conservatively", parsed.TypeString)
		return entry
	}
	if isState {
		// State files: use stepToBlock to find the block boundary for
		// the file's ToStep. Conservative fallback: if not mapped,
		// treat as straddle (the fork retires fresh).
		blockAtTo, mapped := stepToBlock[parsed.To]
		if !mapped {
			entry.Classification = CopyStraddle
			entry.Reason = fmt.Sprintf("ToStep %d not in step→block map — treating as straddle (fork retires)", parsed.To)
			return entry
		}
		blockAtFrom, _ := stepToBlock[parsed.From]
		entry.Classification = classifyRange(blockAtFrom, blockAtTo, cutBlock)
		entry.Reason = fmt.Sprintf("step [%d, %d) → blocks [%d, %d)", parsed.From, parsed.To, blockAtFrom, blockAtTo)
		return entry
	}
	// Block file: From / To are block numbers directly.
	entry.Classification = classifyRange(parsed.From, parsed.To, cutBlock)
	return entry
}

// classifyRange decides where a file's [from, to) block range falls
// relative to cutBlock.
//
//   - Entirely strictly before the cut (to <= cutBlock): PreCut
//   - Entirely strictly after the cut (from > cutBlock): PostCut
//   - Otherwise: Straddle
func classifyRange(from, to, cutBlock uint64) CopyClassification {
	if to <= cutBlock {
		return CopyPreCut
	}
	if from > cutBlock {
		return CopyPostCut
	}
	return CopyStraddle
}
