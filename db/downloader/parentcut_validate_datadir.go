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
	"errors"
	"fmt"
	"os"

	"github.com/erigontech/erigon/execution/chain"
)

// ErrForkPreMergeCut is returned by ValidateForkDatadir when a fork
// chain.Config has a CutBlock strictly before the parent chain's merge
// block. Erigon does not support PoW processing; pre-merge forks
// cannot run.
var ErrForkPreMergeCut = errors.New("fork chain.Config: CutBlock is pre-merge — Erigon does not support PoW; the cut must be at or after the parent's merge block")

// ErrForkDatadirHasPostCutData is returned by ValidateForkDatadir when
// a fork chain.Config is loaded against a datadir already populated
// with snap files that span blocks at or past CutBlock — typically a
// datadir that was running the parent chain past the cut point and is
// being re-pointed at a fork config without being cleared. The fix is
// to use `erigon snapshots fork-from` to produce a clean fork datadir.
var ErrForkDatadirHasPostCutData = errors.New("fork chain.Config: datadir has snap files past CutBlock from the parent's lineage — use `erigon snapshots fork-from` to produce a clean fork datadir, or point --datadir at an empty directory")

// ValidateForkDatadir checks that a (possibly-fork) chain.Config is
// runnable against a given snap dir. Non-fork configs (Parent == "")
// are no-ops. Fork configs trigger two checks:
//
//  1. CutBlock must not be pre-merge. If the derived config's
//     MergeHeight is non-nil and CutBlock < MergeHeight, refuse with
//     ErrForkPreMergeCut. Post-merge guarantee depends on the
//     parent's merge block surviving through DeriveForkChainConfig
//     into the derived config; fork-from preserves it.
//  2. The snap dir must not contain any file whose range extends past
//     CutBlock from the parent's lineage. Equivalent to "every file
//     in the dir classifies as PreCut against CutBlock" — a fresh
//     fork-from output has exactly that shape. The check uses the
//     same classifier as the copy planner, run with an empty
//     stepToBlock — state files conservatively count as straddle
//     (i.e. potential conflict), which is the right safe default.
//
// snapDir is the fork's snap dir (typically datadir/snapshots). If
// snapDir doesn't exist or is empty, no datadir-conflict error fires
// — only the pre-merge guard.
//
// Designed to run early at process startup, before the storage
// Provider opens any handles. A non-nil return means erigon should
// abort with the error message; the caller decides exit behaviour.
func ValidateForkDatadir(cfg *chain.Config, snapDir string) error {
	if cfg == nil {
		return fmt.Errorf("ValidateForkDatadir: nil chain.Config")
	}
	if cfg.Parent == "" {
		// Not a fork config — no validation required. Root chains
		// take this path; their datadirs are governed by other
		// checks (initial-bootstrap, preverified hashes, etc.).
		return nil
	}

	// Check 1: pre-merge cut block.
	if cfg.MergeHeight != nil && cfg.CutBlock < *cfg.MergeHeight {
		return fmt.Errorf("%w (cut_block=%d, parent merge_block=%d)", ErrForkPreMergeCut, cfg.CutBlock, *cfg.MergeHeight)
	}

	// Check 2: datadir-conflict scan.
	if snapDir == "" {
		return nil // tools that don't yet know the snap dir skip the file scan
	}
	info, err := os.Stat(snapDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh datadir; no conflict possible
		}
		return fmt.Errorf("ValidateForkDatadir: stat snap dir %s: %w", snapDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("ValidateForkDatadir: snap dir %s is not a directory", snapDir)
	}

	// Reuse the copy planner's classifier as the conflict detector:
	// every file in the dir should be PreCut or Unknown (non-range
	// chain-wide config). Anything else is a conflict.
	plan, err := BuildCopyPlan(snapDir, cfg.CutBlock, StepToBlock{})
	if err != nil {
		return fmt.Errorf("ValidateForkDatadir: scan snap dir: %w", err)
	}
	if len(plan.Straddle) > 0 || len(plan.PostCut) > 0 {
		offenders := make([]string, 0, 4)
		for _, e := range plan.Straddle {
			offenders = append(offenders, fmt.Sprintf("%s (straddles cut)", e.RelPath))
			if len(offenders) >= 4 {
				break
			}
		}
		for _, e := range plan.PostCut {
			if len(offenders) >= 4 {
				break
			}
			offenders = append(offenders, fmt.Sprintf("%s (post-cut)", e.RelPath))
		}
		return fmt.Errorf("%w (cut_block=%d, %d straddle + %d post-cut files; first offenders: %v)",
			ErrForkDatadirHasPostCutData, cfg.CutBlock, len(plan.Straddle), len(plan.PostCut), offenders)
	}
	return nil
}
