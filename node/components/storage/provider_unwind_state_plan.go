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
	"fmt"

	"github.com/erigontech/erigon/db/kv"
)

// stateFileAction is the verdict the mode-B planner assigns to a
// single state-domain .kv file given a new stepBoundary.
type stateFileAction int

const (
	// actionKeep — file content is entirely valid at the unwind
	// boundary: ToStep <= stepBoundary (file covers steps strictly
	// before the boundary step). No mutation.
	actionKeep stateFileAction = iota

	// actionRegenInPlace — file's [FromStep, ToStep) range ends
	// exactly AT the boundary step (ToStep == stepBoundary, FromStep
	// < stepBoundary). Content reflects state at end of step
	// (ToStep-1) which is past lastTxNum; rewrite content to
	// as-of-lastTxNum but keep the filename.
	actionRegenInPlace

	// actionRegenTruncate — file straddles the boundary (FromStep <
	// stepBoundary AND ToStep > stepBoundary). Truncate the
	// filename's step range to [FromStep, stepBoundary) and rewrite
	// content to as-of-lastTxNum. The original (broad-named) file is
	// removed.
	actionRegenTruncate

	// actionRemove — file's coverage is entirely past the boundary
	// (FromStep >= stepBoundary). Its content reflects state in
	// steps that haven't happened post-unwind. Remove the file
	// outright; the post-unwind forward-exec will rewrite this range
	// into MDBX, and a future retire will produce a fresh canonical
	// file under the same name.
	actionRemove
)

// stateFileRange is the minimal shape the planner needs from a state-
// domain file: its step range. Tests pass synthetic ranges; production
// extracts from snapshot.FileEntry's FromStep/ToStep.
type stateFileRange struct {
	FromStep uint64
	ToStep   uint64
}

// classifyStateFileForUnwind returns the action the mode-B unwind
// planner should take for a single file given a new stepBoundary.
//
// The classification is purely topological — no I/O, no Inventory
// access. The four actions are mutually exclusive and exhaustive over
// all (FromStep, ToStep) pairs with FromStep < ToStep.
//
// Rules (in declaration order; the first matching rule wins):
//
//	ToStep <= stepBoundary - 1   →  actionKeep
//	ToStep == stepBoundary       →  actionRegenInPlace      (FromStep < stepBoundary guaranteed by FromStep<ToStep)
//	FromStep < stepBoundary      →  actionRegenTruncate     (and ToStep > stepBoundary)
//	FromStep >= stepBoundary     →  actionRemove
//
// Equivalently expressed by which side of stepBoundary the file's
// range falls on:
//
//	[F, T) entirely before boundary  →  keep
//	[F, T) ends at boundary           →  regen in place
//	[F, T) straddles boundary         →  regen with truncation
//	[F, T) entirely past boundary     →  remove
func classifyStateFileForUnwind(r stateFileRange, stepBoundary uint64) stateFileAction {
	if r.ToStep < stepBoundary {
		return actionKeep
	}
	if r.ToStep == stepBoundary {
		return actionRegenInPlace
	}
	// ToStep > stepBoundary
	if r.FromStep < stepBoundary {
		return actionRegenTruncate
	}
	return actionRemove
}

// classifiedFiles partitions a domain's state-domain .kv files by
// action against the given stepBoundary. The returned slices are
// disjoint and their union is the input set.
type classifiedFiles struct {
	keep    []stateFileRange
	regen   []stateFileRange // mixed: in-place + truncate; FinalizeUnwind needs to know which is which
	remove  []stateFileRange
	inPlace []bool // parallel to regen: true if regenInPlace, false if regenTruncate
}

// overrideActionForDomain adjusts a per-file action after the
// topological classifier based on the prune-mode IX-horizon check.
//
// Under `--prune.mode=minimal` the IX only covers the last ~100k
// blocks; a deep mode-B target below that horizon makes per-key AsOf
// impossible. Per-domain policy:
//   - Receipt: regen actions become actionRemove (forward-exec
//     restores every value; receipt keys are re-written on every txN,
//     so retire produces a fresh .kv naturally).
//   - Commitment: pass through — regen uses the compute's captured
//     branches (via WriteCommitmentBoundaryFileV4) plus an encoded
//     anchor, not per-key AsOf, so the IX horizon doesn't apply.
//   - Accounts/storage/code: error (silent removal would lose state
//     for keys last written pre-target and never touched since).
//   - actionKeep / actionRemove: no AsOf lookup performed, always
//     pass through.
func overrideActionForDomain(action stateFileAction, domain kv.Domain, ixCoversTarget bool) (stateFileAction, error) {
	if ixCoversTarget {
		return action, nil
	}
	switch action {
	case actionKeep, actionRemove:
		return action, nil
	case actionRegenInPlace, actionRegenTruncate:
		switch domain {
		case kv.ReceiptDomain:
			return actionRemove, nil
		case kv.CommitmentDomain:
			return action, nil
		default:
			return 0, fmt.Errorf("domain %s history pruned past unwind target: regen requires history the pruning contract has removed", domain)
		}
	default:
		return 0, fmt.Errorf("overrideActionForDomain: unknown stateFileAction %d", action)
	}
}

// planStateFileActions iterates the input files and returns the
// per-action partition. Used by regenerateBoundaryStepFiles to drive
// the post-mode-B file-set transformation; pure-function shape lets
// it be tested without a Provider or Aggregator.
func planStateFileActions(files []stateFileRange, stepBoundary uint64) classifiedFiles {
	var out classifiedFiles
	for _, f := range files {
		switch classifyStateFileForUnwind(f, stepBoundary) {
		case actionKeep:
			out.keep = append(out.keep, f)
		case actionRegenInPlace:
			out.regen = append(out.regen, f)
			out.inPlace = append(out.inPlace, true)
		case actionRegenTruncate:
			out.regen = append(out.regen, f)
			out.inPlace = append(out.inPlace, false)
		case actionRemove:
			out.remove = append(out.remove, f)
		}
	}
	return out
}
