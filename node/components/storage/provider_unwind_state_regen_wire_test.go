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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// These tests pin boundaryStepFileForDomain's lookup predicate. The
// soak v14 iter-3 mode-B wedge (depth 30k, target=3,006,443) surfaced
// the bug: the strict `ToStep == stepBoundary` match couldn't find
// the file whose step range CONTAINED the unwind boundary when the
// aggregator had already merged smaller step files into wider chunks.
// stepBoundary=275, file `v2.0-commitment.272-276.kv` (FromStep=272,
// ToStep=276) → strict match returned nil → regen skipped → the
// commitment anchor was never planted at the unwind target → Caplin's
// catchup wedged on `behind commitment: TxNums at 3006443 and behind
// commitment 3027998`.
//
// Contract: the function must find the file whose [FromStep, ToStep)
// interval STRADDLES stepBoundary — i.e. FromStep < stepBoundary AND
// ToStep >= stepBoundary. The aligned case (ToStep == stepBoundary
// exactly) is a strict sub-case of the straddle and must keep working.

// TestBoundaryStepFileForDomain_ExactToStepMatch pins the
// pre-existing aligned case: when stepBoundary lines up exactly on a
// file's ToStep, that file is returned. Regression safety — today's
// working merge-aligned soak iterations (v14 iter-1 5k, iter-2 10k)
// hit this branch.
func TestBoundaryStepFileForDomain_ExactToStepMatch(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	files := []*snapshot.FileEntry{
		{Name: "v2.0-commitment.0-256.kv", Domain: snapshot.DomainCommitment, FromStep: 0, ToStep: 256, Local: true},
		{Name: "v2.0-commitment.256-264.kv", Domain: snapshot.DomainCommitment, FromStep: 256, ToStep: 264, Local: true},
		{Name: "v2.0-commitment.264-266.kv", Domain: snapshot.DomainCommitment, FromStep: 264, ToStep: 266, Local: true},
	}
	for _, e := range files {
		require.NoError(t, inv.AddFile(e))
	}

	p := &Provider{Inventory: inv}
	got := p.boundaryStepFileForDomain(snapshot.DomainCommitment, 266)
	require.NotNil(t, got, "stepBoundary=266 aligns with file v2.0-commitment.264-266.kv's ToStep — must be returned")
	require.Equal(t, "v2.0-commitment.264-266.kv", got.Name)
}

// TestBoundaryStepFileForDomain_FindsStraddleFile is the new
// behaviour that fixes the soak v14 iter-3 wedge: when stepBoundary
// lands STRICTLY INSIDE a merged file's [FromStep, ToStep) range
// (FromStep < stepBoundary < ToStep), that file is returned so its
// content gets regenerated with truncation at lastTxNum. Without
// this, the boundary file is destroyed by the trim path (was:
// `ToStep > stepBoundary`) and no replacement is written →
// regenerated=0 → commitment anchor never planted.
func TestBoundaryStepFileForDomain_FindsStraddleFile(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	// Simulate the v14 iter-3 layout: aggregator has already merged
	// step files into wider chunks. The 272-276 file straddles
	// stepBoundary=275 (FromStep=272 < 275, ToStep=276 > 275).
	files := []*snapshot.FileEntry{
		{Name: "v2.0-commitment.0-256.kv", Domain: snapshot.DomainCommitment, FromStep: 0, ToStep: 256, Local: true},
		{Name: "v2.0-commitment.256-264.kv", Domain: snapshot.DomainCommitment, FromStep: 256, ToStep: 264, Local: true},
		{Name: "v2.0-commitment.264-272.kv", Domain: snapshot.DomainCommitment, FromStep: 264, ToStep: 272, Local: true},
		{Name: "v2.0-commitment.272-276.kv", Domain: snapshot.DomainCommitment, FromStep: 272, ToStep: 276, Local: true},
	}
	for _, e := range files {
		require.NoError(t, inv.AddFile(e))
	}

	p := &Provider{Inventory: inv}
	got := p.boundaryStepFileForDomain(snapshot.DomainCommitment, 275)
	require.NotNil(t, got, "stepBoundary=275 falls inside file v2.0-commitment.272-276.kv's range — must be returned for regen")
	require.Equal(t, "v2.0-commitment.272-276.kv", got.Name,
		"the straddle file (FromStep=272 < stepBoundary=275 < ToStep=276) is the one whose content needs regen-truncation at lastTxNum")
}

// TestBoundaryStepFileForDomain_StepBoundaryBelowAllFiles pins the
// early-history case: every file's FromStep is at or past
// stepBoundary, so no file contains the boundary txNum. Function
// must return nil (no boundary file to regenerate). This is the
// expected outcome before the first step has fully retired.
func TestBoundaryStepFileForDomain_StepBoundaryBelowAllFiles(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	files := []*snapshot.FileEntry{
		{Name: "v2.0-commitment.256-264.kv", Domain: snapshot.DomainCommitment, FromStep: 256, ToStep: 264, Local: true},
		{Name: "v2.0-commitment.264-272.kv", Domain: snapshot.DomainCommitment, FromStep: 264, ToStep: 272, Local: true},
	}
	for _, e := range files {
		require.NoError(t, inv.AddFile(e))
	}

	p := &Provider{Inventory: inv}
	got := p.boundaryStepFileForDomain(snapshot.DomainCommitment, 100)
	require.Nil(t, got, "stepBoundary=100 is below every file's FromStep — no file contains it; must return nil")
}

// TestBoundaryStepFileForDomain_StepBoundaryAboveAllFiles pins the
// past-tip case: every file's ToStep is below stepBoundary, so no
// file extends up to the boundary. Function must return nil — there
// is nothing to regen because the chain hasn't produced any state
// past the existing files yet. Real-world: stepBoundary computed
// from a lastTxNum that lives in the writable shadow, not yet
// retired to a .kv file.
func TestBoundaryStepFileForDomain_StepBoundaryAboveAllFiles(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	files := []*snapshot.FileEntry{
		{Name: "v2.0-commitment.256-264.kv", Domain: snapshot.DomainCommitment, FromStep: 256, ToStep: 264, Local: true},
		{Name: "v2.0-commitment.264-272.kv", Domain: snapshot.DomainCommitment, FromStep: 264, ToStep: 272, Local: true},
	}
	for _, e := range files {
		require.NoError(t, inv.AddFile(e))
	}

	p := &Provider{Inventory: inv}
	got := p.boundaryStepFileForDomain(snapshot.DomainCommitment, 300)
	require.Nil(t, got, "stepBoundary=300 is past every file's ToStep — nothing to regen; must return nil")
}
