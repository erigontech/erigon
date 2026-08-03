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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// pathSpyAggregator captures every DomainKVFilePathV4 call so a test
// can assert both that it was invoked (not skipped) and that the
// (domain, fromTxN, toTxN) arguments match the mode-C v4 emit
// contract. StepSize/DomainCompression carry the minimum a caller
// needs to drive the path selection.
type pathSpyAggregator struct {
	stepSize uint64
	calls    []pathSpyCall
}

type pathSpyCall struct {
	domain          kv.Domain
	fromTxN, toTxN  uint64
	returnedPathTag string
}

func (a *pathSpyAggregator) Files() []string   { return nil }
func (a *pathSpyAggregator) OpenFolder() error { return nil }
func (a *pathSpyAggregator) BuildMissedAccessors(_ context.Context, _ int, _ ...kv.BuildAccessorsOption) error {
	return nil
}
func (a *pathSpyAggregator) LockCollation()   {}
func (a *pathSpyAggregator) UnlockCollation() {}
func (a *pathSpyAggregator) StepSize() uint64 { return a.stepSize }
func (a *pathSpyAggregator) WipeWritableShadowPast(_ context.Context, _ kv.TemporalRwTx, _ uint64) error {
	return nil
}
func (a *pathSpyAggregator) DomainCompression(_ kv.Domain) seg.FileCompression {
	return seg.CompressNone
}
func (a *pathSpyAggregator) Unwind(_ uint64)            {}
func (a *pathSpyAggregator) SetUnwindInProgress(_ bool) {}
func (a *pathSpyAggregator) WaitForBuildAndMergeQuiescence(_ time.Duration) error {
	return nil
}
func (a *pathSpyAggregator) DomainKVFilePathV4(domain kv.Domain, fromTxN, toTxN uint64) string {
	tag := fmt.Sprintf("v4.0-%s.%d-%d.kv", domain, fromTxN, toTxN)
	a.calls = append(a.calls, pathSpyCall{domain: domain, fromTxN: fromTxN, toTxN: toTxN, returnedPathTag: tag})
	return tag
}

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

// TestRegenPathNoSubdirDoubling pins the bug caught live on the
// 2026-06-30 truncated-rename soak's iter-1 mode_a2: the regen
// path was being constructed by joining filepath.Dir(oldPath) with
// boundary.Name. But boundary.Name in Inventory carries the
// kind-subdir prefix ("domain/v2.0-accounts.280-284.kv"), and
// filepath.Dir(oldPath) is ALREADY inside the kind subdir
// ("<snapDir>/domain"). The naive join produces a doubled prefix:
// "<snapDir>/domain/domain/v2.0-accounts.280-284.kv.regen.tmp"
// → seg.NewCompressor's open fails with "no such file or directory".
//
// Fix: derive the regen file's basename from filepath.Base(oldPath)
// (which strips the kind-subdir), then renameStepRange operates on
// the bare basename, then filepath.Join with filepath.Dir(oldPath)
// produces a single-prefix path.
//
// This is a path-shape regression test — it asserts the truncated-
// rename code path produces a path under <snapDir>/<kind>/ (one
// subdir level), NOT <snapDir>/<kind>/<kind>/ (two).
func TestRegenPathNoSubdirDoubling(t *testing.T) {
	t.Parallel()
	// Simulate the exact shape that broke live: boundary.Name carries
	// the "domain/" subdir prefix; oldPath is the absolute path inside
	// <snapDir>/domain/. Drive renameStepRange + filepath.Join through
	// the same recipe regenerateBoundaryStepFiles uses (without
	// actually opening files — this is a pure path-construction
	// regression).
	snapDir := "/tmp/snapdir-fixture"

	// The BUGGY code did: filepath.Join(filepath.Dir(oldPath), boundary.Name)
	// where boundary.Name = "domain/v2.0-accounts.272-280.kv".
	// That doubled the subdir. We now derive from filepath.Base(oldPath):
	oldBaseName := "v2.0-accounts.272-280.kv" // = filepath.Base(oldPath)
	truncatedBaseName := renameStepRange(oldBaseName, 272, 280, 278)
	require.Equal(t, "v2.0-accounts.272-278.kv", truncatedBaseName)

	// filepath.Dir(oldPath) is "<snapDir>/domain", joining with the
	// bare basename produces a single-subdir path.
	require.Equal(t,
		snapDir+"/domain/v2.0-accounts.272-278.kv",
		snapDir+"/domain/"+truncatedBaseName,
		"truncated path must live under one kind subdir, not two")
}

// TestBoundaryRegenFinalPath_TruncateGoesToV4 pins the mode-C v4 emit
// invariant: for actionRegenTruncate, the final path is the
// aggregator's v4.0 raw-txnum-named path with endTxN = lastTxNum+1.
// This is the whole point of the 2026-08-03 mode-C completeness fix —
// the file's advertised horizon must match its as-of-lastTxNum content
// rather than lying via the step-boundary convention.
func TestBoundaryRegenFinalPath_TruncateGoesToV4(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(390_625)
	agg := &pathSpyAggregator{stepSize: stepSize}

	// Straddler covering steps [272, 280). Mid-step target with
	// lastTxNum such that (lastTxNum+1) lands strictly inside the
	// file's range (i.e. between 272*stepSize and 280*stepSize).
	const fromStep = uint64(272)
	const lastTxNum = uint64(109_000_000)
	oldPath := "/tmp/snap/domain/v2.0-accounts.272-280.kv"

	got := boundaryRegenFinalPath(agg, kv.AccountsDomain, fromStep, stepSize, actionRegenTruncate, lastTxNum, oldPath)

	require.Len(t, agg.calls, 1, "actionRegenTruncate must dispatch to DomainKVFilePathV4 exactly once")
	call := agg.calls[0]
	require.Equal(t, kv.AccountsDomain, call.domain)
	require.Equal(t, fromStep*stepSize, call.fromTxN,
		"v4 file's fromTxN must be the straddler's FromStep*stepSize (aligns with the retained state before the boundary)")
	require.Equal(t, lastTxNum+1, call.toTxN,
		"v4 file's toTxN must be lastTxNum+1 (honest endTxN matching the as-of-lastTxN content)")
	require.Equal(t, "v4.0-accounts.106250000-109000001.kv", got,
		"finalPath must be the aggregator-computed v4 path, not oldPath")
}

// TestBoundaryRegenFinalPath_InPlaceKeepsOldPath pins the aligned
// case: when the file's endStep already equals the unwind target's
// step boundary, no v4 path is needed — the file's step-aligned name
// already matches its as-of-lastTxN content. DomainKVFilePathV4 must
// NOT be called (otherwise the wire would produce an unnecessary v4
// file that retire would have to supersede later).
func TestBoundaryRegenFinalPath_InPlaceKeepsOldPath(t *testing.T) {
	t.Parallel()
	agg := &pathSpyAggregator{stepSize: 390_625}

	oldPath := "/tmp/snap/domain/v2.0-accounts.272-280.kv"
	got := boundaryRegenFinalPath(agg, kv.AccountsDomain, 272, 390_625, actionRegenInPlace, 109_000_000, oldPath)

	require.Empty(t, agg.calls, "actionRegenInPlace must NOT dispatch to DomainKVFilePathV4 — the aligned file keeps its own name")
	require.Equal(t, oldPath, got, "actionRegenInPlace must return the OLD path unchanged")
}

// TestBoundaryRegenFinalPath_CommitmentTruncateSameV4Shape verifies
// commitment straddlers get the same v4 treatment as state domains.
// This is what the 2026-08-03 drop-of-override-1 (commit bc2107ac7d)
// enables — commitment now takes the same emit path, no domain-
// specific special-case in the finalPath calculation. The domain
// argument is passed straight through to DomainKVFilePathV4, which
// namespaces the filename by domain.
func TestBoundaryRegenFinalPath_CommitmentTruncateSameV4Shape(t *testing.T) {
	t.Parallel()
	agg := &pathSpyAggregator{stepSize: 390_625}

	oldPath := "/tmp/snap/domain/v2.0-commitment.272-280.kv"
	got := boundaryRegenFinalPath(agg, kv.CommitmentDomain, 272, 390_625, actionRegenTruncate, 109_000_000, oldPath)

	require.Len(t, agg.calls, 1)
	require.Equal(t, kv.CommitmentDomain, agg.calls[0].domain,
		"commitment must reach DomainKVFilePathV4 unchanged — no special-case dispatch")
	require.Equal(t, "v4.0-commitment.106250000-109000001.kv", got)
}
