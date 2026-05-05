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

package lifecycle

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// stubValidator is a minimal Validator for tests: returns a configured
// error and tracks call count.
type stubValidator struct {
	name  string
	err   error
	calls int
}

func (s *stubValidator) Name() string { return s.name }
func (s *stubValidator) Validate(_ *snapshot.FileEntry, _ validation.ContentSource) error {
	s.calls++
	return s.err
}

// stubStepValidator tracks call count + group size for batch tests.
type stubStepValidator struct {
	name      string
	err       error
	calls     int
	lastGroup snapshot.StepGroup
}

func (s *stubStepValidator) Name() string { return s.name }
func (s *stubStepValidator) ValidateStep(_ context.Context, group snapshot.StepGroup) error {
	s.calls++
	s.lastGroup = group
	return s.err
}

func TestBuildOnBatchValidation_StepCompleteAdvancesAtomically(t *testing.T) {
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-256.kv", Domain: snapshot.DomainAccounts,
		FromStep: 0, ToStep: 256,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	dep := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-256.kvi", Domain: snapshot.DomainAccounts,
		FromStep: 0, ToStep: 256,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	inv.AddFile(primary)
	inv.AddFile(dep)

	v := &stubStepValidator{name: "presence"}
	err := BuildOnBatchValidation(validation.StepChain{v}, inv, nil)(context.Background(), primary)
	require.NoError(t, err)
	require.Equal(t, 1, v.calls, "batch validator runs once for the step")
	require.Len(t, v.lastGroup.Files, 2, "validator received both step-siblings")

	for _, name := range []string{primary.Name, dep.Name} {
		state, _ := inv.LifecycleState(name)
		require.Equal(t, snapshot.LifecycleAdvertisable, state,
			"step-sibling %q must advance atomically", name)
	}
}

func TestBuildOnBatchValidation_IncompleteStepNoOps(t *testing.T) {
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-256.kv", Domain: snapshot.DomainAccounts,
		FromStep: 0, ToStep: 256,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	// Sibling still at Downloaded — step is not complete.
	dep := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-256.kvi", Domain: snapshot.DomainAccounts,
		FromStep: 0, ToStep: 256,
		State: snapshot.LifecycleDownloaded, Local: true,
	}
	inv.AddFile(primary)
	inv.AddFile(dep)

	v := &stubStepValidator{name: "presence"}
	require.NoError(t, BuildOnBatchValidation(validation.StepChain{v}, inv, nil)(context.Background(), primary))
	require.Equal(t, 0, v.calls,
		"step incomplete (sibling below Indexed) → batch validator does not run")

	state, _ := inv.LifecycleState(primary.Name)
	require.Equal(t, snapshot.LifecycleIndexed, state,
		"primary stays at Indexed waiting for siblings")
}

func TestBuildOnBatchValidation_ValidationFailureLeavesStepAtIndexed(t *testing.T) {
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-256.kv", Domain: snapshot.DomainAccounts,
		FromStep: 0, ToStep: 256,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	dep := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-256.kvi", Domain: snapshot.DomainAccounts,
		FromStep: 0, ToStep: 256,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	inv.AddFile(primary)
	inv.AddFile(dep)

	wantErr := errors.New("simulated batch failure")
	v := &stubStepValidator{name: "presence", err: wantErr}
	err := BuildOnBatchValidation(validation.StepChain{v}, inv, nil)(context.Background(), primary)
	require.ErrorIs(t, err, wantErr)

	for _, name := range []string{primary.Name, dep.Name} {
		state, _ := inv.LifecycleState(name)
		require.Equal(t, snapshot.LifecycleIndexed, state,
			"validation failure leaves the step at Indexed; sweep retries")
	}
}

func TestBuildOnBatchValidation_SingletonAdvancesDirectly(t *testing.T) {
	// Non-stepped file (caplin / meta / salt) — has no step-siblings,
	// advances individually.
	inv := snapshot.NewInventory()
	e := &snapshot.FileEntry{
		Name: "erigondb.toml", Kind: snapshot.KindMeta,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	inv.AddFile(e)

	v := &stubStepValidator{name: "presence"}
	require.NoError(t, BuildOnBatchValidation(validation.StepChain{v}, inv, nil)(context.Background(), e))

	state, _ := inv.LifecycleState(e.Name)
	require.Equal(t, snapshot.LifecycleAdvertisable, state,
		"singletons skip the batch path and advance directly")
	require.Equal(t, 0, v.calls,
		"batch validator is not invoked for singletons")
}

func TestBuildOnBatchValidation_MinimumAdvancesBeforeExtras(t *testing.T) {
	// Step has minimum (.kv + .kvi) Indexed AND extras (.v) still
	// Downloaded — minimum should advance to Advertisable, extras
	// should stay at Downloaded.
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-256.kv", Domain: snapshot.DomainAccounts,
		FromStep: 0, ToStep: 256,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	primaryAcc := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-256.kvi", Domain: snapshot.DomainAccounts,
		FromStep: 0, ToStep: 256,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	extras := &snapshot.FileEntry{
		Name: "v1.0-accountsHistory.0-256.v", Domain: snapshot.DomainAccounts, Kind: snapshot.KindHistory,
		FromStep: 0, ToStep: 256,
		State: snapshot.LifecycleDownloaded, Local: true,
	}
	require.NoError(t, inv.AddFile(primary))
	require.NoError(t, inv.AddFile(primaryAcc))
	require.NoError(t, inv.AddFile(extras))

	v := &stubStepValidator{name: "presence"}
	require.NoError(t, BuildOnBatchValidation(validation.StepChain{v}, inv, nil)(context.Background(), primary))

	// Minimum advanced.
	state, _ := inv.LifecycleState(primary.Name)
	require.Equal(t, snapshot.LifecycleAdvertisable, state, "primary advances")
	state, _ = inv.LifecycleState(primaryAcc.Name)
	require.Equal(t, snapshot.LifecycleAdvertisable, state, "accessor advances")

	// Extras did NOT advance.
	state, _ = inv.LifecycleState(extras.Name)
	require.Equal(t, snapshot.LifecycleDownloaded, state,
		"extras stay at Downloaded; not yet ready for batch validation")

	require.Equal(t, 1, v.calls,
		"chain ran once across the minimum subset")
}

func TestBuildOnBatchValidation_ExtrasAdvanceOnSecondPass(t *testing.T) {
	// Minimum already at Advertisable from a previous handler invocation;
	// extras now at Indexed → second pass advances extras.
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-256.kv", Domain: snapshot.DomainAccounts,
		FromStep: 0, ToStep: 256,
		State: snapshot.LifecycleAdvertisable, Local: true, Advertisable: true,
	}
	primaryAcc := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-256.kvi", Domain: snapshot.DomainAccounts,
		FromStep: 0, ToStep: 256,
		State: snapshot.LifecycleAdvertisable, Local: true, Advertisable: true,
	}
	extras := &snapshot.FileEntry{
		Name: "v1.0-accountsHistory.0-256.v", Domain: snapshot.DomainAccounts, Kind: snapshot.KindHistory,
		FromStep: 0, ToStep: 256,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	require.NoError(t, inv.AddFile(primary))
	require.NoError(t, inv.AddFile(primaryAcc))
	require.NoError(t, inv.AddFile(extras))

	v := &stubStepValidator{name: "presence"}
	require.NoError(t, BuildOnBatchValidation(validation.StepChain{v}, inv, nil)(context.Background(), extras))

	state, _ := inv.LifecycleState(extras.Name)
	require.Equal(t, snapshot.LifecycleAdvertisable, state,
		"extras advance once full step is Indexed")
	require.Len(t, v.lastGroup.Files, 1, "chain runs across extras-only on second pass")
}

func TestBuildOnBatchValidation_BothPassesInOneCall(t *testing.T) {
	// All files at Indexed simultaneously. Single handler invocation
	// fires both passes: minimum first (advances minimum), then extras
	// (advances extras).
	inv := snapshot.NewInventory()
	files := []*snapshot.FileEntry{
		{Name: "v1.0-accounts.0-256.kv", Domain: snapshot.DomainAccounts, FromStep: 0, ToStep: 256, State: snapshot.LifecycleIndexed, Local: true},
		{Name: "v1.0-accounts.0-256.kvi", Domain: snapshot.DomainAccounts, FromStep: 0, ToStep: 256, State: snapshot.LifecycleIndexed, Local: true},
		{Name: "v1.0-accountsHistory.0-256.v", Domain: snapshot.DomainAccounts, Kind: snapshot.KindHistory, FromStep: 0, ToStep: 256, State: snapshot.LifecycleIndexed, Local: true},
	}
	for _, f := range files {
		require.NoError(t, inv.AddFile(f))
	}

	v := &stubStepValidator{name: "presence"}
	require.NoError(t, BuildOnBatchValidation(validation.StepChain{v}, inv, nil)(context.Background(), files[0]))

	for _, f := range files {
		state, _ := inv.LifecycleState(f.Name)
		require.Equal(t, snapshot.LifecycleAdvertisable, state)
	}
	require.Equal(t, 2, v.calls, "chain runs twice — once per pass")
}

func TestBuildOnBatchValidation_BlockStepWaitsForCommitmentBinding(t *testing.T) {
	// Block-domain step (empty Domain) at Indexed but no commitment
	// binding registered yet → handler returns nil, files stay at
	// Indexed. This is the "block files wait until verified via
	// commitment" semantics — no quarantine, no advance, just wait.
	inv := snapshot.NewInventory()
	headers := &snapshot.FileEntry{
		Name: "v1.1-000900-001000-headers.seg",
		// Block files have empty Domain. FromStep/ToStep are in
		// block-units (after snaptype.ParseFileName's *1000).
		FromStep: 900_000, ToStep: 1_000_000,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	headersIdx := &snapshot.FileEntry{
		Name:     "v1.1-000900-001000-headers.idx",
		FromStep: 900_000, ToStep: 1_000_000,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	require.NoError(t, inv.AddFile(headers))
	require.NoError(t, inv.AddFile(headersIdx))

	v := &stubStepValidator{name: "presence"}
	require.NoError(t, BuildOnBatchValidation(validation.StepChain{v}, inv, nil)(context.Background(), headers))

	// Files stay at Indexed — no binding means no verification possible.
	state, _ := inv.LifecycleState(headers.Name)
	require.Equal(t, snapshot.LifecycleIndexed, state,
		"block files wait at Indexed until a commitment binding covers their range")
	require.Equal(t, 0, v.calls,
		"validator chain doesn't run when waiting for binding")
}

func TestBuildOnBatchValidation_BlockStepAdvancesAfterBindingRegistered(t *testing.T) {
	// Same setup as the wait test, but with a commitment binding
	// registered FIRST. Now the block step is verifiable; the chain
	// runs and the step advances.
	inv := snapshot.NewInventory()

	// Commitment validator would have registered (step, block) for a
	// step covering blocks up to (or beyond) 1_000_000. We simulate
	// that by registering a binding directly.
	inv.RegisterStepBlockBoundary(1024 /* commitment ToStep */, 1_000_000 /* block at end */)

	headers := &snapshot.FileEntry{
		Name:     "v1.1-000900-001000-headers.seg",
		FromStep: 900_000, ToStep: 1_000_000,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	headersIdx := &snapshot.FileEntry{
		Name:     "v1.1-000900-001000-headers.idx",
		FromStep: 900_000, ToStep: 1_000_000,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	require.NoError(t, inv.AddFile(headers))
	require.NoError(t, inv.AddFile(headersIdx))

	v := &stubStepValidator{name: "presence"}
	require.NoError(t, BuildOnBatchValidation(validation.StepChain{v}, inv, nil)(context.Background(), headers))

	for _, name := range []string{headers.Name, headersIdx.Name} {
		state, _ := inv.LifecycleState(name)
		require.Equal(t, snapshot.LifecycleAdvertisable, state,
			"%s should advance once commitment binding covers its range", name)
	}
	require.GreaterOrEqual(t, v.calls, 1,
		"validator chain runs once binding is registered")
}

func TestBuildOnBatchValidation_EmptyChainAcceptsCompleteStep(t *testing.T) {
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-256.kv", Domain: snapshot.DomainAccounts,
		FromStep: 0, ToStep: 256,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	dep := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-256.kvi", Domain: snapshot.DomainAccounts,
		FromStep: 0, ToStep: 256,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	inv.AddFile(primary)
	inv.AddFile(dep)

	require.NoError(t, BuildOnBatchValidation(nil, inv, nil)(context.Background(), primary))

	for _, name := range []string{primary.Name, dep.Name} {
		state, _ := inv.LifecycleState(name)
		require.Equal(t, snapshot.LifecycleAdvertisable, state)
	}
}

func TestBuildOnValidation_EmptyChainAdvancesUnconditionally(t *testing.T) {
	inv := snapshot.NewInventory()
	e := &snapshot.FileEntry{
		Name: "a.kv", Domain: snapshot.DomainAccounts,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	inv.AddFile(e)

	require.NoError(t, BuildOnValidation(nil, nil, inv, nil)(context.Background(), e))

	state, _ := inv.LifecycleState("a.kv")
	require.Equal(t, snapshot.LifecycleAdvertisable, state,
		"empty chain accepts everything; entry must advance")
}

func TestBuildOnValidation_PassingChainAdvances(t *testing.T) {
	inv := snapshot.NewInventory()
	e := &snapshot.FileEntry{
		Name: "a.kv", Domain: snapshot.DomainAccounts,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	inv.AddFile(e)

	v1 := &stubValidator{name: "name-not-empty"}
	v2 := &stubValidator{name: "range-ordering"}
	chain := validation.Chain{v1, v2}

	require.NoError(t, BuildOnValidation(chain, nil, inv, nil)(context.Background(), e))

	state, _ := inv.LifecycleState("a.kv")
	require.Equal(t, snapshot.LifecycleAdvertisable, state)
	require.Equal(t, 1, v1.calls)
	require.Equal(t, 1, v2.calls)
}

func TestBuildOnValidation_FailingChainHaltsAtIndexed(t *testing.T) {
	inv := snapshot.NewInventory()
	e := &snapshot.FileEntry{
		Name: "a.kv", Domain: snapshot.DomainAccounts,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	inv.AddFile(e)

	wantErr := errors.New("file too small")
	v := &stubValidator{name: "size-matches-torrent", err: wantErr}
	chain := validation.Chain{v}

	err := BuildOnValidation(chain, nil, inv, nil)(context.Background(), e)
	require.Error(t, err)
	require.ErrorIs(t, err, wantErr)
	// Failing validator's name is in the error message per Chain.Validate.
	require.Contains(t, err.Error(), "size-matches-torrent")

	// State stays at Indexed for the next sweep to retry.
	state, _ := inv.LifecycleState("a.kv")
	require.Equal(t, snapshot.LifecycleIndexed, state)
}

func TestBuildOnValidation_ContentSourceFactoryFires(t *testing.T) {
	// Verify that contentFor is invoked once per validation call and
	// the produced ContentSource flows into the validator. Confirms
	// the wiring; ContentSource semantics are tested in the validation
	// package itself.
	inv := snapshot.NewInventory()
	e := &snapshot.FileEntry{
		Name: "a.kv", Domain: snapshot.DomainAccounts,
		State: snapshot.LifecycleIndexed, Local: true,
	}
	inv.AddFile(e)

	contentCalled := 0
	expectedContent := validation.BytesContent("hello")

	contentFor := func(entry *snapshot.FileEntry) validation.ContentSource {
		contentCalled++
		require.Equal(t, "a.kv", entry.Name)
		return expectedContent
	}

	v := &contentAssertingValidator{expected: expectedContent}
	chain := validation.Chain{v}

	require.NoError(t, BuildOnValidation(chain, contentFor, inv, nil)(
		context.Background(), e,
	))
	require.Equal(t, 1, contentCalled,
		"contentFor must be invoked exactly once per handler call")
	require.True(t, v.received, "validator must receive the content from contentFor")
}

// contentAssertingValidator records that it received the expected
// ContentSource. Used by TestBuildOnValidation_ContentSourceFactoryFires.
type contentAssertingValidator struct {
	expected validation.ContentSource
	received bool
}

func (v *contentAssertingValidator) Name() string { return "content-asserter" }
func (v *contentAssertingValidator) Validate(_ *snapshot.FileEntry, content validation.ContentSource) error {
	if content != nil {
		v.received = true
	}
	return nil
}
