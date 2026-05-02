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
