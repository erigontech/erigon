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

package validation

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// stubBatchValidator is the test-side BatchValidator analogue of
// stubValidator: returns a configured error and tracks call count.
type stubBatchValidator struct {
	name  string
	err   error
	calls int
}

func (s *stubBatchValidator) Name() string { return s.name }
func (s *stubBatchValidator) ValidateBatch(_ context.Context) error {
	s.calls++
	return s.err
}

func TestBatchChain_EmptyChainAcceptsEverything(t *testing.T) {
	var chain BatchChain
	require.NoError(t, chain.Validate(context.Background()))
}

func TestBatchChain_RunsInOrderStopsOnFirstFailure(t *testing.T) {
	a := &stubBatchValidator{name: "blocks"}
	b := &stubBatchValidator{name: "inverted_index", err: errors.New("ef parse failed")}
	c := &stubBatchValidator{name: "publishable"}
	chain := BatchChain{a, b, c}

	err := chain.Validate(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "inverted_index: ef parse failed")
	require.Equal(t, 1, a.calls)
	require.Equal(t, 1, b.calls)
	require.Equal(t, 0, c.calls, "validators after first failure must not run")
}

func TestBatchChain_AllPassReturnsNil(t *testing.T) {
	a := &stubBatchValidator{name: "a"}
	b := &stubBatchValidator{name: "b"}
	chain := BatchChain{a, b}

	require.NoError(t, chain.Validate(context.Background()))
	require.Equal(t, 1, a.calls)
	require.Equal(t, 1, b.calls)
}

func TestBatchChain_FailureNamesValidatorInError(t *testing.T) {
	bad := &stubBatchValidator{name: "commitment_kvi", err: errors.New("kvi mismatch")}
	chain := BatchChain{bad}

	err := chain.Validate(context.Background())
	require.Error(t, err)
	// Must be possible to attribute the failure WITHOUT unwrapping.
	require.Contains(t, err.Error(), "commitment_kvi")
	require.Contains(t, err.Error(), "kvi mismatch")
}
