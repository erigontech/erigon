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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

// stubRwTx satisfies kv.TemporalRwTx for the purpose of being non-nil;
// no method on it is exercised because Provider.Unwind in commit 2b
// returns the not-yet-implemented error before any tx work happens.
type stubRwTx struct{ kv.TemporalRwTx }

// alignedProvider returns a Provider whose BlockAligned() reports true
// — the minimum state required to clear the mode-B precondition guard
// inside Provider.Unwind without standing up a real Initialize.
func alignedProvider() *Provider {
	return &Provider{blockAlignedBoundaries: true}
}

func TestProviderUnwind_RejectsNonAlignedChain(t *testing.T) {
	t.Parallel()
	// Default Provider has blockAlignedBoundaries == false.
	p := &Provider{}

	err := p.Unwind(context.Background(), 1000, UnwindOpts{Tx: &stubRwTx{}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not block-aligned")
}

func TestProviderUnwind_RejectsNilTx(t *testing.T) {
	t.Parallel()
	p := alignedProvider()
	err := p.Unwind(context.Background(), 1000, UnwindOpts{Tx: nil})
	require.Error(t, err)
	require.Contains(t, err.Error(), "opts.Tx is nil")
}

func TestProviderUnwind_RejectsNilProvider(t *testing.T) {
	t.Parallel()
	var p *Provider
	err := p.Unwind(context.Background(), 1000, UnwindOpts{Tx: &stubRwTx{}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil provider")
}

// TestProviderUnwind_ValidationOK_ReturnsNotYetImplemented pins the
// commit-2b contract: an aligned Provider with a non-nil Tx passes all
// the precondition guards, then returns the explicit
// not-yet-implemented error pointing at commit 2c. When commit 2c lands
// the sub-ops, the assertion here flips to "returns success after
// snapshot-trim + DB-reset + commitment-recompute all run."
func TestProviderUnwind_ValidationOK_ReturnsNotYetImplemented(t *testing.T) {
	t.Parallel()
	p := alignedProvider()
	err := p.Unwind(context.Background(), 1000, UnwindOpts{Tx: &stubRwTx{}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not yet implemented")
	require.Contains(t, err.Error(), "commit 2c",
		"the error must point readers at where the implementation lands so a future failure is diagnosable from the message")
}
