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

// TestProviderUnwind_ValidationOK_ReachesSubOps pins that an aligned
// Provider with a non-nil Tx passes the precondition guards and
// proceeds into the sub-op chain. The minimum-shape Provider here
// (no Inventory, no BlockReader) makes snapshot-trim a no-op and
// fails fast inside ensureCommitmentAtBlock with the BlockReader-nil
// check — that's the next step in the chain, exactly what we want
// to pin without standing up a real harness. The full happy path
// lands with the commit-3 scenario-3 E2E test against a real
// snapshot fixture.
func TestProviderUnwind_ValidationOK_ReachesSubOps(t *testing.T) {
	t.Parallel()
	p := alignedProvider()
	err := p.Unwind(context.Background(), 1000, UnwindOpts{Tx: &stubRwTx{}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "commitment-anchor")
	require.Contains(t, err.Error(), "nil BlockReader",
		"reaching commitment-anchor proves the sub-op chain is wired in; the BlockReader-nil error is the expected next-step failure on this minimum-shape fixture")
}
