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

	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
)

// stubRwDB is a non-nil kv.TemporalRwDB stub used to satisfy the
// non-nil-DB precondition in dispatch-shape tests. The methods are
// never actually called because every test exercises a code path
// that returns before touching the DB (deferred-support errors,
// unknown-check rejection, name attribution). Embedding the
// interface gives us a no-op implementation that compiles without
// having to stub out the dozens of methods kv.TemporalRwDB requires.
type stubRwDB struct{ kv.TemporalRwDB }

// These tests cover the dispatch shape of IntegrityBridge — name
// attribution, nil-DB guard, and the deferred-support error class
// for checks the bridge cannot yet wire (Publishable, Bor*,
// StateRootVerifyByHistory). They DO NOT attempt to run a real
// integrity check against a real TemporalDB; that's an integration
// test that lands when storage.Provider's canonical Inventory and
// the post-batch hook arrive together.

func TestIntegrityBridge_NameMatchesCheck(t *testing.T) {
	cases := []integrity.Check{
		integrity.Blocks,
		integrity.HeaderNoGaps,
		integrity.InvertedIndex,
		integrity.CommitmentKvi,
		integrity.Publishable,
		integrity.StateVerify,
	}
	for _, c := range cases {
		b := &IntegrityBridge{Check: c}
		require.Equal(t, string(c), b.Name(),
			"Name() must equal the underlying integrity.Check string for log + error attribution")
	}
}

func TestIntegrityBridge_NilDBRejected(t *testing.T) {
	b := &IntegrityBridge{Check: integrity.Blocks /* DB intentionally nil */}
	err := b.ValidateBatch(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil DB")
	require.Contains(t, err.Error(), "Blocks")
}

func TestIntegrityBridge_PublishableNotYetSupported(t *testing.T) {
	// Publishable currently calls a CLI-private doPublishable; the
	// bridge returns the structured deferred-support error so
	// callers know to dispatch via the CLI helper or wait for the
	// export.
	b := &IntegrityBridge{Check: integrity.Publishable, DB: stubRwDB{}}
	err := b.ValidateBatch(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "Publishable")
	require.Contains(t, err.Error(), "not yet exposed")
}

func TestIntegrityBridge_BorChecksNotYetSupported(t *testing.T) {
	for _, c := range []integrity.Check{
		integrity.BorEvents, integrity.BorSpans, integrity.BorCheckpoints,
	} {
		b := &IntegrityBridge{Check: c, DB: stubRwDB{}}
		err := b.ValidateBatch(context.Background())
		require.Error(t, err, "Bor check %s should be deferred-supported", c)
		require.Contains(t, err.Error(), "Bor")
		require.Contains(t, err.Error(), "bor-store")
	}
}

func TestIntegrityBridge_StateRootVerifyByHistoryNotYetSupported(t *testing.T) {
	b := &IntegrityBridge{Check: integrity.StateRootVerifyByHistory, DB: stubRwDB{}}
	err := b.ValidateBatch(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "StateRootVerifyByHistory")
	require.Contains(t, err.Error(), "stateProgress")
}

func TestIntegrityBridge_UnknownCheckRejected(t *testing.T) {
	b := &IntegrityBridge{Check: integrity.Check("MadeUpCheck"), DB: stubRwDB{}}
	err := b.ValidateBatch(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown integrity.Check")
	require.Contains(t, err.Error(), "MadeUpCheck")
}

func TestNewIntegrityChain_AssemblesBridges(t *testing.T) {
	checks := []integrity.Check{
		integrity.Blocks,
		integrity.InvertedIndex,
		integrity.CommitmentKvi,
	}
	chain := NewIntegrityChain(
		checks,
		stubRwDB{},
		nil,                    // BlockReader
		nil,                    // Cache
		integrity.SamplerCfg{}, // SamplerCfg
		true,                   // FailFast
		0,                      // FromStep
		nil,                    // Logger
	)
	require.Len(t, chain, 3, "one bridge per check")
	for i, want := range checks {
		require.Equal(t, string(want), chain[i].Name(),
			"chain entry %d should be the bridge for %s", i, want)
	}
}

func TestNewIntegrityChain_DeferredChecksIncludedRaw(t *testing.T) {
	// Including a deferred-support check in the chain is fine; its
	// ValidateBatch returns the structured deferred-support error
	// when called. Callers filter or handle as they prefer.
	chain := NewIntegrityChain(
		[]integrity.Check{integrity.Publishable},
		stubRwDB{}, nil, nil, integrity.SamplerCfg{}, true, 0, nil,
	)
	require.Len(t, chain, 1)
	err := chain.Validate(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "Publishable")
	require.Contains(t, err.Error(), "not yet exposed")
}
