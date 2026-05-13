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

package native

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// newTestMuxTracer is a test helper that constructs a muxTracer from
// pre-built child tracers, bypassing the JSON-based registry lookup
// used by newMuxTracer. This mirrors geth's NewMuxTracer constructor
// which accepts names and tracer slices directly.
func newTestMuxTracer(names []string, children []*tracers.Tracer) *tracers.Tracer {
	return (&muxTracer{names: names, tracers: children}).tracer()
}

// TestMuxForwardsV2StateHooks verifies that the mux tracer fans out the V2
// variants of state-change hooks to child tracers.
//
// A child tracer that only implements OnCodeChangeV2 / OnNonceChangeV2 must
// still receive events when wrapped behind the mux. The mux must also fall
// back to the V1 hook when a child only implements V1, mirroring the
// precedence used in execution/state/state_object.go.
func TestMuxForwardsV2StateHooks(t *testing.T) {
	var (
		codeV2Calls  int
		nonceV2Calls int
		codeV1Calls  int
		nonceV1Calls int
	)

	v2Child := &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnCodeChangeV2: func(_ accounts.Address, _ accounts.CodeHash, _ []byte, _ accounts.CodeHash, _ []byte, _ tracing.CodeChangeReason) {
				codeV2Calls++
			},
			OnNonceChangeV2: func(_ accounts.Address, _, _ uint64, _ tracing.NonceChangeReason) {
				nonceV2Calls++
			},
		},
	}

	v1Child := &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnCodeChange: func(_ accounts.Address, _ accounts.CodeHash, _ []byte, _ accounts.CodeHash, _ []byte) {
				codeV1Calls++
			},
			OnNonceChange: func(_ accounts.Address, _, _ uint64) {
				nonceV1Calls++
			},
		},
	}

	mux := newTestMuxTracer([]string{"v2", "v1"}, []*tracers.Tracer{v2Child, v1Child})

	// The mux must expose V2 hooks so the state processor can invoke them.
	require.NotNil(t, mux.OnCodeChangeV2, "mux does not expose OnCodeChangeV2; V2-only child tracers will miss code changes")
	require.NotNil(t, mux.OnNonceChangeV2, "mux does not expose OnNonceChangeV2; V2-only child tracers will miss nonce changes")

	// Fire the V2 hooks on the mux.
	mux.OnCodeChangeV2(accounts.Address{}, accounts.CodeHash{}, nil, accounts.CodeHash{}, nil, tracing.CodeChangeContractCreation)
	mux.OnNonceChangeV2(accounts.Address{}, 0, 1, tracing.NonceChangeEoACall)

	// V2 child must have received the V2 call.
	require.Equal(t, 1, codeV2Calls, "V2 child OnCodeChangeV2 call count")
	require.Equal(t, 1, nonceV2Calls, "V2 child OnNonceChangeV2 call count")

	// V1 child must have received the fallback from V2 → V1.
	require.Equal(t, 1, codeV1Calls, "V1 child OnCodeChange call count (mux should fall back from V2 to V1)")
	require.Equal(t, 1, nonceV1Calls, "V1 child OnNonceChange call count (mux should fall back from V2 to V1)")
}

// TestMuxForwardsSystemCallV2 verifies that the mux tracer fans out the
// OnSystemCallStartV2 hook, falling back to OnSystemCallStart for children
// that only implement the V1 variant.
func TestMuxForwardsSystemCallV2(t *testing.T) {
	var (
		sysV2Calls int
		sysV1Calls int
	)

	v2Child := &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnSystemCallStartV2: func(_ *tracing.VMContext) {
				sysV2Calls++
			},
		},
	}

	v1Child := &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnSystemCallStart: func() {
				sysV1Calls++
			},
		},
	}

	mux := newTestMuxTracer([]string{"v2", "v1"}, []*tracers.Tracer{v2Child, v1Child})

	require.NotNil(t, mux.OnSystemCallStartV2, "mux does not expose OnSystemCallStartV2")

	mux.OnSystemCallStartV2(nil)

	require.Equal(t, 1, sysV2Calls, "V2 child OnSystemCallStartV2 call count")
	require.Equal(t, 1, sysV1Calls, "V1 child OnSystemCallStart call count (mux should fall back from V2 to V1)")
}

// TestMuxEmptyChildHookFunctionsDoNotPanic verifies that the mux gracefully
// skips children whose individual hook function fields are nil (i.e., a tracer
// that exists but doesn't implement every hook), without panicking.
func TestMuxEmptyChildHookFunctionsDoNotPanic(t *testing.T) {
	// A child with a non-nil Hooks struct but no hook functions set.
	emptyChild := &tracers.Tracer{
		Hooks: &tracing.Hooks{},
	}

	mux := newTestMuxTracer([]string{"empty"}, []*tracers.Tracer{emptyChild})

	// None of these should panic.
	require.NotPanics(t, func() {
		mux.OnCodeChangeV2(accounts.Address{}, accounts.CodeHash{}, nil, accounts.CodeHash{}, nil, tracing.CodeChangeContractCreation)
	}, "OnCodeChangeV2 with nil child hook must not panic")

	require.NotPanics(t, func() {
		mux.OnNonceChangeV2(accounts.Address{}, 0, 1, tracing.NonceChangeEoACall)
	}, "OnNonceChangeV2 with nil child hook must not panic")

	require.NotPanics(t, func() {
		mux.OnSystemCallStartV2(nil)
	}, "OnSystemCallStartV2 with nil child hook must not panic")
}
