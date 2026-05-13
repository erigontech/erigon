// Copyright 2024 The Erigon Authors
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

package misc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// noopSyscall is a no-op system call that satisfies the rules.SystemCall signature.
func noopSyscall(_ accounts.Address, _ []byte) ([]byte, error) { return nil, nil }

// TestApplyBeaconRootEip4788_V2Preferred verifies that when both
// OnSystemCallStartV2 and OnSystemCallStart are set, the V2 hook is
// called (with the provided VMContext) and the V1 hook is NOT called.
func TestApplyBeaconRootEip4788_V2Preferred(t *testing.T) {
	var receivedCtx *tracing.VMContext
	v1Called := false

	vmctx := &tracing.VMContext{BlockNumber: 42}
	root := common.Hash{0x01}

	tracer := &tracing.Hooks{
		OnSystemCallStartV2: func(vm *tracing.VMContext) {
			receivedCtx = vm
		},
		OnSystemCallStart: func() {
			v1Called = true
		},
	}

	ApplyBeaconRootEip4788(&root, noopSyscall, tracer, vmctx)

	require.Same(t, vmctx, receivedCtx,
		"V2 hook must receive the exact VMContext pointer")
	require.False(t, v1Called,
		"V1 hook must not be called when V2 is available and vmctx is non-nil")
}

// TestApplyBeaconRootEip4788_V1FallbackWhenNoV2 verifies that when
// OnSystemCallStartV2 is nil, the legacy OnSystemCallStart hook is called.
func TestApplyBeaconRootEip4788_V1FallbackWhenNoV2(t *testing.T) {
	v1Called := false
	root := common.Hash{0x01}
	vmctx := &tracing.VMContext{BlockNumber: 42}

	tracer := &tracing.Hooks{
		OnSystemCallStart: func() {
			v1Called = true
		},
	}

	ApplyBeaconRootEip4788(&root, noopSyscall, tracer, vmctx)

	require.True(t, v1Called,
		"V1 hook must be called when V2 is nil")
}

// TestApplyBeaconRootEip4788_V1FallbackWhenVMContextNil verifies the
// nil-VMContext guard: when vmctx is nil, the function must fall back to
// the V1 hook even if V2 is set, to prevent nil-pointer panics in tracers
// that dereference the context.
func TestApplyBeaconRootEip4788_V1FallbackWhenVMContextNil(t *testing.T) {
	v2Called := false
	v1Called := false
	root := common.Hash{0x01}

	tracer := &tracing.Hooks{
		OnSystemCallStartV2: func(_ *tracing.VMContext) {
			v2Called = true
		},
		OnSystemCallStart: func() {
			v1Called = true
		},
	}

	ApplyBeaconRootEip4788(&root, noopSyscall, tracer, nil)

	require.False(t, v2Called,
		"V2 hook must not be called when vmctx is nil")
	require.True(t, v1Called,
		"V1 hook must be called as fallback when vmctx is nil")
}

// TestApplyBeaconRootEip4788_OnSystemCallEndDeferred verifies that
// OnSystemCallEnd is called after the syscall completes, regardless
// of whether the start hook is V1 or V2.
func TestApplyBeaconRootEip4788_OnSystemCallEndDeferred(t *testing.T) {
	var callOrder []string
	root := common.Hash{0x01}

	tracer := &tracing.Hooks{
		OnSystemCallStart: func() {
			callOrder = append(callOrder, "start")
		},
		OnSystemCallEnd: func() {
			callOrder = append(callOrder, "end")
		},
	}

	ApplyBeaconRootEip4788(&root, noopSyscall, tracer, nil)

	require.Equal(t, []string{"start", "end"}, callOrder,
		"OnSystemCallEnd must be called after the syscall completes")
}

// TestApplyBeaconRootEip4788_NilTracerNoPanic verifies that passing a nil
// tracer (the common un-traced import path) does not panic.
func TestApplyBeaconRootEip4788_NilTracerNoPanic(t *testing.T) {
	root := common.Hash{0x01}

	require.NotPanics(t, func() {
		ApplyBeaconRootEip4788(&root, noopSyscall, nil, nil)
	})
}
