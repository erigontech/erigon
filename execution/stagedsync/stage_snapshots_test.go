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

package stagedsync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
)

// TestBuildOrDeferE2Indices_LifecycleGateNoOps verifies that when
// SnapshotsCfg.lifecycleDrivenByStorage is set, buildOrDeferE2Indices
// returns nil without calling cfg.blockRetire — proving the storage
// component's lifecycle driver is taking over without the stage
// having to coordinate.
//
// We construct a SnapshotsCfg with a NIL blockRetire to make this
// concrete: if the gate failed, the function would dereference nil
// and panic. Passing without panic IS the assertion.
func TestBuildOrDeferE2Indices_LifecycleGateNoOps(t *testing.T) {
	cfg := SnapshotsCfg{
		chainConfig: &chain.Config{}, // non-Bor → would otherwise reach BuildMissedIndicesIfNeed
		blockRetire: nil,             // would panic if dereferenced
	}
	cfg.SetLifecycleDrivenByStorage(true)

	// Use a fake StageState; no fields read inside the gated path.
	s := &StageState{}
	require.NoError(t, buildOrDeferE2Indices(context.Background(), s, cfg, 0),
		"flag-on must short-circuit before touching blockRetire")
}

func TestBuildOrDeferE3Accessors_LifecycleGateNoOps(t *testing.T) {
	cfg := SnapshotsCfg{}
	cfg.SetLifecycleDrivenByStorage(true)

	s := &StageState{}
	// agg=nil would normally panic via agg.BuildMissedAccessors.
	require.NoError(t, buildOrDeferE3Accessors(context.Background(), s, cfg, nil, 0),
		"flag-on must short-circuit before touching agg")
}

func TestSetLifecycleDrivenByStorage_DefaultsFalse(t *testing.T) {
	cfg := SnapshotsCfg{}
	require.False(t, cfg.lifecycleDrivenByStorage,
		"default must be false — stage drives until production wires the flag on")
	cfg.SetLifecycleDrivenByStorage(true)
	require.True(t, cfg.lifecycleDrivenByStorage)
	cfg.SetLifecycleDrivenByStorage(false)
	require.False(t, cfg.lifecycleDrivenByStorage)
}
