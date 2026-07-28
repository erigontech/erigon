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
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/lifecycle"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

var _ rpchelper.ChainConfigRestartable = (*Provider)(nil)

// TestRestartCycle_LifecycleDriverResumes verifies the Stop → Start
// cycle actually leaves the LifecycleDriver running again — Stop halts
// its sweep loop; Start resumes it on the existing instance.
func TestRestartCycle_LifecycleDriverResumes(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	inv := snapshot.NewInventory()
	driver := &lifecycle.Driver{
		Inv: inv, SnapDir: dir, SweepInterval: 24 * time.Hour,
		Logger: log.Root(),
	}
	require.NoError(t, driver.Start(ctx))

	p := &Provider{
		LifecycleDriver: driver,
		Inventory:       inv,
		eventBus:        event.NewEventBus(nil),
		logger:          log.Root(),
		started:         true,
	}
	t.Cleanup(func() { p.Close() })

	require.NoError(t, p.Stop())
	require.False(t, p.started)

	require.NoError(t, p.Start(ctx))
	require.True(t, p.started)

	// Idempotent — second Start / Stop must not misbehave.
	require.NoError(t, p.Start(ctx))
	require.NoError(t, p.Stop())
	require.NoError(t, p.Stop())
}

// TestSetChainConfig_SwapsPointer verifies the field-level swap and the
// BlockRetire rebuild trigger. The BlockReader/BlockWriter/etc. are all
// nil in this scaffolded Provider — restartDeps.config is enough to
// exercise the NewBlockRetire codepath and confirm p.BlockRetire changes.
func TestSetChainConfig_SwapsPointer(t *testing.T) {
	oldCfg := &chain.Config{ChainID: uint256.NewInt(560048)}
	newCfg := &chain.Config{ChainID: uint256.NewInt(9999999)}

	p := &Provider{
		ChainConfig: oldCfg,
		logger:      log.Root(),
	}
	p.SetChainConfig(newCfg)
	require.Same(t, newCfg, p.ChainConfig)
	require.Nil(t, p.BlockRetire, "no restartDeps → BlockRetire rebuild skipped")

	p.ChainConfig = oldCfg
	p.restartDeps = &storageRestartDeps{
		config: &ethconfig.Config{},
	}
	p.SetChainConfig(newCfg)
	require.Same(t, newCfg, p.ChainConfig)
	require.NotNil(t, p.BlockRetire, "restartDeps set → BlockRetire rebuilt")
}

// TestSetChainConfig_PanicsWhileStarted enforces the invariant: swapping
// chain.Config while the LifecycleDriver + Orchestrator are running
// would race against goroutines already reading the old pointer.
func TestSetChainConfig_PanicsWhileStarted(t *testing.T) {
	p := &Provider{
		ChainConfig: &chain.Config{ChainID: uint256.NewInt(1)},
		started:     true,
	}
	require.Panics(t, func() {
		p.SetChainConfig(&chain.Config{ChainID: uint256.NewInt(2)})
	})
}

// TestStop_NoopWhenNotStarted covers the guard so a bare Provider (never
// Initialize'd — a common test-scaffold shape) tolerates Stop without
// nil-derefing its uninitialised fields.
func TestStop_NoopWhenNotStarted(t *testing.T) {
	p := &Provider{}
	require.NoError(t, p.Stop())
}
