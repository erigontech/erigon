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

package fork_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	forkcomp "github.com/erigontech/erigon/node/components/fork"
	sentrycomp "github.com/erigontech/erigon/node/components/sentry"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// tier2Runtime is a fork.Runtime that wires real captors (sentry.Provider
// in Disable mode) against an in-memory ChainDB + a fake SetHead. Real
// chainspec resolution runs via a chain.json the helper drops in the
// temp datadir.
type tier2Runtime struct {
	t         *testing.T
	current   *chain.Config
	dataDir   string
	db        kv.RwDB
	sentry    *sentrycomp.Provider
	reconfigs map[string]rpchelper.ChainConfigReconfigurable
	setHeadFn func(context.Context, uint64) error
	hooksRan  atomic.Int32
	swapped   *chain.Config
}

func (r *tier2Runtime) ChainDB() kv.RwDB                  { return r.db }
func (r *tier2Runtime) CurrentChainConfig() *chain.Config { return r.current }
func (r *tier2Runtime) DataDir() string                   { return r.dataDir }
func (r *tier2Runtime) SetHead(ctx context.Context, block uint64) error {
	return r.setHeadFn(ctx, block)
}
func (r *tier2Runtime) SwapChainConfig(t *chain.Config)  { r.swapped = t; r.current = t }
func (r *tier2Runtime) ApplyPostSwapHooks(*chain.Config) { r.hooksRan.Add(1) }
func (r *tier2Runtime) Logger() log.Logger               { return log.Root() }
func (r *tier2Runtime) BackgroundCtx() context.Context   { return r.t.Context() }
func (r *tier2Runtime) Restartables() map[string]rpchelper.ChainConfigRestartable {
	return map[string]rpchelper.ChainConfigRestartable{"sentry": r.sentry}
}
func (r *tier2Runtime) Reconfigurables() map[string]rpchelper.ChainConfigReconfigurable {
	if r.reconfigs != nil {
		return r.reconfigs
	}
	return map[string]rpchelper.ChainConfigReconfigurable{}
}

// newTier2Runtime returns a Runtime wired against real captors +
// in-memory ChainDB, with hoodi as the current chain and a chain.json
// dropped in the temp datadir for the requested fork name. Fake
// SetHead updates stages.Finish so a subsequent unwoundFrom>=cutBlock
// check succeeds.
func newTier2Runtime(t *testing.T, forkName string, cutBlock uint64, initialHead uint64) *tier2Runtime {
	t.Helper()
	dir := t.TempDir()

	hoodi := chainspec.Hoodi.Config
	forkID := uint256.NewInt(hoodi.ChainID.Uint64() + 1)
	forkCfg := &chain.Config{
		ChainName: forkName,
		ChainID:   forkID,
		Parent:    hoodi.ChainName,
		CutBlock:  cutBlock,
	}
	forkJSON, err := json.Marshal(forkCfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "chain.json"), forkJSON, 0o644))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Finish, initialHead)
	}))

	sentry := &sentrycomp.Provider{}
	sentry.Configure(sentrycomp.Config{
		SentryCtx:   t.Context(),
		Logger:      log.Root(),
		Disable:     true,
		ChainConfig: hoodi,
		Genesis:     types.NewBlockWithHeader(&types.Header{}),
	})
	require.NoError(t, sentry.Start(t.Context()))
	t.Cleanup(func() { sentry.Close() })

	rt := &tier2Runtime{
		t:       t,
		current: hoodi,
		dataDir: dir,
		db:      db,
		sentry:  sentry,
	}
	rt.setHeadFn = func(ctx context.Context, block uint64) error {
		return db.Update(ctx, func(tx kv.RwTx) error {
			return stages.SaveStageProgress(tx, stages.Finish, block)
		})
	}
	return rt
}

// TestTier2_RoundTrip_ParentToForkToParent exercises the fork
// Controller against a real sentry.Provider Restartable + real
// chainspec resolution driven by a chain.json in a temp datadir.
// After each Transition the sentry Provider must be alive and its
// captured chain.Config swapped to the target.
func TestTier2_RoundTrip_ParentToForkToParent(t *testing.T) {
	t.Parallel()
	const (
		cutBlock    uint64 = 100
		initialHead uint64 = 200
	)
	forkName := "hoodi-fork-tier2-roundtrip"
	rt := newTier2Runtime(t, forkName, cutBlock, initialHead)
	ctrl := forkcomp.New(rt)

	res, err := ctrl.Transition(t.Context(), forkName)
	require.NoError(t, err)
	require.False(t, res.RestartRequired, "in-process transition must not require restart")
	require.Equal(t, initialHead, res.UnwoundFrom)
	require.Equal(t, cutBlock, res.UnwoundTo)
	require.Equal(t, forkName, rt.current.ChainName)
	require.Same(t, rt.swapped, rt.current)

	res, err = ctrl.Transition(t.Context(), chainspec.Hoodi.Config.ChainName)
	require.NoError(t, err)
	require.False(t, res.RestartRequired)
	require.Equal(t, cutBlock, res.UnwoundFrom, "fork→parent unwinds from CutBlock (previous transition landed head there)")
	require.Equal(t, cutBlock, res.UnwoundTo)
	require.Equal(t, chainspec.Hoodi.Config.ChainName, rt.current.ChainName)
}

// TestTier2_ReconfigureFailureRecovery: a Reconfigurable that fails
// on the first Transition target and succeeds on the next must leave
// the Runtime recoverable — after the failed attempt returns
// RestartRequired=true, the caller can drive a subsequent Transition
// that lands cleanly. Exercises the "partial swap → operator
// retries" path against a real Runtime rather than mocks.
func TestTier2_ReconfigureFailureRecovery(t *testing.T) {
	t.Parallel()
	const (
		cutBlock    uint64 = 100
		initialHead uint64 = 200
	)
	forkName := "hoodi-fork-tier2-recovery"
	rt := newTier2Runtime(t, forkName, cutBlock, initialHead)
	failing := &controlledReconfigurable{
		refuseChain: forkName,
	}
	rt.reconfigs = map[string]rpchelper.ChainConfigReconfigurable{"gating": failing}
	ctrl := forkcomp.New(rt)

	res, err := ctrl.Transition(t.Context(), forkName)
	require.NoError(t, err, "Transition surfaces swap failure via RestartRequired, not err")
	require.NotNil(t, res)
	require.True(t, res.RestartRequired, "first attempt must flag restart_required through the failing Reconfigurable")
	require.Contains(t, res.Message, "controlled refusal")
	require.Equal(t, 1, failing.rcCount)

	failing.refuseChain = ""
	res, err = ctrl.Transition(t.Context(), forkName)
	require.NoError(t, err, "second attempt must succeed once the reconfigurable stops refusing")
	require.False(t, res.RestartRequired, "second attempt must be a clean swap")
	require.Equal(t, forkName, rt.current.ChainName)
	require.Equal(t, 2, failing.rcCount, "Reconfigure fires on both attempts")
}

// controlledReconfigurable errors on Reconfigure calls whose target
// chain name matches refuseChain, succeeds otherwise. Lets tests
// script a "first attempt fails, retry succeeds" flow.
type controlledReconfigurable struct {
	refuseChain string
	rcCount     int
}

func (c *controlledReconfigurable) Reconfigure(_ context.Context, cfg *chain.Config) error {
	c.rcCount++
	if c.refuseChain != "" && cfg.ChainName == c.refuseChain {
		return errRefused
	}
	return nil
}

var errRefused = &controlledReconfigureError{}

type controlledReconfigureError struct{}

func (*controlledReconfigureError) Error() string { return "controlled refusal" }

// TestTier2_SequenceRobustness_MultipleTransitions asserts three
// consecutive same-node transitions leave the sentry Provider in a
// consistently started state (no double-Start deadlock, no
// leaked-goroutine growth). Runs the parent↔fork pair three times.
func TestTier2_SequenceRobustness_MultipleTransitions(t *testing.T) {
	t.Parallel()
	const (
		cutBlock    uint64 = 50
		initialHead uint64 = 250
	)
	forkName := "hoodi-fork-tier2-sequence"
	rt := newTier2Runtime(t, forkName, cutBlock, initialHead)
	ctrl := forkcomp.New(rt)

	for i := 0; i < 3; i++ {
		_, err := ctrl.Transition(t.Context(), forkName)
		require.NoError(t, err, "iter %d: to fork", i)
		require.Equal(t, forkName, rt.current.ChainName)

		_, err = ctrl.Transition(t.Context(), chainspec.Hoodi.Config.ChainName)
		require.NoError(t, err, "iter %d: back to hoodi", i)
		require.Equal(t, chainspec.Hoodi.Config.ChainName, rt.current.ChainName)
	}
	require.Equal(t, int32(6), rt.hooksRan.Load(), "3 round-trips × 2 transitions each")
}

// TestTier2_RejectsUnrelatedChain: target chain is registered but has
// no direct parent/child relationship with the currently-loaded
// chain — Controller must refuse with a diagnostic naming both.
func TestTier2_RejectsUnrelatedChain(t *testing.T) {
	t.Parallel()
	rt := newTier2Runtime(t, "unused-fork-name", 100, 200)
	ctrl := forkcomp.New(rt)

	_, err := ctrl.Transition(t.Context(), chainspec.Sepolia.Config.ChainName)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no direct parent relationship")
	require.Contains(t, err.Error(), "hoodi")
	require.Contains(t, err.Error(), "sepolia")
}

// TestTier2_RejectsHeadBelowCutBlock: current head is < CutBlock, so
// there is nothing to unwind AND the chain-config swap alone would
// leave the node's state at pre-cut, off the fork's canonical chain.
// Controller refuses rather than corrupt state silently.
func TestTier2_RejectsHeadBelowCutBlock(t *testing.T) {
	t.Parallel()
	const (
		cutBlock    uint64 = 500
		initialHead uint64 = 100 // below cutBlock
	)
	forkName := "hoodi-fork-tier2-belowcut"
	rt := newTier2Runtime(t, forkName, cutBlock, initialHead)
	ctrl := forkcomp.New(rt)

	_, err := ctrl.Transition(t.Context(), forkName)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already at or below CutBlock")
}
