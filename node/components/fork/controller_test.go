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

package fork

import (
	"context"
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// fakeRuntime is a Runtime that records what the Controller did and
// answers the getters from injected fields. ChainDB is nil — tests
// that would exercise readCurrentBlockNumber via ChainDB fail early
// on the parent-relationship validation instead, keeping the tests
// pure-function.
type fakeRuntime struct {
	current       *chain.Config
	dataDir       string
	swapped       *chain.Config
	hooksRan      int
	restartables  map[string]rpchelper.ChainConfigRestartable
	reconfigs     map[string]rpchelper.ChainConfigReconfigurable
	setHeadCalled bool
	logger        log.Logger
}

func (f *fakeRuntime) ChainDB() kv.RwDB                      { return nil }
func (f *fakeRuntime) CurrentChainConfig() *chain.Config     { return f.current }
func (f *fakeRuntime) DataDir() string                       { return f.dataDir }
func (f *fakeRuntime) SetHead(context.Context, uint64) error { f.setHeadCalled = true; return nil }
func (f *fakeRuntime) SwapChainConfig(t *chain.Config)       { f.swapped = t }
func (f *fakeRuntime) ApplyPostSwapHooks(*chain.Config)      { f.hooksRan++ }
func (f *fakeRuntime) Logger() log.Logger                    { return f.logger }
func (f *fakeRuntime) Restartables() map[string]rpchelper.ChainConfigRestartable {
	return f.restartables
}
func (f *fakeRuntime) Reconfigurables() map[string]rpchelper.ChainConfigReconfigurable {
	return f.reconfigs
}

// TestTransition_RejectsEmptyTarget covers input validation.
func TestTransition_RejectsEmptyTarget(t *testing.T) {
	c := New(&fakeRuntime{
		current: &chain.Config{ChainName: "hoodi", ChainID: uint256.NewInt(560048)},
		logger:  log.Root(),
	})
	_, err := c.Transition(context.Background(), "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "targetChainName is required")
}

// TestTransition_RejectsSelfTarget catches "transition to current
// chain" as a no-op error rather than doing pointless work.
func TestTransition_RejectsSelfTarget(t *testing.T) {
	c := New(&fakeRuntime{
		current: &chain.Config{ChainName: "hoodi", ChainID: uint256.NewInt(560048)},
		logger:  log.Root(),
	})
	_, err := c.Transition(context.Background(), "hoodi")
	require.Error(t, err)
	require.Contains(t, err.Error(), "currently-loaded chain")
}

// TestTransition_RejectsUnknownTarget: chainspec.ChainSpecByName lookup
// fails when the target isn't in the compiled-in registry and no
// fork.toml is on disk for it.
func TestTransition_RejectsUnknownTarget(t *testing.T) {
	c := New(&fakeRuntime{
		current: &chain.Config{ChainName: "hoodi", ChainID: uint256.NewInt(560048)},
		dataDir: t.TempDir(),
		logger:  log.Root(),
	})
	_, err := c.Transition(context.Background(), "no-such-chain")
	require.Error(t, err)
}

// mockRestartable tracks Stop/SetChainConfig/Start calls so tests can
// assert the Controller's sequence.
type mockRestartable struct {
	stopped, started int
	setCfg           *chain.Config
	stopErr          error
	startErr         error
}

func (m *mockRestartable) Stop() error { m.stopped++; return m.stopErr }
func (m *mockRestartable) SetChainConfig(c *chain.Config) {
	m.setCfg = c
}
func (m *mockRestartable) Start(context.Context) error { m.started++; return m.startErr }

// mockReconfigurable records the last Reconfigure call.
type mockReconfigurable struct {
	rcCount int
	rcCfg   *chain.Config
	rcErr   error
}

func (m *mockReconfigurable) Reconfigure(_ context.Context, c *chain.Config) error {
	m.rcCount++
	m.rcCfg = c
	return m.rcErr
}

// TestApplyChainConfigSwap_HappyPath drives the internal swap
// directly (bypassing Transition's chainspec lookup) so we can assert
// Stop-then-Reconfigure-then-SetChainConfig-then-Start ordering plus
// post-swap hook invocation on the mock captors.
func TestApplyChainConfigSwap_HappyPath(t *testing.T) {
	newCfg := &chain.Config{ChainName: "hoodi-fork-42", ChainID: uint256.NewInt(9999999)}
	stor, sen, cap_ := &mockRestartable{}, &mockRestartable{}, &mockRestartable{}
	txp, dl, mx := &mockReconfigurable{}, &mockReconfigurable{}, &mockReconfigurable{}

	rt := &fakeRuntime{
		current: &chain.Config{ChainName: "hoodi", ChainID: uint256.NewInt(560048)},
		logger:  log.Root(),
		restartables: map[string]rpchelper.ChainConfigRestartable{
			"storage": stor, "sentry": sen, "caplin": cap_,
		},
		reconfigs: map[string]rpchelper.ChainConfigReconfigurable{
			"txpool": txp, "downloader": dl, "manifest_exchange": mx,
		},
	}
	c := New(rt)

	restartRequired, err := c.applyChainConfigSwap(context.Background(), newCfg)
	require.NoError(t, err)
	require.False(t, restartRequired)

	for _, m := range []*mockRestartable{stor, sen, cap_} {
		require.Equal(t, 1, m.stopped, "each Restartable must be Stopped once")
		require.Equal(t, 1, m.started, "each Restartable must be Started once")
		require.Same(t, newCfg, m.setCfg, "each Restartable must see the new chain.Config via SetChainConfig")
	}
	for _, m := range []*mockReconfigurable{txp, dl, mx} {
		require.Equal(t, 1, m.rcCount, "each Reconfigurable must be Reconfigured once")
		require.Same(t, newCfg, m.rcCfg)
	}
	require.Same(t, newCfg, rt.swapped, "runtime SwapChainConfig must fire with the new pointer")
	require.Equal(t, 1, rt.hooksRan, "ApplyPostSwapHooks must fire once")
}

// TestApplyChainConfigSwap_StartFailureSurfacesRestartRequired: when
// a post-swap Start errors, the swap has already been applied — the
// process is in a partial state and the caller must restart erigon.
func TestApplyChainConfigSwap_StartFailureSurfacesRestartRequired(t *testing.T) {
	newCfg := &chain.Config{ChainName: "hoodi-fork-42", ChainID: uint256.NewInt(9999999)}
	failing := &mockRestartable{startErr: errors.New("boom")}
	rt := &fakeRuntime{
		current:      &chain.Config{ChainName: "hoodi", ChainID: uint256.NewInt(560048)},
		logger:       log.Root(),
		restartables: map[string]rpchelper.ChainConfigRestartable{"storage": failing},
		reconfigs:    map[string]rpchelper.ChainConfigReconfigurable{},
	}
	c := New(rt)

	restartRequired, err := c.applyChainConfigSwap(context.Background(), newCfg)
	require.True(t, restartRequired)
	require.Error(t, err)
	require.Contains(t, err.Error(), "storage")
}

// TestApplyChainConfigSwap_ReconfigureFailureAbortsSwap: a
// Reconfigurable that errors during phase 2 must abort the swap
// before SwapChainConfig fires. Restartables that were Stopped in
// phase 1 stay stopped (partial state — caller sees
// RestartRequired=true and does a process restart); no captor gets
// SetChainConfig, no post-swap hook runs, and no Start fires.
func TestApplyChainConfigSwap_ReconfigureFailureAbortsSwap(t *testing.T) {
	newCfg := &chain.Config{ChainName: "hoodi-fork-42", ChainID: uint256.NewInt(9999999)}
	stor := &mockRestartable{}
	failingReconfig := &mockReconfigurable{rcErr: errors.New("kaboom")}
	rt := &fakeRuntime{
		current:      &chain.Config{ChainName: "hoodi", ChainID: uint256.NewInt(560048)},
		logger:       log.Root(),
		restartables: map[string]rpchelper.ChainConfigRestartable{"storage": stor},
		reconfigs:    map[string]rpchelper.ChainConfigReconfigurable{"txpool": failingReconfig},
	}
	c := New(rt)

	restartRequired, err := c.applyChainConfigSwap(context.Background(), newCfg)
	require.True(t, restartRequired)
	require.Error(t, err)
	require.Contains(t, err.Error(), "txpool")

	require.Equal(t, 1, stor.stopped, "Restartable should already be Stopped when reconfigure fails")
	require.Nil(t, stor.setCfg, "no SetChainConfig should fire after reconfigure failure")
	require.Equal(t, 0, stor.started, "no Start should fire after reconfigure failure")
	require.Nil(t, rt.swapped, "SwapChainConfig must not fire when reconfigure fails")
	require.Equal(t, 0, rt.hooksRan, "ApplyPostSwapHooks must not fire when reconfigure fails")
}

// TestApplyChainConfigSwap_StopFailureAbortsBeforeSwap: a Restartable
// whose Stop errors must abort before the chain.Config pointer moves.
// Reconfigurables must not be touched, SwapChainConfig must not fire,
// no post-swap hooks. Partial-Stop state across other Restartables
// is caller-visible via RestartRequired=true.
func TestApplyChainConfigSwap_StopFailureAbortsBeforeSwap(t *testing.T) {
	newCfg := &chain.Config{ChainName: "hoodi-fork-42", ChainID: uint256.NewInt(9999999)}
	failing := &mockRestartable{stopErr: errors.New("stop-boom")}
	txp := &mockReconfigurable{}
	rt := &fakeRuntime{
		current:      &chain.Config{ChainName: "hoodi", ChainID: uint256.NewInt(560048)},
		logger:       log.Root(),
		restartables: map[string]rpchelper.ChainConfigRestartable{"storage": failing},
		reconfigs:    map[string]rpchelper.ChainConfigReconfigurable{"txpool": txp},
	}
	c := New(rt)

	restartRequired, err := c.applyChainConfigSwap(context.Background(), newCfg)
	require.True(t, restartRequired)
	require.Error(t, err)
	require.Contains(t, err.Error(), "storage")

	require.Equal(t, 0, txp.rcCount, "Reconfigurables must not be touched when a Stop errors")
	require.Nil(t, rt.swapped, "SwapChainConfig must not fire when Stop errors")
	require.Equal(t, 0, rt.hooksRan, "ApplyPostSwapHooks must not fire when Stop errors")
	require.Equal(t, 0, failing.started, "no Start attempted on a Stop-failure abort")
}

// TestApplyChainConfigSwap_SequenceRobustness: three back-to-back
// successful swaps against the same Runtime + captor set. Each
// captor's counters advance monotonically; the runtime's chain.Config
// tracks the latest target. Catches state leaks between Transitions
// (e.g. a captor left in a stopped state after the first cycle would
// have started != 3 after three cycles).
func TestApplyChainConfigSwap_SequenceRobustness(t *testing.T) {
	stor, sen, cap_ := &mockRestartable{}, &mockRestartable{}, &mockRestartable{}
	txp := &mockReconfigurable{}
	rt := &fakeRuntime{
		current: &chain.Config{ChainName: "hoodi", ChainID: uint256.NewInt(560048)},
		logger:  log.Root(),
		restartables: map[string]rpchelper.ChainConfigRestartable{
			"storage": stor, "sentry": sen, "caplin": cap_,
		},
		reconfigs: map[string]rpchelper.ChainConfigReconfigurable{"txpool": txp},
	}
	c := New(rt)

	targets := []*chain.Config{
		{ChainName: "a", ChainID: uint256.NewInt(1)},
		{ChainName: "b", ChainID: uint256.NewInt(2)},
		{ChainName: "c", ChainID: uint256.NewInt(3)},
	}
	for i, tgt := range targets {
		restartRequired, err := c.applyChainConfigSwap(context.Background(), tgt)
		require.NoError(t, err, "iter %d", i)
		require.False(t, restartRequired, "iter %d", i)
	}

	for _, m := range []*mockRestartable{stor, sen, cap_} {
		require.Equal(t, 3, m.stopped, "each Restartable must be Stopped exactly per iter")
		require.Equal(t, 3, m.started, "each Restartable must be Started exactly per iter")
		require.Same(t, targets[2], m.setCfg, "last SetChainConfig wins")
	}
	require.Equal(t, 3, txp.rcCount)
	require.Same(t, targets[2], rt.swapped)
	require.Equal(t, 3, rt.hooksRan)
}
