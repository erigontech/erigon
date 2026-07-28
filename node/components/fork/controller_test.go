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
