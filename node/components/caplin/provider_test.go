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

package caplin

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/flow"
)

type stubRestarter struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (s *stubRestarter) Restart() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.err
}

func (s *stubRestarter) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// TestUnwindCompletedTriggersRestart pins the happy path: an
// UnwindCompleted event on the bus invokes Restarter.Restart exactly
// once.
func TestUnwindCompletedTriggersRestart(t *testing.T) {
	r := &stubRestarter{}
	p := NewProvider(log.New())
	p.SetRestarter(r)
	bus := event.NewEventBus(nil)
	require.NoError(t, p.BindBus(bus))

	bus.Publish(flow.UnwindCompleted{ToBlock: 2_954_363, TipBlock: 2_984_363})
	bus.WaitAsync()

	require.Equal(t, 1, r.Calls(), "Restart must fire exactly once per UnwindCompleted")
}

// TestUnwindCompletedNoRestarterIsFatalLogNotPanic guards the wiring-error
// case: a Restarter that isn't set means the component logs the failure
// but doesn't panic the storage-bus dispatcher.
func TestUnwindCompletedNoRestarterIsFatalLogNotPanic(t *testing.T) {
	p := NewProvider(log.New()) // no SetRestarter call
	bus := event.NewEventBus(nil)
	require.NoError(t, p.BindBus(bus))

	require.NotPanics(t, func() {
		bus.Publish(flow.UnwindCompleted{ToBlock: 1, TipBlock: 2})
		bus.WaitAsync()
	})
}

// TestUnwindCompletedRestartErrorIsLogged: Restart returning an error
// must NOT propagate to the bus dispatcher; the component logs and
// returns. (Future iterations may add retry / dead-letter handling.)
func TestUnwindCompletedRestartErrorIsLogged(t *testing.T) {
	r := &stubRestarter{err: errors.New("teardown timeout")}
	p := NewProvider(log.New())
	p.SetRestarter(r)
	bus := event.NewEventBus(nil)
	require.NoError(t, p.BindBus(bus))

	require.NotPanics(t, func() {
		bus.Publish(flow.UnwindCompleted{ToBlock: 1, TipBlock: 2})
		bus.WaitAsync()
	})
	require.Equal(t, 1, r.Calls(), "Restart must be attempted even when it errors")
}

// TestBindBusErrorsOnDoubleBind: a second BindBus call after a
// successful subscribe is a programming error and must surface.
func TestBindBusErrorsOnDoubleBind(t *testing.T) {
	p := NewProvider(log.New())
	bus := event.NewEventBus(nil)
	require.NoError(t, p.BindBus(bus))
	err := p.BindBus(bus)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already bound")
}

// TestBindBusErrorsOnNilBus: nil bus is a wiring error.
func TestBindBusErrorsOnNilBus(t *testing.T) {
	p := NewProvider(log.New())
	err := p.BindBus(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil bus")
}
