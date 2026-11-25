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
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/execution/chain"
)

// defaultTeardownTimeout bounds how long Stop/Close wait for the Caplin
// goroutine to exit, and how long Restart waits before handing the relaunch
// to a background waiter rather than hanging its caller.
const defaultTeardownTimeout = 60 * time.Second

// hardTeardownMultiple scales teardownTimeout into the bound past which a
// goroutine that has not exited is treated as hung rather than slow.
const hardTeardownMultiple = 10

// LaunchFn is the runtime closure that runs Caplin to completion. The
// service supplies the context; the closure returns when ctx is
// cancelled or Caplin exits on its own.
type LaunchFn func(ctx context.Context) error

// CaplinService owns the lifetime of the in-process Caplin goroutine.
// The CL flow-orchestrator (Provider) calls Restart on
// flow.UnwindCompleted to tear down + cold-start Caplin so the runtime
// renegotiates its anchor against the post-unwind EL head via the
// normal /finalized checkpoint-sync path. The EL's engineapi
// initialCycle + BlockCollector Case-C nudge then walks Execution
// forward through preserved snapshots until the gap closes.
type CaplinService struct {
	mu               sync.Mutex
	parentCtx        context.Context
	launch           LaunchFn
	dirs             datadir.Dirs
	logger           log.Logger
	onUnexpectedExit func()

	cancel   context.CancelFunc
	done     chan struct{}
	stopping atomic.Bool
	closed   bool

	teardownTimeout time.Duration
}

// NewCaplinService constructs the service. parentCtx is typically
// backend.sentryCtx. dirs is used by Restart to wipe the
// PersistentBlockCollector's MDBX env (dirs.CaplinHistory) — the cache
// references EL chaindata that the mode-B unwind has just deleted, and
// the new Caplin instance must NOT see those orphaned entries. If the
// Caplin goroutine exits WITHOUT a Stop or Restart having been called,
// onUnexpectedExit fires so the backend can propagate the shutdown.
func NewCaplinService(parentCtx context.Context, dirs datadir.Dirs, launch LaunchFn, onUnexpectedExit func(), logger log.Logger) *CaplinService {
	return &CaplinService{
		parentCtx:        parentCtx,
		launch:           launch,
		dirs:             dirs,
		logger:           logger,
		onUnexpectedExit: onUnexpectedExit,
		teardownTimeout:  defaultTeardownTimeout,
	}
}

// Start spawns the Caplin goroutine parented on ctx. Subsequent Start
// calls after Stop are supported: they spawn a fresh goroutine using
// the current launch closure (updated via SetLaunch if the caller
// wants a chain-config-aware relaunch).
func (s *CaplinService) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done != nil {
		return nil
	}
	s.parentCtx = ctx
	s.spawnLocked()
	return nil
}

// SetChainConfig is the ChainConfigRestartable hook. The runtime
// chain.Config isn't stored on CaplinService directly — the launch
// closure carries the full config bundle Caplin needs. The orchestrator
// swaps that closure via SetLaunch before calling Start; this method
// stays a no-op so the trio still type-checks as the interface and the
// orchestrator can drive Caplin the same way it drives any other
// Restartable component.
func (s *CaplinService) SetChainConfig(_ *chain.Config) {}

// SetLaunch replaces the launch closure Start will use on the next
// spawn. The debug_setFork orchestrator invokes it between Stop and
// Start with a closure rebound to the new caplin1.CaplinConfig.
func (s *CaplinService) SetLaunch(fn LaunchFn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.launch = fn
}

// Restart cancels the running goroutine, waits for it to exit, lets
// any in-flight async DB closes settle, then spawns a fresh goroutine.
// The relaunched Caplin re-runs the normal /finalized checkpoint-sync;
// EL recovery is driven by the BlockCollector Case-C nudge +
// engineapi initialCycle walk through preserved snapshots.
func (s *CaplinService) Restart() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done == nil {
		return fmt.Errorf("CaplinService.Restart: never started")
	}

	s.logger.Info("[caplin-service] restart triggered; tearing down Caplin goroutine")
	s.stopping.Store(true)
	s.cancel()
	select {
	case <-s.done:
	case <-time.After(s.teardownTimeout):
		// Caplin's stage loop finishes the stage it is inside before it
		// observes cancellation, so overrunning the bound is slowness, not
		// death. Hand the relaunch to a waiter rather than abandon it: the
		// caller only logs, and a service left stopped here is a node with
		// no consensus layer and nothing to say so.
		s.logger.Warn("[caplin-service] teardown overran the bound; relaunch deferred until the prior goroutine exits",
			"bound", s.teardownTimeout)
		go s.relaunchWhenIdle(s.done)
		return fmt.Errorf("CaplinService.Restart: prior goroutine still running after %s; relaunch deferred until it exits", s.teardownTimeout)
	}

	s.relaunchLocked()
	return nil
}

// relaunchWhenIdle completes a restart whose teardown overran the bound. The
// requirement it upholds is that exactly one Caplin goroutine runs, so it waits
// for the old one instead of spawning alongside it. A goroutine that never
// exits is escalated, never papered over.
func (s *CaplinService) relaunchWhenIdle(done chan struct{}) {
	select {
	case <-done:
	case <-s.parentCtx.Done():
		return
	case <-time.After(s.teardownTimeout * hardTeardownMultiple):
		s.logger.Error("[caplin-service] prior Caplin goroutine never exited; cascading shutdown",
			"waited", s.teardownTimeout*hardTeardownMultiple)
		if s.onUnexpectedExit != nil {
			s.onUnexpectedExit()
		}
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.done != done {
		return
	}
	s.logger.Warn("[caplin-service] prior goroutine exited late; completing deferred relaunch")
	s.relaunchLocked()
}

// relaunchLocked resets the per-instance block-relay cache and spawns the next
// Caplin goroutine. Caller must hold s.mu and must have observed the previous
// goroutine exit.
func (s *CaplinService) relaunchLocked() {
	// Wipe the PersistentBlockCollector's MDBX env. The cache holds
	// blocks whose parents lived in EL chaindata that the just-finished
	// mode-B unwind has DELETED. Without the wipe, the new Caplin
	// instance's BlockCollector retries InsertBlocks forever with
	// "parent's total difficulty not found" and the recovery cycle
	// (snapshot reconciliation + execution stage) never fires. Other
	// CaplinDB dirs are preserved — only the per-instance block-relay
	// cache must be reset. Empty datadir.Dirs (test paths) is a no-op.
	if s.dirs.CaplinHistory != "" {
		if err := dir.RemoveAll(s.dirs.CaplinHistory); err != nil {
			s.logger.Warn("[caplin-service] wipe CaplinHistory failed; new instance may wedge on stale PBC cache",
				"path", s.dirs.CaplinHistory, "err", err)
		} else if err := os.MkdirAll(s.dirs.CaplinHistory, 0o755); err != nil {
			s.logger.Warn("[caplin-service] recreate CaplinHistory dir failed",
				"path", s.dirs.CaplinHistory, "err", err)
		} else {
			s.logger.Info("[caplin-service] wiped CaplinHistory before relaunch (PBC cache reset)",
				"path", s.dirs.CaplinHistory)
		}
	}

	s.stopping.Store(false)
	s.spawnLocked()
	s.logger.Info("[caplin-service] new Caplin goroutine spawned")
}

// Close cancels the running goroutine and waits for it to exit. Called
// by backend shutdown to drain Caplin cleanly. No-op if never started.
// Use Stop (not Close) for a mid-process pause the service can later
// resume via Start.
func (s *CaplinService) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.done == nil {
		return
	}
	s.stopping.Store(true)
	s.cancel()
	select {
	case <-s.done:
	case <-time.After(s.teardownTimeout):
		s.logger.Error("[caplin-service] Close: goroutine did not exit within timeout; leaking",
			"timeout", s.teardownTimeout)
	}
}

// Stop is the ChainConfigRestartable pause: cancels the running Caplin
// goroutine and waits for it to exit within teardownTimeout. Leaves
// launch + parentCtx + dirs intact so a subsequent Start reuses them
// (or the caller can update the launch closure via SetLaunch first).
// Returns an error if the goroutine does not exit within the timeout.
// Idempotent: no-op if not currently running.
func (s *CaplinService) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done == nil {
		return nil
	}
	s.stopping.Store(true)
	s.cancel()
	select {
	case <-s.done:
	case <-time.After(s.teardownTimeout):
		return fmt.Errorf("CaplinService.Stop: goroutine did not exit within %s", s.teardownTimeout)
	}
	s.done = nil
	s.cancel = nil
	s.stopping.Store(false)
	return nil
}

// spawnLocked launches a fresh goroutine. Caller must hold s.mu.
func (s *CaplinService) spawnLocked() {
	ctx, cancel := context.WithCancel(s.parentCtx)
	done := make(chan struct{})
	s.cancel = cancel
	s.done = done
	go func() {
		defer close(done)
		defer cancel()
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("[caplin-service] Caplin goroutine panicked",
					"recover", fmt.Sprintf("%v", r))
			}
		}()
		err := s.launch(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Error("[caplin-service] Caplin goroutine returned error", "err", err)
		}
		if !s.stopping.Load() && s.onUnexpectedExit != nil {
			s.logger.Warn("[caplin-service] Caplin goroutine exited unexpectedly; cascading shutdown")
			s.onUnexpectedExit()
		}
	}()
}
