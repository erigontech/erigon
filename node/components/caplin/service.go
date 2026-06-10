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
)

// teardownTimeout bounds how long Stop/Restart wait for the Caplin
// goroutine to exit. Above this we surface a loud error rather than
// hang the caller indefinitely.
const teardownTimeout = 60 * time.Second

// dbCloseSettleDelay is a short fixed delay after the Caplin goroutine
// drains. Some Caplin-owned MDBX envs (e.g. OpenCaplinDatabase's
// indexing + blob handles) close inside background goroutines that
// select on ctx.Done — they may not have finished by the time
// <-s.done returns. Letting them settle before reopen avoids
// "resource temporarily unavailable" on the new mdbx.New().Open()
// call. PersistentBlockCollector now closes synchronously via
// Cfg.Close so the only remaining async closes are the ones in
// OpenCaplinDatabase, which this delay covers.
const dbCloseSettleDelay = 2 * time.Second

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
	}
}

// Start spawns the first Caplin goroutine. Returns an error if Start
// has already been called.
func (s *CaplinService) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done != nil {
		return fmt.Errorf("CaplinService.Start: already started")
	}
	s.spawnLocked()
	return nil
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
	case <-time.After(teardownTimeout):
		return fmt.Errorf("CaplinService.Restart: prior goroutine did not exit within %s", teardownTimeout)
	}

	select {
	case <-s.parentCtx.Done():
		return s.parentCtx.Err()
	case <-time.After(dbCloseSettleDelay):
	}

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
	return nil
}

// Stop cancels the running goroutine and waits for it to exit. Called
// by backend shutdown to drain Caplin cleanly. No-op if never started.
func (s *CaplinService) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done == nil {
		return
	}
	s.stopping.Store(true)
	s.cancel()
	select {
	case <-s.done:
	case <-time.After(teardownTimeout):
		s.logger.Error("[caplin-service] Stop: goroutine did not exit within timeout; leaking",
			"timeout", teardownTimeout)
	}
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
