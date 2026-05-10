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
// Package lifecycle owns the storage-side file-import driver: the
// state machine that takes a file from Declared through Downloading,
// Downloaded, Indexing, Indexed, Validating, to Advertisable.
//
// See docs/plans/20260501-storage-lifecycle-spec.md for the full
// design. This package is the "Storage component lifecycle driver"
// step in that spec; sequencing-wise it is wired in dormant behind a
// feature flag (Snapshot.LifecycleDrivenByStorage), with the
// existing stage-driven path remaining authoritative until cutover.
package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// DefaultSweepInterval is the cadence at which the driver scans the
// inventory for files behind on transitions. The sweep is a safety net
// for events that get lost or delivered out of order; the primary
// driver of advancement is ChangeSet subscription, which wakes the
// sweep loop on every inventory mutation.
//
// 60 seconds matches the spec's default proposal — short enough that
// stalls are corrected promptly, long enough that idle nodes don't
// burn CPU. Configurable via Driver.SweepInterval if needed.
const DefaultSweepInterval = 60 * time.Second

// defaultQuarantineThreshold is the per-file failure cap before sweep
// stops retrying. 5 gives a handler enough chances to recover from
// transient state (download finishing, segment-loading completing)
// without flooding the log if the underlying problem is permanent.
// See completion plan §5c.
const defaultQuarantineThreshold = 5

// Handler runs one transition's work for a single file. On success it
// calls Inv.AdvanceTo to advance the entry's state; on failure it
// returns the error and leaves the entry at its current state for the
// next sweep to retry.
type Handler func(ctx context.Context, e *snapshot.FileEntry) error

// Driver runs the storage-owned import lifecycle state machine. It
// subscribes to inventory ChangeSets, runs handlers for each
// transition, and periodically sweeps to catch any file that fell
// behind.
//
// The driver is stateless across restarts — its work is to advance
// inventory entries via Inventory.AdvanceTo, which is the persistent
// state. Stopping and restarting the driver is safe.
type Driver struct {
	// Inv is the inventory the driver advances. Required.
	Inv *snapshot.Inventory

	// Logger receives operational logs. nil → log.Root().
	Logger log.Logger

	// SweepInterval overrides DefaultSweepInterval if non-zero.
	SweepInterval time.Duration

	// SnapDir is the snapshots directory the driver scans on every
	// sweep to discover files that landed on disk without firing an
	// inventory event (e.g. stage retire creates a .seg before
	// BuildMissedIndices runs; with LifecycleDrivenByStorage the
	// driver itself runs BuildMissedIndices, so it needs a path to
	// pick up the .seg first).
	//
	// Empty SnapDir → scan is skipped. Tests and lightweight setups
	// can leave it empty and drive the inventory directly.
	SnapDir string

	// OnIndexing handles the Downloaded → Indexed transition: build
	// any dependent index files (.idx, .kvi, .ef, .efi). nil → no-op.
	// Step 4 wires this to the existing index-build code from
	// db/snapshotsync (BuildMissedIndices) and db/state (accessor
	// builders), invoked from the storage component's clock instead
	// of from execution/stagedsync.
	OnIndexing Handler

	// OnValidation handles the Indexed → Advertisable transition: run
	// the producer-side validator chain. nil → no-op. Step 5 wires
	// this to the validation chain (extension-point design from item
	// #3 of app-integration-review-items.md).
	OnValidation Handler

	// QuarantineThreshold caps how many consecutive handler failures
	// for the same file dispatch before the driver stops retrying it.
	// After this many failures the file is "quarantined" — sweep
	// skips it, an Info log fires once. ChangeSet for that file
	// re-enables retries (clears the counter), so legitimate
	// transitions (download landing, manual fix-up) recover.
	//
	// Zero → use defaultQuarantineThreshold. See completion plan §5c
	// for the future structured-error classification that supersedes
	// this stop-the-symptom mechanism.
	QuarantineThreshold int

	// Internal state.
	mu          sync.Mutex
	running     bool
	stopFn      context.CancelFunc
	doneCh      chan struct{}
	subUnsub    func() // returned by Inv.Subscribe; called on Stop
	failures    map[string]int
	quarantined map[string]bool
}

// Start launches the driver. It subscribes to inventory ChangeSets,
// starts the periodic sweep, and returns immediately. Start is a no-op
// if the driver is already running. The driver runs until Stop is
// called (or until ctx is cancelled).
func (d *Driver) Start(ctx context.Context) error {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return nil
	}

	logger := d.Logger
	if logger == nil {
		logger = log.Root()
	}
	interval := d.SweepInterval
	if interval == 0 {
		interval = DefaultSweepInterval
	}

	runCtx, cancel := context.WithCancel(ctx)
	sub, unsub := d.Inv.Subscribe()

	d.running = true
	d.stopFn = cancel
	d.doneCh = make(chan struct{})
	d.subUnsub = unsub
	d.mu.Unlock()

	logger.Info("[storage-lifecycle] driver started",
		"sweep-interval", interval, "snap-dir", d.SnapDir,
		"on-indexing-wired", d.OnIndexing != nil,
		"on-validation-wired", d.OnValidation != nil)

	go d.run(runCtx, sub, interval, logger)
	return nil
}

// Stop signals the driver to shut down and waits for the sweep loop to
// exit. Multi-call safe; subsequent calls return immediately.
func (d *Driver) Stop() {
	d.mu.Lock()
	if !d.running {
		d.mu.Unlock()
		return
	}
	d.running = false
	cancel := d.stopFn
	doneCh := d.doneCh
	unsub := d.subUnsub
	logger := d.Logger
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if unsub != nil {
		unsub()
	}
	if doneCh != nil {
		<-doneCh
	}
	if logger != nil {
		logger.Info("[storage-lifecycle] driver stopped")
	}
}

// run is the driver's main loop. Receives ChangeSet wake-ups, runs
// periodic sweeps, exits on ctx cancellation.
func (d *Driver) run(ctx context.Context, sub <-chan snapshot.ChangeSet, interval time.Duration, logger log.Logger) {
	defer close(d.doneCh)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial sweep on start so any files already past their declared
	// state get processed without waiting for the first tick.
	d.Sweep(ctx, logger)

	for {
		select {
		case <-ctx.Done():
			return
		case cs, ok := <-sub:
			if !ok {
				// Subscription closed (Stop called). Drain through
				// ctx.Done to ensure clean exit.
				continue
			}
			// Clear failure / quarantine for the named files so a fresh
			// ChangeSet can re-trigger dispatch — the underlying state
			// changed, the previous failure may no longer apply.
			for _, name := range cs.Files {
				d.clearFailure(name)
			}
			d.Sweep(ctx, logger)
		case <-ticker.C:
			d.Sweep(ctx, logger)
		}
	}
}

// Sweep iterates the inventory, dispatching each entry to the handler
// matching its current LifecycleState. Idempotent — running it
// repeatedly produces the same result if no inputs changed. Handlers
// own their own concurrency; Sweep itself is single-threaded.
//
// At step 3 of the lifecycle implementation (this commit), default
// handlers are nil and dispatch is a no-op. Subsequent commits
// (steps 4 and 5) wire OnIndexing and OnValidation up to
// BuildMissedIndices and the validator chain respectively.
//
// logger may be nil; resolved to log.Root() inside.
func (d *Driver) Sweep(ctx context.Context, logger log.Logger) {
	if d.Inv == nil {
		return
	}
	if logger == nil {
		logger = log.Root()
	}
	d.discoverNewFiles(logger)
	view := d.Inv.View()
	defer view.Close()

	for _, domain := range []snapshot.Domain{
		snapshot.DomainAccounts, snapshot.DomainStorage,
		snapshot.DomainCode, snapshot.DomainCommitment,
	} {
		for _, e := range view.Files(domain) {
			d.dispatch(ctx, e, logger)
		}
	}
	for _, e := range view.BlockFiles() {
		d.dispatch(ctx, e, logger)
	}
}

// snapshotPrimaryExts is the set of file extensions the disk-scan path
// treats as "primary" — files whose presence triggers an Indexing
// transition. Accessor files (.idx, .kvi, .ef, .efi) are picked up via
// the post-recalc OnFilesChange wire, not the disk scan, because they
// always follow a primary file's index build.
var snapshotPrimaryExts = map[string]struct{}{
	".seg": {},
	".kv":  {},
	".v":   {},
	".ef":  {},
}

// discoverNewFiles scans SnapDir + Erigon's state subdirs (domain/,
// history/, idx/, accessor/) for primary files not yet in Inventory
// and AddFile-s each at LifecycleDownloaded. The set of subdirs is
// derived from snapshot.PathForName so the discovery path and the
// presence-checking path stay in sync. Empty SnapDir, missing subdirs,
// and read errors short-circuit silently — the next sweep retries.
//
// Idempotent: files already in Inventory are skipped. Adds new
// entries with metadata populated from the file name via
// PopulateFromName (Domain, Kind, FromStep/ToStep or
// FromBlock/ToBlock per kind).
//
// Logs an Info summary per directory if any new files were discovered.
func (d *Driver) discoverNewFiles(logger log.Logger) {
	if d.SnapDir == "" {
		return
	}
	// Each entry pairs the directory to scan with the relative-to-
	// SnapDir prefix used to key files in Inventory. The aggregator's
	// onFilesChange / onFilesDelete callbacks key files by their
	// relative-to-snap-dir path (e.g. "domain/v2.0-accounts.0-1.kv");
	// the disk scan keys them the same way so a file the aggregator
	// later deletes (e.g. after a merge) is found and removed instead
	// of leaving an orphaned basename entry behind.
	type scanDir struct{ path, prefix string }
	dirs := []scanDir{
		{d.SnapDir, ""},
		{filepath.Join(d.SnapDir, "domain"), "domain"},
		{filepath.Join(d.SnapDir, "history"), "history"},
		{filepath.Join(d.SnapDir, "idx"), "idx"},
		{filepath.Join(d.SnapDir, "accessor"), "accessor"},
	}
	for _, sd := range dirs {
		entries, err := os.ReadDir(sd.path)
		if err != nil {
			// Subdir might not exist on a fresh datadir; that's
			// fine — Aggregator creates them when state files
			// land. Silently skip.
			continue
		}
		added := 0
		var firstAdded string
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			ext := strings.ToLower(filepath.Ext(e.Name()))
			if _, ok := snapshotPrimaryExts[ext]; !ok {
				continue
			}
			name := e.Name()
			if sd.prefix != "" {
				name = sd.prefix + "/" + e.Name()
			}
			if _, ok := d.Inv.LifecycleState(name); ok {
				continue
			}
			entry := &snapshot.FileEntry{
				Name:  name,
				Local: true, // → derives LifecycleDownloaded
			}
			// Populate step + domain + kind from the name so the
			// entry joins the right step group (drives batch
			// validation dispatch). Returns false silently for
			// unrecognised patterns; the entry still gets added
			// with whatever fields we set.
			snapshot.PopulateFromName(entry)
			_ = d.Inv.AddFile(entry)
			logger.Debug("[storage-lifecycle] discovered new file", "name", name, "dir", sd.path)
			added++
			if firstAdded == "" {
				firstAdded = name
			}
		}
		if added > 0 {
			logger.Info("[storage-lifecycle] discovered new on-disk files",
				"count", added, "first", firstAdded, "dir", sd.path)
		}
	}
}

// dispatch routes a single entry to the handler for its current state.
// Each handler is responsible for advancing the entry on success via
// Inv.AdvanceTo; on handler failure the entry stays at its current
// state and will be retried on the next sweep.
//
// Other states (Declared, Downloading, Indexing, Validating,
// Advertisable) are not driven by sweep — they're driven by external
// triggers (downloader signals, in-progress handlers). Sweep only
// kicks off transitions that have all their inputs ready.
func (d *Driver) dispatch(ctx context.Context, e *snapshot.FileEntry, logger log.Logger) {
	if ctx.Err() != nil {
		return
	}
	if d.isQuarantined(e.Name) {
		// Quarantined files stay skipped until a ChangeSet for them
		// clears the counter (run loop's wake handler does this).
		return
	}
	switch e.State {
	case snapshot.LifecycleDownloaded:
		if d.OnIndexing == nil {
			return
		}
		logger.Debug("[storage-lifecycle] dispatch OnIndexing", "name", e.Name)
		if err := d.OnIndexing(ctx, e); err != nil {
			if errors.Is(err, validation.ErrPause) {
				logger.Debug("[storage-lifecycle] OnIndexing paused (transient)", "name", e.Name, "err", err)
				return
			}
			logger.Debug("[storage-lifecycle] OnIndexing failed", "name", e.Name, "err", err)
			d.recordFailure(e.Name, "OnIndexing", logger)
		} else {
			d.clearFailure(e.Name)
		}
	case snapshot.LifecycleIndexed:
		if d.OnValidation == nil {
			return
		}
		logger.Debug("[storage-lifecycle] dispatch OnValidation", "name", e.Name)
		if err := d.OnValidation(ctx, e); err != nil {
			if errors.Is(err, validation.ErrPause) {
				// Pause is the validator's defensive "wait for some
				// other piece of state to land" signal — neither
				// progress nor failure. Don't tick the per-file
				// quarantine counter, and don't clear prior real
				// failures either. Next sweep retries.
				logger.Debug("[storage-lifecycle] OnValidation paused (transient)", "name", e.Name, "err", err)
				return
			}
			logger.Debug("[storage-lifecycle] OnValidation failed", "name", e.Name, "err", err)
			d.recordFailure(e.Name, "OnValidation", logger)
		} else {
			d.clearFailure(e.Name)
		}
	}
}

// recordFailure increments the per-file failure counter. When the
// counter reaches QuarantineThreshold the file is moved to the
// quarantine map and an Info log fires once. Subsequent failures for
// the same file are silently absorbed.
func (d *Driver) recordFailure(name, phase string, logger log.Logger) {
	threshold := d.QuarantineThreshold
	if threshold == 0 {
		threshold = defaultQuarantineThreshold
	}
	d.mu.Lock()
	if d.failures == nil {
		d.failures = make(map[string]int)
	}
	d.failures[name]++
	count := d.failures[name]
	hitThreshold := count == threshold
	if hitThreshold {
		if d.quarantined == nil {
			d.quarantined = make(map[string]bool)
		}
		d.quarantined[name] = true
	}
	d.mu.Unlock()
	if hitThreshold {
		logger.Info("[storage-lifecycle] quarantining file after repeated failures",
			"file", name, "phase", phase, "failures", count,
			"hint", "the underlying handler keeps failing — operator should investigate (missing segments, corrupt file, etc.); ChangeSet for this file clears the quarantine")
	}
}

// clearFailure resets the failure counter and quarantine flag for a
// file. Called on successful dispatch and on ChangeSet receive.
func (d *Driver) clearFailure(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.failures, name)
	delete(d.quarantined, name)
}

// isQuarantined returns whether the named file is currently quarantined.
func (d *Driver) isQuarantined(name string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.quarantined[name]
}
