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
	"context"
	"sync"
	"time"

	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// initialValidationPollInterval is how often the settle-watcher
// re-scans the inventory for files still moving through the lifecycle.
// The poll is deliberately slow — the watcher gates publication only,
// never foreground work, and the lifecycle driver advances on its own
// clock. A var, not a const, so tests can shorten it.
var initialValidationPollInterval = 2 * time.Second

// fileSettleState tracks one file's progress through the watcher.
type fileSettleState int

const (
	settleUnknown fileSettleState = iota
	settleRedownloading
	settleGood
	settleFailed
)

// watchInitialValidation is the storage Provider's startup pre-flight
// settle-watcher (docs/plans/20260522-publisher-startup-preflight.md).
// It waits for the initial download set to complete, then polls the
// inventory until every file in that set has settled out of the
// lifecycle's pre-Advertisable states — each has either reached
// Advertisable (passed the validator chain, infohash check included)
// or been quarantined after repeated validation failure. It then
// publishes flow.InitialValidationComplete which — together with
// InitialDownloadsComplete — gates the first chain.v2 advertisement.
//
// initialSet scopes the watcher to the fixed initial-download file set.
// A publisher builds new files locally at the tip after the initial
// download; those are NOT in initialSet and must not gate the first
// publish (a tip-region partial-block commitment legitimately stays
// paused until the next step retires). When initialSet is nil — a pure
// peer-download consumer, which has no locally-built files — every
// Local file is checked.
//
// A quarantined file is handled per the revalidation policy: redownload
// re-fetches it (the torrent client re-verifies and heals the on-disk
// bytes), warn excludes it from the first manifest, stop halts the
// first publish entirely (the event is never published).
func (p *Provider) watchInitialValidation(ctx context.Context, downloadsComplete <-chan struct{}, policy snapshotsync.RevalidationPolicy, initialSet map[string]struct{}) {
	select {
	case <-ctx.Done():
		return
	case <-downloadsComplete:
	}

	// Re-download outcomes for files requeued under the redownload
	// policy. A requeued file's heal attempt is "finished" once its
	// torrent reports DownloadComplete or DownloadFailed; until then
	// the watcher keeps waiting rather than declaring it unhealed.
	var mu sync.Mutex
	redownloadDone := make(map[string]bool)
	markDone := func(name string) { mu.Lock(); redownloadDone[name] = true; mu.Unlock() }
	isDone := func(name string) bool { mu.Lock(); defer mu.Unlock(); return redownloadDone[name] }
	onComplete := func(e flow.DownloadComplete) { markDone(e.FileName) }
	onFailed := func(e flow.DownloadFailed) { markDone(e.FileName) }
	_ = p.eventBus.Subscribe(onComplete)
	_ = p.eventBus.Subscribe(onFailed)
	defer func() {
		_ = p.eventBus.Unsubscribe(onComplete)
		_ = p.eventBus.Unsubscribe(onFailed)
	}()

	status := make(map[string]fileSettleState)
	ticker := time.NewTicker(initialValidationPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		allSettled := true
		for _, f := range p.Inventory.AllLocalFiles() {
			name := f.Name
			if initialSet != nil {
				if _, inSet := initialSet[name]; !inSet {
					// Locally-built tip file, not part of the fixed
					// initial download — outside this gate's scope.
					continue
				}
			}
			switch status[name] {
			case settleGood, settleFailed:
				continue
			}
			if state, ok := p.Inventory.LifecycleState(name); ok && state >= snapshot.LifecycleAdvertisable {
				status[name] = settleGood
				continue
			}
			if !p.LifecycleDriver.IsQuarantined(name) {
				// Still indexing / validating — keep waiting.
				allSettled = false
				continue
			}
			// Quarantined: a validator (the infohash check among them)
			// rejected the file past the lifecycle's retry threshold.
			switch policy {
			case snapshotsync.RevalidationStop:
				p.logger.Error("[storage] halting first manifest publish: file failed startup re-validation",
					"file", name, "policy", policy.String())
				return
			case snapshotsync.RevalidationWarn:
				if status[name] != settleFailed {
					p.logger.Warn("[storage] startup re-validation failed; excluding file from first manifest",
						"file", name, "policy", policy.String())
				}
				status[name] = settleFailed
			default: // RevalidationRedownload
				if status[name] != settleRedownloading {
					if p.Orchestrator.RequeueForDownload(name) {
						p.logger.Warn("[storage] startup re-validation failed; re-downloading file",
							"file", name, "policy", policy.String())
						status[name] = settleRedownloading
						mu.Lock()
						redownloadDone[name] = false
						mu.Unlock()
						allSettled = false
					} else {
						p.logger.Warn("[storage] startup re-validation failed; no peer offers file, excluding from first manifest",
							"file", name)
						status[name] = settleFailed
					}
					continue
				}
				// Already re-downloading. Wait for the heal attempt to
				// finish; if it finished and the file is still
				// quarantined, the re-download did not heal it.
				if isDone(name) {
					p.logger.Warn("[storage] re-download did not heal file; excluding from first manifest", "file", name)
					status[name] = settleFailed
				} else {
					allSettled = false
				}
			}
		}
		if allSettled {
			break
		}
	}

	p.eventBus.Publish(flow.InitialValidationComplete{})
	p.logger.Info("[storage] startup re-validation complete; first manifest publish gate cleared")
}
