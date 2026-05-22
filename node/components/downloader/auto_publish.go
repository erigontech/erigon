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

package downloader

import (
	"context"
	"fmt"
	"time"

	dl "github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/flow"
	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// AutoPublishOpts configures the BindAutoPublish lifecycle.
type AutoPublishOpts struct {
	// Bus is the event bus to subscribe to. Required.
	Bus event.EventBus

	// Inventory is the snapshot inventory to read when regenerating
	// chain.v2.<seq>.toml. Required.
	Inventory *snapshotinv.Inventory

	// Debounce coalesces a burst of TrustPromoted events into one
	// publish. Re-publishing on every single download complete would
	// thrash for a fast convergence; one publish per debounce window
	// keeps the .torrent rebuild bounded.
	//
	// Zero defers to a default of 1 second.
	Debounce time.Duration

	// ENRUpdater is called with the new V2 info-hash after each
	// publish. Pass nil to skip ENR updates (useful in tests that
	// drive the ENR another way).
	ENRUpdater func(enr.ChainToml)

	// AuthoritativeBlocks passes through to the rolling publisher.
	// Zero is fine when not exercising block-coverage advertisement.
	AuthoritativeBlocks uint64

	// Publisher, when non-nil, is the rolling publisher to drive from
	// the auto-publish loop. Useful when the caller already holds a
	// publisher (e.g. a test harness that called Publish() at startup
	// and wants the auto-publisher to advance the same generation
	// history rather than spin up a parallel publisher with its own
	// state). When nil, BindAutoPublish constructs a fresh publisher
	// scoped to this binding.
	Publisher *dl.RollingV2Publisher

	// DelegationSource, when non-nil, is wired onto the publisher so
	// every Publish() writes the Authority UCAN sidecar
	// (chain.ucan.authority.<fp>.<rev>.bin) and stamps the V2 manifest's
	// AuthorityUCANHash. Production callers populate
	// this with snapshotauth.LoadOrGenerateDelegation(...) result —
	// see node/components/snapshotauth.LoadOrGenerateDelegation for
	// the JWT-style default-path resolution model.
	//
	// Nil leaves the publisher in V2-only mode; manifests carry no
	// AuthorityUCANHash and consumers running with TrustConfig will reject this
	// peer.
	DelegationSource dl.DelegationSource

	// GateFirstPublish, when true, holds back the FIRST publish until
	// the initial file set is both fully downloaded
	// (flow.InitialDownloadsComplete) AND has settled through the
	// validator chain — infohash check included — to Advertisable or
	// quarantine (flow.InitialValidationComplete). So the first chain.v2
	// advertisement reflects a complete, validated set rather than the
	// empty/partial inventory of a node still syncing. Subsequent
	// generations are unaffected (TrustPromoted-driven, as before).
	//
	// Production sets this true; tests / the harness leave it false to
	// publish eagerly. See
	// docs/plans/20260522-publisher-startup-preflight.md.
	GateFirstPublish bool
}

// BindAutoPublish wires automatic re-publication of chain.v2.<seq>.toml
// to a growing inventory. Subscribes to flow.TrustPromoted; coalesces
// bursts inside the debounce window, then asks the rolling publisher
// to write the next generation, calls ENRUpdater with the new
// info-hash, and seeds the new torrent.
//
// Each republish writes a NEW chain.v2.<seq>.toml (rather than
// overwriting a single file in place). Old generations stay seedable
// up to MaxRetained — peers that captured a stale ENR snapshot at
// handshake time can still fetch the older infohash. The rolling
// buffer is the disk-bound; an explicit Cleanup() call on the
// publisher is the defence-in-depth.
//
// Multiple TrustPromoted events arriving within the debounce window
// trigger exactly one publish. Late-arriving downloads inside an
// in-flight publish reset the timer so the next publish covers them.
//
// Idempotent: a second call before UnbindAutoPublish returns an error.
// UnbindAutoPublish stops the publisher and unsubscribes.
func (p *Provider) BindAutoPublish(ctx context.Context, opts AutoPublishOpts) error {
	if p == nil {
		return fmt.Errorf("downloader.BindAutoPublish: nil provider")
	}
	if opts.Bus == nil {
		return fmt.Errorf("downloader.BindAutoPublish: nil bus")
	}
	if opts.Inventory == nil {
		return fmt.Errorf("downloader.BindAutoPublish: nil inventory")
	}
	if p.Downloader == nil {
		return fmt.Errorf("downloader.BindAutoPublish: provider not initialised (nil Downloader)")
	}
	if p.autoPublishHandler != nil {
		return fmt.Errorf("downloader.BindAutoPublish: already bound")
	}

	debounce := opts.Debounce
	if debounce <= 0 {
		debounce = time.Second
	}

	publisher := opts.Publisher
	if publisher == nil {
		var err error
		publisher, err = dl.NewRollingV2Publisher(p.dirs.Snap, dl.NewAtomicTorrentFS(p.dirs.Snap), p.Downloader)
		if err != nil {
			return fmt.Errorf("constructing rolling V2 publisher: %w", err)
		}
	}
	if opts.DelegationSource != nil {
		publisher.SetDelegationSource(opts.DelegationSource)
	}

	// Re-seed the V2 generations a fresh publisher recovered from disk
	// (see RollingV2Publisher.ResumeSeeding).
	if err := publisher.ResumeSeeding(ctx); err != nil {
		p.logger.Warn("[downloader] resume-seeding retained V2 generations failed", "err", err)
	}

	// Build the first-publish gate channel before spawning the loop so
	// the bus subscriptions are in place well ahead of the events firing.
	var gateCh <-chan struct{}
	if opts.GateFirstPublish {
		gateCh = flow.FirstPublishGateChannel(opts.Bus)
	}

	ctx, cancel := context.WithCancel(ctx)
	tick := make(chan struct{}, 1)
	done := make(chan struct{})

	go p.autoPublishLoop(ctx, opts, publisher, tick, done, debounce, gateCh)

	handler := func(flow.TrustPromoted) {
		// Non-blocking nudge — the loop coalesces.
		select {
		case tick <- struct{}{}:
		default:
		}
	}
	if err := opts.Bus.SubscribeAsync(handler); err != nil {
		cancel()
		<-done
		return fmt.Errorf("subscribe flow.TrustPromoted: %w", err)
	}

	p.autoPublishHandler = handler
	p.autoPublishCancel = cancel
	p.autoPublishDone = done
	return nil
}

// UnbindAutoPublish stops the auto-publisher and unsubscribes the
// TrustPromoted handler. Idempotent.
func (p *Provider) UnbindAutoPublish() error {
	if p == nil || p.autoPublishHandler == nil {
		return nil
	}
	cancel := p.autoPublishCancel
	done := p.autoPublishDone
	handler := p.autoPublishHandler
	p.autoPublishHandler = nil
	p.autoPublishCancel = nil
	p.autoPublishDone = nil

	cancel()
	<-done

	// Best-effort unsubscribe; the goroutine has already exited so a
	// stale subscription only matters if the caller leaks the bus.
	if p.bus != nil {
		_ = p.bus.Unsubscribe(handler)
	}
	return nil
}

// autoPublishLoop runs the debounce + republish state machine. Exits
// when ctx is cancelled.
func (p *Provider) autoPublishLoop(
	ctx context.Context,
	opts AutoPublishOpts,
	publisher *dl.RollingV2Publisher,
	tick <-chan struct{},
	done chan<- struct{},
	debounce time.Duration,
	gateCh <-chan struct{},
) {
	defer close(done)

	// First-publish gate: hold back the initial generation until the
	// node has a complete, validated file set (gateCh closes once both
	// flow.InitialDownloadsComplete and flow.InitialValidationComplete
	// have fired). nil gateCh = ungated (tests).
	if gateCh != nil {
		select {
		case <-ctx.Done():
			return
		case <-gateCh:
		}
		if _, err := publisher.Publish(ctx, opts.Inventory, opts.AuthoritativeBlocks, opts.ENRUpdater); err != nil {
			p.logger.Warn("[downloader] gated first auto-publish failed", "err", err)
		}
	}

	for {
		// Wait for the first nudge.
		select {
		case <-ctx.Done():
			return
		case <-tick:
		}

		// Coalesce: keep resetting the debounce timer while more
		// nudges arrive. Fires once when the burst settles.
		timer := time.NewTimer(debounce)
	collect:
		for {
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-tick:
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(debounce)
			case <-timer.C:
				break collect
			}
		}

		if _, err := publisher.Publish(ctx, opts.Inventory, opts.AuthoritativeBlocks, opts.ENRUpdater); err != nil {
			p.logger.Warn("[downloader] auto-publish failed", "err", err)
		}
	}
}
