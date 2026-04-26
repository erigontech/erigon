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
	// chain.toml.v2. Required.
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

	// AuthoritativeBlocks passes through to PublishChainTomlV2. Zero
	// is fine when not exercising block-coverage advertisement.
	AuthoritativeBlocks uint64
}

// BindAutoPublish wires automatic re-publication of chain.toml.v2 to a
// growing inventory. Subscribes to flow.TrustPromoted; coalesces bursts
// inside the debounce window, then regenerates the V2 manifest, builds
// its .torrent, calls ENRUpdater with the new info-hash, and seeds the
// new chain.toml.v2 torrent.
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

	ctx, cancel := context.WithCancel(ctx)
	tick := make(chan struct{}, 1)
	done := make(chan struct{})

	go p.autoPublishLoop(ctx, opts, tick, done, debounce)

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
	tick <-chan struct{},
	done chan<- struct{},
	debounce time.Duration,
) {
	defer close(done)

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

		if err := p.republishChainTomlV2(opts); err != nil {
			p.logger.Warn("[downloader] auto-publish failed", "err", err)
		}
	}
}

// republishChainTomlV2 generates+writes a fresh chain.toml.v2 from the
// current inventory, advertises the new info-hash via ENR, and seeds
// the V2 torrent. Reuses the helper at db/downloader.PublishChainTomlV2
// so the wire format and ENR fields match the startup publish path.
func (p *Provider) republishChainTomlV2(opts AutoPublishOpts) error {
	torrentFS := dl.NewAtomicTorrentFS(p.dirs.Snap)
	hash, err := dl.PublishChainTomlV2(p.dirs.Snap, torrentFS, opts.Inventory, opts.AuthoritativeBlocks, opts.ENRUpdater)
	if err != nil {
		return err
	}
	// Make the new torrent seedable so connected peers can fetch it.
	if err := p.Downloader.AddNewSeedableFile(p.busCtx, dl.ChainTomlV2FileName); err != nil {
		// Non-fatal — the file is on disk and seeded by AddTorrentsFromDisk
		// during normal lifecycle; this is just a fast-path.
		p.logger.Debug("[downloader] auto-publish: re-add seedable failed", "err", err, "hash", fmt.Sprintf("%x", hash))
	}
	return nil
}
