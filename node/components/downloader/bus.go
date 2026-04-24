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
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/gointerfaces"
	downloaderproto "github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

// BindBus wires the Provider to the framework event bus. It subscribes a
// handler for flow.DownloadRequested that invokes Client.Download and
// publishes flow.DownloadComplete on success or flow.DownloadFailed on
// error.
//
// The subscription is asynchronous: Client.Download blocks until the
// transfer completes, so running in the publisher's goroutine would
// serialise the entire gap-fill pipeline. SubscribeAsync uses the
// Provider's execPool to run each handler on its own worker.
//
// Returns an error if the Provider is not initialised or is already bound.
func (p *Provider) BindBus(ctx context.Context, bus event.EventBus) error {
	if p == nil {
		return fmt.Errorf("downloader.BindBus: nil provider")
	}
	if bus == nil {
		return fmt.Errorf("downloader.BindBus: nil bus")
	}
	if p.Client == nil {
		return fmt.Errorf("downloader.BindBus: provider not initialised (nil Client)")
	}
	if p.busHandler != nil {
		return fmt.Errorf("downloader.BindBus: already bound")
	}

	p.busCtx = ctx
	p.bus = bus
	p.busHandler = p.onDownloadRequested
	if err := bus.SubscribeAsync(p.busHandler); err != nil {
		p.busHandler = nil
		p.bus = nil
		p.busCtx = nil
		return fmt.Errorf("subscribe flow.DownloadRequested: %w", err)
	}
	return nil
}

// UnbindBus removes the subscription installed by BindBus. Idempotent: a
// second call with no prior bind returns nil.
func (p *Provider) UnbindBus() error {
	if p == nil || p.busHandler == nil {
		return nil
	}
	err := p.bus.Unsubscribe(p.busHandler)
	p.busHandler = nil
	p.bus = nil
	p.busCtx = nil
	return err
}

// onDownloadRequested is the materialised handler. Must match the exact
// signature of flow.DownloadRequested consumers for the bus to route to it.
func (p *Provider) onDownloadRequested(req flow.DownloadRequested) {
	protoReq := &downloaderproto.DownloadRequest{
		Items: []*downloaderproto.DownloadItem{{
			Path:        req.FileName,
			TorrentHash: gointerfaces.ConvertAddressToH160(req.InfoHash),
		}},
		LogTarget: "snapshot-flow",
	}

	if err := p.Client.Download(p.busCtx, protoReq); err != nil {
		p.bus.Publish(flow.DownloadFailed{
			FileName: req.FileName,
			Reason:   err.Error(),
		})
		return
	}

	localPath := filepath.Join(p.dirs.Snap, req.FileName)
	var size int64
	if fi, err := os.Stat(localPath); err == nil {
		size = fi.Size()
	}
	p.bus.Publish(flow.DownloadComplete{
		FileName:  req.FileName,
		InfoHash:  req.InfoHash,
		LocalPath: localPath,
		Size:      size,
	})
}
