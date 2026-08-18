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

	"github.com/erigontech/erigon/node/components/storage/flow"
)

// downloaderDeleter is the narrow surface subscribeDownloadSupersededOn
// needs from the downloader client. Extracted so tests can drive the
// subscriber without spinning up a full downloader.
type downloaderDeleter interface {
	Delete(ctx context.Context, paths []string) error
}

// subscribeDownloadSuperseded wires the Provider's default downloader
// client to the orchestrator's DownloadSuperseded stream.
func (p *Provider) subscribeDownloadSuperseded(ctx context.Context, dc downloaderDeleter) {
	subscribeDownloadSupersededOn(ctx, p, dc)
}

// subscribeDownloadSupersededOn is the testable variant: given any
// Provider-shaped subscriber source (eventBus + logger), subscribe a
// handler that calls the deleter for each superseded file. Handler is
// unsubscribed on ctx.Done.
func subscribeDownloadSupersededOn(ctx context.Context, p *Provider, dc downloaderDeleter) {
	if p == nil || p.eventBus == nil || dc == nil {
		return
	}
	handler := func(e flow.DownloadSuperseded) {
		if e.FileName == "" {
			return
		}
		if err := dc.Delete(ctx, []string{e.FileName}); err != nil {
			if p.logger != nil {
				p.logger.Warn("[snapshots] downloader.Delete on supersede", "file", e.FileName, "reason", e.Reason, "err", err)
			}
		}
	}
	if err := p.eventBus.Subscribe(handler); err != nil {
		if p.logger != nil {
			p.logger.Warn("[snapshots] subscribe DownloadSuperseded", "err", err)
		}
		return
	}
	go func() {
		<-ctx.Done()
		_ = p.eventBus.Unsubscribe(handler)
	}()
}
