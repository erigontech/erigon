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
	"net"
	"os"
	"path/filepath"
	"time"

	anatorrent "github.com/anacrolix/torrent"
	anastorage "github.com/anacrolix/torrent/storage"

	dl "github.com/erigontech/erigon/db/downloader"
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

// FetchPeerManifestV2 downloads a peer's chain.toml.v2 into a per-peer
// scoped storage directory and returns the file bytes. Uses a fresh
// torrent with isolated storage so concurrent peer fetches do not
// collide with each other or with the local chain.toml.v2 the node is
// seeding. The torrent is dropped after completion — chain.toml.v2
// fetches are one-shot.
//
// peerIP and peerPort are the seeder's BT endpoint from their ENR;
// passing a zero peerPort means "no direct peer" (relies on other
// static peers or DHT). The underlying torrent client must have at
// least one way to reach the seeder — tests pass both via
// AddStaticPeer and via this call.
func (p *Provider) FetchPeerManifestV2(ctx context.Context, peerID string, infoHash [20]byte, peerIP net.IP, peerPort uint16) ([]byte, error) {
	if p == nil || p.Downloader == nil {
		return nil, fmt.Errorf("downloader.FetchPeerManifestV2: provider or downloader nil")
	}
	client := p.Downloader.TorrentClient()
	if client == nil {
		return nil, fmt.Errorf("downloader.FetchPeerManifestV2: torrent client nil")
	}

	peerDir := filepath.Join(p.dirs.Snap, ".peers", peerID)
	if err := os.MkdirAll(peerDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating peer dir: %w", err)
	}

	peerStorage := anastorage.NewFileOpts(anastorage.NewFileClientOpts{
		ClientBaseDir: peerDir,
	})
	defer func() { _ = peerStorage.Close() }()

	t, _ := client.AddTorrentOpt(anatorrent.AddTorrentOpts{
		InfoHash: infoHash,
		Storage:  peerStorage,
	})
	defer t.Drop()

	if peerIP != nil && peerPort != 0 {
		t.AddPeers([]anatorrent.PeerInfo{{
			Addr:    &net.TCPAddr{IP: peerIP, Port: int(peerPort)},
			Trusted: true,
		}})
	}

	dlCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	select {
	case <-t.GotInfo():
	case <-dlCtx.Done():
		return nil, fmt.Errorf("waiting for chain.toml.v2 info from peer %s: %w", peerID, dlCtx.Err())
	}

	info := t.Info()
	if info == nil {
		return nil, fmt.Errorf("peer %s chain.toml.v2 torrent info missing", peerID)
	}
	if len(info.Files) != 0 {
		return nil, fmt.Errorf("peer %s chain.toml.v2 torrent is multi-file (%d)", peerID, len(info.Files))
	}
	if info.Name != dl.ChainTomlV2FileName {
		return nil, fmt.Errorf("peer %s torrent name %q, expected %q", peerID, info.Name, dl.ChainTomlV2FileName)
	}

	t.DownloadAll()
	select {
	case <-t.Complete().On():
	case <-dlCtx.Done():
		return nil, fmt.Errorf("downloading chain.toml.v2 from peer %s: %w", peerID, dlCtx.Err())
	}

	data, err := os.ReadFile(filepath.Join(peerDir, dl.ChainTomlV2FileName))
	if err != nil {
		return nil, fmt.Errorf("reading downloaded chain.toml.v2 from peer %s: %w", peerID, err)
	}
	return data, nil
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
