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

// FetchPeerManifestV2 downloads a peer's chain.v2.<seq>.toml into a
// per-peer-scoped storage directory and returns the file bytes. See
// fetchPeerSidecar for shared semantics.
func (p *Provider) FetchPeerManifestV2(ctx context.Context, peerID string, infoHash [20]byte, peerIP net.IP, peerPort uint16) ([]byte, error) {
	return p.fetchPeerSidecar(ctx, peerID, infoHash, peerIP, peerPort, sidecarV2Manifest)
}

// FetchPeerUCAN downloads a peer's Authority UCAN sidecar — the
// snapshotauth delegation that roots the publisher's authority. The
// infohash comes from the V2 manifest's AuthorityUCANHash field;
// callers parse the V2 first. Same per-peer-scoped storage + inflight
// dedup as FetchPeerManifestV2.
func (p *Provider) FetchPeerUCAN(ctx context.Context, peerID string, infoHash [20]byte, peerIP net.IP, peerPort uint16) ([]byte, error) {
	return p.fetchPeerSidecar(ctx, peerID, infoHash, peerIP, peerPort, sidecarUCAN)
}

// FetchPeerContentUCAN downloads a peer's Content UCAN sidecar — the
// per-generation attestation binding the V2 manifest's bytes. The
// infohash comes from the peer's ENR chain-toml entry
// (ChainToml.ContentUCANHash). Same per-peer-scoped storage + inflight
// dedup as FetchPeerManifestV2; the fetched torrent's file name is not
// predicate-checked — the Content UCAN's identity is its info-hash plus
// the consumer's cryptographic verification, not its filename.
func (p *Provider) FetchPeerContentUCAN(ctx context.Context, peerID string, infoHash [20]byte, peerIP net.IP, peerPort uint16) ([]byte, error) {
	return p.fetchPeerSidecar(ctx, peerID, infoHash, peerIP, peerPort, sidecarContentUCAN)
}

// peerSidecarKind identifies which sidecar artefact a fetch is
// requesting so the file-name predicate matches and error messages
// say what they're looking for.
type peerSidecarKind int

const (
	sidecarV2Manifest peerSidecarKind = iota
	sidecarUCAN
	sidecarContentUCAN
)

func (k peerSidecarKind) label() string {
	switch k {
	case sidecarV2Manifest:
		return "chain.v2.<genID>.toml"
	case sidecarUCAN:
		return "chain.ucan.<genID>.bin"
	case sidecarContentUCAN:
		return "chain.v2.<genID>.ucan"
	}
	return "?"
}

func (k peerSidecarKind) parseName(name string) bool {
	switch k {
	case sidecarV2Manifest:
		_, _, ok := dl.ParseChainTomlV2FileName(name)
		return ok
	case sidecarUCAN:
		_, _, ok := dl.ParseChainUCANFileName(name)
		return ok
	case sidecarContentUCAN:
		// The Content UCAN's filename is not enforced: it is fetched
		// by info-hash and its authenticity is established by the
		// consumer's signature + capability checks, not its name.
		return true
	}
	return false
}

// fetchPeerSidecar is the shared implementation of FetchPeerManifestV2
// and FetchPeerUCAN. It fetches a single-file torrent into a per-peer
// scoped storage directory and returns the file bytes. Uses a fresh
// torrent with isolated storage so concurrent peer fetches do not
// collide with each other or with locally-seeded artefacts. The
// torrent is dropped after completion — sidecar fetches are one-shot.
//
// peerIP and peerPort are the seeder's BT endpoint from their ENR;
// passing a zero peerPort means "no direct peer" (relies on other
// static peers or DHT). The underlying torrent client must have at
// least one way to reach the seeder — tests pass both via
// AddStaticPeer and via this call.
func (p *Provider) fetchPeerSidecar(ctx context.Context, peerID string, infoHash [20]byte, peerIP net.IP, peerPort uint16, kind peerSidecarKind) ([]byte, error) {
	if p == nil || p.Downloader == nil {
		return nil, fmt.Errorf("downloader.fetchPeerSidecar(%s): provider or downloader nil", kind.label())
	}

	// Deduplicate by infohash. anacrolix keys torrents by infohash, so
	// concurrent callers with the same hash but different per-peer
	// Storage options would have the second caller's Storage silently
	// ignored — only the first caller's peerDir receives bytes. Share
	// one fetch across callers; each gets the same data and can publish
	// downstream events independently under its own peerID.
	newFetch := &peerManifestFetch{done: make(chan struct{})}
	v, loaded := p.peerManifestInflight.LoadOrStore(infoHash, newFetch)
	fetch := v.(*peerManifestFetch)
	if loaded {
		// Another call is fetching this infohash; wait for its result.
		select {
		case <-fetch.done:
			return fetch.data, fetch.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// We are the fetcher. Record the result for waiters.
	defer func() {
		close(fetch.done)
		p.peerManifestInflight.Delete(infoHash)
	}()

	client := p.Downloader.TorrentClient()
	if client == nil {
		fetch.err = fmt.Errorf("downloader.fetchPeerSidecar(%s): torrent client nil", kind.label())
		return nil, fetch.err
	}

	// Local-seed short-circuit. anacrolix keys torrents by infohash; if
	// a peer advertises a sidecar whose content is byte-identical to a
	// generation we are already seeding (same content → same infohash),
	// AddTorrentOpt would return our existing torrent and silently
	// ignore the per-peer Storage we pass below — bytes stay in the
	// local snap-dir while ReadFile(peerDir/...) returns ENOENT.
	// Detect that case up front and read the local file directly.
	//
	// Non-blocking on purpose: a locally-seeded torrent has its info
	// available immediately (we built the .torrent from the on-disk
	// file). A torrent that's registered-but-info-pending is some other
	// code path's in-flight magnet fetch (e.g. the legacy chain.toml
	// discovery loop's AddTorrentInfoHash) — NOT a local seed — so we
	// must not block on its GotInfo(); fall through to the normal path
	// with its own bounded timeout instead.
	if existing, ok := client.Torrent(infoHash); ok {
		if info := existing.Info(); info != nil && len(info.Files) == 0 && kind.parseName(info.Name) {
			if data, rerr := os.ReadFile(filepath.Join(p.dirs.Snap, info.Name)); rerr == nil {
				fetch.data = data
				return data, nil
			}
		}
	}

	peerDir := filepath.Join(p.dirs.Snap, ".peers", peerID)
	if err := os.MkdirAll(peerDir, 0o755); err != nil {
		fetch.err = fmt.Errorf("creating peer dir: %w", err)
		return nil, fetch.err
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

	var addedPeers []anatorrent.PeerInfo
	if peerIP != nil && peerPort != 0 {
		for _, dialIP := range dl.PeerDialIPs(peerIP, p.Downloader.SelfIP()) {
			addedPeers = append(addedPeers, anatorrent.PeerInfo{
				Addr:    &net.TCPAddr{IP: dialIP, Port: int(peerPort)},
				Trusted: true,
			})
		}
		t.AddPeers(addedPeers)
	}
	if p.logger != nil {
		p.logger.Info("[downloader] fetching peer sidecar", "kind", kind.label(), "peer", peerID[:min(16, len(peerID))],
			"infoHash", fmt.Sprintf("%x", infoHash[:8]), "peerIP", peerIP, "peerPort", peerPort, "peerCandidates", len(addedPeers))
	}

	dlCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	select {
	case <-t.GotInfo():
	case <-dlCtx.Done():
		fetch.err = fmt.Errorf("waiting for %s info from peer %s: %w", kind.label(), peerID, dlCtx.Err())
		if p.logger != nil {
			p.logger.Warn("[downloader] peer sidecar: no torrent info", "kind", kind.label(), "peer", peerID[:min(16, len(peerID))], "err", fetch.err)
		}
		return nil, fetch.err
	}

	info := t.Info()
	if info == nil {
		fetch.err = fmt.Errorf("peer %s %s torrent info missing", peerID, kind.label())
		return nil, fetch.err
	}
	if len(info.Files) != 0 {
		fetch.err = fmt.Errorf("peer %s %s torrent is multi-file (%d)", peerID, kind.label(), len(info.Files))
		return nil, fetch.err
	}
	// info.Name is the filename the publisher used for this generation.
	// The seq is opaque to the consumer — we just need the shape match
	// so we know we're not pointed at a different torrent that happens
	// to share an infohash collision.
	if !kind.parseName(info.Name) {
		fetch.err = fmt.Errorf("peer %s torrent name %q is not a %s sidecar", peerID, info.Name, kind.label())
		return nil, fetch.err
	}

	t.DownloadAll()
	select {
	case <-t.Complete().On():
	case <-dlCtx.Done():
		fetch.err = fmt.Errorf("downloading %s from peer %s: %w", info.Name, peerID, dlCtx.Err())
		return nil, fetch.err
	}

	data, err := os.ReadFile(filepath.Join(peerDir, info.Name))
	if err != nil {
		fetch.err = fmt.Errorf("reading downloaded %s from peer %s: %w", info.Name, peerID, err)
		return nil, fetch.err
	}
	fetch.data = data
	return data, nil
}

// onDownloadRequested is the materialised handler. Must match the exact
// signature of flow.DownloadRequested consumers for the bus to route to it.
func (p *Provider) onDownloadRequested(req flow.DownloadRequested) {
	if p.logger != nil {
		p.logger.Debug("[downloader-bus] onDownloadRequested", "file", req.FileName)
	}
	protoReq := &downloaderproto.DownloadRequest{
		Items: []*downloaderproto.DownloadItem{{
			Path:        req.FileName,
			TorrentHash: gointerfaces.ConvertAddressToH160(req.InfoHash),
		}},
		LogTarget: "snapshot-flow",
	}

	if err := p.Client.Download(p.busCtx, protoReq); err != nil {
		if p.logger != nil {
			p.logger.Warn("[downloader-bus] Client.Download failed", "file", req.FileName, "err", err)
		}
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
	if p.logger != nil {
		p.logger.Info("[downloader-bus] publishing DownloadComplete", "file", req.FileName, "size", size)
	}
	p.bus.Publish(flow.DownloadComplete{
		FileName:  req.FileName,
		InfoHash:  req.InfoHash,
		LocalPath: localPath,
		Size:      size,
	})
}
