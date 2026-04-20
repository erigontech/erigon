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

package harness

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"

	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/flow"
)

// TorrentTransport is the real-network Transport implementation: an
// anacrolix/torrent client that serves / fetches real file bytes over TCP
// between peers. It exists to surface transport-level surprises (peer-drop
// behaviour, piece timing, partial transfers) that SimulatedTransport
// cannot reach.
//
// Deliberately minimal: no DHT, no trackers, no preverified registry, no
// webseeds. Peers discover each other only via direct AddPeer calls driven
// by the harness — mirroring the "ENR BT port → direct peer injection"
// pattern the decentralised-snapshot work uses, but without the ENR layer.
type TorrentTransport struct {
	client *torrent.Client
	dir    string
	bus    event.EventBus

	// handler is the method value stored once so Subscribe and Unsubscribe
	// see the same reflect.Value.Pointer(). See SimulatedTransport.handler
	// for the background on why this materialisation matters.
	handler func(flow.DownloadRequested)

	mu        sync.Mutex
	closed    bool
	peerAddrs map[string]ipPortAddr
	ctx       context.Context
	cancel    context.CancelFunc
	pending   sync.WaitGroup
}

// ipPortAddr implements net.Addr for torrent.PeerInfo.Addr. Matches the
// shape used in db/downloader/chaintoml_consumer.go; duplicated here to
// keep the harness package dependency-free of the downloader.
type ipPortAddr struct {
	IP   net.IP
	Port int
}

func (a ipPortAddr) Network() string { return "tcp" }
func (a ipPortAddr) String() string {
	return net.JoinHostPort(a.IP.String(), strconv.Itoa(a.Port))
}

// NewTorrentTransport creates a torrent client that both seeds and leeches
// from dir. Subscribes to DownloadRequested on bus.
//
// The client listens on an OS-assigned port — call LocalPort after
// construction to retrieve it for peer-address exchange.
func NewTorrentTransport(bus event.EventBus, dir string) (*TorrentTransport, error) {
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = dir
	cfg.ListenPort = 0
	cfg.NoDHT = true
	cfg.DisableTrackers = true
	cfg.Seed = true

	client, err := torrent.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("torrent client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t := &TorrentTransport{
		client:    client,
		dir:       dir,
		bus:       bus,
		peerAddrs: make(map[string]ipPortAddr),
		ctx:       ctx,
		cancel:    cancel,
	}
	t.handler = t.onDownloadRequested

	if err := bus.Subscribe(t.handler); err != nil {
		cancel()
		client.Close()
		return nil, fmt.Errorf("subscribe DownloadRequested: %w", err)
	}
	return t, nil
}

// LocalPort returns the TCP port the torrent client is listening on. Use it
// to hand the seeder's address to a remote leecher via AddPeer.
func (t *TorrentTransport) LocalPort() int { return t.client.LocalPort() }

// Seed writes data to dir/fileName, builds a torrent metainfo, adds it to
// the client, and returns the info-hash the remote leecher should reference
// in its download request.
func (t *TorrentTransport) Seed(fileName string, data []byte) ([20]byte, error) {
	path := filepath.Join(t.dir, fileName)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return [20]byte{}, fmt.Errorf("mkdir seed dir: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return [20]byte{}, fmt.Errorf("write seed file: %w", err)
	}

	info := metainfo.Info{PieceLength: 64 * 1024}
	if err := info.BuildFromFilePath(path); err != nil {
		return [20]byte{}, fmt.Errorf("build metainfo: %w", err)
	}
	var mi metainfo.MetaInfo
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		return [20]byte{}, fmt.Errorf("bencode metainfo: %w", err)
	}
	mi.InfoBytes = infoBytes

	if _, err := t.client.AddTorrent(&mi); err != nil {
		return [20]byte{}, fmt.Errorf("add torrent: %w", err)
	}
	return [20]byte(mi.HashInfoBytes()), nil
}

// AddPeer registers a remote peer's host:port so future DownloadRequested
// events whose FromPeers list contains peerID can route to it. Safe to
// call before or after the corresponding torrent is added.
func (t *TorrentTransport) AddPeer(peerID string, host string, port int) {
	t.mu.Lock()
	t.peerAddrs[peerID] = ipPortAddr{IP: net.ParseIP(host), Port: port}
	t.mu.Unlock()
}

// Close unsubscribes first (idempotent), cancels any in-flight downloads,
// waits for them to drain, and then closes the torrent client. The client
// shutdown itself is synchronous.
func (t *TorrentTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.cancel()
	t.mu.Unlock()

	unsubErr := t.bus.Unsubscribe(t.handler)
	t.pending.Wait()

	if errs := t.client.Close(); len(errs) > 0 {
		return errs[0]
	}
	return unsubErr
}

// onDownloadRequested runs its transfer asynchronously so the orchestrator
// can issue multiple DownloadRequested events concurrently without the
// publisher's goroutine blocking on real TCP transfer time. Each request
// gets its own goroutine; Close waits on pending to drain.
//
// SimulatedTransport keeps its handler synchronous because its completion
// is immediate — the goroutine hop would add no value and would surprise
// tests that rely on WaitAsync synchronising everything.
func (t *TorrentTransport) onDownloadRequested(req flow.DownloadRequested) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	var addrs []ipPortAddr
	for _, p := range req.FromPeers {
		if a, ok := t.peerAddrs[p]; ok {
			addrs = append(addrs, a)
		}
	}
	ctx := t.ctx
	t.pending.Add(1)
	t.mu.Unlock()

	go func() {
		defer t.pending.Done()
		t.runDownload(ctx, req, addrs)
	}()
}

// runDownload performs a single transfer and publishes the outcome.
func (t *TorrentTransport) runDownload(ctx context.Context, req flow.DownloadRequested, addrs []ipPortAddr) {
	tor, _ := t.client.AddTorrentInfoHash(req.InfoHash)
	for _, a := range addrs {
		tor.AddPeers([]torrent.PeerInfo{{Addr: a, Trusted: true}})
	}

	select {
	case <-tor.GotInfo():
	case <-ctx.Done():
		t.bus.Publish(flow.DownloadFailed{FileName: req.FileName, Reason: "cancelled before metainfo"})
		return
	}

	tor.DownloadAll()
	select {
	case <-tor.Complete().On():
	case <-ctx.Done():
		t.bus.Publish(flow.DownloadFailed{FileName: req.FileName, Reason: "cancelled during transfer"})
		return
	}

	info := tor.Info()
	t.bus.Publish(flow.DownloadComplete{
		FileName:  req.FileName,
		InfoHash:  req.InfoHash,
		LocalPath: filepath.Join(t.dir, req.FileName),
		Size:      info.TotalLength(),
	})
}
