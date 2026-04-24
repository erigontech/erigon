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
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	anatorrent "github.com/anacrolix/torrent"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	dl "github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/app/workerpool"
	"github.com/erigontech/erigon/node/components/downloader"
	"github.com/erigontech/erigon/node/components/manifest_exchange"
	sentrycomp "github.com/erigontech/erigon/node/components/sentry"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/ethconfig"
)

// P2PNode wires a real anacrolix/torrent client through the production
// downloader.Provider, manifest_exchange, flow.Orchestrator, and a sentry
// Provider (no DevP2P listener — peer-connected events are driven directly
// via sentry.PublishPeerConnected with manually constructed enode.Nodes).
//
// This exercises the real byte-moving, piece-hashing transport while
// leaving DevP2P discovery out of scope. A follow-up P2PNode variant can
// add the DevP2P listener once this smaller loop proves the event contract
// holds against the real torrent layer.
type P2PNode struct {
	T          *testing.T
	Bus        *CapturedBus
	Storage    *MockStorage
	Inventory  *snapshot.Inventory
	Downloader *downloader.Provider
	Sentry     *sentrycomp.Provider
	Mx         *manifest_exchange.Provider
	Orch       *flow.Orchestrator
	Dirs       datadir.Dirs

	dCore   *dl.Downloader
	cfg     *downloadercfg.Cfg
	pool    *testPool
	workers *workerpool.WorkerPool
	ctx     context.Context
	cancel  context.CancelFunc
}

// LocalTorrentAddr returns the BT endpoint the underlying torrent client
// is listening on, usable for constructing ENR bt entries or for
// AddStaticPeer on a peer node.
func (n *P2PNode) LocalTorrentAddr() (net.IP, uint16) {
	port := uint16(n.dCore.TorrentClient().LocalPort())
	return net.IPv4(127, 0, 0, 1), port
}

// DownloaderCore returns the underlying anacrolix-wrapping Downloader for
// tests that need to inspect or drive torrent-level state directly.
func (n *P2PNode) DownloaderCore() *dl.Downloader {
	return n.dCore
}

// AddSeederPeer registers another P2PNode as a BitTorrent peer so new
// torrents this node creates can fetch from it. Mirrors the production
// ENR-derived peer addition but without going through discovery.
func (n *P2PNode) AddSeederPeer(other *P2PNode) {
	ip, port := other.LocalTorrentAddr()
	n.dCore.AddStaticPeer(anatorrent.PeerInfo{
		Addr:    &net.TCPAddr{IP: ip, Port: int(port)},
		Trusted: true,
	})
}

// Close tears down every subcomponent and the underlying torrent client.
// Safe to defer from test callers.
func (n *P2PNode) Close() {
	_ = n.Mx.UnbindBus()
	_ = n.Orch.Close()
	n.Downloader.Close()
	_ = n.Sentry.Close()
	if n.cancel != nil {
		n.cancel()
	}
	if n.cfg != nil {
		_ = n.cfg.CloseTorrentLogFile()
	}
	n.Bus.WaitAsync()
	if n.pool != nil {
		n.pool.wg.Wait()
	}
	if n.workers != nil {
		n.workers.StopWait()
	}
}

// SeedFile writes real content to the node's snap directory, registers it
// with the torrent client as a seedable file, and adds a matching entry
// to the node's inventory at TrustVerified / Local=true. Returns the
// infohash the torrent client computed.
//
// The file name must parse via snaptype.ParseFileName — use canonical
// erigon snapshot names (e.g. "v1.0-accounts.0-2048.kv").
func (n *P2PNode) SeedFile(fileName string, content []byte, domain snapshot.Domain, fromStep, toStep uint64) [20]byte {
	n.T.Helper()
	path := filepath.Join(n.Dirs.Snap, fileName)
	require.NoError(n.T, os.WriteFile(path, content, 0o644))
	require.NoError(n.T, n.dCore.AddNewSeedableFile(n.ctx, fileName))

	torrentFS := dl.NewAtomicTorrentFS(n.Dirs.Snap)
	spec, err := torrentFS.LoadByName(fileName + ".torrent")
	require.NoError(n.T, err)

	n.Inventory.AddFile(&snapshot.FileEntry{
		Domain:      domain,
		FromStep:    fromStep,
		ToStep:      toStep,
		Name:        fileName,
		Size:        int64(len(content)),
		TorrentHash: [20]byte(spec.InfoHash),
		Local:       true,
		Trust:       snapshot.TrustVerified,
	})
	return [20]byte(spec.InfoHash)
}

// PublishV2Manifest generates chain.toml.v2 from the node's inventory,
// writes + seeds it, and returns the V2 infohash suitable for the peer's
// ENR chain-toml entry.
func (n *P2PNode) PublishV2Manifest() [20]byte {
	n.T.Helper()
	torrentFS := dl.NewAtomicTorrentFS(n.Dirs.Snap)
	hash, err := dl.PublishChainTomlV2(n.Dirs.Snap, torrentFS, n.Inventory, 0, nil)
	require.NoError(n.T, err)

	// Register the chain.toml.v2 file with the torrent client so it seeds.
	require.NoError(n.T, n.dCore.AddNewSeedableFile(n.ctx, dl.ChainTomlV2FileName))
	return [20]byte(hash)
}

// NewP2PNode constructs a fully wired P2P node for real-torrent integration
// tests. Uses an ephemeral BT port on loopback with DHT and trackers
// disabled — peers are added via AddSeederPeer.
func NewP2PNode(t *testing.T, logger log.Logger) *P2PNode {
	t.Helper()
	if logger == nil {
		logger = log.New()
		logger.SetHandler(log.DiscardHandler())
	}

	dirs := datadir.New(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())

	cfg, err := downloadercfg.New(
		ctx,
		dirs,
		"", // version string — defaults fine
		log.LvlInfo,
		0,   // ListenPort — 0 = OS-assigned
		20,  // EstablishedConnsPerTorrent — zero would allow NO connections
		nil, // bittorrent webseeds
		"testnet",
		false, // verify torrent data
		downloadercfg.NewCfgOpts{},
	)
	require.NoError(t, err)

	// Strip external discovery so tests are deterministic. Static peers
	// via AddStaticPeer carry all peer info in both directions.
	cfg.ClientConfig.NoDHT = true
	cfg.ClientConfig.DisableTrackers = true
	cfg.ClientConfig.Seed = true
	cfg.ClientConfig.ListenPort = 0 // OS-assigned
	// The downloadercfg default is 0 (no unverified pending bytes), which
	// is the production posture but effectively disables transfers in
	// short-lived tests. Lift the cap so pieces can land in-memory before
	// verification.
	cfg.ClientConfig.MaxUnverifiedBytes = 16 << 20 // 16 MiB
	if runtime.GOOS == "windows" {
		cfg.ClientConfig.DisableUTP = true
	}

	d, err := dl.New(ctx, cfg, logger)
	require.NoError(t, err)

	bts, err := dl.NewGrpcServer(d)
	require.NoError(t, err)
	client := dl.NewRpcClient(dl.DirectGrpcServerClient(bts), dirs.Snap)

	// Downloader Provider — bypass Initialize since we've constructed the
	// Downloader by hand. Configure still runs so dirs/logger are set on
	// the Provider for its BindBus handler's path resolution.
	dlProvider := &downloader.Provider{
		Downloader: d,
		Client:     client,
	}
	dlProvider.Configure(cfg, ethconfig.BlocksFreezing{}, dirs, logger, nil)

	// Wire the event bus and the rest of the stack.
	workers := workerpool.New(perNodePoolSize)
	pool := &testPool{workers: workers}
	innerBus := event.NewEventBus(pool)
	bus := NewCapturedBus(innerBus)

	storage := NewMockStorage()
	orch := flow.NewWithStorage(bus, storage, logger)

	sp := &sentrycomp.Provider{}
	require.NoError(t, sp.BindBus(bus))

	require.NoError(t, dlProvider.BindBus(ctx, bus))

	mx := &manifest_exchange.Provider{}
	require.NoError(t, mx.BindBus(ctx, bus, dlProvider, logger))

	require.NoError(t, orch.Start(ctx))

	n := &P2PNode{
		T:          t,
		Bus:        bus,
		Storage:    storage,
		Inventory:  storage.Inventory(),
		Downloader: dlProvider,
		Sentry:     sp,
		Mx:         mx,
		Orch:       orch,
		Dirs:       dirs,
		dCore:      d,
		cfg:        cfg,
		pool:       pool,
		workers:    workers,
		ctx:        ctx,
		cancel:     cancel,
	}
	t.Cleanup(n.Close)
	return n
}
