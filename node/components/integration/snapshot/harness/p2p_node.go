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

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	dl "github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	execp2p "github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/app/workerpool"
	"github.com/erigontech/erigon/node/components/downloader"
	"github.com/erigontech/erigon/node/components/manifest_exchange"
	sentrycomp "github.com/erigontech/erigon/node/components/sentry"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

// P2PNode wires a real anacrolix/torrent client through the production
// downloader.Provider, manifest_exchange, flow.Orchestrator, plus a real
// sentry.Provider running the eth/68 protocol on a live DevP2P listener.
//
// This exercises the entire production lifecycle: real RLPx handshake,
// real eth status exchange, real peer-event auto-wire into
// flow.PeerManifestReceived, real chain.toml.v2 fetch via BitTorrent,
// real piece-hashed transfer. The only thing test-side is the static-peer
// plumbing — we skip discv5 so peer wiring is deterministic.
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

	dCore     *dl.Downloader
	cfg       *downloadercfg.Cfg
	pool      *testPool
	workers   *workerpool.WorkerPool
	unregPeer execp2p.UnregisterFunc
	ctx       context.Context
	cancel    context.CancelFunc
}

// LocalTorrentAddr returns the BT endpoint the underlying torrent client
// is listening on, usable for AddStaticPeer on a peer node or for
// populating the enr.BT entry on this node's ENR.
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
// torrents this node creates can fetch from it. BitTorrent peering is
// separate from DevP2P peering — production derives BT peers from the
// DevP2P ENR, but in tests we plumb them directly.
func (n *P2PNode) AddSeederPeer(other *P2PNode) {
	ip, port := other.LocalTorrentAddr()
	n.dCore.AddStaticPeer(anatorrent.PeerInfo{
		Addr:    &net.TCPAddr{IP: ip, Port: int(port)},
		Trusted: true,
	})
}

// DevP2PSelf returns the local enode.Node from the first sentry server.
// Use this to hand out our address to peers via AddDevP2PPeer.
func (n *P2PNode) DevP2PSelf() *enode.Node {
	for _, srv := range n.Sentry.Servers {
		if p2pServer := srv.GetP2PServer(); p2pServer != nil {
			return p2pServer.Self()
		}
	}
	return nil
}

// AddDevP2PPeer instructs this node's sentry servers to dial and
// maintain a connection to the given peer. The server adds it as a
// static peer — no discovery involved.
func (n *P2PNode) AddDevP2PPeer(node *enode.Node) {
	for _, srv := range n.Sentry.Servers {
		if p2pServer := srv.GetP2PServer(); p2pServer != nil {
			p2pServer.AddPeer(node)
		}
	}
}

// RemoveDevP2PPeer removes the given peer from this node's static set
// and disconnects any active connection to it. The peer may be
// re-added later via AddDevP2PPeer — soak / churn scenarios use this
// pair to exercise connect/disconnect lifecycle events without
// destroying and rebuilding whole nodes.
func (n *P2PNode) RemoveDevP2PPeer(node *enode.Node) {
	for _, srv := range n.Sentry.Servers {
		if p2pServer := srv.GetP2PServer(); p2pServer != nil {
			p2pServer.RemovePeer(node)
		}
	}
}

// SetDevP2PENREntry installs a custom entry on every sentry server's
// LocalNode. Takes effect immediately — the node re-signs its ENR on
// each mutation and peers see the new version on next handshake.
func (n *P2PNode) SetDevP2PENREntry(entry enr.Entry) {
	for _, srv := range n.Sentry.Servers {
		if p2pServer := srv.GetP2PServer(); p2pServer != nil {
			p2pServer.LocalNode().Set(entry)
		}
	}
}

// Close tears down every subcomponent. Cancels the context up front so
// sentry / downloader background goroutines can exit, then waits for
// them via their respective Close methods.
func (n *P2PNode) Close() {
	if n.unregPeer != nil {
		n.unregPeer()
	}
	if n.cancel != nil {
		n.cancel()
	}
	_ = n.Mx.UnbindBus()
	_ = n.Orch.Close()
	n.Downloader.Close()
	_ = n.Sentry.Close()
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
	require.NoError(n.T, n.dCore.AddNewSeedableFile(n.ctx, dl.ChainTomlV2FileName))
	return [20]byte(hash)
}

// convertGenesisDifficulty wraps gointerfaces.ConvertUint256IntToH256
// for a genesis block — Difficulty() returns a value (uint256.Int) but
// the converter wants a pointer.
func convertGenesisDifficulty(b *types.Block) *typesproto.H256 {
	d := b.Difficulty()
	return gointerfaces.ConvertUint256IntToH256(&d)
}

// NewP2PNode constructs a fully wired P2P node for real-stack integration
// tests. Uses an ephemeral BT port on loopback with DHT and trackers
// disabled, and a real sentry.Provider running eth/68 on an ephemeral
// DevP2P listener.
func NewP2PNode(t *testing.T, logger log.Logger) *P2PNode {
	t.Helper()
	if logger == nil {
		logger = log.New()
		logger.SetHandler(log.DiscardHandler())
	}

	dirs := datadir.New(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())

	// --- Downloader (anacrolix/torrent) -----------------------------------
	cfg, err := downloadercfg.New(
		ctx, dirs, "", log.LvlInfo,
		0,   // ListenPort — 0 = OS-assigned
		20,  // EstablishedConnsPerTorrent
		nil, // webseeds
		"testnet",
		false, // verify torrent data
		downloadercfg.NewCfgOpts{},
	)
	require.NoError(t, err)
	cfg.ClientConfig.NoDHT = true
	cfg.ClientConfig.DisableTrackers = true
	cfg.ClientConfig.Seed = true
	cfg.ClientConfig.ListenPort = 0
	cfg.ClientConfig.MaxUnverifiedBytes = 16 << 20
	if runtime.GOOS == "windows" {
		cfg.ClientConfig.DisableUTP = true
	}

	d, err := dl.New(ctx, cfg, logger)
	require.NoError(t, err)
	bts, err := dl.NewGrpcServer(d)
	require.NoError(t, err)
	client := dl.NewRpcClient(dl.DirectGrpcServerClient(bts), dirs.Snap)

	dlProvider := &downloader.Provider{Downloader: d, Client: client}
	dlProvider.Configure(cfg, ethconfig.BlocksFreezing{}, dirs, logger, nil)

	// --- Event bus and storage / orchestrator -----------------------------
	workers := workerpool.New(perNodePoolSize)
	pool := &testPool{workers: workers}
	innerBus := event.NewEventBus(pool)
	bus := NewCapturedBus(innerBus)

	storage := NewMockStorage()
	orch := flow.NewWithStorage(bus, storage, logger)

	// --- Chain stack for sentry -------------------------------------------
	chainDB := temporal.NewTestDB(t, dbcfg.ChainDB)
	spec, err := chainspec.ChainSpecByName(networkname.Test)
	require.NoError(t, err)

	_, genesisBlock, err := genesiswrite.CommitGenesisBlock(chainDB, spec.Genesis, spec.Name, dirs, logger)
	require.NoError(t, err)

	// Empty snapshots registry — genesis-only state, no frozen files.
	allSnapshots := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{}, dirs.Snap, logger)
	blockReader := freezeblocks.NewBlockReader(allSnapshots, nil)

	// --- Sentry (real eth/68, ephemeral DevP2P listener) ------------------
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	sp := &sentrycomp.Provider{}
	sp.Configure(sentrycomp.Config{
		SentryCtx: ctx,
		P2P: p2p.Config{
			PrivateKey:      privKey,
			MaxPeers:        10,
			MaxPendingPeers: 5,
			NoDiscovery:     true,
			ListenAddr:      "127.0.0.1:0",
			AllowedPorts:    []uint{0}, // ephemeral
			ProtocolVersion: []uint{direct.ETH68},
			NodeDatabase:    t.TempDir(),
			DiscoveryV4:     false,
			DiscoveryV5:     false,
		},
		ChainDB:     chainDB,
		ChainConfig: spec.Config,
		GenesisHash: spec.GenesisHash,
		NetworkID:   spec.Config.ChainID.Uint64(),
		Genesis:     genesisBlock,
		BlockReader: blockReader,
		ChainName:   spec.Name,
		NodesDir:    t.TempDir(),
		Logger:      logger,
	})
	require.NoError(t, sp.Initialize(ctx))
	require.NoError(t, sp.BindBus(bus))

	// Start background goroutines + p2p listeners.
	require.NoError(t, sp.Start(ctx))

	// Push an initial status so each sentry GrpcServer starts its
	// underlying p2p.Server — SetStatus is the lazy trigger. Build the
	// status via the production StatusDataProvider so the fork digest
	// lines up with what peers expect during eth/68 handshake.
	statusData, err := sp.StatusDataProvider.GetStatusData(ctx)
	require.NoError(t, err)
	for _, srv := range sp.Servers {
		_, err := srv.SetStatus(ctx, statusData)
		require.NoError(t, err)
	}

	// Enable peer-event auto-wire now that the servers are live.
	unregPeer, err := sp.EnablePeerEventAutoWire(ctx)
	require.NoError(t, err)

	// Wire the rest of the stack.
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
		unregPeer:  unregPeer,
		ctx:        ctx,
		cancel:     cancel,
	}
	t.Cleanup(n.Close)
	return n
}
