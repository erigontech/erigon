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
	"time"

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
	"github.com/erigontech/erigon/node/components/storage/validation"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

// harnessENRFP is the deterministic 16-hex ENR fingerprint stamped on
// every harness node's RollingV2Publisher. Production resolves this from
// the node's discv5 ID; the harness skips discv5, so a fixed value keeps
// chain.v2.<fp>.<seq>.toml filenames stable and discoverable across the
// manual + auto publish paths.
const harnessENRFP = "a1b2c3d4e5f60718"

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
	Storage    *MockStorage // nil when the node was built with real storage
	Inventory  *snapshot.Inventory
	Downloader *downloader.Provider
	Sentry     *sentrycomp.Provider
	Mx         *manifest_exchange.Provider
	Orch       *flow.Orchestrator
	Dirs       datadir.Dirs

	// InitialStateReady is non-nil only for real-storage nodes (built
	// via NewP2PNodeWithRealStorage). It closes when the orchestrator's
	// minimal-state-domain set is downloaded + recorded — the signal
	// production wiring feeds into OtterSync's wait gate.
	InitialStateReady <-chan struct{}

	dCore       *dl.Downloader
	cfg         *downloadercfg.Cfg
	pool        *testPool
	workers     *workerpool.WorkerPool
	unregPeer   execp2p.UnregisterFunc
	v2Publisher *dl.RollingV2Publisher // lazy — constructed on first PublishV2Manifest
	ctx         context.Context
	cancel      context.CancelFunc
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

// EnableAutoPublishV2 wires the downloader's auto-republish loop: every
// time the orchestrator promotes a file to TrustVerified (download
// complete), the rolling V2 publisher writes the next chain.v2.<seq>.toml
// generation, the new info-hash is written into the local ENR, and the
// new torrent is registered as seedable. Debounced — a burst of
// completions coalesces to one publish.
//
// Reuses the harness's own rolling publisher when one already exists
// (typically from a startup PublishV2Manifest call) so the rolling
// history is consistent across the manual + auto paths.
//
// Required for staggered-entry scenarios so a late joiner sees the
// CURRENT inventory of peers it dials, not the inventory those peers
// had at startup.
func (n *P2PNode) EnableAutoPublishV2(debounce time.Duration) {
	n.T.Helper()
	require.NoError(n.T, n.Downloader.BindAutoPublish(n.ctx, downloader.AutoPublishOpts{
		Bus:       n.Bus,
		Inventory: n.Inventory,
		Debounce:  debounce,
		Publisher: n.v2Publisher,
		ENRUpdater: func(ct enr.ChainToml) {
			n.SetDevP2PENREntry(ct)
		},
	}))
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
	_ = n.Downloader.UnbindAutoPublish()
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
//
// fileName may include a leading subdirectory (e.g. "domain/v1.1-..."
// or "caplin/v1.1-...") — the parent dir is created on demand.
func (n *P2PNode) SeedFile(fileName string, content []byte, domain snapshot.Domain, fromStep, toStep uint64) [20]byte {
	n.T.Helper()
	path := filepath.Join(n.Dirs.Snap, fileName)
	require.NoError(n.T, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(n.T, os.WriteFile(path, content, 0o644))
	return n.registerSeedable(fileName, int64(len(content)), &snapshot.FileEntry{
		Domain:   domain,
		FromStep: fromStep,
		ToStep:   toStep,
		Name:     fileName,
	})
}

// SeedExistingFile registers a file that's already on disk under the
// node's Dirs.Snap (e.g. hardlinked or copied in by the test) as
// seedable, and adds it to the inventory with the supplied entry
// fields. Returns the infohash the torrent client computes.
//
// Handles primary kinds (kv, history, idx, block .seg, caplin .seg)
// plus meta and salt files. The caller fills in Domain / FromStep /
// ToStep / Kind appropriately on entryTemplate; Name / Size / Local /
// Trust / TorrentHash are filled in by this method.
func (n *P2PNode) SeedExistingFile(fileName string, entryTemplate *snapshot.FileEntry) [20]byte {
	n.T.Helper()
	path := filepath.Join(n.Dirs.Snap, fileName)
	info, err := os.Stat(path)
	require.NoError(n.T, err, "SeedExistingFile: %s missing on disk", path)
	tpl := *entryTemplate
	tpl.Name = fileName
	return n.registerSeedable(fileName, info.Size(), &tpl)
}

// registerSeedable is the shared tail of SeedFile / SeedExistingFile.
// Tells the downloader to compute the torrent for fileName, captures
// the infohash, and adds the finalised inventory entry.
func (n *P2PNode) registerSeedable(fileName string, size int64, entryTemplate *snapshot.FileEntry) [20]byte {
	n.T.Helper()
	require.NoError(n.T, n.dCore.AddNewSeedableFile(n.ctx, fileName))

	torrentFS := dl.NewAtomicTorrentFS(n.Dirs.Snap)
	spec, err := torrentFS.LoadByName(fileName + ".torrent")
	require.NoError(n.T, err)

	entry := *entryTemplate
	entry.Size = size
	entry.TorrentHash = [20]byte(spec.InfoHash)
	entry.Local = true
	entry.Trust = snapshot.TrustVerified
	n.Inventory.AddFile(&entry)
	return [20]byte(spec.InfoHash)
}

// PublishV2Manifest writes the next chain.v2.<seq>.toml generation
// from the node's inventory via a long-lived RollingV2Publisher, builds
// + seeds its .torrent, and returns the new V2 infohash suitable for
// the peer's ENR chain-toml entry. Subsequent calls advance seq. Older
// generations stay seedable as long as their name-sets remain a subset
// of the current inventory (validity rule, see RollingV2Publisher).
//
// The publisher is constructed lazily on first call so tests that
// never publish don't pay for it.
func (n *P2PNode) PublishV2Manifest() [20]byte {
	n.T.Helper()
	if n.v2Publisher == nil {
		torrentFS := dl.NewAtomicTorrentFS(n.Dirs.Snap)
		pub, err := dl.NewRollingV2Publisher(n.Dirs.Snap, torrentFS, n.dCore)
		require.NoError(n.T, err)
		pub.SetENRFingerprint(harnessENRFP)
		n.v2Publisher = pub
	}
	hash, err := n.v2Publisher.Publish(n.ctx, n.Inventory, 0, nil)
	require.NoError(n.T, err)
	return [20]byte(hash)
}

// V2Publisher exposes the lazily-constructed rolling V2 publisher so
// callers (e.g. EnableAutoPublishV2) can subscribe to its republish
// loop instead of constructing a parallel publisher that wouldn't
// share the rolling history.
func (n *P2PNode) V2Publisher() *dl.RollingV2Publisher { return n.v2Publisher }

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
	return NewP2PNodeAt(t, t.TempDir(), logger)
}

// NewP2PNodeWithRealStorage builds a P2PNode whose orchestrator runs
// against the production flow.InventoryStorage (real RecordFile +
// validation chain, files resolved on disk) instead of MockStorage.
// Use this for the consumer side of publisher→consumer e2e tests so the
// download → RecordFile → InitialStateReady path exercises the same code
// production does. The node's InitialStateReady channel is wired.
func NewP2PNodeWithRealStorage(t *testing.T, logger log.Logger) *P2PNode {
	t.Helper()
	return newP2PNodeAt(t, t.TempDir(), logger, true)
}

// NewP2PNodeAt is NewP2PNode with an explicit base directory. Use when
// the test needs the node's data to live on a specific filesystem —
// e.g. so hardlinks from a fixture directory on the same filesystem
// work for cheap content sharing across peers. Cleans up at test end
// the same way TempDir does.
func NewP2PNodeAt(t *testing.T, baseDir string, logger log.Logger) *P2PNode {
	return newP2PNodeAt(t, baseDir, logger, false)
}

func newP2PNodeAt(t *testing.T, baseDir string, logger log.Logger, realStorage bool) *P2PNode {
	t.Helper()
	if logger == nil {
		logger = log.New()
		logger.SetHandler(log.DiscardHandler())
	}

	dirs := datadir.New(baseDir)
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

	var mockStorage *MockStorage
	var orchStorage flow.Storage
	var inv *snapshot.Inventory
	var initialStateReady <-chan struct{}
	if realStorage {
		inv = snapshot.NewInventory()
		orchStorage = flow.NewInventoryStorage(inv, validation.DefaultStage1ChainWithDisk(dirs.Snap), dirs.Snap)
		initialStateReady = flow.InitialStateReadyChannel(bus)
	} else {
		mockStorage = NewMockStorage()
		orchStorage = mockStorage
		inv = mockStorage.Inventory()
	}
	orch := flow.NewWithStorage(bus, orchStorage, logger)

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
		T:                 t,
		Bus:               bus,
		Storage:           mockStorage,
		Inventory:         inv,
		InitialStateReady: initialStateReady,
		Downloader:        dlProvider,
		Sentry:            sp,
		Mx:                mx,
		Orch:              orch,
		Dirs:              dirs,
		dCore:             d,
		cfg:               cfg,
		pool:              pool,
		workers:           workers,
		unregPeer:         unregPeer,
		ctx:               ctx,
		cancel:            cancel,
	}
	t.Cleanup(n.Close)
	return n
}
