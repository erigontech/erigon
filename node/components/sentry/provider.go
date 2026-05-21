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

// Package sentry provides the Sentry component — extracted from backend.go
// as part of the Erigon componentization effort.
//
// The Sentry component owns the node's P2P / DevP2P stack: sentry servers,
// the multi-sentry client, the status-data provider, and the execution-P2P
// layer (message listener, peer tracker, publisher). It supports two modes:
//   - Local: in-process sentry servers per protocol version
//   - Remote: gRPC connection(s) to external sentry processes via --sentry.api.addr
//
// Consumers access its public fields (set after Initialize) directly. The
// Provider lifecycle is:
//   - Configure: store config (cheap, no side effects)
//   - Initialize: build sentry clients, multi-client, status-data provider,
//     execution-P2P layer
//   - Start: kick off background work (stream loops, status updates, peer
//     logging)
//   - Close: shut down servers and background goroutines
package sentry

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	execp2p "github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
	"github.com/erigontech/erigon/p2p/sentry/sentry_multi_client"
)

// Config captures the external dependencies the Sentry Provider needs to
// build its P2P stack. Populated by the caller during Configure; the Provider
// reads from it during Initialize.
type Config struct {
	// SentryCtx governs the lifetime of sentry servers and any background
	// goroutines they launch (peer acceptor, message pumps, etc). Typically
	// backend.sentryCtx — cancelled when the node shuts down.
	SentryCtx context.Context

	// P2P configuration (copied per-protocol during server construction).
	P2P p2p.Config

	// External-sentry mode is triggered when P2P.SentryAddr is non-empty:
	// the Provider dials these addresses via gRPC instead of building
	// local servers. The fields below divide into two groups: those
	// required in both modes (StatusDataProvider, MultiClient, Start
	// subscriptions all need them) and those consulted only when
	// building local servers.

	// --- Required in both modes ---

	// ChainDB is consumed by StatusDataProvider (status-message
	// construction) and by the multi-sentry Client built via
	// BuildMultiClient, which opens temporal read transactions for
	// header validation during backward download. In local mode it
	// additionally backs the readNodeInfo callback for ENR refresh.
	// Must satisfy kv.TemporalRoDB.
	ChainDB kv.TemporalRoDB

	// ChainConfig, GenesisHash, NetworkID are consumed by
	// StatusDataProvider to construct status messages and by the
	// MultiClient's stream pumps. In local mode they also feed the
	// readNodeInfo closure so each sentry server can report up-to-date
	// ENR metadata.
	ChainConfig *chain.Config
	GenesisHash common.Hash
	NetworkID   uint64

	// Genesis is the canonical genesis block; required by
	// StatusDataProvider to compute fork IDs and genesis-hash checks
	// on incoming status messages. Both modes.
	Genesis *types.Block

	// BlockReader supplies current header/body access to
	// StatusDataProvider when refreshing status messages on new-head
	// notifications. Both modes.
	BlockReader services.FullBlockReader

	// --- Local-mode only ---

	// EthDiscoveryURLs is the DNS-discovery list advertised alongside
	// any chain-specific bootnodes. Not used in external-sentry mode.
	EthDiscoveryURLs []string

	// ChainName (e.g. "mainnet", "hoodi") selects the chainspec used
	// to look up bootnodes and DNS network. Sourced from
	// config.Snapshot.ChainName. Not used in external-sentry mode.
	ChainName string

	// NodesDir is the node database directory; the Provider appends
	// per-protocol subdirectories beneath it (e.g. "eth68", "eth69").
	// Not used in external-sentry mode.
	NodesDir string

	// EnableWitProtocol toggles the WIT sideprotocol on the direct
	// sentry clients we build locally. Sourced from
	// stack.Config().P2P.EnableWitProtocol. Not used in
	// external-sentry mode.
	EnableWitProtocol bool

	// Events is the node-level shards.Events instance. Start subscribes to
	// AddHeaderSubscription + AddNewSnapshotSubscription on it so
	// StatusDataProvider can refresh on chain head / snapshot changes.
	//
	// Optional: if nil, StatusDataProvider.Run is skipped. Useful for
	// tests or when the node hasn't wired Events yet.
	Events *shards.Events

	Logger  log.Logger
	Disable bool
}

// Provider is the Sentry component's runtime state. After Initialize, the
// public fields are ready for consumers. Consumers that need background work
// (peer-list updates, stream loops) must wait until after Start returns.
type Provider struct {
	// Public: populated by Initialize.

	// Servers is the list of in-process sentry GrpcServers, one per
	// protocol version. Nil in external-sentry mode.
	Servers []*sentry.GrpcServer

	// Sentries is the aggregate list of sentry clients — either direct
	// wrappers around local Servers (local mode) or gRPC clients to
	// external sentry processes (remote mode). Always populated.
	Sentries []sentryproto.SentryClient

	// Multiplexer is a single SentryClient that fans out calls across all
	// Sentries. Used by the execution-P2P layer and by polygon sync.
	Multiplexer sentryproto.SentryClient

	// Client is the multi-sentry client — ownership of the header and body
	// downloaders, peer set broadcasting, and the sentry stream loops.
	// Populated by BuildMultiClient, which is called after the consensus
	// engine is ready (engine is an input the MultiClient needs).
	Client *sentry_multi_client.MultiClient

	// StatusDataProvider supplies up-to-date peer-handshake status payloads
	// (genesis hash, fork ID, total difficulty, chain head). Consumed by the
	// MultiClient, stageloop hook, and execution-P2P layer.
	StatusDataProvider *sentry.StatusDataProvider

	// ExecutionP2PMessageListener, ExecutionP2PPeerTracker,
	// ExecutionP2PPublisher together form the execution-side P2P layer
	// sitting above the sentry multiplexer. The stageloop hook uses the
	// publisher; the Fetcher/BackwardBlockDownloader (kept outside the
	// Provider) uses listener + peer tracker.
	ExecutionP2PMessageListener *execp2p.MessageListener
	ExecutionP2PPeerTracker     *execp2p.PeerTracker
	ExecutionP2PPublisher       *execp2p.Publisher
	ExecutionP2PMessageSender   *execp2p.MessageSender
	ExecutionP2PPeerPenalizer   *execp2p.PeerPenalizer

	// Internal
	cfg             Config
	logger          log.Logger
	started         bool           // guards Start from firing twice
	eg              errgroup.Group // tracks background goroutines launched in Start
	sharedP2PServer *p2p.Server    // shared p2p.Server backing all GrpcServers in local mode; Close() stops it
}

// Configure stores the Provider's configuration. Call before Initialize.
// Cheap: no network, no file I/O, no goroutines.
func (p *Provider) Configure(cfg Config) {
	p.cfg = cfg
	p.logger = cfg.Logger
}

// Initialize builds the sentry stack. In external-sentry mode (P2P.SentryAddr
// set) it dials each external address via gRPC. In local mode it constructs
// one sentry.GrpcServer per requested protocol version, wraps each in a
// direct client, and appends them to Sentries.
//
// After this returns, p.Sentries is ready for multi-client construction,
// p.Servers is the list of local servers (empty in external mode), and
// p.StatusDataProvider / p.Multiplexer / the execution-P2P layer are
// populated in both modes — MultiClient construction assumes these are
// non-nil.
//
// Initialize does NOT start background goroutines — call Start for that.
func (p *Provider) Initialize(ctx context.Context) error {
	if p.cfg.Disable {
		p.buildStatusAndExecutionP2P()
		return nil
	}
	if len(p.cfg.P2P.SentryAddr) > 0 {
		// External sentry: dial each address, collect the clients.
		for _, addr := range p.cfg.P2P.SentryAddr {
			sentryClient, err := sentry_multi_client.GrpcClient(p.cfg.SentryCtx, addr)
			if err != nil {
				return err
			}
			p.Sentries = append(p.Sentries, sentryClient)
		}
		p.buildStatusAndExecutionP2P()
		return nil
	}

	// Local sentry: build one GrpcServer per protocol version, then back them
	// all by a single shared p2p.Server so the node publishes one ENR / one
	// listener port / one Node ID. Running a Server per protocol used to race
	// in the discovery DHT — each Server signed its own ENR under the same
	// Node ID and only the highest seq survived, so peers would dial the
	// wrong listener and inbound stuck at a fraction of MaxPeers.
	//
	// The readNodeInfo callback is captured by every sentry so the ENR can be
	// refreshed on demand from ChainDB.
	readNodeInfo := func() *eth.NodeInfo {
		var res *eth.NodeInfo
		_ = p.cfg.ChainDB.View(context.Background(), func(tx kv.Tx) error {
			res = eth.ReadNodeInfo(tx, p.cfg.ChainConfig, p.cfg.GenesisHash, p.cfg.NetworkID)
			return nil
		})
		return res
	}

	p.cfg.P2P.DiscoveryDNS = p.cfg.EthDiscoveryURLs

	// Chain-specific bootnodes/DNS are only used when the genesis hash
	// matches the chainspec — avoids connecting to mainnet bootnodes when
	// a custom genesis is in use (e.g. Hive tests).
	var chainBootnodes []string
	var chainDNSNetwork string
	if spec, err := chainspec.ChainSpecByName(p.cfg.ChainName); err == nil && spec.GenesisHash == p.cfg.GenesisHash {
		chainBootnodes = spec.Bootnodes
		chainDNSNetwork = spec.DNSNetwork
	}

	sharedCfg, err := p.buildSharedP2PConfig()
	if err != nil {
		return err
	}

	// Create one GrpcServer per protocol version. Each instance keeps its own
	// statusData / goodPeers / message streams (so the MultiClient can address
	// them by protocol), but they will all be wired to the same p2p.Server
	// below — no per-protocol port allocation here.
	for _, protocol := range p.cfg.P2P.ProtocolVersion {
		// Pass the shared p2p.Config to NewGrpcServer. The sentry will not
		// build its own Server (SetP2PServer below blocks the lazy path), but
		// it still uses cfg for things like DiscoveryDNS, NoDiscovery, etc.
		cfgCopy := sharedCfg
		server := sentry.NewGrpcServer(p.cfg.SentryCtx, nil, readNodeInfo, &cfgCopy, protocol, p.logger, chainBootnodes, chainDNSNetwork)
		p.Servers = append(p.Servers, server)

		var sideProtocols []sentryproto.Protocol
		if p.cfg.EnableWitProtocol {
			sideProtocols = append(sideProtocols, sentryproto.Protocol_WIT0)
		}
		sentryClient, err := direct.NewSentryClientDirect(protocol, server, sideProtocols)
		if err != nil {
			return fmt.Errorf("failed to create sentry client: %w", err)
		}
		p.Sentries = append(p.Sentries, sentryClient)
	}

	// Build the single shared p2p.Server and inject it into every GrpcServer.
	if err := p.startSharedP2PServer(&sharedCfg, chainBootnodes, chainDNSNetwork); err != nil {
		return err
	}

	p.buildStatusAndExecutionP2P()
	return nil
}

// buildSharedP2PConfig returns the p2p.Config used by the single shared
// p2p.Server. It picks one listener port (honouring AllowedPorts for fallback)
// and points NodeDatabase at a single unified directory.
func (p *Provider) buildSharedP2PConfig() (p2p.Config, error) {
	cfg := p.cfg.P2P

	// Single enode database. Previously each protocol Server had its own
	// (eth68/, eth69/, …); with one Server we use one DB. Existing
	// per-protocol dirs become inert on upgrade — peer discovery rebuilds
	// quickly from bootnodes.
	cfg.NodeDatabase = filepath.Join(p.cfg.NodesDir, "eth")

	listenHost, listenPort, err := splitAddrIntoHostAndPort(cfg.ListenAddr)
	if err != nil {
		return cfg, err
	}

	if len(cfg.AllowedPorts) == 0 {
		// Caller passed an explicit ListenAddr only — honour it as-is.
		cfg.ListenAddr = fmt.Sprintf("%s:%d", listenHost, listenPort)
		return cfg, nil
	}

	// checkPortIsFree dials the target, so unspecified bind addresses (empty,
	// 0.0.0.0, ::, [::]) would all fail the dial and falsely report the
	// port as free. Normalize to a concrete loopback target for the probe
	// only — the returned ListenAddr keeps the original host so the
	// listener still binds on the configured interface (all interfaces
	// if it was empty).
	probeHost := loopbackProbeHost(listenHost)

	picked := false
	for _, pc := range cfg.AllowedPorts {
		pcInt := int(pc)
		if pcInt == 0 {
			listenPort = 0 // ephemeral; OS picks a port at bind time
			picked = true
			break
		}
		if !checkPortIsFree(fmt.Sprintf("%s:%d", probeHost, pcInt)) {
			p.logger.Warn("[p2p] candidate listen port is busy", "port", pcInt)
			continue
		}
		listenPort = pcInt
		picked = true
		break
	}
	if !picked {
		return cfg, fmt.Errorf("sentry provider: every entry in --p2p.allowed-ports is busy %v; extend the list or free a port", cfg.AllowedPorts)
	}
	cfg.ListenAddr = fmt.Sprintf("%s:%d", listenHost, listenPort)
	return cfg, nil
}

// startSharedP2PServer collects the per-protocol Protocols registered by each
// GrpcServer, builds one p2p.Server with the union (deduplicated by
// name+version so sideprotocols like wit aren't registered N times), starts
// it, and injects it back into every GrpcServer.
//
// Each GrpcServer reports peers via its own goodPeers map (the peers whose
// eth Protocol.Run fired on that sentry), so admin_peers aggregation across
// sentries is naturally non-duplicating and the multi-sentry message router
// maps peers to the correct sentry by negotiated protocol version.
func (p *Provider) startSharedP2PServer(cfg *p2p.Config, chainBootnodes []string, chainDNSNetwork string) error {
	if len(p.Servers) == 0 {
		return errors.New("sentry provider: no GrpcServers to back with a shared p2p.Server")
	}

	// Chain-specific bootnodes — only seeded when the caller hasn't supplied
	// any (matches the legacy makeP2PServer behaviour). An explicit empty
	// slice from --bootnodes= is preserved.
	if cfg.BootstrapNodes == nil && len(chainBootnodes) > 0 {
		bootstrapNodes, err := enode.ParseNodesFromURLs(chainBootnodes)
		if err != nil {
			return fmt.Errorf("sentry provider: parse chain bootnodes: %w", err)
		}
		cfg.BootstrapNodes = bootstrapNodes
		cfg.BootstrapNodesV5 = bootstrapNodes
	}

	// Apply DNS discovery to each Protocol's DialCandidates, then merge.
	if !cfg.NoDiscovery && len(cfg.DiscoveryDNS) == 0 && chainDNSNetwork != "" {
		cfg.DiscoveryDNS = []string{chainDNSNetwork}
	}

	seen := make(map[string]struct{}, 4)
	var protocols []p2p.Protocol
	for _, ss := range p.Servers {
		for i := range ss.Protocols {
			proto := ss.Protocols[i]
			key := fmt.Sprintf("%s/%d", proto.Name, proto.Version)
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			if !cfg.NoDiscovery && len(cfg.DiscoveryDNS) > 0 && proto.DialCandidates == nil {
				dialCandidates, err := sentry.SetupDNSDiscovery(cfg.DiscoveryDNS)
				if err != nil {
					return fmt.Errorf("sentry provider: setup DNS discovery for %s: %w", key, err)
				}
				proto.DialCandidates = dialCandidates
			}
			protocols = append(protocols, proto)
		}
	}

	cfg.Protocols = protocols
	srv := &p2p.Server{Config: *cfg}
	if err := srv.Start(p.cfg.SentryCtx, p.logger); err != nil {
		srv.Stop()
		return fmt.Errorf("sentry provider: start shared p2p server: %w", err)
	}
	p.sharedP2PServer = srv

	for _, ss := range p.Servers {
		if err := ss.SetP2PServer(srv); err != nil {
			// Inject failed after Start: tear the listener back down so
			// Initialize returns without leaking goroutines / sockets.
			srv.Stop()
			p.sharedP2PServer = nil
			return fmt.Errorf("sentry provider: inject shared p2p server: %w", err)
		}
	}
	return nil
}

// buildStatusAndExecutionP2P constructs the StatusDataProvider, the shared
// sentry multiplexer, and the execution-P2P layer built on top of it
// (MessageListener, PeerTracker, Publisher, plus their penalizer and sender
// helpers). Called at the end of Initialize once p.Sentries is populated.
//
// Both external- and local-sentry modes end up here — the multiplexer and
// exec-P2P layer work the same way regardless of where Sentries came from.
func (p *Provider) buildStatusAndExecutionP2P() {
	p.StatusDataProvider = sentry.NewStatusDataProvider(
		p.cfg.ChainDB,
		p.cfg.ChainConfig,
		p.cfg.Genesis,
		p.cfg.NetworkID,
		p.logger,
		p.cfg.BlockReader,
	)

	p.Multiplexer = libsentry.NewSentryMultiplexer(p.Sentries)

	p.ExecutionP2PPeerPenalizer = execp2p.NewPeerPenalizer(p.Multiplexer)
	p.ExecutionP2PMessageListener = execp2p.NewMessageListener(
		p.logger, p.Multiplexer, p.StatusDataProvider.GetStatusData, p.ExecutionP2PPeerPenalizer,
	)
	p.ExecutionP2PPeerTracker = execp2p.NewPeerTracker(p.logger, p.ExecutionP2PMessageListener)
	p.ExecutionP2PMessageSender = execp2p.NewMessageSender(p.Multiplexer)
	p.ExecutionP2PPublisher = execp2p.NewPublisher(
		p.logger, p.ExecutionP2PMessageSender, p.ExecutionP2PPeerTracker,
	)
}

// MultiClientDeps gathers the late-binding inputs needed to construct the
// multi-sentry Client. These aren't known at Configure/Initialize time
// because the consensus engine and the per-chain max-peers callback are
// built AFTER sentries (polygon heimdall + engine rules come between).
// Callers run BuildMultiClient once those are ready.
type MultiClientDeps struct {
	// Dirs is the datadir root; the MultiClient uses it for per-sentry
	// peer persistence and any local caches.
	Dirs datadir.Dirs

	// Engine is the consensus engine (ethash, clique, Bor, Aura, etc).
	// The MultiClient uses it to validate incoming headers during
	// anchor-based backward download.
	Engine rules.Engine

	// SyncCfg carries the staged-sync configuration (batch sizes, etc).
	SyncCfg ethconfig.Sync

	// BlockBufferSize bounds the number of unseen blocks held while waiting
	// for headers to catch up. Pass 0 to use the package default (128).
	BlockBufferSize int

	// LogPeerInfo enables verbose peer-info logging in the MultiClient.
	LogPeerInfo bool

	// MaxBlockBroadcastPeers decides how many peers a NewBlock
	// announcement is gossiped to (header-aware so Bor validators can
	// override the default cap).
	MaxBlockBroadcastPeers func(*types.Header) uint

	// DisableBlockDownload suppresses the header + body downloaders inside
	// the MultiClient. Pass true when blocks are supplied via another path
	// (CL engine, staged sync headers stage).
	DisableBlockDownload bool
}

// BuildMultiClient constructs the multi-sentry Client. Must be called after
// Initialize (which populates Sentries + StatusDataProvider) and once the
// late-binding deps (engine, max-broadcast-peers callback) are ready.
//
// On success, p.Client is ready for consumers.
func (p *Provider) BuildMultiClient(deps MultiClientDeps) error {
	bufSize := deps.BlockBufferSize
	if bufSize == 0 {
		bufSize = sentry_multi_client.DefaultBlockBufferSize
	}

	client, err := sentry_multi_client.NewMultiClient(
		deps.Dirs,
		p.cfg.ChainDB,
		p.cfg.ChainConfig,
		deps.Engine,
		p.Sentries,
		deps.SyncCfg,
		p.cfg.BlockReader,
		bufSize,
		p.StatusDataProvider,
		deps.LogPeerInfo,
		deps.MaxBlockBroadcastPeers,
		deps.DisableBlockDownload,
		p.cfg.EnableWitProtocol,
		p.logger,
	)
	if err != nil {
		return fmt.Errorf("sentry: build multi-client: %w", err)
	}
	p.Client = client
	return nil
}

// Start kicks off background work:
//   - MultiClient stream loops (the sentry→MultiClient gRPC pumps).
//   - StatusDataProvider.Run, which refreshes its cached status message
//     when the chain head or snapshot set changes.
//   - Execution-P2P layer goroutines (MessageListener, PeerTracker,
//     Publisher).
//   - Peer-count logger (local-sentry mode only; logs the set of
//     good-peer counts every 90 seconds).
//
// Requires Initialize (and, for stream loops, BuildMultiClient) to have
// completed successfully. Subsequent calls are a no-op.
//
// The ctx passed here should be Config.SentryCtx in practice — it's the
// context the goroutines honour for shutdown. Caller context cancellation
// doesn't need to match; the Provider follows the SentryCtx it was
// configured with.
func (p *Provider) Start(ctx context.Context) error {
	if p.started {
		return nil
	}
	p.started = true

	// Stream loops — only meaningful if BuildMultiClient has run.
	if p.Client != nil {
		p.Client.StartStreamLoops(p.cfg.SentryCtx)
		// Small sleep to keep startup-log order readable; identical to the
		// legacy backend.go behaviour.
		time.Sleep(10 * time.Millisecond)
	}

	// StatusDataProvider refresh loop, gated on Events being wired.
	if p.StatusDataProvider != nil && p.cfg.Events != nil {
		headersCh, unsubHeaders := p.cfg.Events.AddHeaderSubscription()
		snapshotsCh, unsubSnapshots := p.cfg.Events.AddNewSnapshotSubscription()
		p.eg.Go(func() error {
			defer unsubHeaders()
			defer unsubSnapshots()
			p.StatusDataProvider.Run(p.cfg.SentryCtx, headersCh, snapshotsCh)
			return nil
		})
	}

	// Execution-P2P layer goroutines. Each Run returns on context cancel;
	// non-cancel errors are logged but don't propagate (matches legacy
	// backend.go behaviour — a peer-tracker error shouldn't bring the
	// whole node down).
	if p.ExecutionP2PMessageListener != nil && p.ExecutionP2PPeerTracker != nil && p.ExecutionP2PPublisher != nil {
		p.eg.Go(func() error {
			defer p.logger.Info("[p2p] MessageListener goroutine terminated")
			err := p.ExecutionP2PMessageListener.Run(p.cfg.SentryCtx)
			if err != nil && !errors.Is(err, context.Canceled) {
				p.logger.Error("[p2p] MessageListener failed", "err", err)
			}
			return err
		})
		p.eg.Go(func() error {
			defer p.logger.Info("[p2p] PeerTracker goroutine terminated")
			err := p.ExecutionP2PPeerTracker.Run(p.cfg.SentryCtx)
			if err != nil && !errors.Is(err, context.Canceled) {
				p.logger.Error("[p2p] PeerTracker failed", "err", err)
			}
			return err
		})
		p.eg.Go(func() error {
			defer p.logger.Info("[p2p] publisher goroutine terminated")
			err := p.ExecutionP2PPublisher.Run(p.cfg.SentryCtx)
			if err != nil && !errors.Is(err, context.Canceled) {
				p.logger.Error("[p2p] publisher failed", "err", err)
			}
			return err
		})
	}

	// Peer-count logger — local-sentry mode only (no local Servers in
	// remote mode, nothing to count). Was an anonymous goroutine in
	// backend.go right after server construction; moves here as part of
	// the lifecycle consolidation.
	if len(p.Servers) > 0 {
		p.eg.Go(p.runPeerCountLogger)
	}

	return nil
}

// runPeerCountLogger periodically emits the sum of "good peers" across all
// local sentry servers, grouped by protocol version. Exits when SentryCtx
// is cancelled.
func (p *Provider) runPeerCountLogger() error {
	logEvery := time.NewTicker(90 * time.Second)
	defer logEvery.Stop()

	var logItems []any
	for {
		select {
		case <-p.cfg.SentryCtx.Done():
			return nil
		case <-logEvery.C:
			logItems = logItems[:0]
			peerCountMap := map[uint]int{}
			for _, srv := range p.Servers {
				counts := srv.SimplePeerCount()
				for protocol, count := range counts {
					peerCountMap[protocol] += count
				}
			}
			if len(peerCountMap) == 0 {
				p.logger.Warn("[p2p] No GoodPeers")
			} else {
				for protocol, count := range peerCountMap {
					logItems = append(logItems, eth.ProtocolToString[protocol], strconv.Itoa(count))
				}
				p.logger.Info("[p2p] GoodPeers", logItems...)
			}
		}
	}
}

// Close shuts down the sentry stack:
//   - Closes each local sentry GrpcServer (no-op in external mode).
//   - Waits for background goroutines spawned by Start to finish.
//
// Background goroutines exit when Config.SentryCtx is cancelled; Close
// blocks until they drain. Close does NOT cancel SentryCtx itself — the
// caller owns context lifetime (typically by cancelling backend.sentryCtx
// during node shutdown).
//
// Safe to call multiple times.
func (p *Provider) Close() error {
	for _, srv := range p.Servers {
		srv.Close()
	}
	// Stop the shared p2p.Server (if any). Each GrpcServer.Close above is a
	// no-op for externally-owned Servers, so the listener and discovery
	// goroutines would otherwise outlive Provider.Close until SentryCtx is
	// cancelled.
	if p.sharedP2PServer != nil {
		p.sharedP2PServer.Stop()
		p.sharedP2PServer = nil
	}
	return p.eg.Wait()
}
