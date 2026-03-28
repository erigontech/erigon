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

// Package sentry provides the SentryProvider — the component extracted from
// backend.go responsible for P2P networking, sentry servers/clients, status
// data, and the execution-layer P2P pipeline (message listener, peer tracker,
// publisher, backward block downloader).
package sentry

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	execp2p "github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	sentryserver "github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
	"github.com/erigontech/erigon/p2p/sentry/sentry_multi_client"
	"github.com/erigontech/erigon/polygon/bor"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
)

// Provider holds the sentry/P2P component runtime state. It implements the
// component lifecycle: Configure → Initialize.
//
// After Initialize, all public fields are ready for consumers to use.
type Provider struct {
	// Public outputs — available after Initialize.
	SentriesClient          *sentry_multi_client.MultiClient
	SentryServers           []*sentryserver.GrpcServer
	StatusDataProvider      *sentryserver.StatusDataProvider
	MessageListener         *execp2p.MessageListener
	PeerTracker             *execp2p.PeerTracker
	Publisher               *execp2p.Publisher
	BackwardBlockDownloader *execp2p.BackwardBlockDownloader
	Sentries                []sentryproto.SentryClient
	MaxBlockBroadcastPeers  func(*types.Header) uint

	// Configuration — set by Configure.
	p2pCfg        p2p.Config
	syncCfg       ethconfig.Sync
	networkID     uint64
	discoveryURLs []string
	chainName     string
	genesisHash   common.Hash
}

// Deps holds the external dependencies needed by Initialize.
type Deps struct {
	ChainDB     kv.TemporalRoDB
	ChainConfig *chain.Config
	Genesis     *types.Block
	Engine      rules.Engine
	BlockReader services.FullBlockReader
	Dirs        datadir.Dirs
	Tmpdir      string
	LogPeerInfo bool
	Logger      log.Logger
}

// Configure stores configuration. Must be called before Initialize.
func (p *Provider) Configure(
	p2pCfg p2p.Config,
	syncCfg ethconfig.Sync,
	networkID uint64,
	discoveryURLs []string,
	chainName string,
	genesisHash common.Hash,
) {
	p.p2pCfg = p2pCfg
	p.syncCfg = syncCfg
	p.networkID = networkID
	p.discoveryURLs = discoveryURLs
	p.chainName = chainName
	p.genesisHash = genesisHash
}

// Initialize creates all P2P / sentry components.
// The peer-count logging goroutine is started here (it uses ctx directly).
func (p *Provider) Initialize(ctx context.Context, deps Deps) error {
	logger := deps.Logger

	cfg := p.p2pCfg
	cfg.DiscoveryDNS = p.discoveryURLs

	if len(cfg.SentryAddr) > 0 {
		// Connect to external gRPC sentries.
		for _, addr := range cfg.SentryAddr {
			sentryClient, err := sentry_multi_client.GrpcClient(ctx, addr)
			if err != nil {
				return err
			}
			p.Sentries = append(p.Sentries, sentryClient)
		}
	} else {
		// Build in-process sentry servers.
		var readNodeInfo = func() *eth.NodeInfo {
			var res *eth.NodeInfo
			_ = deps.ChainDB.View(context.Background(), func(tx kv.Tx) error {
				res = eth.ReadNodeInfo(tx, deps.ChainConfig, p.genesisHash, p.networkID)
				return nil
			})
			return res
		}

		// Resolve chain-specific bootnodes and DNS.  Only use them when the
		// genesis hash matches the chain spec to avoid connecting to mainnet
		// bootnodes when running with a custom genesis (e.g. Hive tests).
		var chainBootnodes []string
		var chainDNSNetwork string
		if spec, err := chainspec.ChainSpecByName(p.chainName); err == nil && spec.GenesisHash == p.genesisHash {
			chainBootnodes = spec.Bootnodes
			chainDNSNetwork = spec.DNSNetwork
		}

		listenHost, listenPort, err := splitAddrIntoHostAndPort(cfg.ListenAddr)
		if err != nil {
			return err
		}

		var pi int // index into cfg.AllowedPorts
		for _, protocol := range cfg.ProtocolVersion {
			protoCfg := cfg
			protoCfg.NodeDatabase = fmt.Sprintf("%s/%s", deps.Dirs.Nodes, eth.ProtocolToString[protocol])

			var picked bool
			for ; pi < len(protoCfg.AllowedPorts); pi++ {
				pc := int(protoCfg.AllowedPorts[pi])
				if pc == 0 {
					picked = true
					break
				}
				if !checkPortIsFree(fmt.Sprintf("%s:%d", listenHost, pc)) {
					logger.Warn("bind protocol to port has failed: port is busy",
						"protocols", fmt.Sprintf("eth/%d", protoCfg.ProtocolVersion),
						"port", pc)
					continue
				}
				if listenPort != pc {
					listenPort = pc
				}
				pi++
				picked = true
				break
			}
			if !picked {
				return fmt.Errorf("run out of allowed ports for p2p eth protocols %v. Extend allowed port list via --p2p.allowed-ports", protoCfg.AllowedPorts)
			}

			protoCfg.ListenAddr = fmt.Sprintf("%s:%d", listenHost, listenPort)

			server := sentryserver.NewGrpcServer(ctx, nil, readNodeInfo, &protoCfg, protocol, logger, chainBootnodes, chainDNSNetwork)
			p.SentryServers = append(p.SentryServers, server)

			var sideProtocols []sentryproto.Protocol
			if p.p2pCfg.EnableWitProtocol {
				sideProtocols = append(sideProtocols, sentryproto.Protocol_WIT0)
			}
			sentryClient, err := direct.NewSentryClientDirect(protocol, server, sideProtocols)
			if err != nil {
				return fmt.Errorf("failed to create sentry client: %w", err)
			}
			p.Sentries = append(p.Sentries, sentryClient)
		}

		// Peer-count logging goroutine.
		go func() {
			logEvery := time.NewTicker(90 * time.Second)
			defer logEvery.Stop()

			var logItems []any
			for {
				select {
				case <-ctx.Done():
					return
				case <-logEvery.C:
					logItems = logItems[:0]
					peerCountMap := map[uint]int{}
					for _, srv := range p.SentryServers {
						counts := srv.SimplePeerCount()
						for proto, count := range counts {
							peerCountMap[proto] += count
						}
					}
					if len(peerCountMap) == 0 {
						logger.Warn("[p2p] No GoodPeers")
					} else {
						for proto, count := range peerCountMap {
							logItems = append(logItems, eth.ProtocolToString[proto], strconv.Itoa(count))
						}
						logger.Info("[p2p] GoodPeers", logItems...)
					}
				}
			}
		}()
	}

	// StatusDataProvider.
	statusDataProvider := sentryserver.NewStatusDataProvider(
		deps.ChainDB,
		deps.ChainConfig,
		deps.Genesis,
		p.networkID,
		logger,
		deps.BlockReader,
	)
	p.StatusDataProvider = statusDataProvider

	// Execution-layer P2P pipeline.
	executionSentryClient := sentryMux(p.Sentries)
	executionPeerPenalizer := execp2p.NewPeerPenalizer(executionSentryClient)
	executionMessageListener := execp2p.NewMessageListener(logger, executionSentryClient, statusDataProvider.GetStatusData, executionPeerPenalizer)
	executionPeerTracker := execp2p.NewPeerTracker(logger, executionMessageListener)
	executionMessageSender := execp2p.NewMessageSender(executionSentryClient)
	executionPublisher := execp2p.NewPublisher(logger, executionMessageSender, executionPeerTracker)

	p.MessageListener = executionMessageListener
	p.PeerTracker = executionPeerTracker
	p.Publisher = executionPublisher

	var executionFetcher execp2p.Fetcher
	executionFetcher = execp2p.NewFetcher(logger, executionMessageListener, executionMessageSender)
	executionFetcher = execp2p.NewPenalizingFetcher(logger, executionFetcher, executionPeerPenalizer)
	executionFetcher = execp2p.NewTrackingFetcher(executionFetcher, executionPeerTracker)
	p.BackwardBlockDownloader = execp2p.NewBackwardBlockDownloader(logger, executionFetcher, executionPeerPenalizer, executionPeerTracker, deps.Tmpdir)

	// maxBlockBroadcastPeers: plain 10, overridden for Bor validators.
	maxBlockBroadcastPeers := func(_ *types.Header) uint { return 10 }
	if borEngine, ok := deps.Engine.(*bor.Bor); ok {
		defaultValue := maxBlockBroadcastPeers(nil)
		maxBlockBroadcastPeers = func(header *types.Header) uint {
			isValidator, err := borEngine.IsValidator(header)
			if err != nil {
				logger.Warn("maxBlockBroadcastPeers: borEngine.IsValidator has failed", "err", err)
				return defaultValue
			}
			if isValidator {
				return 0
			}
			return defaultValue
		}
	}
	p.MaxBlockBroadcastPeers = maxBlockBroadcastPeers

	const sentryMcDisableBlockDownload = true
	var err error
	p.SentriesClient, err = sentry_multi_client.NewMultiClient(
		deps.Dirs,
		deps.ChainDB,
		deps.ChainConfig,
		deps.Engine,
		p.Sentries,
		p.syncCfg,
		deps.BlockReader,
		blockBufferSize,
		statusDataProvider,
		deps.LogPeerInfo,
		maxBlockBroadcastPeers,
		sentryMcDisableBlockDownload,
		p.p2pCfg.EnableWitProtocol,
		logger,
	)
	if err != nil {
		return err
	}

	return nil
}

// sentryMux creates a single SentryClient that multiplexes over all provided clients.
func sentryMux(sentries []sentryproto.SentryClient) sentryproto.SentryClient {
	return libsentry.NewSentryMultiplexer(sentries)
}

// blockBufferSize is the internal block buffer used by MultiClient.
const blockBufferSize = 128

// checkPortIsFree returns true when nothing is listening on addr.
func checkPortIsFree(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return true
	}
	c.Close()
	return false
}

// splitAddrIntoHostAndPort splits "host:port" into its components.
func splitAddrIntoHostAndPort(addr string) (host string, port int, err error) {
	idx := len(addr) - 1
	for idx >= 0 && addr[idx] != ':' {
		idx--
	}
	if idx < 0 {
		return "", 0, fmt.Errorf("invalid address format")
	}
	host = addr[:idx]
	port, err = strconv.Atoi(addr[idx+1:])
	return
}
