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
	"fmt"
	"path/filepath"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/sentry"
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
	// local servers. All fields below are only consulted in local mode.

	// ChainDB is queried by the readNodeInfo callback during ENR refresh.
	ChainDB kv.RoDB

	// ChainConfig, GenesisHash, NetworkID are threaded into the
	// readNodeInfo closure so each sentry server can report up-to-date
	// node metadata in its ENR record.
	ChainConfig *chain.Config
	GenesisHash common.Hash
	NetworkID   uint64

	// EthDiscoveryURLs is the DNS-discovery list advertised alongside any
	// chain-specific bootnodes.
	EthDiscoveryURLs []string

	// ChainName (e.g. "mainnet", "hoodi") selects the chainspec used to
	// look up bootnodes and DNS network. Sourced from
	// config.Snapshot.ChainName.
	ChainName string

	// NodesDir is the node database directory; the Provider appends
	// per-protocol subdirectories beneath it (e.g. "eth68", "eth69").
	NodesDir string

	// EnableWitProtocol toggles the WIT sideprotocol on the direct sentry
	// clients. Sourced from stack.Config().P2P.EnableWitProtocol.
	EnableWitProtocol bool

	Logger log.Logger
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

	// Internal
	cfg    Config
	logger log.Logger
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
// After this returns, p.Sentries is ready for multi-client construction and
// p.Servers is the list of local servers (empty in external mode).
//
// Initialize does NOT start background goroutines — call Start for that.
func (p *Provider) Initialize(ctx context.Context) error {
	if len(p.cfg.P2P.SentryAddr) > 0 {
		// External sentry: dial each address, collect the clients.
		for _, addr := range p.cfg.P2P.SentryAddr {
			sentryClient, err := sentry_multi_client.GrpcClient(p.cfg.SentryCtx, addr)
			if err != nil {
				return err
			}
			p.Sentries = append(p.Sentries, sentryClient)
		}
		return nil
	}

	// Local sentry: build one server per protocol version.
	//
	// The readNodeInfo callback is captured by each server so the ENR can be
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

	listenHost, listenPort, err := splitAddrIntoHostAndPort(p.cfg.P2P.ListenAddr)
	if err != nil {
		return err
	}

	var pi int // index into AllowedPorts
	for _, protocol := range p.cfg.P2P.ProtocolVersion {
		cfg := p.cfg.P2P
		cfg.NodeDatabase = filepath.Join(p.cfg.NodesDir, eth.ProtocolToString[protocol])

		// Pick a listen port from the allowed list.
		var picked bool
		for ; pi < len(cfg.AllowedPorts); pi++ {
			pc := int(cfg.AllowedPorts[pi])
			if pc == 0 {
				// Ephemeral port — skip the availability probe.
				picked = true
				break
			}
			if !checkPortIsFree(fmt.Sprintf("%s:%d", listenHost, pc)) {
				p.logger.Warn("bind protocol to port has failed: port is busy",
					"protocols", fmt.Sprintf("eth/%d", cfg.ProtocolVersion), "port", pc)
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
			return fmt.Errorf("run out of allowed ports for p2p eth protocols %v. Extend allowed port list via --p2p.allowed-ports", cfg.AllowedPorts)
		}

		cfg.ListenAddr = fmt.Sprintf("%s:%d", listenHost, listenPort)

		server := sentry.NewGrpcServer(p.cfg.SentryCtx, nil, readNodeInfo, &cfg, protocol, p.logger, chainBootnodes, chainDNSNetwork)
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

	return nil
}

// Start kicks off background work. No-op in this commit; subsequent commits
// migrate the peer-count logger and stream loops here.
func (p *Provider) Start(ctx context.Context) error {
	return nil
}

// Close shuts down sentry servers and cancels background goroutines.
// Safe to call multiple times.
func (p *Provider) Close() error {
	return nil
}
