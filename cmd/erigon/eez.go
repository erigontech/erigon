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

package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cmd/erigon/node"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/eez"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1"
	eezrpc "github.com/erigontech/erigon/eez/rpc"
	eezrules "github.com/erigontech/erigon/eez/rules"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/eth"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/node/rulesconfig"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
)

// exitDiverged is the exit status of a follower that halted because its
// chain contradicts canonical L1.
const exitDiverged = 3

// exitError carries the exit status of a halted follower; the CLI exits
// with it.
type exitError struct {
	error
	code int
}

func (e exitError) ExitCode() int { return e.code }

// haltError is the error a node exits with after its follower halted.
func haltError(err error) error {
	if errors.Is(err, deriver.ErrDiverged) {
		return exitError{err, exitDiverged}
	}
	return err
}

// errTestingAPI rejects the API that builds blocks on a follower's chain.
var errTestingAPI = fmt.Errorf("%w: the testing API builds blocks; the follower must be the chain's only driver", eez.ErrConfig)

// configureFollowerNode keeps the chain under <datadir>/eez.
func configureFollowerNode(cfg *nodecfg.Config) error {
	if slices.Contains(cfg.Http.API, "testing") {
		return errTestingAPI
	}
	cfg.Dirs = eez.Dirs(cfg.Dirs)
	return nil
}

// configureFollowerEth turns off every other driver and source of the
// chain: consensus layer, engine API, txpool and snapshot downloader.
func configureFollowerEth(cfg *ethconfig.Config) {
	cfg.InternalCL = false
	cfg.NoEngineAPI = true
	cfg.TxPool.Disable = true
	cfg.Snapshot.NoDownloader = true
}

// allInOneConfigs returns the configuration of the EEZ chain cfg.Chain that
// an all-in-one node runs beside the L1 chain of l1Node and l1Eth: the L1's
// settings under eez.AllInOneDirs, off the P2P network, on its own HTTP-RPC
// port, without private API, engine API, consensus layer, txpool, downloader
// or MCP server, and with none of the L1's chain-specific settings.
func allInOneConfigs(l1Node *nodecfg.Config, l1Eth *ethconfig.Config, cfg *eez.Config) (*nodecfg.Config, *ethconfig.Config, error) {
	if slices.Contains(cfg.HTTPAPI, "testing") {
		return nil, nil, errTestingAPI
	}
	spec, err := chainspec.ChainSpecByName(cfg.Chain)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: --%s: %w", eez.ErrConfig, eez.ChainFlag.Name, err)
	}
	if _, ok := spec.Config.L2.(*config.L2Config); !ok {
		return nil, nil, fmt.Errorf("%w: --%s %s is not an EEZ network", eez.ErrConfig, eez.ChainFlag.Name, cfg.Chain)
	}
	if g := l1Eth.Genesis; g != nil && g.Config.IsL2() {
		return nil, nil, fmt.Errorf("%w: --%s with --%s names the L1 chain, not an EEZ network", eez.ErrConfig, "chain", eez.ChainFlag.Name)
	}

	n := *l1Node
	n.Dirs = eez.AllInOneDirs(l1Node.Dirs)
	n.P2P.MaxPeers, n.P2P.NoDiscovery = 0, true
	n.DisableSentry = true
	n.PrivateApiAddr = ""
	n.Http.Dirs, n.Http.DataDir = n.Dirs, n.Dirs.DataDir
	n.Http.HttpPort, n.Http.WebsocketPort = cfg.HTTPPort, cfg.HTTPPort
	n.Http.HttpURL, n.Http.HttpsURL = "", ""
	n.Http.API = cfg.HTTPAPI
	n.Http.PrivateApiAddr, n.Http.TxPoolApiAddr = "", ""
	n.Http.HttpsServerEnabled, n.Http.GRPCServerEnabled, n.Http.SocketServerEnabled = false, false, false
	n.Http.HttpListener, n.Http.AuthRpcListener = nil, nil

	e := *l1Eth
	e.Dirs = n.Dirs
	e.Genesis = spec.Genesis
	e.NetworkID = cmp.Or(spec.NetworkID, spec.Config.ChainID.Uint64())
	e.EthDiscoveryURLs = nil
	e.BadBlockHash = common.Hash{}
	e.Snapshot.ChainName = spec.Name
	e.Snapshot.NoDownloader = true
	e.Snapshot.DownloaderAddr = ""
	e.Downloader = nil
	e.ExternalSnapshotDownloaderAddr = ""
	e.Whitelist = nil
	e.OverrideOsakaTime, e.OverrideAmsterdamTime = nil, nil
	e.InternalCL = false
	e.NoEngineAPI = true
	e.TxPool.Disable = true
	e.Shutter.Enabled = false
	e.Ethstats, e.MCPAddress = "", ""
	e.ExtraAPIs = nil
	e.WarmupKzgCtxOnInit = false
	n.Http.Snap = e.Snapshot
	return &n, &e, nil
}

// startStandalone registers on n, the node of an EEZ chain, its follower,
// which reads the L1 node of cfg and syncs from L2 peers loopBlockLimit
// blocks at a time.
func startStandalone(ctx context.Context, n *node.ErigonNode, cfg *eez.Config, api *eezrpc.API, dirs datadir.Dirs, loopBlockLimit uint, logger log.Logger) (*follower.Service, error) {
	reader, err := dialL1(ctx, cfg, dirs, logger)
	if err != nil {
		return nil, err
	}
	return startFollower(ctx, n, n, reader, cfg, api, dirs, eez.Dir(dirs), loopBlockLimit, logger)
}

// allInOneTracer returns the EEZ chain's own instance of the --vmtrace tracer
// of cmd: tracer hooks keep state that the L1 and EEZ chains, executing
// concurrently, must not share.
func allInOneTracer(cmd *cli.Command) (*tracers.Tracer, error) {
	return debug.SetupTracerCtx(cmd)
}

// startAllInOne creates the EEZ chain of nodeCfg and ethCfg with its
// follower, which reads the L1 chain of l1Node in place, and its own
// --vmtrace tracer of cmd, and registers it on l1Node, which then starts it
// after and stops it before the L1 services.
func startAllInOne(ctx context.Context, l1Node *node.ErigonNode, l1Dirs datadir.Dirs, nodeCfg *nodecfg.Config, ethCfg *ethconfig.Config, cfg *eez.Config, api *eezrpc.API, logger log.Logger, cmd *cli.Command) (*follower.Service, error) {
	tracer, err := allInOneTracer(cmd)
	if err != nil {
		return nil, err
	}
	l2Node, err := node.New(ctx, nodeCfg, ethCfg, logger.New("chain", cfg.Chain), tracer)
	if err != nil {
		return nil, err
	}
	var svc *follower.Service
	reader, err := localL1(ctx, l1Node.Backend(), l1Dirs, logger)
	if err == nil {
		svc, err = startFollower(ctx, l2Node, l1Node, reader, cfg, api, nodeCfg.Dirs, nodeCfg.Dirs.DataDir, ethCfg.Sync.LoopBlockLimit, logger)
	}
	if err != nil {
		l2Node.Close()
		return nil, err
	}
	l1Node.RegisterLifecycle(l2Node)
	return svc, nil
}

// localL1 returns the reader of the L1 chain of backend, in the same process.
// Receipt regeneration uses dirs for temporary files.
func localL1(ctx context.Context, backend *eth.Ethereum, dirs datadir.Dirs, logger log.Logger) (*l1.Local, error) {
	db, ok := backend.ChainDB().(kv.TemporalRoDB)
	if !ok {
		return nil, fmt.Errorf("L1 chain database %T is not temporal", backend.ChainDB())
	}
	chainConfig := backend.ChainConfig()
	blockReader, _ := backend.BlockIO()
	engine := rulesconfig.CreateRulesEngineBareBones(ctx, chainConfig, logger)
	gen := receipts.NewGenerator(dirs, blockReader, engine, nil, time.Minute)
	return l1.NewLocal(db, blockReader, gen, engine, chainConfig, backend.Notifications().Events), nil
}

// dialL1 connects to the L1 node of cfg.
func dialL1(ctx context.Context, cfg *eez.Config, dirs datadir.Dirs, logger log.Logger) (l1.ChainReader, error) {
	if cfg.L1KV != "" {
		return l1.DialRemote(ctx, cfg.L1KV, dirs, logger)
	}
	return l1.DialRPC(ctx, cfg.L1RPC, logger)
}

// startFollower registers on n the follower of its EEZ chain, reading L1
// from reader and keeping its stores in dir. When it halts on its own it
// closes stop, the node whose exit ends the process.
func startFollower(ctx context.Context, n *node.ErigonNode, stop interface{ Close() }, reader l1.ChainReader, cfg *eez.Config, api *eezrpc.API, dirs datadir.Dirs, dir string, loopBlockLimit uint, logger log.Logger) (*follower.Service, error) {
	backend := n.Backend()
	chainConfig := backend.ChainConfig()
	if err := cfg.L2.Check(chainConfig); err != nil {
		return nil, err
	}
	l1Only, err := cfg.L1Only(chainConfig)
	if err != nil {
		return nil, err
	}
	if err := cfg.CheckChain(chainConfig); err != nil {
		return nil, err
	}
	db, ok := backend.ChainDB().(kv.TemporalRwDB)
	if !ok {
		return nil, fmt.Errorf("chain database %T is not temporal", backend.ChainDB())
	}
	engine, err := eezrules.New(chainConfig)
	if err != nil {
		return nil, err
	}
	blockReader, _ := backend.BlockIO()
	chain := deriver.NewExecChain(backend.ExecutionModule(), db, blockReader,
		receipts.NewGenerator(dirs, blockReader, engine, nil, time.Minute), chainConfig)
	fc := follower.DefaultConfig
	fc.Dir, fc.Poll, fc.ReorgDepth, fc.LogChunk = dir, cfg.L1Poll, cfg.L1ReorgDepth, cfg.L1LogChunk
	if cfg.L1Beacon != "" {
		fc.Blobs = l1.NewBeacon(cfg.L1Beacon)
	}
	if cfg.L1Blobs != "" {
		fc.Blobs = l1.NewBlobDir(cfg.L1Blobs, fc.Blobs)
	}
	if cfg.SequencerRPC != "" {
		if fc.Sequencer, err = follower.DialSequencer(ctx, cfg.SequencerRPC, logger); err != nil {
			return nil, err
		}
	}
	if !l1Only {
		fc.Peers = follower.NewFetcher(backend.BackwardBlockDownloader(), backend, chain, logger)
		fc.LoopBlockLimit = uint64(loopBlockLimit)
	}
	if cfg.P2PListen != nil {
		if fc.Heads, err = startOverlay(n, cfg, chainConfig, logger); err != nil {
			return nil, err
		}
	}
	f, err := follower.New(reader, chain, chainConfig, fc, logger)
	if err != nil {
		return nil, err
	}
	api.Attach(f)
	svc := follower.NewService(f, func(err error) {
		logger.Crit("[eez] follower halted, stopping the node", "err", err)
		go stop.Close()
	})
	n.RegisterLifecycle(svc)
	l1Source := cmp.Or(cfg.L1RPC, cfg.L1KV)
	if cfg.AllInOne() {
		l1Source = "in-process"
	}
	logger.Info("[eez] follower mode", "chaindata", dirs.Chaindata, "l1", l1Source, "l1Only", l1Only)
	return svc, nil
}
