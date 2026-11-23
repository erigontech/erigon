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
	"github.com/erigontech/erigon/eez/announce"
	"github.com/erigontech/erigon/eez/composer"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/post"
	"github.com/erigontech/erigon/eez/prove"
	eezrpc "github.com/erigontech/erigon/eez/rpc"
	eezrules "github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/eez/witness"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/eth"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/node/rulesconfig"
	"github.com/erigontech/erigon/rpc/jsonrpc"
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

// setHeadRefusal is why an EEZ node refuses debug_setHead and
// ETHBACKEND.SetHead, which would move the head behind the follower.
const setHeadRefusal = "the EEZ follower is the chain's only driver"

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
// chain: consensus layer, engine API, head moves on request, txpool and
// snapshot downloader. A sequencer keeps the txpool, without gossip or blob
// transactions.
func configureFollowerEth(cfg *ethconfig.Config, sequencer bool) {
	cfg.InternalCL = false
	cfg.NoEngineAPI = true
	cfg.SetHeadRefusal = setHeadRefusal
	cfg.TxPool.Disable = !sequencer
	cfg.Snapshot.NoDownloader = true
	if sequencer {
		cfg.TxPool.NoGossip = true
		cfg.Builder.MaxBlobsPerBlock = new(uint64(0))
		// The pool would never yield a blob transaction to a block without
		// blob gas, yet yield its sender's later ones, which wait on it.
		cfg.TxPool.TotalBlobPoolLimit = 0
	}
}

// allInOneConfigs returns the configuration of the EEZ chain cfg.Chain that
// an all-in-one node runs beside the L1 chain of l1Node and l1Eth: the L1's
// settings under eez.AllInOneDirs, with its own devp2p network
// (configureAllInOneNetwork) and HTTP-RPC port, without private API, engine
// API, consensus layer, head moves on request, txpool, downloader or MCP
// server, and with none of the L1's chain-specific settings.
func allInOneConfigs(ctx context.Context, l1Node *nodecfg.Config, l1Eth *ethconfig.Config, cfg *eez.Config, logger log.Logger) (*nodecfg.Config, *ethconfig.Config, error) {
	if slices.Contains(cfg.HTTPAPI, "testing") {
		return nil, nil, errTestingAPI
	}
	spec, err := chainspec.ChainSpecByName(cfg.Chain)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: --%s: %w", eez.ErrConfig, eez.ChainFlag.Name, err)
	}
	l2, ok := spec.Config.L2.(*config.L2Config)
	if !ok {
		return nil, nil, fmt.Errorf("%w: --%s %s is not an EEZ network", eez.ErrConfig, eez.ChainFlag.Name, cfg.Chain)
	}
	if l2.RollupX != nil && l1Eth.InternalCL && !l1Eth.CaplinConfig.ArchiveBlobs && !l1Eth.CaplinConfig.ImmediateBlobsBackfilling {
		logger.Warn("[eez] since Fulu the L1's consensus layer keeps full blobs only with --caplin.blobs-immediate-backfill or --caplin.blobs-archive, or as a supernode; without them the EEZ chain reads blobs from --eez.l1.blobs and --eez.l1.beacon alone", "chain", cfg.Chain)
	}
	if g := l1Eth.Genesis; g != nil && g.Config.IsL2() {
		return nil, nil, fmt.Errorf("%w: --%s with --%s names the L1 chain, not an EEZ network", eez.ErrConfig, "chain", eez.ChainFlag.Name)
	}

	n := *l1Node
	n.Dirs = eez.AllInOneDirs(l1Node.Dirs)
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
	e.SetHeadRefusal = setHeadRefusal
	e.TxPool.Disable = true
	e.Shutter.Enabled = false
	e.Ethstats, e.MCPAddress = "", ""
	e.ExtraAPIs = nil
	e.WarmupKzgCtxOnInit = false
	if err := configureAllInOneNetwork(ctx, &n, &e, cfg, logger); err != nil {
		return nil, nil, err
	}
	n.Http.Snap = e.Snapshot
	return &n, &e, nil
}

// startStandalone registers on n, the node of an EEZ chain, its follower,
// which reads the L1 node of cfg and syncs from L2 peers loopBlockLimit
// blocks at a time; debug serves the debug-only eez methods.
func startStandalone(ctx context.Context, n *node.ErigonNode, cfg *eez.Config, api *eezrpc.API, dirs datadir.Dirs, loopBlockLimit uint, debug bool, logger log.Logger) (halting, error) {
	reader, err := dialL1(ctx, cfg, dirs, logger)
	if err != nil {
		return nil, err
	}
	return startFollower(ctx, n, n, reader, nil, cfg, api, dirs, eez.Dir(dirs), loopBlockLimit, debug, logger)
}

// allInOneTracer returns the EEZ chain's own instance of the --vmtrace tracer
// of cmd: tracer hooks keep state that the L1 and EEZ chains, executing
// concurrently, must not share.
func allInOneTracer(cmd *cli.Command) (*tracers.Tracer, error) {
	return debug.SetupTracerCtx(cmd)
}

// startAllInOne creates the EEZ chain of nodeCfg and ethCfg with its
// follower, which reads the L1 chain of l1Node and the blobs its consensus
// layer stores in place, and its own --vmtrace tracer of cmd, and registers
// it on l1Node, which then starts it after and stops it before the L1
// services.
func startAllInOne(ctx context.Context, l1Node *node.ErigonNode, l1Dirs datadir.Dirs, nodeCfg *nodecfg.Config, ethCfg *ethconfig.Config, cfg *eez.Config, api *eezrpc.API, logger log.Logger, cmd *cli.Command) (halting, error) {
	tracer, err := allInOneTracer(cmd)
	if err != nil {
		return nil, err
	}
	l2Node, err := node.New(ctx, nodeCfg, ethCfg, logger.New("chain", cfg.Chain), tracer)
	if err != nil {
		return nil, err
	}
	var svc halting
	reader, err := localL1(ctx, l1Node.Backend(), l1Dirs, logger)
	if err == nil {
		svc, err = startFollower(ctx, l2Node, l1Node, reader, caplinBlobs(l1Node.Backend()), cfg, api, nodeCfg.Dirs, nodeCfg.Dirs.DataDir, ethCfg.Sync.LoopBlockLimit, false, logger)
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

// sentryPeers counts the peers of the sentries of backend.
func sentryPeers(backend *eth.Ethereum) func(context.Context) (uint64, error) {
	return func(ctx context.Context) (uint64, error) {
		var n uint64
		for _, s := range backend.SentryControlServer().Sentries() {
			r, err := s.PeerCount(ctx, &sentryproto.PeerCountRequest{})
			if err != nil {
				return 0, err
			}
			n += r.Count
		}
		return n, nil
	}
}

// halting is a service that can stop the node on its own; Err returns why.
type halting interface{ Err() error }

// firstHalt reports the first error among services that stopped the node.
type firstHalt []halting

func (h firstHalt) Err() error {
	for _, s := range h {
		if err := s.Err(); err != nil {
			return err
		}
	}
	return nil
}

// newSequencer returns the sequencer of the chain of backend, producing on
// f from the backend's txpool; one that halts closes stop.
func newSequencer(ctx context.Context, backend *eth.Ethereum, chain *deriver.ExecChain, f *follower.Follower, stop interface{ Close() }, cfg *eez.SequencerConfig, witnesses sequencer.Witnesses, announcements sequencer.Announcements, composer sequencer.SyncSlotComposer, logger log.Logger) (*sequencer.Service, error) {
	pool := backend.TxPool()
	if pool == nil {
		return nil, errors.New("the sequencer needs the txpool")
	}
	chainConfig := backend.ChainConfig()
	timing := sequencerTiming(chainConfig.L2.(*config.L2Config), cfg)
	budget := daBudget(cfg, timing.K())
	s, err := sequencer.New(chainConfig, sequencer.Config{Timing: timing, FeeRecipient: cfg.FeeRecipient, MaxDepth: cfg.MaxDepth, DABudget: budget, Witnesses: witnesses, Announcements: announcements, Composer: composer}, f, pool, logger)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", eez.ErrConfig, err)
	}
	// The node has yet to start the pool, so the limit applies to the
	// transactions it reloads too.
	pool.SetMaxTxnSize(sequencer.MaxTxnSize(budget))
	genesis, err := chain.HeaderByNumber(ctx, 0)
	if err != nil {
		return nil, err
	}
	halt := func(err error) {
		logger.Crit("[eez] sequencer halted, stopping the node", "err", err)
		go stop.Close()
	}
	return sequencer.NewService(s, sequencer.NewSchedule(timing, genesis.Time), halt, logger), nil
}

// daBudget is the DA budget of a block of a slot of k blocks: a slot must
// settle in one posting within the composer's gas limit.
func daBudget(cfg *eez.SequencerConfig, k uint64) int {
	gas := uint64(sequencer.MaxBatchGas)
	if cfg.Composer != nil {
		gas = cfg.Composer.MaxGas
	}
	return sequencer.DABudget(k, gas)
}

func sequencerTiming(l2 *config.L2Config, cfg *eez.SequencerConfig) sequencer.Timing {
	return sequencer.Timing{
		L1:    time.Duration(l2.L1BlockTime) * time.Second,
		L2:    time.Duration(l2.BlockInterval) * time.Second,
		Proof: cfg.ProofTime,
		Slack: cfg.SubmissionSlack,
	}
}

// newComposer returns the composer settling the blocks of chain, whose
// witnesses are in witnesses, from the derivation state of f; stop releases
// it once the sequencer has stopped.
func newComposer(ctx context.Context, chainConfig *chain.Config, cfg *eez.SequencerConfig, chain *deriver.ExecChain, witnesses *witness.Store, f *follower.Follower, logger log.Logger) (comp *composer.Composer, stop func(), err error) {
	c := cfg.Composer
	l2 := chainConfig.L2.(*config.L2Config)
	node, err := l1.DialRPC(ctx, c.L1, logger)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			node.Close()
		}
	}()
	poster, err := post.New(ctx, c.PosterKey, l2.Registry, c.PriorityFee, c.Builder, node, logger)
	if err != nil {
		return nil, nil, err
	}
	if id := poster.ChainID(); id != l2.L1ChainID {
		return nil, nil, fmt.Errorf("%w: --%s is chain %d, not the L1 %d", eez.ErrConfig, eez.L1RPCFlag.Name, id, l2.L1ChainID)
	}
	// The ECDSA proof system registers its attester's address as its key.
	prover, err := prove.Dial(c.Prover, c.Attester, common.BytesToHash(c.Attester[:]))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", eez.ErrConfig, err)
	}
	timing := sequencerTiming(l2, cfg)
	k := timing.K()
	comp = composer.New(composer.Config{
		Batcher:   &composer.Batcher{RollupID: l2.RollupID, ProofSystem: c.ProofSystem, Signer: types.LatestSignerForChainID(chainConfig.ChainID)},
		K:         k,
		MaxBlocks: composer.GridCap(c.MaxBlocks, k),
		MaxGas:    c.MaxGas,
		Proof:     timing.Proof,
		Slack:     timing.Slack,
	}, chain, witnesses, prover, poster,
		&composer.Observer{Node: node, RollupID: l2.RollupID, EEZ: l2.Registry, Every: 500 * time.Millisecond, Logger: logger},
		f.Settled, func() uint64 { return f.Status().Finalized }, f.HeadTo, logger)
	logger.Info("[eez] composer mode", "poster", poster.From(), "attester", c.Attester, "proofSystem", c.ProofSystem,
		"maxBlocks", composer.GridCap(c.MaxBlocks, k), "maxGas", c.MaxGas, "priorityFee", c.PriorityFee)
	return comp, func() {
		comp.Close()
		_ = prover.Close()
		node.Close()
	}, nil
}

// newHeadSigner returns the signer of the heads of the chain of chainConfig,
// whose blocks it reads from blocks and whose envelopes publisher, when set,
// gossips; nil without a key, when the sequencer signs nothing.
func newHeadSigner(cfg *eez.SequencerConfig, chainConfig *chain.Config, blocks sequencer.HeadBlocks, publisher sequencer.Publisher, logger log.Logger) (*sequencer.HeadSigner, error) {
	if cfg.Key == nil {
		logger.Warn("[eez] sequencer signs no heads: --"+eez.SequencerKeyFileFlag.Name+" is not set", "event", "eez.node.head_signer.disabled")
		return nil, nil
	}
	domain, err := headDomain(chainConfig)
	if err != nil {
		return nil, err
	}
	s, err := sequencer.NewHeadSigner(sequencer.SignerConfig{Key: cfg.Key, Domain: domain, Blocks: blocks, Publisher: publisher}, logger)
	if err != nil {
		return nil, err
	}
	logger.Info("[eez] sequencer signs its heads", "signer", s.Address(), "publish", publisher != nil)
	return s, nil
}

// announces reports whether the chain of chainConfig has App. D.6
// unsafe-block announcements. Only there does the sequencer's key sign them,
// and there it signs no heads, so that it never signs two schemes.
func announces(chainConfig *chain.Config) bool {
	l2, ok := chainConfig.L2.(*config.L2Config)
	return ok && config.Announces(l2.RuleSet())
}

// onStop is a lifecycle that runs f when stopped; a pointer, as the node
// compares the lifecycles it registers and funcs do not compare.
type onStop struct{ f func() }

func (*onStop) Start() error  { return nil }
func (s *onStop) Stop() error { s.f(); return nil }

// dialL1 connects to the L1 node of cfg.
func dialL1(ctx context.Context, cfg *eez.Config, dirs datadir.Dirs, logger log.Logger) (l1.ChainReader, error) {
	if cfg.L1KV != "" {
		return l1.DialRemote(ctx, cfg.L1KV, dirs, logger)
	}
	return l1.DialRPC(ctx, cfg.L1RPC, logger)
}

// blobSources returns the blob source of the follower of cfg: the
// directory of --eez.l1.blobs, then local, the L1's consensus layer in this
// process, then the beacon API of --eez.l1.beacon, each when set; nil with
// none.
func blobSources(cfg *eez.Config, local l1.BeaconStore) l1.BlobSource {
	var src l1.BlobSource
	if cfg.L1Beacon != "" {
		src = l1.NewBeacon(cfg.L1Beacon)
	}
	if local != nil {
		src = l1.NewLocalBeacon(local, src)
	}
	if cfg.L1Blobs != "" {
		src = l1.NewBlobDir(cfg.L1Blobs, src)
	}
	return src
}

// caplinBlobs returns the blob store of the consensus layer embedded in
// backend, nil without one.
func caplinBlobs(backend *eth.Ethereum) l1.BeaconStore {
	if r := backend.CaplinBlobs(); r != nil {
		return r
	}
	return nil
}

// startFollower registers on n the follower of its EEZ chain, reading L1
// from reader, blobs also from local when not nil, and keeping its stores in
// dir, and its sequencer when cfg has one, whose witnesses api serves with
// debug. Where the rule set has App. D.6 announcements, api serves those the
// sequencer signs, or those the follower takes from the sequencer it follows.
// When the follower or the sequencer halts on its own it closes stop, the node
// whose exit ends the process.
func startFollower(ctx context.Context, n *node.ErigonNode, stop interface{ Close() }, reader l1.ChainReader, local l1.BeaconStore, cfg *eez.Config, api *eezrpc.API, dirs datadir.Dirs, dir string, loopBlockLimit uint, debug bool, logger log.Logger) (halting, error) {
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
	fc.Blobs = blobSources(cfg, local)
	if cfg.SequencerRPC != "" {
		if fc.Sequencer, err = follower.DialSequencer(ctx, cfg.SequencerRPC, logger); err != nil {
			return nil, err
		}
	}
	fc.PeerCount = sentryPeers(backend)
	fetcher := follower.NewFetcher(backend.BackwardBlockDownloader(), fc.PeerCount, chain, logger)
	if !l1Only {
		fc.Peers = fetcher
		fc.LoopBlockLimit = uint64(loopBlockLimit)
	}
	var publisher sequencer.Publisher
	if cfg.P2PListen != nil {
		o, err := startOverlay(n, cfg, chainConfig, logger)
		if err != nil {
			return nil, err
		}
		fc.OverlayPeerCount = o.Peers
		if cfg.Sequencer == nil {
			fc.Heads, fc.Fetcher = o.Heads(), fetcher
		} else {
			// The sequencer writes its own head: no signed head moves it.
			publisher = o
		}
	}
	var seq *sequencer.Service
	var signer *sequencer.HeadSigner
	if cfg.Sequencer != nil {
		// Run hands heads only once started, after seq is set.
		fc.OnHead = func(h l1.Header) { seq.Head(h) }
		fc.DropUnsettledTail = cfg.Sequencer.Composer != nil
		if !announces(chainConfig) {
			if signer, err = newHeadSigner(cfg.Sequencer, chainConfig, chain, publisher, logger); err != nil {
				return nil, err
			}
		}
		if signer != nil {
			fc.OnL2Head = signer.Moved
		}
	}
	// Those the sequencer signs, or those the follower takes with its blocks.
	var announcements *announce.Store
	if announces(chainConfig) && (cfg.SequencerRPC != "" || (cfg.Sequencer != nil && cfg.Sequencer.Key != nil)) {
		if announcements, err = announce.Open(ctx, dir, logger); err != nil {
			return nil, err
		}
		fc.Announcements = announcements
	}
	var witnesses *witness.Store
	var stopComposer func()
	closeStores := func() {
		if witnesses != nil {
			witnesses.Close()
		}
		if announcements != nil {
			announcements.Close()
		}
	}
	f, err := follower.New(reader, chain, chainConfig, fc, logger)
	if err != nil {
		closeStores()
		return nil, err
	}
	if cfg.Sequencer != nil {
		if witnesses, err = witness.Open(ctx, dir, logger); err != nil {
			closeStores()
			return nil, err
		}
		capture := witness.NewCapture(jsonrpc.NewHeadWitnesses(db, blockReader, engine), witnesses, func() uint64 { return f.Status().Finalized })
		var announcer sequencer.Announcements
		if key := cfg.Sequencer.Key; key != nil && announcements != nil {
			a := announce.NewSigner(key, chainConfig.ChainID, announcements, f, func() uint64 { return f.Status().Finalized }, logger)
			announcer = a
			logger.Info("[eez] sequencer signs unsafe-block announcements", "signer", a.Address())
		} else if announces(chainConfig) {
			logger.Warn("[eez] sequencer signs no unsafe-block announcements: --"+eez.SequencerKeyFileFlag.Name+" is not set", "event", "eez.node.announcer.disabled")
		}
		var comp sequencer.SyncSlotComposer
		if cfg.Sequencer.Composer != nil {
			var c *composer.Composer
			if c, stopComposer, err = newComposer(ctx, chainConfig, cfg.Sequencer, chain, witnesses, f, logger); err != nil {
				closeStores()
				return nil, err
			}
			comp = c
		}
		if seq, err = newSequencer(ctx, backend, chain, f, stop, cfg.Sequencer, capture, announcer, comp, logger); err != nil {
			if stopComposer != nil {
				stopComposer()
			}
			closeStores()
			return nil, err
		}
		if debug {
			api.AttachWitnesses(witnesses)
		}
	}
	if announcements != nil {
		api.AttachAnnouncements(announcements)
	}
	api.Attach(f)
	if seq != nil {
		api.AttachSequencer(seq.Sequencer())
	}
	svc := follower.NewService(f, func(err error) {
		logger.Crit("[eez] follower halted, stopping the node", "err", err)
		go stop.Close()
	})
	// Lifecycles stop in reverse: the sequencer, the follower, the signer of
	// its heads, then the stores they write.
	if witnesses != nil || announcements != nil {
		n.RegisterLifecycle(&onStop{closeStores})
	}
	if stopComposer != nil {
		// Stops after the sequencer, which hands it the Sync blocks, and
		// before the witness store it proves from.
		n.RegisterLifecycle(&onStop{stopComposer})
	}
	if signer != nil {
		// Stops after the follower, the writer of the heads it signs, and
		// before the overlay, whose stop ends a publication in flight.
		n.RegisterLifecycle(signer)
	}
	n.RegisterLifecycle(svc)
	if seq != nil {
		n.RegisterLifecycle(seq)
		logger.Info("[eez] sequencer mode", "feeRecipient", cfg.Sequencer.FeeRecipient, "proofTime", cfg.Sequencer.ProofTime,
			"submissionSlack", cfg.Sequencer.SubmissionSlack, "maxDepth", cfg.Sequencer.MaxDepth)
	}
	l1Source := cmp.Or(cfg.L1RPC, cfg.L1KV)
	if cfg.AllInOne() {
		l1Source = "in-process"
	}
	logger.Info("[eez] follower mode", "chaindata", dirs.Chaindata, "l1", l1Source, "l1Only", l1Only)
	if seq != nil {
		return firstHalt{svc, seq}, nil
	}
	return svc, nil
}
