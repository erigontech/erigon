// Copyright 2025 The Erigon Authors
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

package engineapitester

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"net"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/eth"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/rpc/contracts"
	"github.com/erigontech/erigon/rpc/requests"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// DefaultEngineApiTester builds an engine-api tester with default settings.
// The caller must invoke EngineApiTester.Close on the returned tester to
// release the underlying resources.
func DefaultEngineApiTester(ctx context.Context, logger log.Logger, dataDir string) (EngineApiTester, error) {
	genesis, coinbasePrivKey, err := DefaultEngineApiTesterGenesis()
	if err != nil {
		return EngineApiTester{}, err
	}
	return InitialiseEngineApiTester(ctx, EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     dataDir,
		Genesis:     genesis,
		CoinbaseKey: coinbasePrivKey,
	})
}

func DefaultEngineApiTesterGenesis() (*types.Genesis, *ecdsa.PrivateKey, error) {
	coinbasePrivKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, nil, fmt.Errorf("generate coinbase key: %w", err)
	}
	coinbaseAddr := crypto.PubkeyToAddress(coinbasePrivKey.PublicKey)
	var consolidationRequestCode hexutil.Bytes
	err = consolidationRequestCode.UnmarshalText([]byte("0x3373fffffffffffffffffffffffffffffffffffffffe1460d35760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff1461019a57600182026001905f5b5f82111560685781019083028483029004916001019190604d565b9093900492505050366060146088573661019a573461019a575f5260205ff35b341061019a57600154600101600155600354806004026004013381556001015f358155600101602035815560010160403590553360601b5f5260605f60143760745fa0600101600355005b6003546002548082038060021160e7575060025b5f5b8181146101295782810160040260040181607402815460601b815260140181600101548152602001816002015481526020019060030154905260010160e9565b910180921461013b5790600255610146565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff141561017357505f5b6001546001828201116101885750505f61018e565b01600190035b5f555f6001556074025ff35b5f5ffd"))
	if err != nil {
		return nil, nil, fmt.Errorf("decode consolidation request code: %w", err)
	}
	var withdrawalRequestCode hexutil.Bytes
	err = withdrawalRequestCode.UnmarshalText([]byte("0x3373fffffffffffffffffffffffffffffffffffffffe1460cb5760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff146101f457600182026001905f5b5f82111560685781019083028483029004916001019190604d565b909390049250505036603814608857366101f457346101f4575f5260205ff35b34106101f457600154600101600155600354806003026004013381556001015f35815560010160203590553360601b5f5260385f601437604c5fa0600101600355005b6003546002548082038060101160df575060105b5f5b8181146101835782810160030260040181604c02815460601b8152601401816001015481526020019060020154807fffffffffffffffffffffffffffffffff00000000000000000000000000000000168252906010019060401c908160381c81600701538160301c81600601538160281c81600501538160201c81600401538160181c81600301538160101c81600201538160081c81600101535360010160e1565b910180921461019557906002556101a0565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff14156101cd57505f5b6001546002828201116101e25750505f6101e8565b01600290035b5f555f600155604c025ff35b5f5ffd"))
	if err != nil {
		return nil, nil, fmt.Errorf("decode withdrawal request code: %w", err)
	}
	// EIP-4788: beacon block root contract (deployed as a system contract on Cancun+ chains).
	var beaconRootsCode hexutil.Bytes
	err = beaconRootsCode.UnmarshalText([]byte("0x3373fffffffffffffffffffffffffffffffffffffffe14604d57602036146024575f5ffd5b5f35801560495762001fff810690815414603c575f5ffd5b62001fff01545f5260205ff35b5f5ffd5b62001fff42064281555f359062001fff015500"))
	if err != nil {
		return nil, nil, fmt.Errorf("decode beacon roots code: %w", err)
	}
	var chainConfig chain.Config
	err = copier.CopyWithOption(&chainConfig, chain.AllProtocolChanges, copier.Option{DeepCopy: true})
	if err != nil {
		return nil, nil, fmt.Errorf("copy chain config: %w", err)
	}
	genesis := &types.Genesis{
		Config:     &chainConfig,
		Coinbase:   coinbaseAddr,
		Difficulty: merge.ProofOfStakeDifficulty,
		GasLimit:   1_000_000_000,
		Alloc: types.GenesisAlloc{
			coinbaseAddr: {
				Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil), // 1_000 ETH
			},
			chainConfig.GetConsolidationRequestContract().Value(): {
				Code:    consolidationRequestCode, // can't be empty
				Storage: make(map[common.Hash]common.Hash),
				Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
				Nonce:   1,
			},
			chainConfig.GetWithdrawalRequestContract().Value(): {
				Code:    withdrawalRequestCode, // can't be empty
				Storage: make(map[common.Hash]common.Hash),
				Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
				Nonce:   1,
			},
			params.BeaconRootsAddress.Value(): {
				Code:    beaconRootsCode,
				Nonce:   1,
				Balance: new(big.Int),
			},
		},
	}
	return genesis, coinbasePrivKey, nil
}

// localhostEphemeral asks the kernel to pick a free TCP port on the loopback
// interface. Used by the engine-api test harness to avoid TOCTOU races with
// concurrent tests selecting the same port.
const localhostEphemeral = "127.0.0.1:0"

// closeListenerCleanup builds a cleanup callback that closes the given
// listener. Errors from a listener already closed by the http server during
// happy-path shutdown (net.ErrClosed) are dropped so the cleanup only reports
// real failures.
func closeListenerCleanup(l net.Listener) func() error {
	return func() error {
		err := l.Close()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	}
}

// cleanupHandle owns the LIFO stack of cleanup callbacks for a tester. It is
// shared by all copies of an EngineApiTester so Close is safely idempotent
// regardless of which copy the caller invokes it on.
type cleanupHandle struct {
	once     sync.Once
	cleanups []func() error
	err      error
}

func (h *cleanupHandle) close() error {
	h.once.Do(func() {
		var errs []error
		for i := len(h.cleanups) - 1; i >= 0; i-- {
			err := h.cleanups[i]()
			if err != nil {
				errs = append(errs, err)
			}
		}
		h.cleanups = nil
		h.err = errors.Join(errs...)
	})
	return h.err
}

// InitialiseEngineApiTester builds a full engine-api tester around a real
// erigon node. The supplied ctx is used during initialisation and by the node
// for its lifetime. The caller must invoke EngineApiTester.Close on the
// returned tester to release the underlying resources. On initialisation error
// any resources that have already been acquired are cleaned up before
// returning.
func InitialiseEngineApiTester(ctx context.Context, args EngineApiTesterInitArgs) (EngineApiTester, error) {
	logger := args.Logger
	// Derive a child ctx tied to the tester's lifetime so background goroutines
	// spawned by the eth backend (rpcdaemon state-change loop, etc.) terminate
	// when Close is called, even if the caller's ctx outlives this tester.
	// Tests/CLIs that create many testers in one process would otherwise leak
	// these goroutines.
	ctx, cancel := context.WithCancel(ctx)
	cleanup := &cleanupHandle{}
	success := false
	defer func() {
		if success {
			return
		}
		cancel()
		// Run accumulated cleanups LIFO if init failed before hand-off.
		err := cleanup.close()
		if err != nil {
			logger.Error("InitialiseEngineApiTester rollback cleanup error", "err", err)
		}
	}()
	addCleanup := func(fn func() error) {
		cleanup.cleanups = append(cleanup.cleanups, fn)
	}

	dirs := datadir.New(args.DataDir)
	genesis := args.Genesis

	// Pre-bind the HTTP listeners on a kernel-assigned port to avoid TOCTOU
	// port races with concurrent tests. Each listener is held open until the
	// matching server takes ownership of the socket. The cleanup callbacks are
	// safety nets if InitialiseEngineApiTester aborts before hand-off; on the
	// happy path the http server's Shutdown closes the listener first and the
	// cleanup is a silent no-op. The sentry/P2P stack picks its own kernel-
	// assigned port directly via its config below, so no pre-bind there.
	jsonRpcListener, err := net.Listen("tcp", localhostEphemeral)
	if err != nil {
		return EngineApiTester{}, fmt.Errorf("listen json-rpc: %w", err)
	}
	addCleanup(closeListenerCleanup(jsonRpcListener))
	jsonRpcPort := jsonRpcListener.Addr().(*net.TCPAddr).Port
	engineApiListener, err := net.Listen("tcp", localhostEphemeral)
	if err != nil {
		return EngineApiTester{}, fmt.Errorf("listen engine-api: %w", err)
	}
	addCleanup(closeListenerCleanup(engineApiListener))
	engineApiPort := engineApiListener.Addr().(*net.TCPAddr).Port
	logger.Debug("[engine-api-tester] selected ports", "engineApi", engineApiPort, "jsonRpc", jsonRpcPort)

	httpConfig := httpcfg.HttpCfg{
		Enabled:                  true,
		HttpServerEnabled:        true,
		WebsocketEnabled:         true,
		HttpListenAddress:        "127.0.0.1",
		HttpPort:                 jsonRpcPort,
		HttpListener:             jsonRpcListener,
		API:                      []string{"eth"},
		AuthRpcHTTPListenAddress: "127.0.0.1",
		AuthRpcPort:              engineApiPort,
		AuthRpcListener:          engineApiListener,
		JWTSecretPath:            path.Join(args.DataDir, "jwt.hex"),
		ReturnDataLimit:          100_000,
		EvmCallTimeout:           rpccfg.DefaultEvmCallTimeout,
		AuthRpcTimeouts:          rpccfg.DefaultHTTPTimeouts,
		HTTPTimeouts:             rpccfg.DefaultHTTPTimeouts,
		RpcTxSyncDefaultTimeout:  rpccfg.DefaultRpcTxSyncDefaultTimeout,
		RpcTxSyncMaxTimeout:      rpccfg.DefaultRpcTxSyncMaxTimeout,
	}

	nodeKeyConfig := p2p.NodeKeyConfig{}
	nodeKey, err := nodeKeyConfig.LoadOrGenerateAndSave(nodeKeyConfig.DefaultPath(args.DataDir))
	if err != nil {
		return EngineApiTester{}, fmt.Errorf("load/generate node key: %w", err)
	}
	mdbxDBSizeLimit := args.MdbxDBSizeLimit
	if mdbxDBSizeLimit == 0 {
		mdbxDBSizeLimit = 1 * datasize.GB
	}
	nodeConfig := nodecfg.Config{
		Dirs: dirs,
		Http: httpConfig,
		P2P: p2p.Config{
			ListenAddr:      localhostEphemeral,
			MaxPeers:        1,
			MaxPendingPeers: 1,
			NoDiscovery:     true,
			NoDial:          true,
			ProtocolVersion: []uint{direct.ETH68},
			PrivateKey:      nodeKey,
		},
		MdbxDBSizeLimit: mdbxDBSizeLimit,
		DisableSentry:   args.DisableSentry,
	}
	txPoolConfig := txpoolcfg.DefaultConfig
	txPoolConfig.DBDir = dirs.TxPool
	txPoolConfig.Disable = args.DisableTxPool
	syncDefault := ethconfig.Defaults.Sync
	syncDefault.ParallelStateFlushing = false
	ethConfig := ethconfig.Config{
		Sync: syncDefault,
		Dirs: dirs,
		Snapshot: ethconfig.BlocksFreezing{
			NoDownloader: true,
		},
		TxPool: txPoolConfig,
		Builder: buildercfg.BuilderConfig{
			EnabledPOS: true,
		},
		BatchSize:             512 * datasize.MB,
		KeepStoredChainConfig: true,
	}
	if args.EthConfigTweaker != nil {
		args.EthConfigTweaker(&ethConfig)
	}

	ethNode, err := node.New(ctx, &nodeConfig, logger)
	if err != nil {
		return EngineApiTester{}, fmt.Errorf("node.New: %w", err)
	}
	addCleanup(func() error {
		err := ethNode.Close()
		if errors.Is(err, node.ErrNodeStopped) {
			return nil
		}
		return err
	})

	chainDB, err := node.OpenDatabase(ctx, ethNode.Config(), dbcfg.ChainDB, "", false, logger)
	if err != nil {
		return EngineApiTester{}, fmt.Errorf("open chain db: %w", err)
	}
	addCleanup(func() error { chainDB.Close(); return nil })
	_, genesisBlock, err := genesiswrite.CommitGenesisBlock(chainDB, genesis, networkname.Mainnet, ethNode.Config().Dirs, logger)
	if err != nil {
		return EngineApiTester{}, fmt.Errorf("commit genesis block: %w", err)
	}
	chainDB.Close()

	// note we need to create jwt secret before calling ethBackend.Init to avoid race conditions
	jwtSecret, err := cli.ObtainJWTSecret(&httpConfig, logger)
	if err != nil {
		return EngineApiTester{}, fmt.Errorf("obtain jwt secret: %w", err)
	}
	ethBackend, err := eth.New(ctx, ethNode, &ethConfig, logger, nil)
	if err != nil {
		return EngineApiTester{}, fmt.Errorf("eth.New: %w", err)
	}
	err = ethBackend.Init(ethNode, &ethConfig, genesis.Config)
	if err != nil {
		return EngineApiTester{}, fmt.Errorf("ethBackend.Init: %w", err)
	}
	err = ethNode.Start()
	if err != nil {
		return EngineApiTester{}, fmt.Errorf("ethNode.Start: %w", err)
	}

	rpcDaemonHttpUrl := fmt.Sprintf("%s:%d", httpConfig.HttpListenAddress, httpConfig.HttpPort)
	rpcApiClient := requests.NewRequestGenerator(rpcDaemonHttpUrl, logger)
	contractBackend := contracts.NewJsonRpcBackend(rpcDaemonHttpUrl, logger)
	//goland:noinspection HttpUrlsUsage
	engineApiClientOpts := []engineapi.JsonRpcClientOption{
		// requests should not take more than 5 secs in a test env, yet we can spam frequently
		engineapi.WithJsonRpcClientRetryBackOff(50 * time.Millisecond),
		engineapi.WithJsonRpcClientMaxRetries(100),
		engineapi.WithRetryableErrCheckers(
			engineapi.ErrContainsRetryableErrChecker("connection refused"),
			// below happened on win CI
			engineapi.ErrContainsRetryableErrChecker("No connection could be made because the target machine actively refused it"),
		),
	}
	if args.EngineApiClientTimeout != nil {
		engineApiClientOpts = append(engineApiClientOpts, engineapi.WithJsonRpcClientTimeout(*args.EngineApiClientTimeout))
	}
	engineApiUrl := fmt.Sprintf("http://%s:%d", httpConfig.AuthRpcHTTPListenAddress, httpConfig.AuthRpcPort)
	engineApiClient, err := engineapi.DialJsonRpcClient(
		engineApiUrl,
		jwtSecret,
		logger,
		engineApiClientOpts...,
	)
	if err != nil {
		return EngineApiTester{}, fmt.Errorf("dial engine api: %w", err)
	}
	var mockCl *MockCl
	if args.MockClState != nil {
		mockCl = NewMockCl(logger, engineApiClient, genesisBlock, args.Genesis.Config, WithMockClState(args.MockClState))
	} else {
		mockCl = NewMockCl(logger, engineApiClient, genesisBlock, args.Genesis.Config)
	}
	if !args.NoEmptyBlock1 {
		// build 1 empty block before proceeding to properly initialise everything
		_, err = mockCl.BuildCanonicalBlock(ctx)
		if err != nil {
			return EngineApiTester{}, fmt.Errorf("build initial empty block 1: %w", err)
		}
	}
	// Cancel runs as the FIRST cleanup on Close (cleanups are LIFO, so this
	// must be appended last) — that way ctx-watching background goroutines
	// see Done before downstream resources (DB, node) are torn down.
	addCleanup(func() error { cancel(); return nil })
	success = true
	return EngineApiTester{
		GenesisBlock:         genesisBlock,
		CoinbaseKey:          args.CoinbaseKey,
		ChainConfig:          genesis.Config,
		EngineApiClient:      engineApiClient,
		RpcApiClient:         rpcApiClient,
		ContractBackend:      contractBackend,
		MockCl:               mockCl,
		Transactor:           NewTransactor(rpcApiClient, genesis.Config.ChainID),
		TxnInclusionVerifier: NewTxnInclusionVerifier(rpcApiClient),
		Node:                 ethNode,
		NodeKey:              nodeKey,
		ChainDB:              ethBackend.ChainKV(),
		cleanup:              cleanup,
	}, nil
}

type EngineApiTesterInitArgs struct {
	Logger                 log.Logger
	DataDir                string
	Genesis                *types.Genesis
	CoinbaseKey            *ecdsa.PrivateKey
	EthConfigTweaker       func(*ethconfig.Config)
	MockClState            *MockClState
	NoEmptyBlock1          bool
	EngineApiClientTimeout *time.Duration
	DisableTxPool          bool
	DisableSentry          bool
	MdbxDBSizeLimit        datasize.ByteSize
}

type EngineApiTester struct {
	GenesisBlock         *types.Block
	CoinbaseKey          *ecdsa.PrivateKey
	ChainConfig          *chain.Config
	EngineApiClient      *engineapi.JsonRpcClient
	RpcApiClient         requests.RequestGenerator
	ContractBackend      contracts.JsonRpcBackend
	MockCl               *MockCl
	Transactor           Transactor
	TxnInclusionVerifier TxnInclusionVerifier
	Node                 *node.Node
	NodeKey              *ecdsa.PrivateKey
	// ChainDB is the running backend's temporal DB. Retained on the tester so
	// callers (e.g. EngineXTestRunner.Run between tests in the same group) can
	// reach the aggregator's BranchCache and clear it without rebuilding the
	// whole tester. The reference must NOT be Closed by callers — the tester's
	// cleanup owns the lifecycle.
	ChainDB kv.RwDB
	cleanup *cleanupHandle
}

func (eat EngineApiTester) Run(t *testing.T, test func(ctx context.Context, t *testing.T, eat EngineApiTester)) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	test(t.Context(), t, eat)
}

func (eat EngineApiTester) ChainId() *uint256.Int {
	return eat.ChainConfig.ChainID
}

// Close releases all resources acquired by the tester. Cleanup callbacks run
// LIFO; an error from any callback is collected and joined into the returned
// error so a single late failure does not skip earlier cleanups. Close is
// idempotent across copies of the tester via a shared cleanup handle.
func (eat EngineApiTester) Close() error {
	if eat.cleanup == nil {
		return nil
	}
	return eat.cleanup.close()
}
