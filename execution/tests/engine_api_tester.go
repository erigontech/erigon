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

package executiontests

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/testutil"
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

func DefaultEngineApiTester(t *testing.T) EngineApiTester {
	genesis, coinbasePrivKey := blockgen.DefaultEngineApiTesterGenesis(t)
	return InitialiseEngineApiTester(t, EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, log.LvlDebug),
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbasePrivKey,
	})
}

func InitialiseEngineApiTester(t *testing.T, args EngineApiTesterInitArgs) EngineApiTester {
	ctx := t.Context()
	logger := args.Logger
	dirs := datadir.New(args.DataDir)
	genesis := args.Genesis
	sentryPort, err := testutil.NextFreePort()
	require.NoError(t, err)
	engineApiPort, err := testutil.NextFreePort()
	require.NoError(t, err)
	jsonRpcPort, err := testutil.NextFreePort()
	require.NoError(t, err)
	logger.Debug("[engine-api-tester] selected ports", "sentry", sentryPort, "engineApi", engineApiPort, "jsonRpc", jsonRpcPort)

	httpConfig := httpcfg.HttpCfg{
		Enabled:                  true,
		HttpServerEnabled:        true,
		WebsocketEnabled:         true,
		HttpListenAddress:        "127.0.0.1",
		HttpPort:                 jsonRpcPort,
		API:                      []string{"eth"},
		AuthRpcHTTPListenAddress: "127.0.0.1",
		AuthRpcPort:              engineApiPort,
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
	require.NoError(t, err)
	nodeConfig := nodecfg.Config{
		Dirs: dirs,
		Http: httpConfig,
		P2P: p2p.Config{
			ListenAddr:      fmt.Sprintf("127.0.0.1:%d", sentryPort),
			MaxPeers:        1,
			MaxPendingPeers: 1,
			NoDiscovery:     true,
			NoDial:          true,
			ProtocolVersion: []uint{direct.ETH68},
			AllowedPorts:    []uint{uint(sentryPort)},
			PrivateKey:      nodeKey,
		},
	}
	txPoolConfig := txpoolcfg.DefaultConfig
	txPoolConfig.DBDir = dirs.TxPool
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
		KeepStoredChainConfig: true,
	}
	if args.EthConfigTweaker != nil {
		args.EthConfigTweaker(&ethConfig)
	}

	ethNode, err := node.New(ctx, &nodeConfig, logger)
	require.NoError(t, err)
	cleanNode := func(ethNode *node.Node) func() {
		return func() {
			err := ethNode.Close()
			if errors.Is(err, node.ErrNodeStopped) {
				return
			}
			require.NoError(t, err)
		}
	}
	t.Cleanup(cleanNode(ethNode))

	chainDB, err := node.OpenDatabase(ctx, ethNode.Config(), dbcfg.ChainDB, "", false, logger)
	require.NoError(t, err)
	t.Cleanup(chainDB.Close)
	_, genesisBlock, err := genesiswrite.CommitGenesisBlock(chainDB, genesis, ethNode.Config().Dirs, logger)
	require.NoError(t, err)
	chainDB.Close()

	// note we need to create jwt secret before calling ethBackend.Init to avoid race conditions
	jwtSecret, err := cli.ObtainJWTSecret(&httpConfig, logger)
	require.NoError(t, err)
	ethBackend, err := eth.New(ctx, ethNode, &ethConfig, logger, nil)
	require.NoError(t, err)
	err = ethBackend.Init(ethNode, &ethConfig, genesis.Config)
	require.NoError(t, err)
	err = ethNode.Start()
	require.NoError(t, err)

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
	require.NoError(t, err)
	var mockCl *MockCl
	if args.MockClState != nil {
		mockCl = NewMockCl(ctx, logger, engineApiClient, ethBackend.StateDiffClient(), genesisBlock, WithMockClState(args.MockClState))
	} else {
		mockCl = NewMockCl(ctx, logger, engineApiClient, ethBackend.StateDiffClient(), genesisBlock)
	}
	if !args.NoEmptyBlock1 {
		// build 1 empty block before proceeding to properly initialise everything
		_, err = mockCl.BuildCanonicalBlock(ctx)
	}
	require.NoError(t, err)
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
	}
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
}

func (eat EngineApiTester) Run(t *testing.T, test func(ctx context.Context, t *testing.T, eat EngineApiTester)) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	test(t.Context(), t, eat)
}

func (eat EngineApiTester) ChainId() *big.Int {
	return eat.ChainConfig.ChainID
}

func (eat EngineApiTester) Close(t *testing.T) {
	err := eat.Node.Close()
	if errors.Is(err, node.ErrNodeStopped) {
		return
	}
	require.NoError(t, err)
}
