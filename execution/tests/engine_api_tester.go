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

	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
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
	genesis, coinbasePrivKey := DefaultEngineApiTesterGenesis(t)
	return InitialiseEngineApiTester(t, EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, log.LvlDebug),
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbasePrivKey,
	})
}

func DefaultEngineApiTesterGenesis(t *testing.T) (*types.Genesis, *ecdsa.PrivateKey) {
	coinbasePrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	coinbaseAddr := crypto.PubkeyToAddress(coinbasePrivKey.PublicKey)
	var consolidationRequestCode hexutil.Bytes
	err = consolidationRequestCode.UnmarshalText([]byte("0x3373fffffffffffffffffffffffffffffffffffffffe1460d35760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff1461019a57600182026001905f5b5f82111560685781019083028483029004916001019190604d565b9093900492505050366060146088573661019a573461019a575f5260205ff35b341061019a57600154600101600155600354806004026004013381556001015f358155600101602035815560010160403590553360601b5f5260605f60143760745fa0600101600355005b6003546002548082038060021160e7575060025b5f5b8181146101295782810160040260040181607402815460601b815260140181600101548152602001816002015481526020019060030154905260010160e9565b910180921461013b5790600255610146565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff141561017357505f5b6001546001828201116101885750505f61018e565b01600190035b5f555f6001556074025ff35b5f5ffd"))
	require.NoError(t, err)
	var withdrawalRequestCode hexutil.Bytes
	err = withdrawalRequestCode.UnmarshalText([]byte("0x3373fffffffffffffffffffffffffffffffffffffffe1460cb5760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff146101f457600182026001905f5b5f82111560685781019083028483029004916001019190604d565b909390049250505036603814608857366101f457346101f4575f5260205ff35b34106101f457600154600101600155600354806003026004013381556001015f35815560010160203590553360601b5f5260385f601437604c5fa0600101600355005b6003546002548082038060101160df575060105b5f5b8181146101835782810160030260040181604c02815460601b8152601401816001015481526020019060020154807fffffffffffffffffffffffffffffffff00000000000000000000000000000000168252906010019060401c908160381c81600701538160301c81600601538160281c81600501538160201c81600401538160181c81600301538160101c81600201538160081c81600101535360010160e1565b910180921461019557906002556101a0565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff14156101cd57505f5b6001546002828201116101e25750505f6101e8565b01600290035b5f555f600155604c025ff35b5f5ffd"))
	require.NoError(t, err)
	var chainConfig chain.Config
	err = copier.CopyWithOption(&chainConfig, chain.AllProtocolChanges, copier.Option{DeepCopy: true})
	require.NoError(t, err)
	genesis := &types.Genesis{
		Config:     &chainConfig,
		Coinbase:   coinbaseAddr,
		Difficulty: merge.ProofOfStakeDifficulty,
		GasLimit:   1_000_000_000,
		Alloc: types.GenesisAlloc{
			coinbaseAddr: {
				Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil), // 1_000 ETH
			},
			params.ConsolidationRequestAddress: {
				Code:    consolidationRequestCode, // can't be empty
				Storage: make(map[common.Hash]common.Hash),
				Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
				Nonce:   1,
			},
			params.WithdrawalRequestAddress: {
				Code:    withdrawalRequestCode, // can't be empty'
				Storage: make(map[common.Hash]common.Hash),
				Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
				Nonce:   1,
			},
		},
	}
	return genesis, coinbasePrivKey
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
		HttpListenAddress:        "127.0.0.1",
		HttpPort:                 jsonRpcPort,
		API:                      []string{"eth"},
		AuthRpcHTTPListenAddress: "127.0.0.1",
		AuthRpcPort:              engineApiPort,
		JWTSecretPath:            path.Join(args.DataDir, "jwt.hex"),
		ReturnDataLimit:          100_000,
		EvmCallTimeout:           rpccfg.DefaultEvmCallTimeout,
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
		Miner: buildercfg.MiningConfig{
			EnabledPOS: true,
		},
		KeepStoredChainConfig:     true,
		ErigonDBStepSize:          config3.DefaultStepSize,
		ErigonDBStepsInFrozenFile: config3.DefaultStepsInFrozenFile,
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
	engineApiUrl := fmt.Sprintf("http://%s:%d", httpConfig.AuthRpcHTTPListenAddress, httpConfig.AuthRpcPort)
	engineApiClient, err := engineapi.DialJsonRpcClient(
		engineApiUrl,
		jwtSecret,
		logger,
		// requests should not take more than 5 secs in a test env, yet we can spam frequently
		engineapi.WithJsonRpcClientRetryBackOff(50*time.Millisecond),
		engineapi.WithJsonRpcClientMaxRetries(100),
		engineapi.WithRetryableErrCheckers(
			engineapi.ErrContainsRetryableErrChecker("connection refused"),
			// below happened on win CI
			engineapi.ErrContainsRetryableErrChecker("No connection could be made because the target machine actively refused it"),
		),
	)
	require.NoError(t, err)
	var mockCl *MockCl
	if args.MockClState != nil {
		mockCl = NewMockCl(logger, engineApiClient, genesisBlock, WithMockClState(args.MockClState))
	} else {
		mockCl = NewMockCl(logger, engineApiClient, genesisBlock)
	}
	_, err = mockCl.BuildCanonicalBlock(ctx) // build 1 empty block before proceeding to properly initialise everything
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
	Logger           log.Logger
	DataDir          string
	Genesis          *types.Genesis
	CoinbaseKey      *ecdsa.PrivateKey
	EthConfigTweaker func(*ethconfig.Config)
	MockClState      *MockClState
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
