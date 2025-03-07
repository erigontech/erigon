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

//go:build integration

package shutter_test

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/downloader/downloadercfg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/devnet/requests"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/contracts"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/eth"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/turbo/engineapi"
	"github.com/erigontech/erigon/turbo/testlog"
	"github.com/erigontech/erigon/txnprovider/shutter"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestShutterBlockBuilding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		// do a goroutine dump if we haven't finished in a long time
		err := libcommon.Sleep(ctx, time.Minute)
		if err != nil {
			// means we've finished before sleep time - no need for a pprof dump
			return
		}

		err = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		require.NoError(t, err)
	}()

	uni := initBlockBuildingUniverse(ctx, t)

	t.Run("deploy first keyper set", func(t *testing.T) {
		currentBlock, err := uni.rpcApiClient.BlockNumber()
		require.NoError(t, err)
		ekg := testhelpers.MockEonKeyGeneration(t, shutter.EonIndex(1), 1, 2, currentBlock+1)
		_, _, err = uni.contractsDeployer.DeployKeyperSet(ctx, uni.contractsDeployment.Ksm, ekg)
		require.NoError(t, err)
		uni.eons[ekg.EonIndex] = ekg.Eon()
	})

	t.Run("build shutter block", func(t *testing.T) {
		sender1 := uni.acc1PrivKey
		sender2 := uni.acc2PrivKey
		receiver := uni.acc3
		amount := big.NewInt(1)

		// submit 1 shutter txn
		eon := uni.eons[1]
		encryptedSubmissionTxn, nonEncryptedTxn, err := uni.transactor.SubmitEncryptedTransfer(
			ctx,
			sender1,
			receiver,
			amount,
			eon,
		)
		require.NoError(t, err)
		block, err := uni.cl.BuildBlock(ctx)
		require.NoError(t, err)
		err = testhelpers.VerifyTxnsInclusion(block, encryptedSubmissionTxn.Hash())
		require.NoError(t, err)

		// submit 1 non shutter txn
		simpleTxn, err := uni.transactor.SubmitSimpleTransfer(sender2, receiver, amount)
		require.NoError(t, err)

		// build block and verify both txns are included (shutter at beginning of block)
		block, err = uni.cl.BuildBlock(ctx)
		require.NoError(t, err)
		err = testhelpers.VerifyTxnsOrderedInclusion(
			block,
			testhelpers.OrderedInclusion{TxnIndex: 0, TxnHash: nonEncryptedTxn.Hash()},
			testhelpers.OrderedInclusion{TxnIndex: 1, TxnHash: simpleTxn.Hash()},
		)
		require.NoError(t, err)
	})

	t.Run("build shutter block after eon change", func(t *testing.T) {
		//
		// TODO
		//
	})

	t.Run("build shutter block without breaching encrypted gas limit", func(t *testing.T) {
		//
		// TODO
		//
	})

	t.Run("build shutter block without blob txns", func(t *testing.T) {
		//
		// TODO
		//
	})
}

type blockBuildingUniverse struct {
	rpcApiClient        requests.RequestGenerator
	contractsDeployer   testhelpers.ContractsDeployer
	contractsDeployment testhelpers.ContractsDeployment
	bank                testhelpers.Bank
	cl                  *testhelpers.MockCl
	transactor          testhelpers.EncryptedTransactor
	eons                map[shutter.EonIndex]shutter.Eon
	acc1PrivKey         *ecdsa.PrivateKey
	acc1                libcommon.Address
	acc2PrivKey         *ecdsa.PrivateKey
	acc2                libcommon.Address
	acc3PrivKey         *ecdsa.PrivateKey
	acc3                libcommon.Address
}

func initBlockBuildingUniverse(ctx context.Context, t *testing.T) blockBuildingUniverse {
	logger := testlog.Logger(t, log.LvlDebug)
	dataDir := t.TempDir()
	dirs := datadir.New(dataDir)
	p2pPort, cleanP2pPort := testhelpers.ConsumeFreeTcpPort(t)
	t.Cleanup(cleanP2pPort)
	engineApiPort, cleanEngineApiPort := testhelpers.ConsumeFreeTcpPort(t)
	t.Cleanup(cleanEngineApiPort)
	jsonRpcPort, cleanJsonRpcPort := testhelpers.ConsumeFreeTcpPort(t)
	t.Cleanup(cleanJsonRpcPort)
	sentryPort, cleanSentryPort := testhelpers.ConsumeFreeTcpPort(t)
	t.Cleanup(cleanSentryPort)
	shutterPort, cleanShutterPort := testhelpers.ConsumeFreeTcpPort(t)
	t.Cleanup(cleanShutterPort)

	const localhost = "127.0.0.1"
	httpConfig := httpcfg.HttpCfg{
		Enabled:                  true,
		HttpServerEnabled:        true,
		HttpListenAddress:        localhost,
		HttpPort:                 jsonRpcPort,
		API:                      []string{"eth"},
		AuthRpcHTTPListenAddress: localhost,
		AuthRpcPort:              engineApiPort,
		JWTSecretPath:            path.Join(dataDir, "jwt.hex"),
		ReturnDataLimit:          100_000,
	}

	nodeKeyConfig := p2p.NodeKeyConfig{}
	nodeKey, err := nodeKeyConfig.LoadOrGenerateAndSave(nodeKeyConfig.DefaultPath(dataDir))
	require.NoError(t, err)
	nodeConfig := nodecfg.Config{
		Dirs: dirs,
		Http: httpConfig,
		P2P: p2p.Config{
			ListenAddr:      fmt.Sprintf("127.0.0.1:%d", p2pPort),
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

	contractDeployerPrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	contractDeployer := crypto.PubkeyToAddress(contractDeployerPrivKey.PublicKey)
	shutterConfig := shutter.ConfigByChainName(params.ChiadoChainConfig.ChainName)
	shutterConfig.Enabled = false // first we need to deploy the shutter smart contracts
	shutterConfig.BootstrapNodes = nil
	shutterConfig.PrivateKey = nodeKey
	shutterConfig.ListenPort = uint64(shutterPort)
	shutterConfig.InstanceId = 1234567890
	shutterConfig.SequencerContractAddress = crypto.CreateAddress(contractDeployer, 0).String()
	shutterConfig.KeyperSetManagerContractAddress = crypto.CreateAddress(contractDeployer, 1).String()
	shutterConfig.KeyBroadcastContractAddress = crypto.CreateAddress(contractDeployer, 2).String()

	ethConfig := ethconfig.Config{
		Dirs: dirs,
		Downloader: &downloadercfg.Cfg{
			Dirs: dirs,
		},
		TxPool: txPoolConfig,
		Miner: params.MiningConfig{
			EnabledPOS: true,
		},
		Shutter: shutterConfig,
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

	chainConfig := *params.ChiadoChainConfig
	chainConfig.ChainName = "shutter-devnet"
	chainConfig.ChainID = big.NewInt(987656789)
	chainConfig.TerminalTotalDifficulty = big.NewInt(0)
	chainConfig.ShanghaiTime = big.NewInt(0)
	chainConfig.CancunTime = big.NewInt(0)
	chainConfig.PragueTime = big.NewInt(0)
	genesis := core.ChiadoGenesisBlock()
	genesis.Timestamp = uint64(time.Now().Unix() - 1)
	genesis.Config = &chainConfig
	// 1_000 ETH in wei in the bank
	bank := testhelpers.NewBank(new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil))
	bank.RegisterGenesisAlloc(genesis)
	chainDB, err := node.OpenDatabase(ctx, ethNode.Config(), kv.ChainDB, "", false, logger)
	require.NoError(t, err)
	_, gensisBlock, err := core.CommitGenesisBlock(chainDB, genesis, ethNode.Config().Dirs, logger)
	require.NoError(t, err)
	chainDB.Close()

	ethBackend, err := eth.New(ctx, ethNode, &ethConfig, logger)
	require.NoError(t, err)
	err = ethBackend.Init(ethNode, &ethConfig, &chainConfig)
	require.NoError(t, err)
	err = ethNode.Start()
	require.NoError(t, err)

	rpcDaemonHttpUrl := fmt.Sprintf("%s:%d", httpConfig.HttpListenAddress, httpConfig.HttpPort)
	rpcApiClient := requests.NewRequestGenerator(rpcDaemonHttpUrl, logger)
	contractBackend := contracts.NewJsonRpcBackend(rpcDaemonHttpUrl, logger)
	jwtSecret, err := cli.ObtainJWTSecret(&httpConfig, logger)
	require.NoError(t, err)
	//goland:noinspection HttpUrlsUsage
	engineApiUrl := fmt.Sprintf("http://%s:%d", httpConfig.AuthRpcHTTPListenAddress, httpConfig.AuthRpcPort)
	engineApiClient, err := engineapi.DialJsonRpcClient(engineApiUrl, jwtSecret, logger)
	require.NoError(t, err)
	cl := testhelpers.NewMockCl(engineApiClient, bank.Address(), gensisBlock.Hash())
	_, err = cl.BuildBlock(ctx)
	require.NoError(t, err)

	transactor := testhelpers.NewTransactor(rpcApiClient, chainConfig.ChainID)
	acc1PrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	acc1 := crypto.PubkeyToAddress(acc1PrivKey.PublicKey)
	acc2PrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	acc2 := crypto.PubkeyToAddress(acc2PrivKey.PublicKey)
	acc3PrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	acc3 := crypto.PubkeyToAddress(acc3PrivKey.PublicKey)
	oneEth := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	topUp1, err := transactor.SubmitSimpleTransfer(bank.PrivKey(), acc1, oneEth)
	require.NoError(t, err)
	topUp2, err := transactor.SubmitSimpleTransfer(bank.PrivKey(), acc2, oneEth)
	require.NoError(t, err)
	topUp3, err := transactor.SubmitSimpleTransfer(bank.PrivKey(), acc3, oneEth)
	require.NoError(t, err)
	topUp4, err := transactor.SubmitSimpleTransfer(bank.PrivKey(), contractDeployer, oneEth)
	block, err := cl.BuildBlock(ctx)
	require.NoError(t, err)
	err = testhelpers.VerifyTxnsInclusion(block, topUp1.Hash(), topUp2.Hash(), topUp3.Hash(), topUp4.Hash())
	require.NoError(t, err)
	deployer := testhelpers.NewContractsDeployer(contractDeployerPrivKey, contractBackend, cl, chainConfig.ChainID)
	contractsDeployment, err := deployer.DeployCore(ctx)
	require.NoError(t, err)
	// these addresses are determined by the order of deployment (deployerAddr+nonce)
	// if there are mismatches need to check order in config initialization and in deployment order
	require.Equal(t, shutterConfig.SequencerContractAddress, contractsDeployment.SequencerAddr.String())
	require.Equal(t, shutterConfig.KeyperSetManagerContractAddress, contractsDeployment.KsmAddr.String())
	require.Equal(t, shutterConfig.KeyBroadcastContractAddress, contractsDeployment.KeyBroadcastAddr.String())

	// now that we've deployed all shutter contracts - we can restart erigon with shutter enabled
	err = ethNode.Close()
	require.NoError(t, err)
	shutterConfig.Enabled = true
	ethConfig.Shutter = shutterConfig
	ethNode, err = node.New(ctx, &nodeConfig, logger)
	require.NoError(t, err)
	ethBackend, err = eth.New(ctx, ethNode, &ethConfig, logger)
	require.NoError(t, err)
	err = ethBackend.Init(ethNode, &ethConfig, &chainConfig)
	require.NoError(t, err)
	err = ethNode.Start()
	require.NoError(t, err)
	t.Cleanup(cleanNode(ethNode))

	encryptedTransactor := testhelpers.NewEncryptedTransactor(transactor, shutterConfig.SequencerContractAddress, contractBackend)
	return blockBuildingUniverse{
		rpcApiClient:        rpcApiClient,
		contractsDeployer:   deployer,
		contractsDeployment: contractsDeployment,
		bank:                bank,
		cl:                  cl,
		transactor:          encryptedTransactor,
		eons:                map[shutter.EonIndex]shutter.Eon{},
		acc1PrivKey:         acc1PrivKey,
		acc1:                acc1,
		acc2PrivKey:         acc2PrivKey,
		acc2:                acc2,
		acc3PrivKey:         acc3PrivKey,
		acc3:                acc3,
	}
}
