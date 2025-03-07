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
	"fmt"
	"math/big"
	"path"
	"testing"

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
		encryptedSubmissionTxn, baseTxn, err := uni.transactor.SubmitEncryptedTransfer(ctx, sender1, receiver, amount, eon)
		require.NoError(t, err)
		block, err := uni.cl.BuildBlock(ctx)
		require.NoError(t, err)
		err = testhelpers.VerifyTxnsInclusion(block, encryptedSubmissionTxn.Hash())
		require.NoError(t, err)

		// submit 1 non shutter txn
		simpleTxn, err := uni.transactor.SubmitSimpleTransfer(sender2, receiver, amount)
		require.NoError(t, err)

		// build block and verify both txns are included
		block, err = uni.cl.BuildBlock(ctx)
		require.NoError(t, err)
		err = testhelpers.VerifyTxnsInclusion(block, baseTxn.Hash(), simpleTxn.Hash())
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

	err := uni.ethBackend.Stop()
	require.NoError(t, err)
}

type blockBuildingUniverse struct {
	rpcApiClient        requests.RequestGenerator
	contractsDeployer   testhelpers.ContractsDeployer
	contractsDeployment testhelpers.ContractsDeployment
	ethBackend          *eth.Ethereum
	bank                testhelpers.Bank
	cl                  *testhelpers.MockCl
	transactor          testhelpers.Transactor
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
	defer cleanSentryPort()

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

	ethConfig := ethconfig.Config{
		Dirs: dirs,
		Downloader: &downloadercfg.Cfg{
			Dirs: dirs,
		},
		TxPool: txPoolConfig,
		Miner: params.MiningConfig{
			EnabledPOS: true,
		},
	}

	ethNode, err := node.New(ctx, &nodeConfig, logger)
	require.NoError(t, err)

	chainConfig := *params.ChiadoChainConfig
	chainConfig.ChainName = "shutter-devnet"
	chainConfig.ChainID = big.NewInt(987656789)
	chainConfig.TerminalTotalDifficulty = big.NewInt(0)
	chainConfig.ShanghaiTime = big.NewInt(0)
	chainConfig.CancunTime = big.NewInt(0)
	chainConfig.PragueTime = big.NewInt(0)
	genesis := core.ChiadoGenesisBlock()
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

	err = ethBackend.Start()
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

	deployer := testhelpers.NewContractsDeployer(contractBackend, cl, bank, chainConfig.ChainID)
	contractsDeployment, err := deployer.DeployCore(ctx)
	require.NoError(t, err)

	transactor := testhelpers.NewTransactor(rpcApiClient, chainConfig.ChainID, contractsDeployment.Sequencer)
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
	block, err := cl.BuildBlock(ctx)
	require.NoError(t, err)
	err = testhelpers.VerifyTxnsInclusion(block, topUp1.Hash(), topUp2.Hash(), topUp3.Hash())
	require.NoError(t, err)

	return blockBuildingUniverse{
		ethBackend:          ethBackend,
		rpcApiClient:        rpcApiClient,
		contractsDeployer:   deployer,
		contractsDeployment: contractsDeployment,
		bank:                bank,
		cl:                  cl,
		transactor:          transactor,
		eons:                map[shutter.EonIndex]shutter.Eon{},
		acc1PrivKey:         acc1PrivKey,
		acc1:                acc1,
		acc2PrivKey:         acc2PrivKey,
		acc2:                acc2,
		acc3PrivKey:         acc3PrivKey,
		acc3:                acc3,
	}
}
