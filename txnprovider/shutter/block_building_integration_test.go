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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/downloader/downloadercfg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/contracts"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/turbo/engineapi"
	enginetypes "github.com/erigontech/erigon/turbo/engineapi/engine_types"
	"github.com/erigontech/erigon/turbo/testlog"
	shuttercontracts "github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestShutterBlockBuilding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := testlog.Logger(t, log.LvlDebug)
	dataDir := t.TempDir()
	dirs := datadir.New(dataDir)
	p2pPort, cleanP2pPort := ConsumeFreeTcpPort(t)
	defer cleanP2pPort()
	engineApiPort, cleanEngineApiPort := ConsumeFreeTcpPort(t)
	defer cleanEngineApiPort()
	jsonRpcPort, cleanJsonRpcPort := ConsumeFreeTcpPort(t)
	defer cleanJsonRpcPort()

	const localhost = "127.0.0.1"
	httpConfig := httpcfg.HttpCfg{
		Enabled:                  true,
		HttpServerEnabled:        true,
		HttpListenAddress:        localhost,
		HttpPort:                 jsonRpcPort,
		API:                      []string{"eth"},
		AuthRpcHTTPListenAddress: localhost,
		AuthRpcPort:              engineApiPort,
	}

	nodeConfig := nodecfg.Config{
		Dirs: dirs,
		Http: httpConfig,
		P2P: p2p.Config{
			ListenAddr:      fmt.Sprintf("127.0.0.1:%d", p2pPort),
			MaxPeers:        1,
			MaxPendingPeers: 1,
			NoDiscovery:     true,
			NoDial:          true,
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

	stack, err := node.New(ctx, &nodeConfig, logger)
	require.NoError(t, err)

	bankPrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	bank := NativeTokenBank{privKey: bankPrivKey}
	chainConfig := *params.ChiadoChainConfig
	chainConfig.ChainName = "shutter-devnet"
	chainConfig.ChainID = big.NewInt(987656789)
	chainConfig.TerminalTotalDifficulty = big.NewInt(0)
	chainConfig.ShanghaiTime = big.NewInt(0)
	chainConfig.CancunTime = big.NewInt(0)
	chainConfig.PragueTime = big.NewInt(0)
	genesis := core.ChiadoGenesisBlock()
	genesis.Config = &chainConfig
	genesis.Alloc[bank.Address()] = types.GenesisAccount{
		Balance: big.NewInt(100_000_000_000),
	}
	chainDB, err := node.OpenDatabase(ctx, stack.Config(), kv.ChainDB, "", false, logger)
	require.NoError(t, err)
	_, gensisBlock, err := core.CommitGenesisBlock(chainDB, genesis, stack.Config().Dirs, logger)
	require.NoError(t, err)
	chainDB.Close()

	ethBackend, err := eth.New(ctx, stack, &ethConfig, logger)
	require.NoError(t, err)

	err = ethBackend.Init(stack, &ethConfig, ethBackend.ChainConfig())
	require.NoError(t, err)

	err = ethBackend.Start()
	require.NoError(t, err)

	rpcDaemonHttpUrl := fmt.Sprintf("%s:%d", httpConfig.HttpListenAddress, httpConfig.HttpPort)
	contractBackend := contracts.NewJsonRpcBackend(rpcDaemonHttpUrl, logger)
	jwtSecret, err := cli.ObtainJWTSecret(&httpConfig, logger)
	require.NoError(t, err)
	//goland:noinspection HttpUrlsUsage
	engineApiUrl := fmt.Sprintf("http://%s:%d", httpConfig.AuthRpcHTTPListenAddress, httpConfig.AuthRpcPort)
	engineApiClient, err := engineapi.DialJsonRpcClient(engineApiUrl, jwtSecret, logger)
	require.NoError(t, err)
	cl := MockCl{
		engineApiClient:       engineApiClient,
		suggestedFeeRecipient: bank.Address(),
		prevBlockHash:         gensisBlock.Hash(),
		prevRandao:            big.NewInt(0),
		prevBeaconBlockRoot:   big.NewInt(10_000),
	}
	_, err = cl.BuildBlock(ctx)
	require.NoError(t, err)

	deployer := ShutterContractsDeployer{
		contractBackend: contractBackend,
		cl:              cl,
		bank:            bank,
		chainId:         ethBackend.ChainConfig().ChainID,
	}

	err = deployer.DeploySequencer(ctx)
	require.NoError(t, err)
	err = ethBackend.Stop()
	require.NoError(t, err)
}

var consumedPorts = sync.Map{}

// ConsumeFreeTcpPort can be used by many goroutines at the same time.
// It uses port 0 and the OS to find a random free port. Note it opens the port
// so we have to close it. But closing a port makes it eligible for selection
// by the OS again. So we need to remember which ones have been already touched.
func ConsumeFreeTcpPort(t *testing.T) (int, func()) {
	var port int
	var done bool
	var iterations int
	for !done {
		func() {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer func() {
				err := listener.Close()
				require.NoError(t, err)
			}()

			port = listener.Addr().(*net.TCPAddr).Port
			_, ok := consumedPorts.Swap(port, struct{}{})
			done = !ok
		}()
		iterations++
		if iterations > 1024 {
			require.FailNow(t, "failed to find a free port", "after %d iterations", iterations)
		}
	}
	return port, func() { consumedPorts.Delete(port) }
}

type NativeTokenBank struct {
	privKey        *ecdsa.PrivateKey
	initialBalance *big.Int
}

func (b NativeTokenBank) Address() libcommon.Address {
	return crypto.PubkeyToAddress(b.privKey.PublicKey)
}

func (b NativeTokenBank) InitialBalance() *big.Int {
	return b.initialBalance
}

type ShutterContractsDeployer struct {
	contractBackend bind.ContractBackend
	cl              MockCl
	bank            NativeTokenBank
	chainId         *big.Int
}

func (d ShutterContractsDeployer) DeploySequencer(ctx context.Context) error {
	transactOpts := bind.TransactOpts{
		From: d.bank.Address(),
		Signer: func(address libcommon.Address, txn types.Transaction) (types.Transaction, error) {
			return types.SignTx(txn, *types.LatestSignerForChainID(d.chainId), d.bank.privKey)
		},
	}
	_, deployTxn, _, err := shuttercontracts.DeploySequencer(&transactOpts, d.contractBackend)
	if err != nil {
		return err
	}

	block, err := d.cl.BuildBlock(ctx)
	if err != nil {
		return err
	}

	var found bool
	for _, txnBytes := range block.Transactions {
		txn, err := types.DecodeTransaction(txnBytes)
		if err != nil {
			return err
		}
		if txn.Hash() == deployTxn.Hash() {
			found = true
			break
		}
	}
	if !found {
		return errors.New("deploy txn not found in block")
	}

	return nil
}

type MockCl struct {
	engineApiClient       *engineapi.JsonRpcClient
	suggestedFeeRecipient libcommon.Address
	prevBlockHash         libcommon.Hash
	prevRandao            *big.Int
	prevBeaconBlockRoot   *big.Int
}

func (cl MockCl) BuildBlock(ctx context.Context) (*enginetypes.ExecutionPayload, error) {
	forkChoiceState := enginetypes.ForkChoiceState{
		FinalizedBlockHash: cl.prevBlockHash,
		SafeBlockHash:      cl.prevBlockHash,
		HeadHash:           cl.prevBlockHash,
	}

	parentBeaconBlockRoot := libcommon.BigToHash(cl.prevBeaconBlockRoot)
	payloadAttributes := enginetypes.PayloadAttributes{
		Timestamp:             hexutil.Uint64(uint64(time.Now().Unix())),
		PrevRandao:            libcommon.BigToHash(cl.prevRandao),
		SuggestedFeeRecipient: cl.suggestedFeeRecipient,
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &parentBeaconBlockRoot,
	}

	fcuRes, err := cl.engineApiClient.ForkchoiceUpdatedV3(ctx, &forkChoiceState, &payloadAttributes)
	if err != nil {
		return nil, err
	}
	if fcuRes.PayloadStatus.Status != enginetypes.ValidStatus {
		return nil, fmt.Errorf("payload status is not valid: %s", fcuRes.PayloadStatus.Status)
	}

	payloadRes, err := cl.engineApiClient.GetPayloadV4(ctx, *fcuRes.PayloadId)
	if err != nil {
		return nil, err
	}

	err = libcommon.Sleep(ctx, 500*time.Millisecond)
	if err != nil {
		return nil, err
	}

	newHash := payloadRes.ExecutionPayload.BlockHash
	forkChoiceState = enginetypes.ForkChoiceState{
		FinalizedBlockHash: newHash,
		SafeBlockHash:      newHash,
		HeadHash:           newHash,
	}
	fcuRes, err = cl.engineApiClient.ForkchoiceUpdatedV3(ctx, &forkChoiceState, nil)
	if err != nil {
		return nil, err
	}
	if fcuRes.PayloadStatus.Status != enginetypes.AcceptedStatus {
		return nil, fmt.Errorf("payload status is not accepted: %s", fcuRes.PayloadStatus.Status)
	}

	cl.prevBlockHash = newHash
	cl.prevRandao.Add(cl.prevRandao, big.NewInt(1))
	cl.prevBeaconBlockRoot.Add(cl.prevBeaconBlockRoot, big.NewInt(1))
	return payloadRes.ExecutionPayload, nil
}
