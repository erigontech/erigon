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
	"path"
	"sync"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/downloader/downloadercfg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/cmd/devnet/requests"
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
	"github.com/erigontech/erigon/txnprovider/shutter"
	shuttercontracts "github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
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
	sentryPort, cleanSentryPort := ConsumeFreeTcpPort(t)
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

	stack, err := node.New(ctx, &nodeConfig, logger)
	require.NoError(t, err)

	// 1_000 ETH in wei in the bank
	bank := NewNativeTokenBank(new(big.Int).Mul(big.NewInt(1_000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
	chainConfig := *params.ChiadoChainConfig
	chainConfig.ChainName = "shutter-devnet"
	chainConfig.ChainID = big.NewInt(987656789)
	chainConfig.TerminalTotalDifficulty = big.NewInt(0)
	chainConfig.ShanghaiTime = big.NewInt(0)
	chainConfig.CancunTime = big.NewInt(0)
	chainConfig.PragueTime = big.NewInt(0)
	genesis := core.ChiadoGenesisBlock()
	genesis.Config = &chainConfig
	genesis.Alloc[bank.Address()] = types.GenesisAccount{Balance: bank.InitialBalance()}
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
	rpcDaemonClient := requests.NewRequestGenerator(rpcDaemonHttpUrl, logger)
	contractBackend := contracts.NewJsonRpcBackend(rpcDaemonHttpUrl, logger)
	jwtSecret, err := cli.ObtainJWTSecret(&httpConfig, logger)
	require.NoError(t, err)
	//goland:noinspection HttpUrlsUsage
	engineApiUrl := fmt.Sprintf("http://%s:%d", httpConfig.AuthRpcHTTPListenAddress, httpConfig.AuthRpcPort)
	engineApiClient, err := engineapi.DialJsonRpcClient(engineApiUrl, jwtSecret, logger)
	require.NoError(t, err)
	cl := NewMockCl(engineApiClient, bank.Address(), gensisBlock.Hash())
	_, err = cl.BuildBlock(ctx)
	require.NoError(t, err)

	deployer := NewShutterContractsDeployer(contractBackend, cl, bank, chainConfig.ChainID)
	shutterContractsDeployment, err := deployer.DeployCoreContracts(ctx)
	require.NoError(t, err)

	currentBlock, err := rpcDaemonClient.BlockNumber()
	require.NoError(t, err)
	ekg := testhelpers.MockEonKeyGeneration(t, shutter.EonIndex(1), 1, 2, currentBlock+1)
	_, _, err = deployer.DeployKeyperSet(ctx, shutterContractsDeployment.Ksm, ekg)
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

func NewNativeTokenBank(initialBalance *big.Int) NativeTokenBank {
	privKey, err := crypto.GenerateKey()
	if err != nil {
		panic(err)
	}

	return NativeTokenBank{
		privKey:        privKey,
		initialBalance: initialBalance,
	}
}

func (b NativeTokenBank) Address() libcommon.Address {
	return crypto.PubkeyToAddress(b.privKey.PublicKey)
}

func (b NativeTokenBank) InitialBalance() *big.Int {
	return b.initialBalance
}

type ShutterContractsDeployer struct {
	contractBackend bind.ContractBackend
	cl              *MockCl
	bank            NativeTokenBank
	chainId         *big.Int
}

func NewShutterContractsDeployer(cb bind.ContractBackend, cl *MockCl, bank NativeTokenBank, chainId *big.Int) ShutterContractsDeployer {
	return ShutterContractsDeployer{
		contractBackend: cb,
		cl:              cl,
		bank:            bank,
		chainId:         chainId,
	}
}

func (d ShutterContractsDeployer) DeployCoreContracts(ctx context.Context) (ShutterContractsDeployment, error) {
	transactOpts := d.transactOpts()
	sequencerAddr, sequencerDeployTxn, sequencer, err := shuttercontracts.DeploySequencer(
		transactOpts,
		d.contractBackend,
	)
	if err != nil {
		return ShutterContractsDeployment{}, err
	}

	ksmAddr, ksmDeployTxn, ksm, err := shuttercontracts.DeployKeyperSetManager(
		transactOpts,
		d.contractBackend,
		d.bank.Address(),
	)
	if err != nil {
		return ShutterContractsDeployment{}, err
	}

	keyBroadcastAddr, keyBroadcastDeployTxn, keyBroadcast, err := shuttercontracts.DeployKeyBroadcastContract(
		transactOpts,
		d.contractBackend,
		ksmAddr,
	)
	if err != nil {
		return ShutterContractsDeployment{}, err
	}

	deployTxns := []libcommon.Hash{
		sequencerDeployTxn.Hash(),
		ksmDeployTxn.Hash(),
		keyBroadcastDeployTxn.Hash(),
	}
	err = d.cl.IncludeTxns(ctx, deployTxns)
	if err != nil {
		return ShutterContractsDeployment{}, err
	}

	ksmInitTxn, err := ksm.Initialize(transactOpts, d.bank.Address(), d.bank.Address())
	if err != nil {
		return ShutterContractsDeployment{}, err
	}

	err = d.cl.IncludeTxns(ctx, []libcommon.Hash{ksmInitTxn.Hash()})
	if err != nil {
		return ShutterContractsDeployment{}, err
	}

	res := ShutterContractsDeployment{
		Sequencer:        sequencer,
		SequencerAddr:    sequencerAddr,
		Ksm:              ksm,
		KsmAddr:          ksmAddr,
		KeyBroadcast:     keyBroadcast,
		KeyBroadcastAddr: keyBroadcastAddr,
	}

	return res, nil
}

func (d ShutterContractsDeployer) DeployKeyperSet(
	ctx context.Context,
	ksm *shuttercontracts.KeyperSetManager,
	ekg testhelpers.EonKeyGeneration,
) (libcommon.Address, *shuttercontracts.KeyperSet, error) {
	transactOpts := d.transactOpts()
	keyperSetAddr, keyperSetDeployTxn, keyperSet, err := shuttercontracts.DeployKeyperSet(transactOpts, d.contractBackend)
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	err = d.cl.IncludeTxns(ctx, []libcommon.Hash{keyperSetDeployTxn.Hash()})
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	setPublisherTxn, err := keyperSet.SetPublisher(transactOpts, d.bank.Address())
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	setThresholdTxn, err := keyperSet.SetThreshold(transactOpts, ekg.Threshold)
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	addMembersTxn, err := keyperSet.AddMembers(transactOpts, ekg.Members())
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	setFinalizedTxn, err := keyperSet.SetFinalized(transactOpts)
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	err = d.cl.IncludeTxns(ctx, []libcommon.Hash{
		setPublisherTxn.Hash(),
		setThresholdTxn.Hash(),
		addMembersTxn.Hash(),
		setFinalizedTxn.Hash(),
	})
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	addKeyperSetTxn, err := ksm.AddKeyperSet(transactOpts, ekg.ActivationBlock, keyperSetAddr)
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	err = d.cl.IncludeTxns(ctx, []libcommon.Hash{addKeyperSetTxn.Hash()})
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	return keyperSetAddr, keyperSet, nil
}

func (d ShutterContractsDeployer) transactOpts() *bind.TransactOpts {
	return &bind.TransactOpts{
		From: d.bank.Address(),
		Signer: func(address libcommon.Address, txn types.Transaction) (types.Transaction, error) {
			return types.SignTx(txn, *types.LatestSignerForChainID(d.chainId), d.bank.privKey)
		},
	}
}

type ShutterContractsDeployment struct {
	Sequencer        *shuttercontracts.Sequencer
	SequencerAddr    libcommon.Address
	Ksm              *shuttercontracts.KeyperSetManager
	KsmAddr          libcommon.Address
	KeyBroadcast     *shuttercontracts.KeyBroadcastContract
	KeyBroadcastAddr libcommon.Address
}

type MockCl struct {
	engineApiClient       *engineapi.JsonRpcClient
	suggestedFeeRecipient libcommon.Address
	prevBlockHash         libcommon.Hash
	prevRandao            *big.Int
	prevBeaconBlockRoot   *big.Int
}

func NewMockCl(elClient *engineapi.JsonRpcClient, feeRecipient libcommon.Address, elGenesis libcommon.Hash) *MockCl {
	return &MockCl{
		engineApiClient:       elClient,
		suggestedFeeRecipient: feeRecipient,
		prevBlockHash:         elGenesis,
		prevRandao:            big.NewInt(0),
		prevBeaconBlockRoot:   big.NewInt(10_000),
	}
}

func (cl *MockCl) IncludeTxns(ctx context.Context, txns []libcommon.Hash) error {
	block, err := cl.BuildBlock(ctx)
	if err != nil {
		return err
	}

	txnHashes := mapset.NewSet[libcommon.Hash](txns...)
	for _, txnBytes := range block.Transactions {
		txn, err := types.DecodeTransaction(txnBytes)
		if err != nil {
			return err
		}

		txnHashes.Remove(txn.Hash())
	}

	if txnHashes.Cardinality() == 0 {
		return nil
	}

	err = errors.New("deploy txn not found in block")
	txnHashes.Each(func(txnHash libcommon.Hash) bool {
		err = fmt.Errorf("%w: %s", err, txnHash)
		return true // continue
	})
	return err
}

func (cl *MockCl) BuildBlock(ctx context.Context) (*enginetypes.ExecutionPayload, error) {
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

	// start block building process
	fcuRes, err := cl.engineApiClient.ForkchoiceUpdatedV3(ctx, &forkChoiceState, &payloadAttributes)
	if err != nil {
		return nil, err
	}
	if fcuRes.PayloadStatus.Status != enginetypes.ValidStatus {
		return nil, fmt.Errorf("payload status is not valid: %s", fcuRes.PayloadStatus.Status)
	}

	// give block builder time to build a block
	err = libcommon.Sleep(ctx, time.Second)
	if err != nil {
		return nil, err
	}

	// get the newly built block
	payloadRes, err := cl.engineApiClient.GetPayloadV4(ctx, *fcuRes.PayloadId)
	if err != nil {
		return nil, err
	}

	// insert the newly built block
	payloadStatus, err := cl.engineApiClient.NewPayloadV4(ctx, payloadRes.ExecutionPayload, []libcommon.Hash{}, &parentBeaconBlockRoot, []hexutil.Bytes{})
	if err != nil {
		return nil, err
	}
	if payloadStatus.Status != enginetypes.ValidStatus {
		return nil, fmt.Errorf("payload status is not valid: %s", payloadStatus.Status)
	}

	// set the newly built block as canonical
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
	if fcuRes.PayloadStatus.Status != enginetypes.ValidStatus {
		return nil, fmt.Errorf("payload status is not valid: %s", fcuRes.PayloadStatus.Status)
	}

	cl.prevBlockHash = newHash
	cl.prevRandao.Add(cl.prevRandao, big.NewInt(1))
	cl.prevBeaconBlockRoot.Add(cl.prevBeaconBlockRoot, big.NewInt(1))
	return payloadRes.ExecutionPayload, nil
}
