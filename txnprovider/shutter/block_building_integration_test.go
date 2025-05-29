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

package shutter_test

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/chain"
	params2 "github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon-lib/types"
	p2p "github.com/erigontech/erigon-p2p"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/eth"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc/contracts"
	"github.com/erigontech/erigon/rpc/requests"
	"github.com/erigontech/erigon/turbo/engineapi"
	"github.com/erigontech/erigon/txnprovider/shutter"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestShutterBlockBuilding(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	if runtime.GOOS == "darwin" {
		// We run race detector for medium tests which fails on macOS.
		t.Skip("issue #15007")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	uni := initBlockBuildingUniverse(ctx, t)
	sender1 := uni.acc1PrivKey
	sender2 := uni.acc2PrivKey
	sender3 := uni.acc3PrivKey
	sender4 := uni.acc4PrivKey
	sender5 := uni.acc5PrivKey
	receiver := uni.acc3
	amount := big.NewInt(1)
	// amount of blocks it takes to deploy a new keyper set based on contractsDeployer.DeployKeyperSet
	keyperSetDeploymentBlocks := uint64(4)
	txnPointer := uint64(0)

	// deploy initial eon 0
	currentBlock, err := uni.rpcApiClient.BlockNumber()
	require.NoError(t, err)
	ekg, err := testhelpers.MockEonKeyGeneration(shutter.EonIndex(0), 1, 2, currentBlock+keyperSetDeploymentBlocks)
	require.NoError(t, err)
	_, _, err = uni.contractsDeployer.DeployKeyperSet(ctx, uni.contractsDeployment, ekg)
	require.NoError(t, err)

	t.Run("eon 0", func(t *testing.T) {
		require.Equal(t, shutter.EonIndex(0), ekg.EonIndex)

		t.Run("build shutter block", func(t *testing.T) {
			// submit 1 shutter txn
			encryptedSubmission, err := uni.transactor.SubmitEncryptedTransfer(ctx, sender1, receiver, amount, ekg.Eon())
			require.NoError(t, err)
			block, err := uni.shutterCoordinator.BuildBlock(ctx, ekg, &txnPointer)
			require.NoError(t, err)
			err = uni.txnInclusionVerifier.VerifyTxnsInclusion(ctx, block, encryptedSubmission.SubmissionTxn.Hash())
			require.NoError(t, err)

			// submit 1 non shutter txn
			simpleTxn, err := uni.transactor.SubmitSimpleTransfer(sender2, receiver, amount)
			require.NoError(t, err)

			// send decryption keys to block builder, build block and verify both txns are included (shutter at beginning of block)
			block, err = uni.shutterCoordinator.BuildBlock(ctx, ekg, &txnPointer, encryptedSubmission.IdentityPreimage)
			require.NoError(t, err)
			err = uni.txnInclusionVerifier.VerifyTxnsOrderedInclusion(
				ctx,
				block,
				testhelpers.OrderedInclusion{TxnIndex: 0, TxnHash: encryptedSubmission.OriginalTxn.Hash()},
				testhelpers.OrderedInclusion{TxnIndex: 1, TxnHash: simpleTxn.Hash()},
			)
			require.NoError(t, err)
		})

		t.Run("build shutter block without breaching encrypted gas limit", func(t *testing.T) {
			// submit 4 shutter encrypted transfers
			encryptedSubmission1, err := uni.transactor.SubmitEncryptedTransfer(ctx, sender1, receiver, amount, ekg.Eon())
			require.NoError(t, err)
			encryptedSubmission2, err := uni.transactor.SubmitEncryptedTransfer(ctx, sender2, receiver, amount, ekg.Eon())
			require.NoError(t, err)
			encryptedSubmission3, err := uni.transactor.SubmitEncryptedTransfer(ctx, sender3, receiver, amount, ekg.Eon())
			require.NoError(t, err)
			encryptedSubmission4, err := uni.transactor.SubmitEncryptedTransfer(ctx, sender4, receiver, amount, ekg.Eon())
			require.NoError(t, err)
			block, err := uni.shutterCoordinator.BuildBlock(ctx, ekg, &txnPointer)
			require.NoError(t, err)
			err = uni.txnInclusionVerifier.VerifyTxnsInclusion(
				ctx,
				block,
				encryptedSubmission1.SubmissionTxn.Hash(),
				encryptedSubmission2.SubmissionTxn.Hash(),
				encryptedSubmission3.SubmissionTxn.Hash(),
				encryptedSubmission4.SubmissionTxn.Hash(),
			)
			require.NoError(t, err)

			// submit 1 simple txn
			simpleTxn, err := uni.transactor.SubmitSimpleTransfer(sender5, receiver, amount)
			require.NoError(t, err)

			// build shutter block
			block, err = uni.shutterCoordinator.BuildBlock(
				ctx,
				ekg,
				&txnPointer,
				encryptedSubmission1.IdentityPreimage,
				encryptedSubmission2.IdentityPreimage,
				encryptedSubmission3.IdentityPreimage,
				encryptedSubmission4.IdentityPreimage,
			)
			require.NoError(t, err)

			// only 3 shutter txns should get in the block since the EncryptedGasLimit only allows for that many
			// simple txn should get into the block regardless of that
			require.Len(t, block.Transactions, 4)
			err = uni.txnInclusionVerifier.VerifyTxnsOrderedInclusion(
				ctx,
				block,
				testhelpers.OrderedInclusion{TxnIndex: 0, TxnHash: encryptedSubmission1.OriginalTxn.Hash()},
				testhelpers.OrderedInclusion{TxnIndex: 1, TxnHash: encryptedSubmission2.OriginalTxn.Hash()},
				testhelpers.OrderedInclusion{TxnIndex: 2, TxnHash: encryptedSubmission3.OriginalTxn.Hash()},
				testhelpers.OrderedInclusion{TxnIndex: 3, TxnHash: simpleTxn.Hash()},
			)
			require.NoError(t, err)
		})

		t.Run("build shutter block without blob txns", func(t *testing.T) {
			//
			//  TODO
			//
		})
	})

	t.Run("eon 1", func(t *testing.T) {
		currentBlock, err := uni.rpcApiClient.BlockNumber()
		require.NoError(t, err)
		ekg, err := testhelpers.MockEonKeyGeneration(shutter.EonIndex(1), 2, 3, currentBlock+keyperSetDeploymentBlocks)
		require.NoError(t, err)
		_, _, err = uni.contractsDeployer.DeployKeyperSet(ctx, uni.contractsDeployment, ekg)
		require.NoError(t, err)
		require.Equal(t, shutter.EonIndex(1), ekg.EonIndex)

		t.Run("build shutter block", func(t *testing.T) {
			// submit 1 shutter txn using the new eon
			encryptedSubmission, err := uni.transactor.SubmitEncryptedTransfer(ctx, sender2, receiver, amount, ekg.Eon())
			require.NoError(t, err)
			block, err := uni.shutterCoordinator.BuildBlock(ctx, ekg, &txnPointer)
			require.NoError(t, err)
			err = uni.txnInclusionVerifier.VerifyTxnsInclusion(ctx, block, encryptedSubmission.SubmissionTxn.Hash())
			require.NoError(t, err)

			// build block and verify new txn with new eon is included
			block, err = uni.shutterCoordinator.BuildBlock(ctx, ekg, &txnPointer, encryptedSubmission.IdentityPreimage)
			require.NoError(t, err)
			err = uni.txnInclusionVerifier.VerifyTxnsOrderedInclusion(
				ctx,
				block,
				testhelpers.OrderedInclusion{TxnIndex: 0, TxnHash: encryptedSubmission.OriginalTxn.Hash()},
			)
			require.NoError(t, err)
		})
	})
}

type blockBuildingUniverse struct {
	rpcApiClient         requests.RequestGenerator
	contractsDeployer    testhelpers.ContractsDeployer
	contractsDeployment  testhelpers.ContractsDeployment
	acc1PrivKey          *ecdsa.PrivateKey
	acc1                 common.Address
	acc2PrivKey          *ecdsa.PrivateKey
	acc2                 common.Address
	acc3PrivKey          *ecdsa.PrivateKey
	acc3                 common.Address
	acc4PrivKey          *ecdsa.PrivateKey
	acc4                 common.Address
	acc5PrivKey          *ecdsa.PrivateKey
	acc5                 common.Address
	transactor           testhelpers.EncryptedTransactor
	txnInclusionVerifier testhelpers.TxnInclusionVerifier
	shutterConfig        shuttercfg.Config
	shutterCoordinator   testhelpers.ShutterBlockBuildingCoordinator
}

func initBlockBuildingUniverse(ctx context.Context, t *testing.T) blockBuildingUniverse {
	logger := testlog.Logger(t, log.LvlDebug)
	dataDir := t.TempDir()
	dirs := datadir.New(dataDir)
	sentryPort, err := testhelpers.NextFreePort()
	require.NoError(t, err)
	engineApiPort, err := testhelpers.NextFreePort()
	require.NoError(t, err)
	jsonRpcPort, err := testhelpers.NextFreePort()
	require.NoError(t, err)
	shutterPort, err := testhelpers.NextFreePort()
	require.NoError(t, err)
	decryptionKeySenderPort, err := testhelpers.NextFreePort()
	require.NoError(t, err)

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

	chainId := big.NewInt(987656789)
	chainIdU256, _ := uint256.FromBig(chainId)
	decryptionKeySenderPrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	decryptionKeySenderPrivKeyBytes := make([]byte, 32)
	decryptionKeySenderPrivKey.D.FillBytes(decryptionKeySenderPrivKeyBytes)
	decryptionKeySenderP2pPrivKey, err := libp2pcrypto.UnmarshalSecp256k1PrivateKey(decryptionKeySenderPrivKeyBytes)
	require.NoError(t, err)
	decryptionKeySenderPeerId, err := peer.IDFromPrivateKey(decryptionKeySenderP2pPrivKey)
	require.NoError(t, err)
	decryptionKeySenderPeerAddr := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", decryptionKeySenderPort, decryptionKeySenderPeerId)
	contractDeployerPrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	contractDeployer := crypto.PubkeyToAddress(contractDeployerPrivKey.PublicKey)
	shutterConfig := shuttercfg.ConfigByChainName(params.ChiadoChainConfig.ChainName)
	shutterConfig.Enabled = false // first we need to deploy the shutter smart contracts
	shutterConfig.BootstrapNodes = []string{decryptionKeySenderPeerAddr}
	shutterConfig.PrivateKey = nodeKey
	shutterConfig.ListenPort = uint64(shutterPort)
	shutterConfig.InstanceId = 1234567890
	shutterConfig.ChainId = chainIdU256
	shutterConfig.SecondsPerSlot = 1
	shutterConfig.EncryptedGasLimit = 3 * 21_000 // max 3 simple encrypted transfers per block
	shutterConfig.SequencerContractAddress = crypto.CreateAddress(contractDeployer, 0).String()
	shutterConfig.KeyperSetManagerContractAddress = crypto.CreateAddress(contractDeployer, 1).String()
	shutterConfig.KeyBroadcastContractAddress = crypto.CreateAddress(contractDeployer, 2).String()

	ethConfig := ethconfig.Config{
		Dirs: dirs,
		Snapshot: ethconfig.BlocksFreezing{
			NoDownloader: true,
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

	var chainConfig chain.Config
	copier.Copy(&chainConfig, params.ChiadoChainConfig)
	chainConfig.ChainName = "shutter-devnet"
	chainConfig.ChainID = chainId
	chainConfig.TerminalTotalDifficulty = big.NewInt(0)
	chainConfig.ShanghaiTime = big.NewInt(0)
	chainConfig.CancunTime = big.NewInt(0)
	chainConfig.PragueTime = big.NewInt(0)
	genesis := core.ChiadoGenesisBlock()
	genesis.Timestamp = uint64(time.Now().Unix() - 1)
	genesis.Config = &chainConfig
	genesis.Alloc[params2.ConsolidationRequestAddress] = types.GenesisAccount{
		Code:    []byte{0}, // Can't be empty
		Storage: make(map[common.Hash]common.Hash, 0),
		Balance: big.NewInt(0),
		Nonce:   0,
	}
	genesis.Alloc[params2.WithdrawalRequestAddress] = types.GenesisAccount{
		Code:    []byte{0}, // Can't be empty
		Storage: make(map[common.Hash]common.Hash, 0),
		Balance: big.NewInt(0),
		Nonce:   0,
	}
	// 1_000 ETH in wei in the bank
	bank := testhelpers.NewBank(new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil))
	bank.RegisterGenesisAlloc(genesis)
	chainDB, err := node.OpenDatabase(ctx, ethNode.Config(), kv.ChainDB, "", false, logger)
	require.NoError(t, err)
	_, gensisBlock, err := core.CommitGenesisBlock(chainDB, genesis, ethNode.Config().Dirs, logger)
	require.NoError(t, err)
	chainDB.Close()

	ethBackend, err := eth.New(ctx, ethNode, &ethConfig, logger, nil)
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
	engineApiClient, err := engineapi.DialJsonRpcClient(
		engineApiUrl,
		jwtSecret,
		logger,
		// requests should not take more than 5 secs in a test env, yet we can spam frequently
		engineapi.WithJsonRpcClientRetryBackOff(50*time.Millisecond),
		engineapi.WithJsonRpcClientMaxRetries(100),
	)
	require.NoError(t, err)
	slotCalculator := shutter.NewBeaconChainSlotCalculator(shutterConfig.BeaconChainGenesisTimestamp, shutterConfig.SecondsPerSlot)
	cl := testhelpers.NewMockCl(slotCalculator, engineApiClient, bank.Address(), gensisBlock)
	_, err = cl.BuildBlock(ctx)
	require.NoError(t, err)

	txnInclusionVerifier := testhelpers.NewTxnInclusionVerifier(rpcApiClient)
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
	acc4PrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	acc4 := crypto.PubkeyToAddress(acc4PrivKey.PublicKey)
	acc5PrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	acc5 := crypto.PubkeyToAddress(acc5PrivKey.PublicKey)
	encryptorAccPrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	encryptorAcc := crypto.PubkeyToAddress(encryptorAccPrivKey.PublicKey)
	oneEth := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	topUp1, err := transactor.SubmitSimpleTransfer(bank.PrivKey(), acc1, oneEth)
	require.NoError(t, err)
	topUp2, err := transactor.SubmitSimpleTransfer(bank.PrivKey(), acc2, oneEth)
	require.NoError(t, err)
	topUp3, err := transactor.SubmitSimpleTransfer(bank.PrivKey(), acc3, oneEth)
	require.NoError(t, err)
	topUp4, err := transactor.SubmitSimpleTransfer(bank.PrivKey(), acc4, oneEth)
	require.NoError(t, err)
	topUp5, err := transactor.SubmitSimpleTransfer(bank.PrivKey(), acc5, oneEth)
	require.NoError(t, err)
	topUp6, err := transactor.SubmitSimpleTransfer(bank.PrivKey(), contractDeployer, oneEth)
	require.NoError(t, err)
	topUp7, err := transactor.SubmitSimpleTransfer(bank.PrivKey(), encryptorAcc, oneEth)
	require.NoError(t, err)
	block, err := cl.BuildBlock(ctx)
	require.NoError(t, err)
	err = txnInclusionVerifier.VerifyTxnsInclusion(
		ctx,
		block,
		topUp1.Hash(),
		topUp2.Hash(),
		topUp3.Hash(),
		topUp4.Hash(),
		topUp5.Hash(),
		topUp6.Hash(),
		topUp7.Hash(),
	)
	require.NoError(t, err)
	deployer := testhelpers.NewContractsDeployer(contractDeployerPrivKey, contractBackend, cl, chainConfig.ChainID, txnInclusionVerifier)
	contractsDeployment, err := deployer.DeployCore(ctx)
	require.NoError(t, err)
	// these addresses are determined by the order of deployment (deployerAddr+nonce)
	// if there are mismatches need to check order in config initialization and in deployment order
	require.Equal(t, shutterConfig.SequencerContractAddress, contractsDeployment.SequencerAddr.String())
	require.Equal(t, shutterConfig.KeyperSetManagerContractAddress, contractsDeployment.KsmAddr.String())
	require.Equal(t, shutterConfig.KeyBroadcastContractAddress, contractsDeployment.KeyBroadcastAddr.String())
	logger.Debug(
		"shutter contracts deployed",
		"sequencer", contractsDeployment.SequencerAddr,
		"ksm", contractsDeployment.KsmAddr,
		"keyBroadcast", contractsDeployment.KeyBroadcastAddr,
	)
	// start up shutter test decryption key sender p2p node
	decryptionKeySender, err := testhelpers.DialDecryptionKeysSender(ctx, logger, decryptionKeySenderPort, decryptionKeySenderP2pPrivKey)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := decryptionKeySender.Close()
		require.NoError(t, err)
	})

	// now that we've deployed all shutter contracts - we can restart erigon with shutter enabled
	err = ethNode.Close()
	require.NoError(t, err)
	shutterConfig.Enabled = true
	ethConfig.Shutter = shutterConfig
	ethNode, err = node.New(ctx, &nodeConfig, logger)
	require.NoError(t, err)
	ethBackend, err = eth.New(ctx, ethNode, &ethConfig, logger, nil)
	require.NoError(t, err)
	err = ethBackend.Init(ethNode, &ethConfig, &chainConfig)
	require.NoError(t, err)
	err = ethNode.Start()
	require.NoError(t, err)
	t.Cleanup(cleanNode(ethNode))

	// wait for shutter validator to connect to our test decryptionKeySender bootstrap node
	shutterValidatorP2pPrivKeyBytes := make([]byte, 32)
	shutterConfig.PrivateKey.D.FillBytes(shutterValidatorP2pPrivKeyBytes)
	shutterValidatorP2pPrivKey, err := libp2pcrypto.UnmarshalSecp256k1PrivateKey(shutterValidatorP2pPrivKeyBytes)
	require.NoError(t, err)
	shutterValidatorPeerId, err := peer.IDFromPrivateKey(shutterValidatorP2pPrivKey)
	require.NoError(t, err)
	err = decryptionKeySender.WaitExternalPeerConnection(ctx, shutterValidatorPeerId)
	require.NoError(t, err)

	encryptedTransactor := testhelpers.NewEncryptedTransactor(transactor, encryptorAccPrivKey, shutterConfig.SequencerContractAddress, contractBackend)
	coordinator := testhelpers.NewShutterBlockBuildingCoordinator(cl, decryptionKeySender, slotCalculator, shutterConfig.InstanceId)
	return blockBuildingUniverse{
		rpcApiClient:         rpcApiClient,
		contractsDeployer:    deployer,
		contractsDeployment:  contractsDeployment,
		acc1PrivKey:          acc1PrivKey,
		acc1:                 acc1,
		acc2PrivKey:          acc2PrivKey,
		acc2:                 acc2,
		acc3PrivKey:          acc3PrivKey,
		acc3:                 acc3,
		acc4PrivKey:          acc4PrivKey,
		acc4:                 acc4,
		acc5PrivKey:          acc5PrivKey,
		acc5:                 acc5,
		transactor:           encryptedTransactor,
		txnInclusionVerifier: txnInclusionVerifier,
		shutterConfig:        shutterConfig,
		shutterCoordinator:   coordinator,
	}
}
