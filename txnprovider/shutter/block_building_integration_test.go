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
	"fmt"
	"math/big"
	"runtime"
	"testing"
	"time"

	"github.com/holiman/uint256"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/race"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/eth/ethconfig"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	executiontests "github.com/erigontech/erigon/execution/tests"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/requests"
	"github.com/erigontech/erigon/txnprovider/shutter"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
)

func TestShutterBlockBuilding(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	//goland:noinspection GoBoolExpressions
	if race.Enabled && runtime.GOOS == "darwin" {
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
				executiontests.OrderedInclusion{TxnIndex: 0, TxnHash: encryptedSubmission.OriginalTxn.Hash()},
				executiontests.OrderedInclusion{TxnIndex: 1, TxnHash: simpleTxn.Hash()},
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
				executiontests.OrderedInclusion{TxnIndex: 0, TxnHash: encryptedSubmission1.OriginalTxn.Hash()},
				executiontests.OrderedInclusion{TxnIndex: 1, TxnHash: encryptedSubmission2.OriginalTxn.Hash()},
				executiontests.OrderedInclusion{TxnIndex: 2, TxnHash: encryptedSubmission3.OriginalTxn.Hash()},
				executiontests.OrderedInclusion{TxnIndex: 3, TxnHash: simpleTxn.Hash()},
			)
			require.NoError(t, err)
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
				executiontests.OrderedInclusion{TxnIndex: 0, TxnHash: encryptedSubmission.OriginalTxn.Hash()},
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
	txnInclusionVerifier executiontests.TxnInclusionVerifier
	shutterConfig        shuttercfg.Config
	shutterCoordinator   testhelpers.ShutterBlockBuildingCoordinator
}

func initBlockBuildingUniverse(ctx context.Context, t *testing.T) blockBuildingUniverse {
	logger := testlog.Logger(t, log.LvlDebug)
	dataDir := t.TempDir()
	genesis, coinbasePrivKey := executiontests.DefaultEngineApiTesterGenesis(t)
	chainConfig := genesis.Config
	chainConfig.ChainName = "shutter-devnet"
	chainConfig.TerminalTotalDifficulty = big.NewInt(0)
	chainConfig.ShanghaiTime = big.NewInt(0)
	chainConfig.CancunTime = big.NewInt(0)
	chainConfig.PragueTime = big.NewInt(0)
	genesis.Timestamp = uint64(time.Now().Unix() - 1)
	// 1_000 ETH in wei in the bank
	bank := testhelpers.NewBank(new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil))
	bank.RegisterGenesisAlloc(genesis)
	// first we need to deploy the shutter smart contracts, so we start an engine api tester without shutter
	eat := executiontests.InitialiseEngineApiTester(t, executiontests.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     dataDir,
		Genesis:     genesis,
		CoinbaseKey: coinbasePrivKey,
	})
	// prepare shutter config for the next engine api tester
	shutterPort, err := testutil.NextFreePort()
	require.NoError(t, err)
	decryptionKeySenderPort, err := testutil.NextFreePort()
	require.NoError(t, err)
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
	chainIdU256, _ := uint256.FromBig(genesis.Config.ChainID)
	shutterConfig := shuttercfg.ConfigByChainName(chainspec.Chiado.Config.ChainName)
	shutterConfig.BootstrapNodes = []string{decryptionKeySenderPeerAddr}
	shutterConfig.PrivateKey = eat.NodeKey
	shutterConfig.ListenPort = uint64(shutterPort)
	shutterConfig.InstanceId = 1234567890
	shutterConfig.ChainId = chainIdU256
	shutterConfig.SecondsPerSlot = 1
	shutterConfig.EncryptedGasLimit = 3 * 21_000 // max 3 simple encrypted transfers per block
	shutterConfig.SequencerContractAddress = types.CreateAddress(contractDeployer, 0).String()
	shutterConfig.KeyperSetManagerContractAddress = types.CreateAddress(contractDeployer, 1).String()
	shutterConfig.KeyBroadcastContractAddress = types.CreateAddress(contractDeployer, 2).String()
	// top up a few accounts with some ETH and deploy the shutter contracts
	slotCalculator := shutter.NewBeaconChainSlotCalculator(shutterConfig.BeaconChainGenesisTimestamp, shutterConfig.SecondsPerSlot)
	cl := testhelpers.NewMockCl(logger, eat.MockCl, slotCalculator)
	require.NoError(t, err)
	err = cl.Initialise(ctx)
	require.NoError(t, err)
	transactor := executiontests.NewTransactor(eat.RpcApiClient, chainConfig.ChainID)
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
	err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(
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
	deployer := testhelpers.NewContractsDeployer(contractDeployerPrivKey, eat.ContractBackend, cl, chainConfig.ChainID, eat.TxnInclusionVerifier)
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
	eat.Close(t)
	eat = executiontests.InitialiseEngineApiTester(t, executiontests.EngineApiTesterInitArgs{
		Logger:           logger,
		DataDir:          dataDir,
		Genesis:          genesis,
		CoinbaseKey:      coinbasePrivKey,
		EthConfigTweaker: func(ethConfig *ethconfig.Config) { ethConfig.Shutter = shutterConfig },
		MockClState:      eat.MockCl.State(),
	})
	// need to recreate these since we have a new engine api tester with new ports
	cl = testhelpers.NewMockCl(logger, eat.MockCl, slotCalculator)
	err = cl.Initialise(ctx)
	require.NoError(t, err)
	transactor = executiontests.NewTransactor(eat.RpcApiClient, chainConfig.ChainID)
	deployer = testhelpers.NewContractsDeployer(contractDeployerPrivKey, eat.ContractBackend, cl, chainConfig.ChainID, eat.TxnInclusionVerifier)
	// wait for the shutter validator to connect to our test decryptionKeySender bootstrap node
	shutterValidatorP2pPrivKeyBytes := make([]byte, 32)
	shutterConfig.PrivateKey.D.FillBytes(shutterValidatorP2pPrivKeyBytes)
	shutterValidatorP2pPrivKey, err := libp2pcrypto.UnmarshalSecp256k1PrivateKey(shutterValidatorP2pPrivKeyBytes)
	require.NoError(t, err)
	shutterValidatorPeerId, err := peer.IDFromPrivateKey(shutterValidatorP2pPrivKey)
	require.NoError(t, err)
	err = decryptionKeySender.WaitExternalPeerConnection(ctx, shutterValidatorPeerId)
	require.NoError(t, err)
	encryptedTransactor := testhelpers.NewEncryptedTransactor(transactor, encryptorAccPrivKey, shutterConfig.SequencerContractAddress, eat.ContractBackend)
	coordinator := testhelpers.NewShutterBlockBuildingCoordinator(cl, decryptionKeySender, slotCalculator, shutterConfig.InstanceId)
	return blockBuildingUniverse{
		rpcApiClient:         eat.RpcApiClient,
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
		txnInclusionVerifier: eat.TxnInclusionVerifier,
		shutterConfig:        shutterConfig,
		shutterCoordinator:   coordinator,
	}
}
