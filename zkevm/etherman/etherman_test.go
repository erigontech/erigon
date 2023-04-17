package etherman

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/0xPolygonHermez/zkevm-node/etherman/smartcontracts/polygonzkevm"
	"github.com/0xPolygonHermez/zkevm-node/etherman/smartcontracts/polygonzkevmbridge"
	ethmanTypes "github.com/0xPolygonHermez/zkevm-node/etherman/types"
	"github.com/0xPolygonHermez/zkevm-node/log"
	"github.com/0xPolygonHermez/zkevm-node/state"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/accounts/abi/bind/backends"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	log.Init(log.Config{
		Level:   "debug",
		Outputs: []string{"stderr"},
	})
}

// This function prepare the blockchain, the wallet with funds and deploy the smc
func newTestingEnv() (ethman *Client, ethBackend *backends.SimulatedBackend, auth *bind.TransactOpts, maticAddr common.Address, br *polygonzkevmbridge.Polygonzkevmbridge) {
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		log.Fatal(err)
	}
	auth, err = bind.NewKeyedTransactorWithChainID(privateKey, big.NewInt(1337))
	if err != nil {
		log.Fatal(err)
	}
	ethman, ethBackend, maticAddr, br, err = NewSimulatedEtherman(Config{}, auth)
	if err != nil {
		log.Fatal(err)
	}
	err = ethman.AddOrReplaceAuth(*auth)
	if err != nil {
		log.Fatal(err)
	}
	return ethman, ethBackend, auth, maticAddr, br
}

func TestGEREvent(t *testing.T) {
	// Set up testing environment
	etherman, ethBackend, auth, _, br := newTestingEnv()

	// Read currentBlock
	ctx := context.Background()
	initBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)

	amount := big.NewInt(1000000000000000)
	auth.Value = amount
	_, err = br.BridgeAsset(auth, 1, auth.From, amount, common.Address{}, true, []byte{})
	require.NoError(t, err)

	// Mine the tx in a block
	ethBackend.Commit()

	// Now read the event
	finalBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)
	finalBlockNumber := finalBlock.NumberU64()
	blocks, _, err := etherman.GetRollupInfoByBlockRange(ctx, initBlock.NumberU64(), &finalBlockNumber)
	require.NoError(t, err)
	t.Log("Blocks: ", blocks)
	assert.Equal(t, uint64(2), blocks[1].GlobalExitRoots[0].BlockNumber)
	assert.NotEqual(t, common.Hash{}, blocks[1].GlobalExitRoots[0].MainnetExitRoot)
	assert.Equal(t, common.Hash{}, blocks[1].GlobalExitRoots[0].RollupExitRoot)
}

func TestForcedBatchEvent(t *testing.T) {
	// Set up testing environment
	etherman, ethBackend, auth, _, _ := newTestingEnv()

	// Read currentBlock
	ctx := context.Background()
	initBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)

	amount, err := etherman.PoE.GetForcedBatchFee(&bind.CallOpts{Pending: false})
	require.NoError(t, err)
	rawTxs := "f84901843b9aca00827b0c945fbdb2315678afecb367f032d93f642f64180aa380a46057361d00000000000000000000000000000000000000000000000000000000000000048203e9808073efe1fa2d3e27f26f32208550ea9b0274d49050b816cadab05a771f4275d0242fd5d92b3fb89575c070e6c930587c520ee65a3aa8cfe382fcad20421bf51d621c"
	data, err := hex.DecodeString(rawTxs)
	require.NoError(t, err)
	_, err = etherman.PoE.ForceBatch(auth, data, amount)
	require.NoError(t, err)

	// Mine the tx in a block
	ethBackend.Commit()

	// Now read the event
	finalBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)
	finalBlockNumber := finalBlock.NumberU64()
	blocks, _, err := etherman.GetRollupInfoByBlockRange(ctx, initBlock.NumberU64(), &finalBlockNumber)
	require.NoError(t, err)
	t.Log("Blocks: ", blocks)
	assert.Equal(t, uint64(2), blocks[1].BlockNumber)
	assert.Equal(t, uint64(2), blocks[1].ForcedBatches[0].BlockNumber)
	assert.NotEqual(t, common.Hash{}, blocks[1].ForcedBatches[0].GlobalExitRoot)
	assert.NotEqual(t, time.Time{}, blocks[1].ForcedBatches[0].ForcedAt)
	assert.Equal(t, uint64(1), blocks[1].ForcedBatches[0].ForcedBatchNumber)
	assert.Equal(t, rawTxs, hex.EncodeToString(blocks[1].ForcedBatches[0].RawTxsData))
	assert.Equal(t, auth.From, blocks[1].ForcedBatches[0].Sequencer)
}

func TestSequencedBatchesEvent(t *testing.T) {
	// Set up testing environment
	etherman, ethBackend, auth, _, br := newTestingEnv()

	// Read currentBlock
	ctx := context.Background()
	initBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)

	// Make a bridge tx
	auth.Value = big.NewInt(1000000000000000)
	_, err = br.BridgeAsset(auth, 1, auth.From, auth.Value, common.Address{}, true, []byte{})
	require.NoError(t, err)
	ethBackend.Commit()
	auth.Value = big.NewInt(0)

	// Get the last ger
	ger, err := etherman.GlobalExitRootManager.GetLastGlobalExitRoot(nil)
	require.NoError(t, err)

	amount, err := etherman.PoE.GetForcedBatchFee(&bind.CallOpts{Pending: false})
	require.NoError(t, err)
	rawTxs := "f84901843b9aca00827b0c945fbdb2315678afecb367f032d93f642f64180aa380a46057361d00000000000000000000000000000000000000000000000000000000000000048203e9808073efe1fa2d3e27f26f32208550ea9b0274d49050b816cadab05a771f4275d0242fd5d92b3fb89575c070e6c930587c520ee65a3aa8cfe382fcad20421bf51d621c"
	data, err := hex.DecodeString(rawTxs)
	require.NoError(t, err)
	_, err = etherman.PoE.ForceBatch(auth, data, amount)
	require.NoError(t, err)
	require.NoError(t, err)
	ethBackend.Commit()

	// Now read the event
	currentBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)
	currentBlockNumber := currentBlock.NumberU64()
	blocks, _, err := etherman.GetRollupInfoByBlockRange(ctx, initBlock.NumberU64(), &currentBlockNumber)
	require.NoError(t, err)
	t.Log("Blocks: ", blocks)
	var sequences []polygonzkevm.PolygonZkEVMBatchData
	sequences = append(sequences, polygonzkevm.PolygonZkEVMBatchData{
		GlobalExitRoot:     ger,
		Timestamp:          currentBlock.Time(),
		MinForcedTimestamp: uint64(blocks[2].ForcedBatches[0].ForcedAt.Unix()),
		Transactions:       common.Hex2Bytes(rawTxs),
	})
	sequences = append(sequences, polygonzkevm.PolygonZkEVMBatchData{
		GlobalExitRoot:     ger,
		Timestamp:          currentBlock.Time() + 1,
		MinForcedTimestamp: 0,
		Transactions:       common.Hex2Bytes(rawTxs),
	})
	_, err = etherman.PoE.SequenceBatches(auth, sequences, auth.From)
	require.NoError(t, err)

	// Mine the tx in a block
	ethBackend.Commit()

	// Now read the event
	finalBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)
	finalBlockNumber := finalBlock.NumberU64()
	blocks, order, err := etherman.GetRollupInfoByBlockRange(ctx, initBlock.NumberU64(), &finalBlockNumber)
	require.NoError(t, err)
	t.Log("Blocks: ", blocks)
	assert.Equal(t, 4, len(blocks))
	assert.Equal(t, 1, len(blocks[3].SequencedBatches))
	assert.Equal(t, common.Hex2Bytes(rawTxs), blocks[3].SequencedBatches[0][1].Transactions)
	assert.Equal(t, currentBlock.Time(), blocks[3].SequencedBatches[0][0].Timestamp)
	assert.Equal(t, ger, blocks[3].SequencedBatches[0][0].GlobalExitRoot)
	assert.Equal(t, auth.From, blocks[3].SequencedBatches[0][0].Coinbase)
	assert.Equal(t, auth.From, blocks[3].SequencedBatches[0][0].SequencerAddr)
	assert.Equal(t, currentBlock.Time(), blocks[3].SequencedBatches[0][0].MinForcedTimestamp)
	assert.Equal(t, 0, order[blocks[3].BlockHash][0].Pos)
}

func TestVerifyBatchEvent(t *testing.T) {
	// Set up testing environment
	etherman, ethBackend, auth, _, _ := newTestingEnv()

	// Read currentBlock
	ctx := context.Background()

	initBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)

	rawTxs := "f84901843b9aca00827b0c945fbdb2315678afecb367f032d93f642f64180aa380a46057361d00000000000000000000000000000000000000000000000000000000000000048203e9808073efe1fa2d3e27f26f32208550ea9b0274d49050b816cadab05a771f4275d0242fd5d92b3fb89575c070e6c930587c520ee65a3aa8cfe382fcad20421bf51d621c"
	tx := polygonzkevm.PolygonZkEVMBatchData{
		GlobalExitRoot:     common.Hash{},
		Timestamp:          initBlock.Time(),
		MinForcedTimestamp: 0,
		Transactions:       common.Hex2Bytes(rawTxs),
	}
	_, err = etherman.PoE.SequenceBatches(auth, []polygonzkevm.PolygonZkEVMBatchData{tx}, auth.From)
	require.NoError(t, err)

	// Mine the tx in a block
	ethBackend.Commit()

	_, err = etherman.PoE.VerifyBatchesTrustedAggregator(auth, uint64(0), uint64(0), uint64(1), [32]byte{}, [32]byte{}, []byte{})
	require.NoError(t, err)

	// Mine the tx in a block
	ethBackend.Commit()

	// Now read the event
	finalBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)
	finalBlockNumber := finalBlock.NumberU64()
	blocks, order, err := etherman.GetRollupInfoByBlockRange(ctx, initBlock.NumberU64(), &finalBlockNumber)
	require.NoError(t, err)
	t.Log("Blocks: ", blocks)
	assert.Equal(t, uint64(3), blocks[2].BlockNumber)
	assert.Equal(t, uint64(1), blocks[2].VerifiedBatches[0].BatchNumber)
	assert.NotEqual(t, common.Address{}, blocks[2].VerifiedBatches[0].Aggregator)
	assert.NotEqual(t, common.Hash{}, blocks[2].VerifiedBatches[0].TxHash)
	assert.Equal(t, GlobalExitRootsOrder, order[blocks[2].BlockHash][0].Name)
	assert.Equal(t, TrustedVerifyBatchOrder, order[blocks[2].BlockHash][1].Name)
	assert.Equal(t, 0, order[blocks[2].BlockHash][0].Pos)
	assert.Equal(t, 0, order[blocks[2].BlockHash][1].Pos)
}

func TestSequenceForceBatchesEvent(t *testing.T) {
	// Set up testing environment
	etherman, ethBackend, auth, _, _ := newTestingEnv()

	// Read currentBlock
	ctx := context.Background()
	initBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)

	amount, err := etherman.PoE.GetForcedBatchFee(&bind.CallOpts{Pending: false})
	require.NoError(t, err)
	rawTxs := "f84901843b9aca00827b0c945fbdb2315678afecb367f032d93f642f64180aa380a46057361d00000000000000000000000000000000000000000000000000000000000000048203e9808073efe1fa2d3e27f26f32208550ea9b0274d49050b816cadab05a771f4275d0242fd5d92b3fb89575c070e6c930587c520ee65a3aa8cfe382fcad20421bf51d621c"
	data, err := hex.DecodeString(rawTxs)
	require.NoError(t, err)
	_, err = etherman.PoE.ForceBatch(auth, data, amount)
	require.NoError(t, err)
	ethBackend.Commit()

	err = ethBackend.AdjustTime((24*7 + 1) * time.Hour)
	require.NoError(t, err)
	ethBackend.Commit()

	// Now read the event
	finalBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)
	finalBlockNumber := finalBlock.NumberU64()
	blocks, _, err := etherman.GetRollupInfoByBlockRange(ctx, initBlock.NumberU64(), &finalBlockNumber)
	require.NoError(t, err)
	t.Log("Blocks: ", blocks)

	forceBatchData := polygonzkevm.PolygonZkEVMForcedBatchData{
		Transactions:       blocks[1].ForcedBatches[0].RawTxsData,
		GlobalExitRoot:     blocks[1].ForcedBatches[0].GlobalExitRoot,
		MinForcedTimestamp: uint64(blocks[1].ForcedBatches[0].ForcedAt.Unix()),
	}
	_, err = etherman.PoE.SequenceForceBatches(auth, []polygonzkevm.PolygonZkEVMForcedBatchData{forceBatchData})
	require.NoError(t, err)
	ethBackend.Commit()

	// Now read the event
	finalBlock, err = etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)
	finalBlockNumber = finalBlock.NumberU64()
	blocks, order, err := etherman.GetRollupInfoByBlockRange(ctx, initBlock.NumberU64(), &finalBlockNumber)
	require.NoError(t, err)
	t.Log("Blocks: ", blocks)
	assert.Equal(t, uint64(4), blocks[2].BlockNumber)
	assert.Equal(t, uint64(1), blocks[2].SequencedForceBatches[0][0].BatchNumber)
	assert.Equal(t, uint64(20), blocks[2].SequencedForceBatches[0][0].MinForcedTimestamp)
	assert.Equal(t, 0, order[blocks[2].BlockHash][0].Pos)
}

func TestSendSequences(t *testing.T) {
	// Set up testing environment
	etherman, ethBackend, auth, _, br := newTestingEnv()

	// Read currentBlock
	ctx := context.Background()
	initBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)

	// Make a bridge tx
	auth.Value = big.NewInt(1000000000000000)
	_, err = br.BridgeAsset(auth, 1, auth.From, auth.Value, common.Address{}, true, []byte{})
	require.NoError(t, err)
	ethBackend.Commit()
	auth.Value = big.NewInt(0)

	// Get the last ger
	ger, err := etherman.GlobalExitRootManager.GetLastGlobalExitRoot(nil)
	require.NoError(t, err)

	currentBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)

	tx1 := types.NewTransaction(uint64(0), common.Address{}, big.NewInt(10), uint64(1), big.NewInt(10), []byte{})
	batchL2Data, err := state.EncodeTransactions([]types.Transaction{*tx1})
	require.NoError(t, err)
	sequence := ethmanTypes.Sequence{
		GlobalExitRoot: ger,
		Timestamp:      int64(currentBlock.Time() - 1),
		BatchL2Data:    batchL2Data,
	}
	tx, err := etherman.sequenceBatches(*auth, []ethmanTypes.Sequence{sequence})
	require.NoError(t, err)
	log.Debug("TX: ", tx.Hash())
	ethBackend.Commit()

	// Now read the event
	finalBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)
	finalBlockNumber := finalBlock.NumberU64()
	blocks, order, err := etherman.GetRollupInfoByBlockRange(ctx, initBlock.NumberU64(), &finalBlockNumber)
	require.NoError(t, err)
	t.Log("Blocks: ", blocks)
	assert.Equal(t, 3, len(blocks))
	assert.Equal(t, 1, len(blocks[2].SequencedBatches))
	assert.Equal(t, currentBlock.Time()-1, blocks[2].SequencedBatches[0][0].Timestamp)
	assert.Equal(t, ger, blocks[2].SequencedBatches[0][0].GlobalExitRoot)
	assert.Equal(t, auth.From, blocks[2].SequencedBatches[0][0].Coinbase)
	assert.Equal(t, auth.From, blocks[2].SequencedBatches[0][0].SequencerAddr)
	assert.Equal(t, uint64(0), blocks[2].SequencedBatches[0][0].MinForcedTimestamp)
	assert.Equal(t, 0, order[blocks[2].BlockHash][0].Pos)
}

func TestGasPrice(t *testing.T) {
	// Set up testing environment
	etherman, _, _, _, _ := newTestingEnv()
	etherscanM := new(etherscanMock)
	ethGasStationM := new(ethGasStationMock)
	etherman.GasProviders.Providers = []ethereum.GasPricer{etherman.EthClient, etherscanM, ethGasStationM}
	ctx := context.Background()

	etherscanM.On("SuggestGasPrice", ctx).Return(big.NewInt(765625003), nil)
	ethGasStationM.On("SuggestGasPrice", ctx).Return(big.NewInt(765625002), nil)
	gp := etherman.GetL1GasPrice(ctx)
	assert.Equal(t, big.NewInt(765625003), gp)

	etherman.GasProviders.Providers = []ethereum.GasPricer{etherman.EthClient, ethGasStationM}

	gp = etherman.GetL1GasPrice(ctx)
	assert.Equal(t, big.NewInt(765625002), gp)
}

func TestErrorEthGasStationPrice(t *testing.T) {
	// Set up testing environment
	etherman, _, _, _, _ := newTestingEnv()
	ethGasStationM := new(ethGasStationMock)
	etherman.GasProviders.Providers = []ethereum.GasPricer{etherman.EthClient, ethGasStationM}
	ctx := context.Background()

	ethGasStationM.On("SuggestGasPrice", ctx).Return(big.NewInt(0), fmt.Errorf("error getting gasPrice from ethGasStation"))
	gp := etherman.GetL1GasPrice(ctx)
	assert.Equal(t, big.NewInt(765625001), gp)

	etherscanM := new(etherscanMock)
	etherman.GasProviders.Providers = []ethereum.GasPricer{etherman.EthClient, etherscanM, ethGasStationM}

	etherscanM.On("SuggestGasPrice", ctx).Return(big.NewInt(765625003), nil)
	gp = etherman.GetL1GasPrice(ctx)
	assert.Equal(t, big.NewInt(765625003), gp)
}

func TestErrorEtherScanPrice(t *testing.T) {
	// Set up testing environment
	etherman, _, _, _, _ := newTestingEnv()
	etherscanM := new(etherscanMock)
	ethGasStationM := new(ethGasStationMock)
	etherman.GasProviders.Providers = []ethereum.GasPricer{etherman.EthClient, etherscanM, ethGasStationM}
	ctx := context.Background()

	etherscanM.On("SuggestGasPrice", ctx).Return(big.NewInt(0), fmt.Errorf("error getting gasPrice from etherscan"))
	ethGasStationM.On("SuggestGasPrice", ctx).Return(big.NewInt(765625002), nil)
	gp := etherman.GetL1GasPrice(ctx)
	assert.Equal(t, big.NewInt(765625002), gp)
}

func TestGetForks(t *testing.T) {
	// Set up testing environment
	etherman, _, _, _, _ := newTestingEnv()
	ctx := context.Background()
	forks, err := etherman.GetForks(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, len(forks))
	assert.Equal(t, uint64(1), forks[0].ForkId)
	assert.Equal(t, uint64(1), forks[0].FromBatchNumber)
	assert.Equal(t, uint64(math.MaxUint64), forks[0].ToBatchNumber)
	assert.Equal(t, "v1", forks[0].Version)
	// Now read the event
	finalBlock, err := etherman.EthClient.BlockByNumber(ctx, nil)
	require.NoError(t, err)
	finalBlockNumber := finalBlock.NumberU64()
	blocks, order, err := etherman.GetRollupInfoByBlockRange(ctx, 0, &finalBlockNumber)
	require.NoError(t, err)
	t.Logf("Blocks: %+v", blocks)
	assert.Equal(t, 1, len(blocks))
	assert.Equal(t, 1, len(blocks[0].ForkIDs))
	assert.Equal(t, 0, order[blocks[0].BlockHash][0].Pos)
	assert.Equal(t, ForkIDsOrder, order[blocks[0].BlockHash][0].Name)
	assert.Equal(t, uint64(0), blocks[0].ForkIDs[0].BatchNumber)
	assert.Equal(t, uint64(1), blocks[0].ForkIDs[0].ForkID)
	assert.Equal(t, "v1", blocks[0].ForkIDs[0].Version)
}
