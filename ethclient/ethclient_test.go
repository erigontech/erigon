// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethclient_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"path"
	"reflect"
	"testing"
	"time"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/ethclient"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/app"
	"github.com/erigontech/erigon/turbo/stages"
	"github.com/erigontech/erigon/turbo/testlog"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

// Verify that Client implements the ethereum interfaces.
var (
	_ = ethereum.ChainReader(&ethclient.Client{})
	_ = ethereum.TransactionReader(&ethclient.Client{})
	_ = ethereum.ChainStateReader(&ethclient.Client{})
	_ = ethereum.ChainSyncReader(&ethclient.Client{})
	_ = ethereum.ContractCaller(&ethclient.Client{})
	_ = ethereum.GasEstimator(&ethclient.Client{})
	_ = ethereum.GasPricer(&ethclient.Client{})
	_ = ethereum.LogFilterer(&ethclient.Client{})
	_ = ethereum.PendingStateReader(&ethclient.Client{})
	// _ = ethereum.PendingStateEventer(&ethclient.Client{})
	_ = ethereum.PendingContractCaller(&ethclient.Client{})
)

var (
	testKey, _         = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddr           = crypto.PubkeyToAddress(testKey.PublicKey)
	testBalance        = big.NewInt(2e15)
	revertContractAddr = common.HexToAddress("290f1b36649a61e369c6276f6d29463335b4400c")
	revertCode         = common.FromHex("7f08c379a0000000000000000000000000000000000000000000000000000000006000526020600452600a6024527f75736572206572726f7200000000000000000000000000000000000000000000604452604e6000fd")
)

var (
	testNodeKey, _ = crypto.GenerateKey()
)

// var genesis = &types.Genesis{
// 	Config: params.AllDevChainProtocolChanges,
// 	Alloc: types.GenesisAlloc{
// 		testAddr:           {Balance: testBalance},
// 		revertContractAddr: {Code: revertCode, Balance: testBalance},
// 	},
// 	ExtraData: []byte("test genesis"),
// 	Timestamp: 9000,
// 	BaseFee:   big.NewInt(params.InitialBaseFee),
// }

var chainId = big.NewInt(987656789)

var genesisBlock *types.Block

func getChainConfig() *chain.Config {
	chainConfig := *params.ChiadoChainConfig
	chainConfig.ChainName = "ethclient-test"
	chainConfig.ChainID = chainId
	chainConfig.TerminalTotalDifficulty = big.NewInt(0)
	chainConfig.ShanghaiTime = big.NewInt(0)
	chainConfig.CancunTime = big.NewInt(0)
	chainConfig.PragueTime = big.NewInt(0)
	return &chainConfig
}

var testTx1 = types.MustSignNewTx(testKey, *types.LatestSigner(getChainConfig()), &types.LegacyTx{
	CommonTx: types.CommonTx{
		Nonce:    0,
		Value:    uint256.NewInt(12),
		GasLimit: params.TxGas,
		To:       &common.Address{2},
	},
	GasPrice: uint256.NewInt(params.InitialBaseFee),
})

var testTx2 = types.MustSignNewTx(testKey, *types.LatestSigner(getChainConfig()), &types.LegacyTx{
	CommonTx: types.CommonTx{
		Nonce:    1,
		Value:    uint256.NewInt(8),
		GasLimit: params.TxGas,
		To:       &common.Address{2},
	},
	GasPrice: uint256.NewInt(params.InitialBaseFee),
})

var generateTestChain = func(i int, g *core.BlockGen) {
	g.OffsetTime(5)
	g.SetExtra([]byte("test"))
	g.SetDifficulty(big.NewInt(0))
	if i == 1 {
		// Test transactions are included in block #2.
		g.AddTx(testTx1)
		g.AddTx(testTx2)
	}
}

func newTestBackend(t *testing.T) (*eth.Ethereum, *core.ChainPack, error) {
	logger := testlog.Logger(t, log.LvlDebug)
	dataDir := t.TempDir()
	dirs := datadir.New(dataDir)
	const localhost = "127.0.0.1"
	jsonRpcPort := 9999 // todo: calculate dynamically
	httpConfig := httpcfg.HttpCfg{
		Enabled:                  true,
		HttpServerEnabled:        true,
		HttpListenAddress:        localhost,
		HttpPort:                 jsonRpcPort,
		API:                      []string{"eth"},
		AuthRpcHTTPListenAddress: localhost,
		JWTSecretPath:            path.Join(dataDir, "jwt.hex"),
		ReturnDataLimit:          100_000,
	}

	nodeConfig := nodecfg.Config{
		Dirs: dirs,
		Http: httpConfig,
		P2P:  nodecfg.DefaultConfig.P2P,
	}
	nodeConfig.P2P.ProtocolVersion = []uint{direct.ETH67}
	nodeConfig.P2P.AllowedPorts = []uint{30303}
	nodeConfig.P2P.PrivateKey = testNodeKey

	chainConfig := getChainConfig()
	genesis := core.ChiadoGenesisBlock()
	// genesis.Timestamp = uint64(time.Now().Unix() - 1)
	genesis.Timestamp = 1743777048
	genesis.Config = chainConfig
	genesis.Alloc = types.GenesisAlloc{
		testAddr:           {Balance: testBalance},
		revertContractAddr: {Code: revertCode, Balance: testBalance},
	}

	ethConfig := ethconfig.Defaults
	ethConfig.ImportMode = true // this is to avoid getting stuck on a never-ending loop waiting for headers
	ethConfig.Snapshot.NoDownloader = true
	ethConfig.Snapshot.DisableDownloadE3 = true
	ethConfig.NetworkID = 55
	ethConfig.Genesis = genesis
	ethConfig.Dirs = dirs
	// ethConfig.TxPool.Disable = true
	ethConfig.TxPool.DBDir = dirs.TxPool
	return newBackend(&nodeConfig, &ethConfig, logger)

}

func newBackend(nodeCfg *nodecfg.Config, ethCfg *ethconfig.Config, logger log.Logger) (*eth.Ethereum, *core.ChainPack, error) {
	// Generate test chain.
	// blocks := generateTestChain()

	// Create node
	if nodeCfg == nil {
		nodeCfg = new(nodecfg.Config)
	}
	ctx := context.Background()
	n, err := node.New(ctx, nodeCfg, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("can't create new node: %v", err)
	}

	// Create Ethereum Service
	ethservice, err := eth.New(ctx, n, ethCfg, logger, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("can't create new ethereum service: %v", err)
	}

	_, genesisBlock, err = core.CommitGenesisBlock(ethservice.ChainDB(), ethCfg.Genesis, n.Config().Dirs, logger)
	if err != nil {
		return nil, nil, err
	}

	err = ethservice.Init(n, ethCfg, ethCfg.Genesis.Config)
	if err != nil {
		return nil, nil, err
	}

	// ethservice.Start()
	blockReader, _ := ethservice.BlockIO()
	err = stages.StageLoopIteration(ctx, ethservice.ChainDB(), wrap.TxContainer{}, ethservice.StagedSync(), false, false, logger, blockReader, nil)
	if err != nil {
		return nil, nil, err
	}

	chain, err := core.GenerateChainWithReader(ethservice.ChainConfig(), genesisBlock, blockReader, ethservice.SentryControlServer().Engine, ethservice.ChainDB(), 2, generateTestChain)
	if err != nil {
		return nil, nil, err
	}
	firstHash := chain.Headers[0].Hash()
	fmt.Printf("firstHash = %x\n", firstHash)
	parentHash := chain.Headers[1].ParentHash
	fmt.Printf("parentHash = %x\n", parentHash)

	if err := n.Start(); err != nil {
		return nil, nil, fmt.Errorf("can't start test node: %v", err)
	}

	err = app.InsertChain(ethservice, chain, logger)
	if err != nil {
		return nil, nil, err
	}
	return ethservice, chain, nil
}

func TestEthClient(t *testing.T) {
	// t.Skip()
	backend, chain, err := newTestBackend(t)
	require.NoError(t, err)
	defer backend.Stop()

	client := backend.Attach()

	defer client.Close()

	tests := map[string]struct {
		test func(t *testing.T)
	}{
		"Header": {
			func(t *testing.T) { testHeader(t, genesisBlock, chain.Blocks, client) },
		},
		"BalanceAt": {
			func(t *testing.T) { testBalanceAt(t, client) },
		},
		"TxInBlockInterrupted": {
			func(t *testing.T) { testTransactionInBlock(t, client) },
		},
		// "ChainID": {
		// 	func(t *testing.T) { testChainID(t, client) },
		// },
		// "GetBlock": {
		// 	func(t *testing.T) { testGetBlock(t, client) },
		// },
		// "StatusFunctions": {
		// 	func(t *testing.T) { testStatusFunctions(t, client) },
		// },
		// "CallContract": {
		// 	func(t *testing.T) { testCallContract(t, client) },
		// },
		// "CallContractAtHash": {
		// 	func(t *testing.T) { testCallContractAtHash(t, client) },
		// },
		// "AtFunctions": {
		// 	func(t *testing.T) { testAtFunctions(t, client) },
		// },
		// "TransactionSender": {
		// 	func(t *testing.T) { testTransactionSender(t, client) },
		// },
	}

	// t.Parallel()
	for name, tt := range tests {
		t.Run(name, tt.test)
	}
}

func testHeader(t *testing.T, genesisBlock *types.Block, chain []*types.Block, client *rpc.Client) {
	tests := map[string]struct {
		block   *big.Int
		want    *types.Header
		wantErr error
	}{
		"genesis": {
			block: big.NewInt(0),
			want:  genesisBlock.Header(),
		},
		"first_block": {
			block: big.NewInt(1),
			want:  chain[0].Header(),
		},
		"future_block": {
			block:   big.NewInt(1000000000),
			wantErr: ethereum.NotFound,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ec := ethclient.NewClient(client)
			ctx := context.Background()

			got, err := ec.HeaderByNumber(ctx, tt.block)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("HeaderByNumber(%v) error = %q, want %q", tt.block, err, tt.wantErr)
			}
			if got != nil && got.Number != nil && got.Number.Sign() == 0 {
				got.Number = big.NewInt(0) // hack to make DeepEqual work
			}
			if got != nil && got.Hash() != tt.want.Hash() {
				t.Fatalf("HeaderByNumber(%v) got = %#v, want %#v", tt.block, got, tt.want)
			}
		})
	}
}

func testBalanceAt(t *testing.T, client *rpc.Client) {
	tests := map[string]struct {
		account common.Address
		block   *big.Int
		want    *big.Int
		wantErr error
	}{
		"valid_account_genesis": {
			account: testAddr,
			block:   big.NewInt(0),
			want:    testBalance,
		},
		"valid_account": {
			account: testAddr,
			block:   big.NewInt(1),
			want:    testBalance,
		},
		"non_existent_account": {
			account: common.Address{1},
			block:   big.NewInt(1),
			want:    big.NewInt(0),
		},
		// "future_block": { // disabled due to incompatibility with geth
		// 	account: testAddr,
		// 	block:   big.NewInt(1000000000),
		// 	wantErr: errors.New("header not found"),
		// },
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ec := ethclient.NewClient(client)
			ctx := context.Background()

			got, err := ec.BalanceAt(ctx, tt.account, tt.block)
			if tt.wantErr != nil && (err == nil || err.Error() != tt.wantErr.Error()) {
				t.Fatalf("BalanceAt(%x, %v) error = %q, want %q", tt.account, tt.block, err, tt.wantErr)
			}
			if got != nil && got.Cmp(tt.want) != 0 {
				t.Fatalf("BalanceAt(%x, %v) = %v, want %v", tt.account, tt.block, got, tt.want)
			}
		})
	}
}

func testTransactionInBlock(t *testing.T, client *rpc.Client) {
	ec := ethclient.NewClient(client)

	// Get current block by number.
	block, err := ec.BlockByNumber(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Test tx in block not found.
	if _, err := ec.TransactionInBlock(context.Background(), block.Hash(), 20); err != ethereum.NotFound {
		t.Fatal("error should be ethereum.NotFound")
	}

	// Test tx in block found.
	tx, err := ec.TransactionInBlock(context.Background(), block.Hash(), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.Hash() != testTx1.Hash() {
		t.Fatalf("unexpected transaction: %v", tx)
	}

	tx, err = ec.TransactionInBlock(context.Background(), block.Hash(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.Hash() != testTx2.Hash() {
		t.Fatalf("unexpected transaction: %v", tx)
	}
}

func testChainID(t *testing.T, client *rpc.Client) {
	ec := ethclient.NewClient(client)
	id, err := ec.ChainID(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id == nil || id.Cmp(chainId) != 0 {
		t.Fatalf("ChainID returned wrong number: %+v", id)
	}
}

func testGetBlock(t *testing.T, client *rpc.Client) {
	ec := ethclient.NewClient(client)

	// Get current block number
	blockNumber, err := ec.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if blockNumber != 2 {
		t.Fatalf("BlockNumber returned wrong number: %d", blockNumber)
	}
	// Get current block by number
	block, err := ec.BlockByNumber(context.Background(), new(big.Int).SetUint64(blockNumber))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if block.NumberU64() != blockNumber {
		t.Fatalf("BlockByNumber returned wrong block: want %d got %d", blockNumber, block.NumberU64())
	}
	// Get current block by hash
	blockH, err := ec.BlockByHash(context.Background(), block.Hash())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if block.Hash() != blockH.Hash() {
		t.Fatalf("BlockByHash returned wrong block: want %v got %v", block.Hash().Hex(), blockH.Hash().Hex())
	}
	// Get header by number
	header, err := ec.HeaderByNumber(context.Background(), new(big.Int).SetUint64(blockNumber))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if block.Header().Hash() != header.Hash() {
		t.Fatalf("HeaderByNumber returned wrong header: want %v got %v", block.Header().Hash().Hex(), header.Hash().Hex())
	}
	// Get header by hash
	headerH, err := ec.HeaderByHash(context.Background(), block.Hash())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if block.Header().Hash() != headerH.Hash() {
		t.Fatalf("HeaderByHash returned wrong header: want %v got %v", block.Header().Hash().Hex(), headerH.Hash().Hex())
	}
}

func testStatusFunctions(t *testing.T, client *rpc.Client) {
	ec := ethclient.NewClient(client)

	// Sync progress
	progress, err := ec.SyncProgress(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if progress != nil {
		t.Fatalf("unexpected progress: %v", progress)
	}

	// // NetworkID
	// networkID, err := ec.NetworkID(context.Background())
	// if err != nil {
	// 	t.Fatalf("unexpected error: %v", err)
	// }
	// if networkID.Cmp(big.NewInt(1337)) != 0 {
	// 	t.Fatalf("unexpected networkID: %v", networkID)
	// }

	// SuggestGasPrice
	gasPrice, err := ec.SuggestGasPrice(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gasPrice.Cmp(big.NewInt(1000000000)) != 0 {
		t.Fatalf("unexpected gas price: %v", gasPrice)
	}

	// SuggestGasTipCap
	gasTipCap, err := ec.SuggestGasTipCap(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gasTipCap.Cmp(big.NewInt(234375000)) != 0 {
		t.Fatalf("unexpected gas tip cap: %v", gasTipCap)
	}

	// BlobBaseFee
	blobBaseFee, err := ec.BlobBaseFee(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if blobBaseFee.Cmp(big.NewInt(1)) != 0 {
		t.Fatalf("unexpected blob base fee: %v", blobBaseFee)
	}

	// FeeHistory
	history, err := ec.FeeHistory(context.Background(), 1, big.NewInt(2), []float64{95, 99})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := &ethereum.FeeHistory{
		OldestBlock: big.NewInt(2),
		Reward: [][]*big.Int{
			{
				big.NewInt(234375000),
				big.NewInt(234375000),
			},
		},
		BaseFee: []*big.Int{
			big.NewInt(765625000),
			big.NewInt(671627818),
		},
		GasUsedRatio: []float64{0.008912678667376286},
	}
	if !reflect.DeepEqual(history, want) {
		t.Fatalf("FeeHistory result doesn't match expected: (got: %v, want: %v)", history, want)
	}
}

func testCallContractAtHash(t *testing.T, client *rpc.Client) {
	ec := ethclient.NewClient(client)

	// EstimateGas
	msg := ethereum.CallMsg{
		From:  testAddr,
		To:    &common.Address{},
		Gas:   21000,
		Value: uint256.NewInt(1),
	}
	gas, err := ec.EstimateGas(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gas != 21000 {
		t.Fatalf("unexpected gas price: %v", gas)
	}
	block, err := ec.HeaderByNumber(context.Background(), big.NewInt(1))
	if err != nil {
		t.Fatalf("BlockByNumber error: %v", err)
	}
	// CallContract
	if _, err := ec.CallContractAtHash(context.Background(), msg, block.Hash()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testCallContract(t *testing.T, client *rpc.Client) {
	ec := ethclient.NewClient(client)

	// EstimateGas
	msg := ethereum.CallMsg{
		From:  testAddr,
		To:    &common.Address{},
		Gas:   21000,
		Value: uint256.NewInt(1),
	}
	gas, err := ec.EstimateGas(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gas != 21000 {
		t.Fatalf("unexpected gas price: %v", gas)
	}
	// CallContract
	if _, err := ec.CallContract(context.Background(), msg, big.NewInt(1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// PendingCallContract
	if _, err := ec.PendingCallContract(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testAtFunctions(t *testing.T, client *rpc.Client) {
	ec := ethclient.NewClient(client)

	block, err := ec.HeaderByNumber(context.Background(), big.NewInt(1))
	if err != nil {
		t.Fatalf("BlockByNumber error: %v", err)
	}

	// send a transaction for some interesting pending status
	// and wait for the transaction to be included in the pending block
	sendTransaction(ec)

	// wait for the transaction to be included in the pending block
	for {
		// Check pending transaction count
		pending, err := ec.PendingTransactionCount(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pending == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Query balance
	balance, err := ec.BalanceAt(context.Background(), testAddr, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hashBalance, err := ec.BalanceAtHash(context.Background(), testAddr, block.Hash())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if balance.Cmp(hashBalance) == 0 {
		t.Fatalf("unexpected balance at hash: %v %v", balance, hashBalance)
	}
	penBalance, err := ec.PendingBalanceAt(context.Background(), testAddr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if balance.Cmp(penBalance) == 0 {
		t.Fatalf("unexpected balance: %v %v", balance, penBalance)
	}
	// NonceAt
	nonce, err := ec.NonceAt(context.Background(), testAddr, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hashNonce, err := ec.NonceAtHash(context.Background(), testAddr, block.Hash())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hashNonce == nonce {
		t.Fatalf("unexpected nonce at hash: %v %v", nonce, hashNonce)
	}
	penNonce, err := ec.PendingNonceAt(context.Background(), testAddr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if penNonce != nonce+1 {
		t.Fatalf("unexpected nonce: %v %v", nonce, penNonce)
	}
	// StorageAt
	storage, err := ec.StorageAt(context.Background(), testAddr, common.Hash{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hashStorage, err := ec.StorageAtHash(context.Background(), testAddr, common.Hash{}, block.Hash())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(storage, hashStorage) {
		t.Fatalf("unexpected storage at hash: %v %v", storage, hashStorage)
	}
	penStorage, err := ec.PendingStorageAt(context.Background(), testAddr, common.Hash{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(storage, penStorage) {
		t.Fatalf("unexpected storage: %v %v", storage, penStorage)
	}
	// CodeAt
	code, err := ec.CodeAt(context.Background(), testAddr, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hashCode, err := ec.CodeAtHash(context.Background(), common.Address{}, block.Hash())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(code, hashCode) {
		t.Fatalf("unexpected code at hash: %v %v", code, hashCode)
	}
	penCode, err := ec.PendingCodeAt(context.Background(), testAddr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(code, penCode) {
		t.Fatalf("unexpected code: %v %v", code, penCode)
	}
}

func testTransactionSender(t *testing.T, client *rpc.Client) {
	ec := ethclient.NewClient(client)
	ctx := context.Background()

	// Retrieve testTx1 via RPC.
	block2, err := ec.HeaderByNumber(ctx, big.NewInt(2))
	if err != nil {
		t.Fatal("can't get block 1:", err)
	}
	tx1, err := ec.TransactionInBlock(ctx, block2.Hash(), 0)
	if err != nil {
		t.Fatal("can't get tx:", err)
	}
	if tx1.Hash() != testTx1.Hash() {
		t.Fatalf("wrong tx hash %v, want %v", tx1.Hash(), testTx1.Hash())
	}

	// The sender address is cached in tx1, so no additional RPC should be required in
	// TransactionSender. Ensure the server is not asked by canceling the context here.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	<-canceledCtx.Done() // Ensure the close of the Done channel
	sender1, err := ec.TransactionSender(canceledCtx, tx1, block2.Hash(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if sender1 != testAddr {
		t.Fatal("wrong sender:", sender1)
	}

	// Now try to get the sender of testTx2, which was not fetched through RPC.
	// TransactionSender should query the server here.
	sender2, err := ec.TransactionSender(ctx, testTx2, block2.Hash(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if sender2 != testAddr {
		t.Fatal("wrong sender:", sender2)
	}
}

func sendTransaction(ec *ethclient.Client) error {
	chainID, err := ec.ChainID(context.Background())
	if err != nil {
		return err
	}
	nonce, err := ec.NonceAt(context.Background(), testAddr, nil)
	if err != nil {
		return err
	}

	signer := types.LatestSignerForChainID(chainID)
	tx, err := types.SignNewTx(testKey, *signer, &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    nonce,
			To:       &common.Address{2},
			Value:    uint256.NewInt(1),
			GasLimit: 22000,
		},
		GasPrice: uint256.NewInt(params.InitialBaseFee),
	})
	if err != nil {
		return err
	}
	return ec.SendTransaction(context.Background(), tx)
}

// // Here we show how to get the error message of reverted contract call.
// func ExampleRevertErrorData() {
// 	// First create an ethclient.Client instance.
// 	ctx := context.Background()
// 	ec, _ := ethclient.DialContext(ctx, exampleNode.HTTPEndpoint())

// 	// Call the contract.
// 	// Note we expect the call to return an error.
// 	contract := common.HexToAddress("290f1b36649a61e369c6276f6d29463335b4400c")
// 	call := ethereum.CallMsg{To: &contract, Gas: 30000}
// 	result, err := ec.CallContract(ctx, call, nil)
// 	if len(result) > 0 {
// 		panic("got result")
// 	}
// 	if err == nil {
// 		panic("call did not return error")
// 	}

// 	// Extract the low-level revert data from the error.
// 	revertData, ok := ethclient.RevertErrorData(err)
// 	if !ok {
// 		panic("unpacking revert failed")
// 	}
// 	fmt.Printf("revert: %x\n", revertData)

// 	// Parse the revert data to obtain the error message.
// 	message, err := abi.UnpackRevert(revertData)
// 	if err != nil {
// 		panic("parsing ABI error failed: " + err.Error())
// 	}
// 	fmt.Println("message:", message)

// 	// Output:
// 	// revert: 08c379a00000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000a75736572206572726f72
// 	// message: user error
// }
