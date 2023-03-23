package commands

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon/turbo/trie"

	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages"
)

func TestEstimateGas(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, stages.Mock(t))
	mining := txpool.NewMiningClient(conn)
	ff := rpchelper.New(ctx, nil, nil, mining, func() {})
	api := NewEthAPI(NewBaseApi(ff, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs), m.DB, nil, nil, nil, 5000000, 100_000)
	var from = libcommon.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = libcommon.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	if _, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From: &from,
		To:   &to,
	}, nil); err != nil {
		t.Errorf("calling EstimateGas: %v", err)
	}
}

func TestEthCallNonCanonical(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs), m.DB, nil, nil, nil, 5000000, 100_000)
	var from = libcommon.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = libcommon.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	if _, err := api.Call(context.Background(), ethapi.CallArgs{
		From: &from,
		To:   &to,
	}, rpc.BlockNumberOrHashWithHash(libcommon.HexToHash("0x3fcb7c0d4569fddc89cbea54b42f163e0c789351d98810a513895ab44b47020b"), true), nil); err != nil {
		if fmt.Sprintf("%v", err) != "hash 3fcb7c0d4569fddc89cbea54b42f163e0c789351d98810a513895ab44b47020b is not currently canonical" {
			t.Errorf("wrong error: %v", err)
		}
	}
}

func TestEthCallToPrunedBlock(t *testing.T) {
	pruneTo := uint64(3)
	ethCallBlockNumber := rpc.BlockNumber(2)

	m, bankAddress, contractAddress := chainWithDeployedContract(t)
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3)

	doPrune(t, m.DB, pruneTo)

	agg := m.HistoryV3Components()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs), m.DB, nil, nil, nil, 5000000, 100_000)

	callData := hexutil.MustDecode("0x2e64cec1")
	callDataBytes := hexutil.Bytes(callData)

	if _, err := api.Call(context.Background(), ethapi.CallArgs{
		From: &bankAddress,
		To:   &contractAddress,
		Data: &callDataBytes,
	}, rpc.BlockNumberOrHashWithNumber(ethCallBlockNumber), nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

type valueNode []byte
type hashNode libcommon.Hash

type shortNode struct {
	Key trie.Keybytes
	Val any
}

type fullNode struct {
	Children [17]any
}

func decodeRef(t *testing.T, buf []byte) (any, []byte) {
	t.Helper()
	kind, val, rest, err := rlp.Split(buf)
	require.NoError(t, err)
	switch {
	case kind == rlp.List:
		require.Less(t, len(buf)-len(rest), length.Hash, "embedded nodes must be less than hash size")
		return decodeNode(t, buf), rest
	case kind == rlp.String && len(val) == 0:
		return nil, rest
	case kind == rlp.String && len(val) == 32:
		return hashNode(libcommon.CastToHash(val)), rest
	default:
		t.Fatalf("invalid RLP string size %d (want 0 through 32)", len(val))
		return nil, rest
	}
}

func decodeFull(t *testing.T, elems []byte) fullNode {
	t.Helper()
	n := fullNode{}
	for i := 0; i < 16; i++ {
		n.Children[i], elems = decodeRef(t, elems)
	}
	val, _, err := rlp.SplitString(elems)
	require.NoError(t, err)
	if len(val) > 0 {
		n.Children[16] = valueNode(val)
	}
	return n
}

func decodeShort(t *testing.T, elems []byte) shortNode {
	t.Helper()
	kbuf, rest, err := rlp.SplitString(elems)
	require.NoError(t, err)
	kb := trie.CompactToKeybytes(kbuf)
	if kb.Terminating {
		val, _, err := rlp.SplitString(rest)
		require.NoError(t, err)
		return shortNode{
			Key: kb,
			Val: valueNode(val),
		}
	}

	val, _ := decodeRef(t, rest)
	return shortNode{
		Key: kb,
		Val: val,
	}
}

func decodeNode(t *testing.T, encoded []byte) any {
	t.Helper()
	require.NotEmpty(t, encoded)
	elems, _, err := rlp.SplitList(encoded)
	require.NoError(t, err)
	switch c, _ := rlp.CountValues(elems); c {
	case 2:
		return decodeShort(t, elems)
	case 17:
		return decodeFull(t, elems)
	default:
		t.Fatalf("invalid number of list elements: %v", c)
		return nil // unreachable
	}
}

// proofMap creates a map from hash to proof node
func proofMap(t *testing.T, proof []hexutil.Bytes) map[libcommon.Hash]any {
	res := map[libcommon.Hash]any{}
	for _, proofB := range proof {
		res[crypto.Keccak256Hash(proofB)] = decodeNode(t, proofB)
	}
	return res
}

func verifyProof(t *testing.T, root libcommon.Hash, key []byte, proofs map[libcommon.Hash]any) []byte {
	t.Helper()
	key = (&trie.Keybytes{Data: key}).ToHex()
	var node any = hashNode(root)
	for {
		switch nt := node.(type) {
		case fullNode:
			require.NotEmpty(t, key, "full nodes should not have values")
			node, key = nt.Children[key[0]], key[1:]
		case shortNode:
			shortHex := nt.Key.ToHex()[:nt.Key.Nibbles()] // There is a trailing 0 on odd otherwise
			require.LessOrEqual(t, len(shortHex), len(key))
			require.Equal(t, shortHex, key[:len(shortHex)])
			node, key = nt.Val, key[len(shortHex):]
		case hashNode:
			var ok bool
			node, ok = proofs[libcommon.Hash(nt)]
			require.True(t, ok, "missing hash %x", nt)
		case valueNode:
			require.Len(t, key, 0)
			return nt
		default:
			t.Fatalf("unexpected type: %T", node)
		}
	}
}

func verifyAccountProof(t *testing.T, stateRoot libcommon.Hash, proof *accounts.AccProofResult) {
	t.Helper()
	accountKey := crypto.Keccak256(proof.Address[:])
	pm := proofMap(t, proof.AccountProof)
	value := verifyProof(t, stateRoot, accountKey, pm)

	expected, err := rlp.EncodeToBytes([]any{
		uint64(proof.Nonce),
		proof.Balance.ToInt().Bytes(),
		proof.StorageHash,
		proof.CodeHash,
	})
	require.NoError(t, err)

	require.Equal(t, expected, value)
}

func verifyStorageProof(t *testing.T, storageRoot libcommon.Hash, proof accounts.StorProofResult) {
	t.Helper()

	storageKey := crypto.Keccak256(proof.Key[:])
	pm := proofMap(t, proof.Proof)
	value := verifyProof(t, storageRoot, storageKey, pm)

	expected, err := rlp.EncodeToBytes(proof.Value.ToInt().Bytes())
	require.NoError(t, err)

	require.Equal(t, expected, value)
}

func TestGetProof(t *testing.T) {
	maxGetProofRewindBlockCount = 1 // Note, this is unsafe for parallel tests, but, this test is the only consumer for now

	m, bankAddress, _ := chainWithDeployedContract(t)
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3)

	if m.HistoryV3 {
		t.Skip("not supported by Erigon3")
	}
	agg := m.HistoryV3Components()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs), m.DB, nil, nil, nil, 5000000, 100_000)

	tests := []struct {
		name        string
		blockNum    uint64
		storageKeys []libcommon.Hash
		expectedErr string
	}{
		{
			name:     "currentBlock",
			blockNum: 3,
		},
		{
			name:        "withState",
			blockNum:    3,
			storageKeys: []libcommon.Hash{{1}},
			expectedErr: "the method is currently not implemented: eth_getProof with storageKeys",
		},
		{
			name:     "olderBlock",
			blockNum: 2,
		},
		{
			name:        "tooOldBlock",
			blockNum:    1,
			expectedErr: "requested block is too old, block must be within 1 blocks of the head block number (currently 3)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proof, err := api.GetProof(
				context.Background(),
				bankAddress,
				tt.storageKeys,
				rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(tt.blockNum)),
			)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				require.Nil(t, proof)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, proof)

			tx, err := m.DB.BeginRo(context.Background())
			assert.NoError(t, err)
			defer tx.Rollback()
			header, err := api.headerByRPCNumber(rpc.BlockNumber(tt.blockNum), tx)
			require.NoError(t, err)

			require.Equal(t, bankAddress, proof.Address)
			verifyAccountProof(t, header.Root, proof)

			require.Equal(t, len(tt.storageKeys), len(proof.StorageProof))
			for _, storageKey := range tt.storageKeys {
				for _, storageProof := range proof.StorageProof {
					if storageProof.Key != storageKey {
						continue
					}
					verifyStorageProof(t, proof.StorageHash, storageProof)
				}
			}
		})
	}
}

func TestGetBlockByTimestampLatestTime(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3)
	tx, err := m.DB.BeginRo(ctx)
	if err != nil {
		t.Errorf("fail at beginning tx")
	}
	defer tx.Rollback()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewErigonAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs), m.DB, nil)

	latestBlock := rawdb.ReadCurrentBlock(tx)
	response, err := ethapi.RPCMarshalBlockDeprecated(latestBlock, true, false)

	if err != nil {
		t.Error("couldn't get the rpc marshal block")
	}

	if err == nil && rpc.BlockNumber(latestBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(latestBlock.Header().Time), false)
	if err != nil {
		t.Errorf("couldn't retrieve block %v", err)
	}

	if block["timestamp"] != response["timestamp"] || block["hash"] != response["hash"] {
		t.Errorf("Retrieved the wrong block.\nexpected block hash: %s expected timestamp: %d\nblock hash retrieved: %s timestamp retrieved: %d", response["hash"], response["timestamp"], block["hash"], block["timestamp"])
	}
}

func TestGetBlockByTimestampOldestTime(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3)
	tx, err := m.DB.BeginRo(ctx)
	if err != nil {
		t.Errorf("failed at beginning tx")
	}
	defer tx.Rollback()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewErigonAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs), m.DB, nil)

	oldestBlock, err := rawdb.ReadBlockByNumber(tx, 0)
	if err != nil {
		t.Error("couldn't retrieve oldest block")
	}

	response, err := ethapi.RPCMarshalBlockDeprecated(oldestBlock, true, false)

	if err != nil {
		t.Error("couldn't get the rpc marshal block")
	}

	if err == nil && rpc.BlockNumber(oldestBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(oldestBlock.Header().Time), false)
	if err != nil {
		t.Errorf("couldn't retrieve block %v", err)
	}

	if block["timestamp"] != response["timestamp"] || block["hash"] != response["hash"] {
		t.Errorf("Retrieved the wrong block.\nexpected block hash: %s expected timestamp: %d\nblock hash retrieved: %s timestamp retrieved: %d", response["hash"], response["timestamp"], block["hash"], block["timestamp"])
	}
}

func TestGetBlockByTimeHigherThanLatestBlock(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3)
	tx, err := m.DB.BeginRo(ctx)
	if err != nil {
		t.Errorf("fail at beginning tx")
	}
	defer tx.Rollback()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewErigonAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs), m.DB, nil)

	latestBlock := rawdb.ReadCurrentBlock(tx)

	response, err := ethapi.RPCMarshalBlockDeprecated(latestBlock, true, false)

	if err != nil {
		t.Error("couldn't get the rpc marshal block")
	}

	if err == nil && rpc.BlockNumber(latestBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(latestBlock.Header().Time+999999999999), false)
	if err != nil {
		t.Errorf("couldn't retrieve block %v", err)
	}

	if block["timestamp"] != response["timestamp"] || block["hash"] != response["hash"] {
		t.Errorf("Retrieved the wrong block.\nexpected block hash: %s expected timestamp: %d\nblock hash retrieved: %s timestamp retrieved: %d", response["hash"], response["timestamp"], block["hash"], block["timestamp"])
	}
}

func TestGetBlockByTimeMiddle(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3)
	tx, err := m.DB.BeginRo(ctx)
	if err != nil {
		t.Errorf("fail at beginning tx")
	}
	defer tx.Rollback()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewErigonAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs), m.DB, nil)

	currentHeader := rawdb.ReadCurrentHeader(tx)
	oldestHeader, err := api._blockReader.HeaderByNumber(ctx, tx, 0)
	if err != nil {
		t.Errorf("error getting the oldest header %s", err)
	}
	if oldestHeader == nil {
		t.Error("couldn't find oldest header")
	}

	middleNumber := (currentHeader.Number.Uint64() + oldestHeader.Number.Uint64()) / 2
	middleBlock, err := rawdb.ReadBlockByNumber(tx, middleNumber)
	if err != nil {
		t.Error("couldn't retrieve middle block")
	}

	response, err := ethapi.RPCMarshalBlockDeprecated(middleBlock, true, false)

	if err != nil {
		t.Error("couldn't get the rpc marshal block")
	}

	if err == nil && rpc.BlockNumber(middleBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(middleBlock.Header().Time), false)
	if err != nil {
		t.Errorf("couldn't retrieve block %v", err)
	}
	if block["timestamp"] != response["timestamp"] || block["hash"] != response["hash"] {
		t.Errorf("Retrieved the wrong block.\nexpected block hash: %s expected timestamp: %d\nblock hash retrieved: %s timestamp retrieved: %d", response["hash"], response["timestamp"], block["hash"], block["timestamp"])
	}
}

func TestGetBlockByTimestamp(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3)
	tx, err := m.DB.BeginRo(ctx)
	if err != nil {
		t.Errorf("fail at beginning tx")
	}
	defer tx.Rollback()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewErigonAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs), m.DB, nil)

	highestBlockNumber := rawdb.ReadCurrentHeader(tx).Number
	pickedBlock, err := rawdb.ReadBlockByNumber(tx, highestBlockNumber.Uint64()/3)
	if err != nil {
		t.Errorf("couldn't get block %v", pickedBlock.Number())
	}

	if pickedBlock == nil {
		t.Error("couldn't retrieve picked block")
	}
	response, err := ethapi.RPCMarshalBlockDeprecated(pickedBlock, true, false)

	if err != nil {
		t.Error("couldn't get the rpc marshal block")
	}

	if err == nil && rpc.BlockNumber(pickedBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(pickedBlock.Header().Time), false)
	if err != nil {
		t.Errorf("couldn't retrieve block %v", err)
	}

	if block["timestamp"] != response["timestamp"] || block["hash"] != response["hash"] {
		t.Errorf("Retrieved the wrong block.\nexpected block hash: %s expected timestamp: %d\nblock hash retrieved: %s timestamp retrieved: %d", response["hash"], response["timestamp"], block["hash"], block["timestamp"])
	}
}

func chainWithDeployedContract(t *testing.T) (*stages.MockSentry, libcommon.Address, libcommon.Address) {
	var (
		signer      = types.LatestSignerForChainID(nil)
		bankKey, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		bankAddress = crypto.PubkeyToAddress(bankKey.PublicKey)
		bankFunds   = big.NewInt(1e9)
		contract    = hexutil.MustDecode("0x608060405234801561001057600080fd5b50610150806100206000396000f3fe608060405234801561001057600080fd5b50600436106100365760003560e01c80632e64cec11461003b5780636057361d14610059575b600080fd5b610043610075565b60405161005091906100d9565b60405180910390f35b610073600480360381019061006e919061009d565b61007e565b005b60008054905090565b8060008190555050565b60008135905061009781610103565b92915050565b6000602082840312156100b3576100b26100fe565b5b60006100c184828501610088565b91505092915050565b6100d3816100f4565b82525050565b60006020820190506100ee60008301846100ca565b92915050565b6000819050919050565b600080fd5b61010c816100f4565b811461011757600080fd5b5056fea26469706673582212209a159a4f3847890f10bfb87871a61eba91c5dbf5ee3cf6398207e292eee22a1664736f6c63430008070033")
		gspec       = &core.Genesis{
			Config: params.TestChainConfig,
			Alloc:  core.GenesisAlloc{bankAddress: {Balance: bankFunds}},
		}
	)
	m := stages.MockWithGenesis(t, gspec, bankKey, false)
	db := m.DB

	var contractAddr libcommon.Address

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, block *core.BlockGen) {
		nonce := block.TxNonce(bankAddress)
		switch i {
		case 0:
			tx, err := types.SignTx(types.NewContractCreation(nonce, new(uint256.Int), 1e6, new(uint256.Int), contract), *signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
			contractAddr = crypto.CreateAddress(bankAddress, nonce)
		case 1, 2:
			txn, err := types.SignTx(types.NewTransaction(nonce, contractAddr, new(uint256.Int), 90000, new(uint256.Int), nil), *signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(txn)
		}
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	err = m.InsertChain(chain)
	assert.NoError(t, err)

	tx, err := db.BeginRo(context.Background())
	if err != nil {
		t.Fatalf("read only db tx to read state: %v", err)
	}
	defer tx.Rollback()

	stateReader, err := rpchelper.CreateHistoryStateReader(tx, 1, 0, m.HistoryV3, "")
	assert.NoError(t, err)
	st := state.New(stateReader)
	assert.NoError(t, err)
	assert.False(t, st.Exist(contractAddr), "Contract should not exist at block #1")

	stateReader, err = rpchelper.CreateHistoryStateReader(tx, 2, 0, m.HistoryV3, "")
	assert.NoError(t, err)
	st = state.New(stateReader)
	assert.NoError(t, err)
	assert.True(t, st.Exist(contractAddr), "Contract should exist at block #2")

	return m, bankAddress, contractAddr
}

func doPrune(t *testing.T, db kv.RwDB, pruneTo uint64) {
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	assert.NoError(t, err)

	logEvery := time.NewTicker(20 * time.Second)

	err = rawdb.PruneTableDupSort(tx, kv.AccountChangeSet, "", pruneTo, logEvery, ctx)
	assert.NoError(t, err)

	err = rawdb.PruneTableDupSort(tx, kv.StorageChangeSet, "", pruneTo, logEvery, ctx)
	assert.NoError(t, err)

	err = rawdb.PruneTable(tx, kv.Receipts, pruneTo, ctx, math.MaxInt32)
	assert.NoError(t, err)

	err = rawdb.PruneTable(tx, kv.Log, pruneTo, ctx, math.MaxInt32)
	assert.NoError(t, err)

	err = rawdb.PruneTableDupSort(tx, kv.CallTraceSet, "", pruneTo, logEvery, ctx)
	assert.NoError(t, err)

	err = tx.Commit()
	assert.NoError(t, err)
}
