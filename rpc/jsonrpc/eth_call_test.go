// Copyright 2024 The Erigon Authors
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

package jsonrpc

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func TestEstimateGas(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTestEthAPIWithFilters(t, m)
	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	_, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From: &from,
		To:   &to,
	}, nil, nil, nil)
	require.NoError(t, err)
}

// TestEstimateGasBlockOverridesGasLimit verifies that blockOverrides.gasLimit is
// used as the binary-search ceiling rather than the on-chain header gas limit.
// A contract call is used to bypass the plain-transfer short-circuit path.
func TestEstimateGasBlockOverridesGasLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, bankAddr, contractAddr, _ := chainWithDeployedContract(t)
	api := newTestEthAPIWithFilters(t, m)

	callData := hexutil.Bytes(contractInvocationData(1))

	// Sanity check: without overrides the estimation succeeds.
	_, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From: &bankAddr,
		To:   &contractAddr,
		Data: &callData,
	}, nil, nil, nil)
	require.NoError(t, err)

	// Override gasLimit to below intrinsic gas (21000). The binary search ceiling
	// becomes 20999, so execution must fail regardless of the actual gas needed.
	lowGasLimit := hexutil.Uint64(params.TxGas - 1)
	_, err = api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From: &bankAddr,
		To:   &contractAddr,
		Data: &callData,
	}, nil, nil, &ethapi.BlockOverrides{GasLimit: &lowGasLimit})
	require.EqualError(t, err, fmt.Sprintf("gas required exceeds allowance (%d)", params.TxGas-1))
}

func TestEstimateGasBlockOverridesBlobBaseFee(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, bankAddr, contractAddr, _ := chainWithDeployedContractAndConfig(t, chain.AllProtocolChanges)
	api := newTestEthAPIWithFilters(t, m)

	callData := hexutil.Bytes(contractInvocationData(1))
	blobFeeCap := (*hexutil.Big)(big.NewInt(10))
	blobBaseFee := (*hexutil.Big)(big.NewInt(11))
	args := &ethapi.CallArgs{
		From:                &bankAddr,
		To:                  &contractAddr,
		Data:                &callData,
		MaxFeePerBlobGas:    blobFeeCap,
		BlobVersionedHashes: []common.Hash{{1}},
	}

	_, err := api.EstimateGas(context.Background(), args, nil, nil, nil)
	require.NoError(t, err)

	_, err = api.EstimateGas(context.Background(), args, nil, nil, &ethapi.BlockOverrides{BlobBaseFee: blobBaseFee})
	require.ErrorIs(t, err, protocol.ErrMaxFeePerBlobGas)
}

func TestEstimateGasBlockOverridesBlobBaseFeeSkipsZeroBlobFeeCap(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, bankAddr, contractAddr, _ := chainWithDeployedContractAndConfig(t, chain.AllProtocolChanges)
	api := newTestEthAPIWithFilters(t, m)

	callData := hexutil.Bytes(contractInvocationData(1))
	_, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From:                &bankAddr,
		To:                  &contractAddr,
		Data:                &callData,
		BlobVersionedHashes: []common.Hash{{1}},
	}, nil, nil, &ethapi.BlockOverrides{
		BlobBaseFee: (*hexutil.Big)(big.NewInt(11)),
	})
	require.NoError(t, err)
}

func TestEthCallBlockOverridesBaseFeeAffectsGasPrice(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, bankAddr, contractAddr, _ := chainWithDeployedContractAndConfig(t, chain.AllProtocolChanges)
	api := newTestEthAPIWithFilters(t, m)

	callData := hexutil.Bytes{0x3a, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
	result, err := api.Call(context.Background(), ethapi.CallArgs{
		From:                 &bankAddr,
		To:                   &contractAddr,
		Data:                 &callData,
		MaxFeePerGas:         (*hexutil.Big)(big.NewInt(100)),
		MaxPriorityFeePerGas: (*hexutil.Big)(big.NewInt(2)),
	}, nil, &ethapi.StateOverrides{
		accounts.InternAddress(contractAddr): {
			Code: &callData,
		},
	}, &ethapi.BlockOverrides{
		BaseFeePerGas: (*hexutil.Big)(big.NewInt(10)),
	})
	require.NoError(t, err)
	require.Equal(t, "0x000000000000000000000000000000000000000000000000000000000000000c", result.String())
}

func newTestEthAPIWithFilters(t *testing.T, m *execmoduletester.ExecModuleTester) *APIImpl {
	t.Helper()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, execmoduletester.New(t))
	mining := txpoolproto.NewMiningClient(conn)
	filters := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log, nil)
	return newEthApiForTest(newBaseApiWithFiltersForTest(filters, stateCache, m), m.DB, nil, nil)
}

type stubTxPoolClient struct{ txpoolproto.TxpoolClient }

func (stubTxPoolClient) Nonce(context.Context, *txpoolproto.NonceRequest, ...grpc.CallOption) (*txpoolproto.NonceReply, error) {
	return &txpoolproto.NonceReply{}, nil
}

func TestCreateAccessListContractCreationWithoutFromDoesNotPanic(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)

	var (
		res *accessListResult
		err error
	)
	require.NotPanics(t, func() {
		res, err = api.CreateAccessList(context.Background(), ethapi.CallArgs{}, nil, nil, nil)
	})
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestEthCallNonCanonical(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(nil, stateCache, m), m.DB, nil, nil)
	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	blockNumberOrHash := rpc.BlockNumberOrHashWithHash(common.HexToHash("0x3fcb7c0d4569fddc89cbea54b42f163e0c789351d98810a513895ab44b47020b"), true)
	var blockNumberOrHashRef = &blockNumberOrHash

	_, err := api.Call(context.Background(), ethapi.CallArgs{
		From: &from,
		To:   &to,
	}, blockNumberOrHashRef, nil, nil)
	require.EqualError(t, err, "hash 3fcb7c0d4569fddc89cbea54b42f163e0c789351d98810a513895ab44b47020b is not currently canonical")
}

func TestEthCallToPrunedBlock(t *testing.T) {
	pruneTo := uint64(3)
	ethCallBlockNumber := rpc.BlockNumber(2)

	m, bankAddress, contractAddress, _ := chainWithDeployedContract(t)
	doPrune(t, m.DB, pruneTo)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	callData := hexutil.MustDecode("0x2e64cec1")
	callDataBytes := hexutil.Bytes(callData)

	blockNumberOrHash := rpc.BlockNumberOrHashWithNumber(ethCallBlockNumber)
	var blockNumberOrHashRef = &blockNumberOrHash

	_, err := api.Call(context.Background(), ethapi.CallArgs{
		From: &bankAddress,
		To:   &contractAddress,
		Data: &callDataBytes,
	}, blockNumberOrHashRef, nil, nil)
	require.NoError(t, err)
}

func TestGetProof(t *testing.T) {
	var maxGetProofRewindBlockCount = 1   // Note, this is unsafe for parallel tests, but, this test is the only consumer for now
	statecfg.EnableHistoricalCommitment() // enable commitment history to test historical proofs
	m, bankAddr, contractAddr, receiverAddress := chainWithDeployedContract(t)
	cfg := &EthApiConfig{
		GasCap:                      5000000,
		FeeCap:                      ethconfig.Defaults.RPCTxFeeCap,
		ReturnDataLimit:             100_000,
		AllowUnprotectedTxs:         false,
		MaxGetProofRewindBlockCount: maxGetProofRewindBlockCount,
		SubscribeLogsChannelSize:    128,
		RpcTxSyncDefaultTimeout:     20 * time.Second,
		RpcTxSyncMaxTimeout:         1 * time.Minute,
	}
	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, cfg, log.New())

	key := func(b byte) hexutil.Bytes {
		result := common.Hash{}
		result[31] = b
		return result[:]
	}
	_ = bankAddr

	tests := []struct {
		name        string
		blockNum    uint64
		addr        common.Address
		storageKeys []hexutil.Bytes
		stateVal    uint64
		expectedErr string
	}{
		{
			name:     "currentBlockNoState",
			addr:     contractAddr,
			blockNum: 6,
		},
		{
			name:     "currentBlockEOA",
			addr:     bankAddr,
			blockNum: 6,
		},
		{
			name:     "currentBlockNoAccount",
			addr:     common.HexToAddress("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead0"),
			blockNum: 6,
		},
		{
			name:        "currentBlockWithState",
			addr:        contractAddr,
			blockNum:    6,
			storageKeys: []hexutil.Bytes{key(0), key(4), key(8), key(10)},
			stateVal:    2,
		},
		{
			name:        "currentBlockWithStateAndShortKeys",
			addr:        contractAddr,
			blockNum:    6,
			storageKeys: []hexutil.Bytes{{0x0}, {0x4}, {0x8}, {0x0a}},
			stateVal:    2,
		},
		{
			name:        "currentBlockWithMissingState",
			addr:        contractAddr,
			storageKeys: []hexutil.Bytes{hexutil.FromHex("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddead")},
			blockNum:    6,
			stateVal:    0,
		},
		{
			name:        "currentBlockEOAMissingState",
			addr:        bankAddr,
			storageKeys: []hexutil.Bytes{hexutil.FromHex("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddead")},
			blockNum:    6,
			stateVal:    0,
		},
		{
			name:        "currentBlockNoAccountMissingState",
			addr:        common.HexToAddress("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead0"),
			storageKeys: []hexutil.Bytes{hexutil.FromHex("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddead")},
			blockNum:    6,
			stateVal:    0,
		},
		{
			name:        "olderBlockWithState",
			addr:        contractAddr,
			blockNum:    2,
			storageKeys: []hexutil.Bytes{key(1), key(5), key(9), key(13)},
			stateVal:    1,
		},
		{
			name:     "notCreatedYetAccount",
			addr:     receiverAddress, // receiver address only starts existing at block 4
			blockNum: 3,
		},
		{
			name:     "createdAccountAtBlock", // account created at block 4, proof requested at block 4, latest=6
			addr:     receiverAddress,         // receiver address only starts existing at block 4
			blockNum: 4,
		},
		{
			name:     "createdAccountBlockAfter", // account created at block 4, proof requested at block 5, latest=6
			addr:     receiverAddress,            // receiver address only starts existing at block 4
			blockNum: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proof, err := api.GetProof(
				context.Background(),
				tt.addr,
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

			tx, err := m.DB.BeginTemporalRo(context.Background())
			require.NoError(t, err)
			defer tx.Rollback()
			header, err := api.headerByNumber(context.Background(), rpc.BlockNumber(tt.blockNum), tx)
			require.NoError(t, err)

			require.Equal(t, tt.addr, proof.Address)
			err = trie.VerifyAccountProof(header.Root, proof)
			require.NoError(t, err)

			require.Len(t, proof.StorageProof, len(tt.storageKeys))
			for _, storageKey := range tt.storageKeys {
				found := false
				for _, storageProof := range proof.StorageProof {
					var proofKeyHashBytes, storageKeyBytes []byte
					proofKeyHashBytes = hexutil.FromHex(storageProof.Key)
					storageKeyBytes = storageKey
					if !bytes.Equal(proofKeyHashBytes, storageKeyBytes) {
						continue
					}
					found = true
					require.Equal(t, tt.stateVal, (*big.Int)(storageProof.Value).Uint64())
					err = trie.VerifyStorageProof(proof.StorageHash, storageProof)
					require.NoError(t, err)
				}
				require.True(t, found, "did not find storage proof for key=%x", storageKey)
			}
		})
	}
}

func TestGetBlockByTimestampLatestTime(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	api := NewErigonAPI(newBaseApiForTest(m), m.DB, nil)

	latestBlock, err := m.BlockReader.CurrentBlock(tx)
	require.NoError(t, err)
	response, err := ethapi.RPCMarshalBlockDeprecated(latestBlock, true, false)
	require.NoError(t, err)

	if err == nil && rpc.BlockNumber(latestBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(latestBlock.Header().Time), false)
	require.NoError(t, err)

	require.Equal(t, response["timestamp"], block["timestamp"])
	require.Equal(t, response["hash"], block["hash"])
}

func TestGetBlockByTimestampOldestTime(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	api := NewErigonAPI(newBaseApiForTest(m), m.DB, nil)

	oldestBlock, err := m.BlockReader.BlockByNumber(m.Ctx, tx, 0)
	require.NoError(t, err)

	response, err := ethapi.RPCMarshalBlockDeprecated(oldestBlock, true, false)
	require.NoError(t, err)

	if err == nil && rpc.BlockNumber(oldestBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(oldestBlock.Header().Time), false)
	require.NoError(t, err)

	require.Equal(t, response["timestamp"], block["timestamp"])
	require.Equal(t, response["hash"], block["hash"])
}

func TestGetBlockByTimeHigherThanLatestBlock(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	api := NewErigonAPI(newBaseApiForTest(m), m.DB, nil)

	latestBlock, err := m.BlockReader.CurrentBlock(tx)
	require.NoError(t, err)

	response, err := ethapi.RPCMarshalBlockDeprecated(latestBlock, true, false)
	require.NoError(t, err)

	if err == nil && rpc.BlockNumber(latestBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(latestBlock.Header().Time+999999999999), false)
	require.NoError(t, err)

	require.Equal(t, response["timestamp"], block["timestamp"])
	require.Equal(t, response["hash"], block["hash"])
}

func TestGetBlockByTimeMiddle(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	api := NewErigonAPI(newBaseApiForTest(m), m.DB, nil)

	currentHeader := rawdb.ReadCurrentHeader(tx)
	oldestHeader, err := api._blockReader.HeaderByNumber(ctx, tx, 0)
	require.NoError(t, err)
	require.NotNil(t, oldestHeader)

	middleNumber := (currentHeader.Number.Uint64() + oldestHeader.Number.Uint64()) / 2
	middleBlock, err := m.BlockReader.BlockByNumber(m.Ctx, tx, middleNumber)
	require.NoError(t, err)

	response, err := ethapi.RPCMarshalBlockDeprecated(middleBlock, true, false)
	require.NoError(t, err)

	if err == nil && rpc.BlockNumber(middleBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(middleBlock.Header().Time), false)
	require.NoError(t, err)
	require.Equal(t, response["timestamp"], block["timestamp"])
	require.Equal(t, response["hash"], block["hash"])
}

func TestGetBlockByTimestamp(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	api := NewErigonAPI(newBaseApiForTest(m), m.DB, nil)

	highestBlockNumber := rawdb.ReadCurrentHeader(tx).Number
	pickedBlock, err := m.BlockReader.BlockByNumber(m.Ctx, tx, highestBlockNumber.Uint64()/3)
	require.NoError(t, err)
	require.NotNil(t, pickedBlock)
	response, err := ethapi.RPCMarshalBlockDeprecated(pickedBlock, true, false)
	require.NoError(t, err)

	if err == nil && rpc.BlockNumber(pickedBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(pickedBlock.Header().Time), false)
	require.NoError(t, err)

	require.Equal(t, response["timestamp"], block["timestamp"])
	require.Equal(t, response["hash"], block["hash"])
}

// contractHexString is the output of compiling the following solidity contract:
//
// pragma solidity ^0.8.0;
//
//	contract Box {
//	    uint256 private _value0x0;
//	    uint256 private _value0x1;
//	    uint256 private _value0x2;
//	    uint256 private _value0x3;
//	    uint256 private _value0x4;
//	    uint256 private _value0x5;
//	    uint256 private _value0x6;
//	    uint256 private _value0x7;
//	    uint256 private _value0x8;
//	    uint256 private _value0x9;
//	    uint256 private _value0xa;
//	    uint256 private _value0xb;
//	    uint256 private _value0xc;
//	    uint256 private _value0xd;
//	    uint256 private _value0xe;
//	    uint256 private _value0xf;
//	    uint256 private _value0x10;
//
//	    // Emitted when the stored value changes
//	    event ValueChanged(uint256 value);
//
//	    // Stores a new value in the contract
//	    function store(uint256 value) public {
//	        _value0x0 = value;
//	        _value0x1 = value;
//	        _value0x2 = value;
//	        _value0x3 = value;
//	        _value0x4 = value;
//	        _value0x5 = value;
//	        _value0x6 = value;
//	        _value0x7 = value;
//	        _value0x8 = value;
//	        _value0x9 = value;
//	        _value0xa = value;
//	        _value0xb = value;
//	        _value0xc = value;
//	        _value0xd = value;
//	        _value0xe = value;
//	        _value0xf = value;
//	        _value0x10 = value;
//	        emit ValueChanged(value);
//	    }
//
//	    // Reads the last stored value
//	    function retrieve() public view returns (uint256) {
//	        return _value0x0;
//	    }
//	}
//
// You may produce this hex string by saving the contract into a file
// Box.sol and invoking
//
//	solc Box.sol --bin --abi --optimize
//
// This contract is a slight modification of Box.sol to use more storage nodes
// and ensure the contract storage will contain at least 1 non-leaf node (by
// storing 17 values).
const contractHexString = "0x608060405234801561001057600080fd5b5061013f806100206000396000f3fe608060405234801561001057600080fd5b50600436106100365760003560e01c80632e64cec11461003b5780636057361d14610050575b600080fd5b60005460405190815260200160405180910390f35b61006361005e3660046100f0565b610065565b005b6000819055600181905560028190556003819055600481905560058190556006819055600781905560088190556009819055600a819055600b819055600c819055600d819055600e819055600f81905560108190556040518181527f93fe6d397c74fdf1402a8b72e47b68512f0510d7b98a4bc4cbdf6ac7108b3c599060200160405180910390a150565b60006020828403121561010257600080fd5b503591905056fea2646970667358221220031e17f1bd1d1dcbee088287a905b152410b180064c149763590a0bbc516d95e64736f6c63430008130033"

var contractFuncSelector = crypto.Keccak256([]byte("store(uint256)"))[:4]

// contractInvocationData returns data suitable for invoking the 'store'
// function of the contract in contractHexString, note
func contractInvocationData(val byte) []byte {
	return hexutil.MustDecode(fmt.Sprintf("0x%x00000000000000000000000000000000000000000000000000000000000000%02x", contractFuncSelector, val))
}

func generatePseudoRandomECDSAKey(rand io.Reader) (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(crypto.S256(), rand)
}

func generatePseudoRandomECDSAKeyPairs(rand io.Reader, n int) ([]*ecdsa.PrivateKey, []*ecdsa.PublicKey, error) {
	privateKeys := make([]*ecdsa.PrivateKey, n)
	publicKeys := make([]*ecdsa.PublicKey, n)
	var err error
	for i := 0; i < n; i++ {
		privateKeys[i], err = generatePseudoRandomECDSAKey(rand)
		if err != nil {
			return nil, nil, err
		}
		publicKeys[i] = &privateKeys[i].PublicKey
	}
	return privateKeys, publicKeys, nil
}

func chainWithDeployedContract(t *testing.T) (*execmoduletester.ExecModuleTester, common.Address, common.Address, common.Address) {
	t.Helper()
	return chainWithDeployedContractAndConfig(t, chain.TestChainBerlinConfig)
}

func chainWithDeployedContractAndConfig(t *testing.T, cfg *chain.Config) (*execmoduletester.ExecModuleTester, common.Address, common.Address, common.Address) {
	t.Helper()

	var (
		seed            = int64(12345)
		rng             = rand.New(rand.NewSource(seed)) // rng for filler accounts
		nFillerAccounts = 400                            // nr. of accounts to fill up MPT
		signer          = types.LatestSignerForChainID(nil)
		txFeeCap        = uint256.NewInt(1_000_000_000_000)
		contract        = hexutil.MustDecode(contractHexString)
		chainConfig     = new(chain.Config)
	)
	bankKey, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	receiverKey, err := crypto.HexToECDSA("a71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f292")
	require.NoError(t, err)

	bankAddress := crypto.PubkeyToAddress(bankKey.PublicKey)
	receiverAddress := crypto.PubkeyToAddress(receiverKey.PublicKey)

	bankFunds, ok := new(big.Int).SetString("100000000000000000000", 10)
	require.True(t, ok)
	require.NoError(t, copier.CopyWithOption(chainConfig, cfg, copier.Option{DeepCopy: true}))
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc:  types.GenesisAlloc{bankAddress: {Balance: bankFunds}},
		//Alloc:  types.GenesisAlloc{bankAddress: {Balance: bankFunds, Storage: map[common.Hash]common.Hash{crypto.Keccak256Hash([]byte{0x1}): crypto.Keccak256Hash([]byte{0xf})}}}, // TODO (antonis19)
	}
	// accounts to fill up MPT
	_, fillerPublicKeys, err := generatePseudoRandomECDSAKeyPairs(rng, nFillerAccounts)
	require.NoError(t, err)

	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(bankKey))
	db := m.DB

	var contractAddr common.Address

	chain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 6, func(i int, block *blockgen.BlockGen) {
		nonce := block.TxNonce(bankAddress)
		switch i {
		case 0:
			tx, err := types.SignTx(&types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    nonce,
					GasLimit: 1e6,
					Value:    uint256.Int{},
					Data:     contract,
				},
				GasPrice: *txFeeCap,
			}, *signer, bankKey)
			require.NoError(t, err)
			block.AddTx(tx)
			contractAddr = types.CreateAddress(bankAddress, nonce)
		case 1:
			txn, err := types.SignTx(&types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    nonce,
					To:       &contractAddr,
					GasLimit: 900000,
					Value:    uint256.Int{},
					Data:     contractInvocationData(1),
				},
				GasPrice: *txFeeCap,
			}, *signer, bankKey)
			require.NoError(t, err)
			block.AddTx(txn)
			// send txs to filler addresses, so that MPT may be populated ( populate only half in this block, to not exceed gas limit)
			nonce++
			for idx := 0; idx < nFillerAccounts/2; idx++ {
				transferAmount := big.NewInt(1e1)
				fillerAddress := crypto.PubkeyToAddress(*fillerPublicKeys[idx])
				txn, err := types.SignTx(&types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce:    nonce,
						To:       &fillerAddress,
						GasLimit: 21000,
						Value:    *uint256.MustFromBig(transferAmount),
					},
					GasPrice: *txFeeCap,
				}, *signer, bankKey)
				require.NoError(t, err)
				block.AddTx(txn)
				nonce++
			}
		case 2:
			txn, err := types.SignTx(&types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    nonce,
					To:       &contractAddr,
					GasLimit: 900000,
					Value:    uint256.Int{},
					Data:     contractInvocationData(2),
				},
				GasPrice: *txFeeCap,
			}, *signer, bankKey)
			require.NoError(t, err)
			block.AddTx(txn)
			// send txs to filler addresses, so that MPT may be populated
			// ( populate the second half in this block)
			nonce++
			for idx := nFillerAccounts / 2; idx < nFillerAccounts; idx++ {
				transferAmount := big.NewInt(1e1)
				fillerAddress := crypto.PubkeyToAddress(*fillerPublicKeys[idx])
				txn, err := types.SignTx(&types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce:    nonce,
						To:       &fillerAddress,
						GasLimit: 21000,
						Value:    *uint256.MustFromBig(transferAmount),
					},
					GasPrice: *txFeeCap,
				}, *signer, bankKey)
				require.NoError(t, err)
				block.AddTx(txn)
				nonce++
			}

		case 3:
			transferAmount := big.NewInt(1e2)
			txn, err := types.SignTx(&types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    nonce,
					To:       &receiverAddress,
					GasLimit: 21000,
					Value:    *uint256.MustFromBig(transferAmount),
				},
				GasPrice: *txFeeCap,
			}, *signer, bankKey)
			require.NoError(t, err)
			block.AddTx(txn)
		case 4:
			// empty block
		case 5:
			// empty block
		}
	})
	require.NoError(t, err)

	err = m.InsertChain(chain)
	require.NoError(t, err)

	ctx := context.Background()
	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	stateReader, err := rpchelper.CreateHistoryStateReader(ctx, tx, 1, 0, rawdbv3.TxNums)
	require.NoError(t, err)
	st := state.New(stateReader)
	exist, err := st.Exist(accounts.InternAddress(contractAddr))
	require.NoError(t, err)
	assert.False(t, exist, "Contract should not exist at block #1")

	stateReader, err = rpchelper.CreateHistoryStateReader(ctx, tx, 2, 0, rawdbv3.TxNums)
	require.NoError(t, err)
	st = state.New(stateReader)
	exist, err = st.Exist(accounts.InternAddress(contractAddr))
	require.NoError(t, err)
	assert.True(t, exist, "Contract should exist at block #2")

	return m, bankAddress, contractAddr, receiverAddress
}

func doPrune(t *testing.T, db kv.RwDB, pruneTo uint64) {
	ctx := context.Background()
	//logger := testlog.Logger(t, log.LvlCrit)
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	err = rawdb.PruneTableDupSort(tx, kv.TblAccountVals, "", pruneTo, logEvery, ctx)
	require.NoError(t, err)

	// kv.StorageChangeSetDeprecated is no longer part of the active
	// schema (drop_legacy_e2_tables migration drops it), so there is
	// nothing to prune from that table.

	//err = rawdb.PruneTable(tx, kv.RCacheDomain, pruneTo, ctx, math.MaxInt32, time.Hour, logger, "")
	//require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)
}

func TestOptimizeWarmAddrAndAdjustGas(t *testing.T) {
	addr := common.HexToAddress("0xbeefbabeea323f07c59926295205d3b7a17e8638")
	other := common.HexToAddress("0x0000000000000000000000000000000000000001")
	slot := common.HexToHash("0x01")
	const baseGas = hexutil.Uint64(50000)

	t.Run("zero_slots_removed_gas_adjusted", func(t *testing.T) {
		al := types.AccessList{{Address: addr, StorageKeys: []common.Hash{}}}
		res := &accessListResult{Accesslist: &al, GasUsed: baseGas}
		optimizeWarmAddrAndAdjustGas(res, addr)
		require.Empty(t, *res.Accesslist)
		require.Equal(t, baseGas-hexutil.Uint64(params.TxAccessListAddressGas), res.GasUsed)
	})

	t.Run("with_slots_not_removed", func(t *testing.T) {
		al := types.AccessList{{Address: addr, StorageKeys: []common.Hash{slot}}}
		res := &accessListResult{Accesslist: &al, GasUsed: baseGas}
		optimizeWarmAddrAndAdjustGas(res, addr)
		require.Len(t, *res.Accesslist, 1)
		require.Equal(t, baseGas, res.GasUsed)
	})

	t.Run("addr_not_in_list_noop", func(t *testing.T) {
		al := types.AccessList{{Address: other, StorageKeys: []common.Hash{}}}
		res := &accessListResult{Accesslist: &al, GasUsed: baseGas}
		optimizeWarmAddrAndAdjustGas(res, addr)
		require.Len(t, *res.Accesslist, 1)
		require.Equal(t, baseGas, res.GasUsed)
	})

	t.Run("gas_no_underflow", func(t *testing.T) {
		al := types.AccessList{{Address: addr, StorageKeys: []common.Hash{}}}
		res := &accessListResult{Accesslist: &al, GasUsed: hexutil.Uint64(100)}
		optimizeWarmAddrAndAdjustGas(res, addr)
		require.Empty(t, *res.Accesslist)
		require.Equal(t, hexutil.Uint64(100), res.GasUsed) // 100 < 2400, no underflow
	})
}
