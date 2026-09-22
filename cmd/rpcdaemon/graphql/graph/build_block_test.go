package graph

import (
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/graphql/graph/model"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

// buildBlock reads whatever buildBlockDetailsResponse put under "block", and the
// compiler cannot check that: the response is a map[string]any. Feed it a real
// marshalled block so a change to the marshaller's type is caught here rather
// than as a panic on every GraphQL block query.
func TestBuildBlockReadsMarshalledBlock(t *testing.T) {
	t.Parallel()

	withdrawalsHash := common.HexToHash("0x77")
	blobGasUsed, excessBlobGas := uint64(0x20000), uint64(0x40000)
	header := &types.Header{
		Number:          *uint256.NewInt(21_000_000),
		ParentHash:      common.HexToHash("0x11"),
		Root:            common.HexToHash("0x22"),
		TxHash:          common.HexToHash("0x33"),
		ReceiptHash:     common.HexToHash("0x44"),
		UncleHash:       common.HexToHash("0x66"),
		GasLimit:        36_000_000,
		GasUsed:         17_000_000,
		Time:            1_750_000_000,
		Extra:           []byte("erigon"),
		BaseFee:         uint256.NewInt(1_234_567_890),
		Coinbase:        common.HexToAddress("0xabcdef0123456789abcdef0123456789abcdef01"),
		Difficulty:      *uint256.NewInt(0x2a),
		MixDigest:       common.HexToHash("0x55"),
		Nonce:           types.EncodeNonce(0x1234),
		WithdrawalsHash: &withdrawalsHash,
		BlobGasUsed:     &blobGasUsed,
		ExcessBlobGas:   &excessBlobGas,
	}
	header.Bloom[0], header.Bloom[types.BloomByteLength-1] = 0xab, 0x01
	uncle := &types.Header{Number: *uint256.NewInt(20_999_999)}
	block := types.NewBlockFromStorage(header.Hash(), header, nil, []*types.Header{uncle}, nil, nil)

	marshalled := ethapi.RPCMarshalBlock(block, true, false)
	marshalled.TotalDifficulty = (*hexutil.U256)(uint256.NewInt(99))
	var txCount hexutil.Uint64
	marshalled.TransactionCount = &txCount

	r := &queryResolver{}
	got, err := r.buildBlock(map[string]any{
		"block":    marshalled,
		"receipts": []*jsonrpc.GraphQLReceipt{},
		"withdrawals": []jsonrpc.GraphQLWithdrawal{{
			Index:     7,
			Validator: 8,
			Address:   common.HexToAddress("0xAbCdEf0123456789aBcDeF0123456789AbCdEf02"),
			Amount:    9,
		}},
	})
	require.NoError(t, err)

	require.Equal(t, uint64(21_000_000), got.Number)
	require.Equal(t, block.Hash().Hex(), got.Hash)
	require.Equal(t, uint64(36_000_000), got.GasLimit)
	require.Equal(t, uint64(17_000_000), got.GasUsed)
	require.Equal(t, header.Root.Hex(), got.StateRoot)
	require.Equal(t, header.ParentHash.Hex(), got.Parent.Hash)
	require.Equal(t, header.TxHash.Hex(), got.TransactionsRoot)
	require.Equal(t, header.ReceiptHash.Hex(), got.ReceiptsRoot)
	require.Equal(t, header.MixDigest.Hex(), got.MixHash)
	require.Equal(t, header.UncleHash.Hex(), got.OmmerHash)
	require.Equal(t, "0x2a", got.Difficulty)
	require.Equal(t, "0x63", got.TotalDifficulty)
	require.Equal(t, "0x657269676f6e", got.ExtraData)
	require.Equal(t, "0x684ee180", got.Timestamp)
	require.Equal(t, "0x0000000000001234", got.Nonce)
	require.Equal(t, "0xab"+strings.Repeat("00", types.BloomByteLength-2)+"01", got.LogsBloom)
	require.Equal(t, "0xabcdef0123456789abcdef0123456789abcdef01", got.Miner.Address)
	require.NotNil(t, got.BaseFeePerGas)
	require.Equal(t, "0x499602d2", *got.BaseFeePerGas)
	require.NotNil(t, got.TransactionCount)
	require.Equal(t, uint64(0), *got.TransactionCount)
	require.Len(t, got.Ommers, 1)
	require.Equal(t, uncle.Hash().Hex(), got.Ommers[0].Hash)
	require.NotNil(t, got.OmmerCount)
	require.Equal(t, uint64(1), *got.OmmerCount)
	require.Equal(t, uint64(21_000_000), got.Miner.BlockNum)
	require.NotNil(t, got.WithdrawalsRoot)
	require.Equal(t, withdrawalsHash.Hex(), *got.WithdrawalsRoot)
	require.NotNil(t, got.BlobGasUsed)
	require.Equal(t, blobGasUsed, *got.BlobGasUsed)
	require.NotNil(t, got.ExcessBlobGas)
	require.Equal(t, excessBlobGas, *got.ExcessBlobGas)
	require.Equal(t, []*model.Withdrawal{{Index: 7, Validator: 8, Address: "0xabcdef0123456789abcdef0123456789abcdef02", Amount: "0x9"}}, got.Withdrawals)
}

// A pending block has no hash, miner or nonce, and buildBlock must not deref them.
func TestBuildBlockHandlesPendingBlock(t *testing.T) {
	t.Parallel()

	block := types.NewBlock(&types.Header{Number: *uint256.NewInt(1)}, nil, nil, nil, nil, nil)
	marshalled := ethapi.RPCMarshalBlock(block, true, false)
	marshalled.MarkPending()

	r := &queryResolver{}
	got, err := r.buildBlock(map[string]any{
		"block":    marshalled,
		"receipts": []*jsonrpc.GraphQLReceipt{},
	})
	require.NoError(t, err)
	require.Empty(t, got.Hash)
	require.Empty(t, got.Nonce)
	require.Empty(t, got.Miner.Address)
}

// A withdrawals payload of the wrong type must be an error, not a block that
// silently reports no withdrawals at all.
func TestBuildBlockRejectsUnexpectedWithdrawalsType(t *testing.T) {
	t.Parallel()

	withdrawalsHash := common.HexToHash("0x77")
	header := &types.Header{Number: *uint256.NewInt(1), WithdrawalsHash: &withdrawalsHash}
	block := types.NewBlockFromStorage(header.Hash(), header, nil, nil, nil, nil)

	r := &queryResolver{}
	_, err := r.buildBlock(map[string]any{
		"block":       ethapi.RPCMarshalBlock(block, true, false),
		"receipts":    []*jsonrpc.GraphQLReceipt{},
		"withdrawals": []map[string]any{{"index": hexutil.Uint64(7)}},
	})
	require.Error(t, err)
}

// An unexpected type must be an error, not a panic.
func TestBuildBlockRejectsUnexpectedType(t *testing.T) {
	t.Parallel()

	r := &queryResolver{}
	_, err := r.buildBlock(map[string]any{"block": map[string]any{"number": "0x1"}})
	require.Error(t, err)
}
