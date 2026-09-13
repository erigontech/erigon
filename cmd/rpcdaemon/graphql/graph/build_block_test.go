package graph

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/ethapi"
)

// buildBlock reads whatever buildBlockDetailsResponse put under "block", and the
// compiler cannot check that: the response is a map[string]any. Feed it a real
// marshalled block so a change to the marshaller's type is caught here rather
// than as a panic on every GraphQL block query.
func TestBuildBlockReadsMarshalledBlock(t *testing.T) {
	t.Parallel()

	header := &types.Header{
		Number:      *uint256.NewInt(21_000_000),
		ParentHash:  common.HexToHash("0x11"),
		Root:        common.HexToHash("0x22"),
		TxHash:      common.HexToHash("0x33"),
		ReceiptHash: common.HexToHash("0x44"),
		GasLimit:    36_000_000,
		GasUsed:     17_000_000,
		Time:        1_750_000_000,
		Extra:       []byte("erigon"),
		BaseFee:     uint256.NewInt(1_234_567_890),
	}
	block := types.NewBlock(header, nil, nil, nil, nil, nil)

	marshalled := ethapi.RPCMarshalBlock(block, true, false)
	marshalled.TotalDifficulty = (*hexutil.U256)(uint256.NewInt(99))
	marshalled.TransactionCount = hexutil.Uint64(0)

	r := &queryResolver{}
	got, err := r.buildBlock(map[string]any{
		"block":    marshalled,
		"receipts": []map[string]any{},
	})
	require.NoError(t, err)

	require.Equal(t, uint64(21_000_000), got.Number)
	require.Equal(t, block.Hash().Hex(), got.Hash)
	require.Equal(t, uint64(36_000_000), got.GasLimit)
	require.Equal(t, uint64(17_000_000), got.GasUsed)
	require.Equal(t, header.Root.Hex(), got.StateRoot)
	require.Equal(t, header.ParentHash.Hex(), got.Parent.Hash)
	require.NotNil(t, got.BaseFeePerGas)
	require.NotNil(t, got.TransactionCount)
	require.Equal(t, uint64(0), *got.TransactionCount)
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
		"receipts": []map[string]any{},
	})
	require.NoError(t, err)
	require.Empty(t, got.Hash)
	require.Empty(t, got.Nonce)
}

// An unexpected type must be an error, not a panic.
func TestBuildBlockRejectsUnexpectedType(t *testing.T) {
	t.Parallel()

	r := &queryResolver{}
	_, err := r.buildBlock(map[string]any{"block": map[string]any{"number": "0x1"}})
	require.Error(t, err)
}
