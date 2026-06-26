package execmodule

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func makeTx(nonce uint64) types.Transaction {
	return types.NewTransaction(nonce, common.Address{1}, uint256.NewInt(1), 21000, uint256.NewInt(1), nil)
}

func TestCheckFlashblockUpdate_NoState(t *testing.T) {
	fv := newForkValidator(context.Background(), 0, nil, nil, 64)
	txs := []types.Transaction{makeTx(0), makeTx(1)}
	result := fv.CheckFlashblockUpdate(1, txs)
	assert.False(t, result.IsUpdate)
}

func TestCheckFlashblockUpdate_PrefixMatch(t *testing.T) {
	fv := newForkValidator(context.Background(), 0, nil, nil, 64)

	tx0 := makeTx(0)
	tx1 := makeTx(1)
	tx2 := makeTx(2)

	// Simulate a previous flashblock at block 5 with tx0, tx1
	fv.extendingForkNumber = 5
	fv.sharedDom = nil // would normally be set, but CheckFlashblockUpdate requires non-nil
	fv.RecordFlashblockTxHashes([]types.Transaction{tx0, tx1})

	// Without sharedDom, should not report as update
	result := fv.CheckFlashblockUpdate(5, []types.Transaction{tx0, tx1, tx2})
	assert.False(t, result.IsUpdate, "should be false without sharedDom")
}

func TestCheckFlashblockUpdate_DifferentBlockNumber(t *testing.T) {
	fv := newForkValidator(context.Background(), 0, nil, nil, 64)

	tx0 := makeTx(0)

	fv.extendingForkNumber = 5
	fv.RecordFlashblockTxHashes([]types.Transaction{tx0})

	result := fv.CheckFlashblockUpdate(6, []types.Transaction{tx0})
	assert.False(t, result.IsUpdate, "different block number should not match")
}

func TestCheckFlashblockUpdate_PrefixMismatch(t *testing.T) {
	fv := newForkValidator(context.Background(), 0, nil, nil, 64)

	tx0 := makeTx(0)
	tx1 := makeTx(1)
	tx2 := makeTx(2)

	fv.extendingForkNumber = 5
	fv.RecordFlashblockTxHashes([]types.Transaction{tx0, tx1})

	// New block has tx2 where tx1 was — prefix mismatch
	result := fv.CheckFlashblockUpdate(5, []types.Transaction{tx0, tx2})
	assert.False(t, result.IsUpdate, "prefix mismatch should not be an update")
}

func TestCheckFlashblockUpdate_ShorterBlock(t *testing.T) {
	fv := newForkValidator(context.Background(), 0, nil, nil, 64)

	tx0 := makeTx(0)
	tx1 := makeTx(1)

	fv.extendingForkNumber = 5
	fv.RecordFlashblockTxHashes([]types.Transaction{tx0, tx1})

	// New block is shorter than what we already executed
	result := fv.CheckFlashblockUpdate(5, []types.Transaction{tx0})
	assert.False(t, result.IsUpdate, "shorter block should not be an update")
}

func TestRecordFlashblockTxHashes(t *testing.T) {
	fv := newForkValidator(context.Background(), 0, nil, nil, 64)

	tx0 := makeTx(0)
	tx1 := makeTx(1)

	fv.RecordFlashblockTxHashes([]types.Transaction{tx0, tx1})

	require.Len(t, fv.flashblockTxHashes, 2)
	assert.Equal(t, tx0.Hash(), fv.flashblockTxHashes[0])
	assert.Equal(t, tx1.Hash(), fv.flashblockTxHashes[1])
}

func TestClearResetsFlashblockState(t *testing.T) {
	fv := newForkValidator(context.Background(), 0, nil, nil, 64)

	fv.RecordFlashblockTxHashes([]types.Transaction{makeTx(0)})
	require.Len(t, fv.flashblockTxHashes, 1)

	fv.ClearWithUnwind()
	assert.Nil(t, fv.flashblockTxHashes, "clear should reset flashblock tx hashes")
}
