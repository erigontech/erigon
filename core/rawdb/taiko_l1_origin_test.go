package rawdb_test

import (
	"crypto/rand"
	"math/big"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/turbo/stages/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// randomBigInt generates a random big integer.
func randomBigInt() *big.Int {
	randomBigInt, err := rand.Int(rand.Reader, common.Big256)
	if err != nil {
		panic(err)
	}

	return randomBigInt
}

// randomHash generates a random blob of data and returns it as a hash.
func randomHash() common.Hash {
	var hash common.Hash
	if n, err := rand.Read(hash[:]); n != length.Hash || err != nil {
		panic(err)
	}
	return hash
}

func TestL1Origin(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	defer tx.Rollback()
	testL1Origin := &rawdb.L1Origin{
		BlockID:       randomBigInt(),
		L2BlockHash:   randomHash(),
		L1BlockHeight: randomBigInt(),
		L1BlockHash:   randomHash(),
	}
	rawdb.WriteL1Origin(tx, testL1Origin.BlockID, testL1Origin)
	l1Origin, err := rawdb.ReadL1Origin(tx, testL1Origin.BlockID)
	require.Nil(t, err)
	require.NotNil(t, l1Origin)
	assert.Equal(t, testL1Origin.BlockID, l1Origin.BlockID)
	assert.Equal(t, testL1Origin.L2BlockHash, l1Origin.L2BlockHash)
	assert.Equal(t, testL1Origin.L1BlockHeight, l1Origin.L1BlockHeight)
	assert.Equal(t, testL1Origin.L1BlockHash, l1Origin.L1BlockHash)
}

func TestHeadL1Origin(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	defer tx.Rollback()
	testBlockID := randomBigInt()
	rawdb.WriteHeadL1Origin(tx, testBlockID)
	blockID, err := rawdb.ReadHeadL1Origin(tx)
	require.Nil(t, err)
	require.NotNil(t, blockID)
	assert.Equal(t, testBlockID, blockID)
}
