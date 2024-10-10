package db

import (
	"context"
	"math/big"
	"testing"

	"github.com/gateway-fm/cdk-erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestEriDb(t *testing.T) {
	dbi, _ := mdbx.NewTemporaryMdbx()
	tx, _ := dbi.BeginRw(context.Background())
	db := NewEriDb(tx)
	err := CreateEriDbBuckets(tx)
	assert.NoError(t, err)

	// The key and value we're going to test
	key := utils.NodeKey{1, 2, 3, 4}
	value := utils.NodeValue12{big.NewInt(1), big.NewInt(2), big.NewInt(3), big.NewInt(4), big.NewInt(5), big.NewInt(6),
		big.NewInt(7), big.NewInt(8), big.NewInt(9), big.NewInt(10), big.NewInt(11), big.NewInt(12)}

	// Testing Insert method
	err = db.Insert(key, value)
	assert.NoError(t, err)

	// Testing Get method
	retrievedValue, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrievedValue)
}

func TestEriDbBatch(t *testing.T) {
	dbi, _ := mdbx.NewTemporaryMdbx()
	tx, _ := dbi.BeginRw(context.Background())
	db := NewEriDb(tx)
	err := CreateEriDbBuckets(tx)
	assert.NoError(t, err)

	// The key and value we're going to test
	key := utils.NodeKey{1, 2, 3, 4}
	value := utils.NodeValue12{big.NewInt(1), big.NewInt(2), big.NewInt(3), big.NewInt(4), big.NewInt(5), big.NewInt(6),
		big.NewInt(7), big.NewInt(8), big.NewInt(9), big.NewInt(10), big.NewInt(11), big.NewInt(12)}

	quit := make(chan struct{})

	// Start a new batch
	db.OpenBatch(quit)

	// Inserting a key-value pair within a batch
	err = db.Insert(key, value)
	assert.NoError(t, err)

	// Commit the batch
	err = db.CommitBatch()
	assert.NoError(t, err)

	// Testing Get method after committing the batch
	retrievedValue, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrievedValue)

	// Start another batch
	db.OpenBatch(quit)

	// Inserting a different key-value pair within a batch
	altKey := utils.NodeKey{5, 6, 7, 8}
	altValue := utils.NodeValue12{big.NewInt(13), big.NewInt(14), big.NewInt(15), big.NewInt(16), big.NewInt(17), big.NewInt(18),
		big.NewInt(19), big.NewInt(20), big.NewInt(21), big.NewInt(22), big.NewInt(23), big.NewInt(24)}

	err = db.Insert(altKey, altValue)
	assert.NoError(t, err)

	// Testing Get method before rollback or commit, expecting no value for the altKey
	altValRes, err := db.Get(altKey)
	assert.NoError(t, err)
	assert.Equal(t, altValue, altValRes)

	// Rollback the batch
	db.RollbackBatch()

	// Testing Get method after rollback, expecting no value for the altKey
	val, err := db.Get(altKey)
	assert.NoError(t, err)
	assert.Equal(t, utils.NodeValue12{}, val)
}
