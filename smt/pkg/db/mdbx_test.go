package db

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestEriDb(t *testing.T) {
	dbi, _ := mdbx.NewTemporaryMdbx(context.Background(), t.TempDir())
	tx, _ := dbi.BeginRw(context.Background())
	db := NewEriDb(tx)
	err := CreateEriDbBuckets(tx)
	assert.NoError(t, err)

	// The key and value we're going to test
	key := utils.NodeKey{1, 2, 3, 4}
	value := utils.NodeValue12{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

	// Testing Insert method
	err = db.Insert(key, value)
	assert.NoError(t, err)

	// Testing Get method
	retrievedValue, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrievedValue)
}

func TestEriDbBatch(t *testing.T) {
	dbi, _ := mdbx.NewTemporaryMdbx(context.Background(), t.TempDir())
	tx, _ := dbi.BeginRw(context.Background())
	db := NewEriDb(tx)
	err := CreateEriDbBuckets(tx)
	assert.NoError(t, err)

	// The key and value we're going to test
	key := utils.NodeKey{1, 2, 3, 4}
	value := utils.NodeValue12{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

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
	altValue := utils.NodeValue12{13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}

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
