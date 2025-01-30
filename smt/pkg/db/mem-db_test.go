package db

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon/smt/pkg/utils"
)

func TestMemDb(t *testing.T) {
	db := NewMemDb()

	// The key and value we're going to test
	key := utils.NodeKey{1, 2, 3, 4}
	value := utils.NodeValue12{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

	// Testing Insert method
	err := db.Insert(key, value)
	assert.NoError(t, err)

	// Testing Get method
	retrievedValue, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrievedValue)
}
