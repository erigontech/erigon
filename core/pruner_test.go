package core

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculateNumOfPrunedBlocks(t *testing.T) {
	testcases := []struct {
		CurrentBlock        uint64
		LastPrunedBlock     uint64
		BlocksBeforePruning uint64
		BlocksBatch         uint64

		From        uint64
		To          uint64
		Result      bool
		Description string
	}{
		{

			0,
			0,
			10,
			2,
			0,
			0,
			false,
			"It checks start pruning without sync",
		},
		{
			0,
			0,
			10,
			500,
			0,
			0,
			false,
			"It checks overflow when BlocksBatch>BlocksBeforePruning without sync",
		},
		{
			10,
			5,
			5,
			2,
			5,
			5,
			false,
			"It checks that LastPrunedBlock and BlocksBeforePruning works well",
		},
		{
			10,
			7,
			5,
			2,
			7,
			7,
			false,
			"It checks that LastPrunedBlock and BlocksBeforePruning works well in incorrect state",
		},
		{
			10,
			8,
			5,
			3,
			8,
			8,
			false,
			"It checks that LastPrunedBlock and BlocksBeforePruning works well in incorrect state",
		},
		{
			10,
			0,
			5,
			2,
			0,
			2,
			true,
			"It checks success case",
		},
		{
			10,
			7,
			1,
			2,
			7,
			9,
			true,
			"It checks success case after sync",
		},
		{
			30,
			20,
			1,
			10,
			20,
			29,
			true,
			"It checks success case after sync",
		},
		{
			25,
			20,
			1,
			10,
			20,
			24,
			true,
			"It checks that diff calculates correctly",
		},
	}

	for i := range testcases {
		i := i
		v := testcases[i]
		t.Run("case "+strconv.Itoa(i)+" "+v.Description, func(t *testing.T) {
			from, to, res := calculateNumOfPrunedBlocks(v.CurrentBlock, v.LastPrunedBlock, v.BlocksBeforePruning, v.BlocksBatch)
			if from != v.From || to != v.To || res != v.Result {
				t.Log("res", res, "from", from, "to", to)
				t.Fatal("Failed case", i)
			}
		})
	}
}

func TestPruneStorageOfSelfDestructedAccounts(t *testing.T) {
	t.Skip("disable test, because pruner doesn't delete anything yet, just printing")

	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	defer db.Close()

	storageKey := func(k string) []byte {
		return dbutils.GenerateCompositeStorageKey(common.HexToHash(k), 1, common.HexToHash(k))
	}
	putCache := func(k string, v string) {
		require.NoError(db.Put(dbutils.TrieOfStorageBucket, common.Hex2Bytes(k), common.Hex2Bytes(v)))
	}
	putStorage := func(k string, v string) {
		require.NoError(db.Put(dbutils.HashedStorageBucket, storageKey(k), common.Hex2Bytes(v)))
	}

	k0 := fmt.Sprintf("%02x%062x", 0, 0)
	k1 := fmt.Sprintf("%02x%062x", 1, 0)
	k2 := fmt.Sprintf("%02x%062x", 2, 0)
	putCache(k0, "0000")
	putCache(k1, "")
	putCache(k2, "2222")

	putStorage(k0, "9999")
	putStorage(k1, "9999")
	putStorage(k2, "9999")

	err := PruneStorageOfSelfDestructedAccounts(db)
	require.NoError(err)

	v, err := db.Get(dbutils.HashedStorageBucket, storageKey(k1))
	require.True(errors.Is(err, ethdb.ErrKeyNotFound))
	assert.Nil(v)

	v, err = db.Get(dbutils.HashedStorageBucket, storageKey(k2))
	require.NoError(err)

	assert.Equal("9999", common.Bytes2Hex(v))
}
