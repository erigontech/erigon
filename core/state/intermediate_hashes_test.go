package state_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClearTombstonesForReCreatedAccount(t *testing.T) {
	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()

	accKey := fmt.Sprintf("11%062x", 0)
	k1 := "11"
	k2 := "2211"
	k3 := "2233"
	k4 := "44"

	putTombStone := func(k string, v []byte) {
		err := db.Put(dbutils.IntermediateTrieHashBucket, common.FromHex(k), v)
		require.NoError(err)
	}

	//printBucket := func() {
	//	fmt.Printf("IH bucket print\n")
	//	_ = db.KV().View(func(tx *bolt.Tx) error {
	//		tx.Bucket(dbutils.IntermediateTrieHashBucket).ForEach(func(k, v []byte) error {
	//			if len(v) == 0 {
	//				fmt.Printf("IH: %x\n", k)
	//			}
	//			return nil
	//		})
	//		return nil
	//	})
	//	fmt.Printf("IH bucket print END\n")
	//}

	acc := accounts.NewAccount()
	acc.Incarnation = 1
	encodedAcc := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(encodedAcc)
	err := db.Put(dbutils.AccountsBucket, common.FromHex(accKey), encodedAcc)
	require.NoError(err)

	putTombStone(accKey+k1, []byte{})
	putTombStone(accKey+k2, []byte{1})
	putTombStone(accKey+k3, []byte{1})
	putTombStone(accKey+k4, []byte{1})

	// step 1: delete account
	batch := db.NewBatch()
	err = state.PutTombstoneForDeletedAccount(batch, common.FromHex(accKey))
	require.NoError(err)
	_, err = batch.Commit()
	require.NoError(err)
	//printBucket()

	untouchedAcc := fmt.Sprintf("99%062x", 0)
	checks := map[string]bool{
		accKey:       true,
		untouchedAcc: false,
	}

	for k, expect := range checks {
		ok, err1 := HasTombstone(db, common.FromHex(k))
		require.NoError(err1, k)
		assert.Equal(expect, ok, k)
	}

	// step 2: re-create storage
	batch = db.NewBatch()
	err = state.ClearTombstonesForNewStorage(batch, common.FromHex(accKey+k2+fmt.Sprintf("%062x", 0)))
	require.NoError(err)
	_, err = batch.Commit()
	require.NoError(err)
	//printBucket()

	checks = map[string]bool{
		accKey:            false,
		accKey + "11":     true,
		accKey + k2:       false,
		accKey + "22":     false,
		accKey + "2200":   false,
		accKey + "2211":   false,
		accKey + "2233":   true,
		accKey + "223300": false,
		accKey + "22ab":   false,
		accKey + "44":     true,
	}

	for k, expect := range checks {
		ok, err1 := HasTombstone(db, common.FromHex(k))
		require.NoError(err1, k)
		assert.Equal(expect, ok, k)
	}

	// step 3: create one new storage
	batch = db.NewBatch()
	err = state.ClearTombstonesForNewStorage(batch, common.FromHex(accKey+k4+fmt.Sprintf("%062x", 0)))
	require.NoError(err)
	_, err = batch.Commit()
	require.NoError(err)
	//printBucket()

	checks = map[string]bool{
		accKey + k2:         false, // results of step2 preserved
		accKey + "22":       false, // results of step2 preserved
		accKey + "2211":     false, // results of step2 preserved
		accKey + "22110000": false, // results of step2 preserved
		accKey + "2233":     true,  // results of step2 preserved
		accKey + "44":       false, // results of step2 preserved
	}

	for k, expect := range checks {
		ok, err := HasTombstone(db, common.FromHex(k))
		require.NoError(err, k)
		assert.Equal(expect, ok, k)
	}

	// step 4: delete account again - it must remove all tombstones and keep only 1 which will cover account itself
	batch = db.NewBatch()
	err = state.PutTombstoneForDeletedAccount(batch, common.FromHex(accKey))
	require.NoError(err)
	_, err = batch.Commit()
	require.NoError(err)
	//printBucket()

	checks = map[string]bool{
		accKey:       true,
		untouchedAcc: false,

		// accKey + "2233" was true on previous step, don't delete this tombstone even one with shorter prefix exists.
		// Because account creation must do predictable amount of operations.
		accKey + "2233": true,
	}

	for k, expect := range checks {
		ok, err1 := HasTombstone(db, common.FromHex(k))
		require.NoError(err1, k)
		assert.Equal(expect, ok, k)
	}

}

func HasTombstone(db ethdb.MinDatabase, prefix []byte) (bool, error) {
	v, err := db.Get(dbutils.IntermediateTrieHashBucket, prefix)
	if err != nil {
		if errors.Is(err, ethdb.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return v != nil && len(v) == 0, nil
}
