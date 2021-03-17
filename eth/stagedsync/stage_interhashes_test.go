package stagedsync

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
)

func addTestAccount(db *ethdb.ObjectDatabase, hash common.Hash, balance uint64) error {
	acc := accounts.NewAccount()
	acc.Balance.SetUint64(balance)
	encoded := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(encoded)
	return db.Put(dbutils.HashedAccountsBucket, hash[:], encoded)
}

func TestRegenerateIntermediateHashes(t *testing.T) {
	db := ethdb.NewMemDatabase()

	hash1 := common.HexToHash("0x0B00000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(db, hash1, 300_000_000_000))

	hash2 := common.HexToHash("0x0B00040000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(db, hash2, 100_000_000_000))

	hash3 := common.HexToHash("0x0B00040100000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(db, hash3, 200_000_000_000))

	hash4 := common.HexToHash("0x0B02000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(db, hash4, 400_000_000_000))

	hash5 := common.HexToHash("0x0B03010000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(db, hash5, 800_000_000_000))

	hash6 := common.HexToHash("0x0B03040000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(db, hash6, 100_000_000_000))

	err := RegenerateIntermediateHashes("IH", db, false /* checkRoot */, nil /* cache */, getTmpDir(), common.Hash{} /* expectedRootHash */, nil /* quit */)
	assert.Nil(t, err)

	// TODO: check all trie_account entries

	// TODO: check that HashBuilder returns the same root
}
