package stagedsync

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
	"github.com/stretchr/testify/assert"
)

func addTestAccount(db ethdb.Putter, hash common.Hash, balance uint64) error {
	acc := accounts.NewAccount()
	acc.Balance.SetUint64(balance)
	encoded := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(encoded)
	return db.Put(dbutils.HashedAccountsBucket, hash[:], encoded)
}

func TestTrieOfAccountsLayout(t *testing.T) {
	db := ethdb.NewMemDatabase()

	hash1 := common.HexToHash("0xB000000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(db, hash1, 300_000_000_000))

	hash2 := common.HexToHash("0xB040000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(db, hash2, 100_000_000_000))

	hash3 := common.HexToHash("0xB041000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(db, hash3, 200_000_000_000))

	hash4 := common.HexToHash("0xB100000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(db, hash4, 400_000_000_000))

	hash5 := common.HexToHash("0xB310000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(db, hash5, 800_000_000_000))

	hash6 := common.HexToHash("0xB340000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(db, hash6, 100_000_000_000))

	err := RegenerateIntermediateHashes("IH", db, false /* checkRoot */, nil /* cache */, getTmpDir(), common.Hash{} /* expectedRootHash */, nil /* quit */)
	assert.Nil(t, err)

	account_trie := make(map[string][]byte)

	err = db.Walk(dbutils.TrieOfAccountsBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		account_trie[string(k)] = v
		return true, nil
	})
	assert.Nil(t, err)

	assert.Equal(t, 2, len(account_trie))

	hasState1, hasTree1, hasHash1, _, _ := trie.UnmarshalTrieNode(account_trie[string(common.FromHex("0B"))])
	assert.Equal(t, uint16(0b1011), hasState1)
	assert.Equal(t, uint16(0b0001), hasTree1)
	assert.Equal(t, uint16(0b1001), hasHash1)

	hasState2, hasTree2, hasHash2, _, _ := trie.UnmarshalTrieNode(account_trie[string(common.FromHex("0B00"))])
	assert.Equal(t, uint16(0b10001), hasState2)
	assert.Equal(t, uint16(0b00000), hasTree2)
	assert.Equal(t, uint16(0b10000), hasHash2)
}
