package trie

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRebuild(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth")

	db := ethdb.NewMemDatabase()
	defer db.Close()
	bucket := dbutils.AccountsBucket
	tr := New(common.Hash{})

	keys := []string{
		"FIRSTFIRSTFIRSTFIRSTFIRSTFIRSTFI",
		"SECONDSECONDSECONDSECONDSECONDSE",
		"FISECONDSECONDSECONDSECONDSECOND",
		"FISECONDSECONDSECONDSECONDSECONB",
		"THIRDTHIRDTHIRDTHIRDTHIRDTHIRDTH",
	}
	values := []string{
		"FIRST",
		"SECOND",
		"THIRD",
		"FORTH",
		"FIRTH",
	}

	for i := 0; i < len(keys); i++ {
		key := []byte(keys[i])
		value := []byte(values[i])
		v1, err := rlp.EncodeToBytes(bytes.TrimLeft(value, "\x00"))
		if err != nil {
			t.Errorf("Could not encode value: %v", err)
		}
		tr.Update(key, v1, 0)
		tr.PrintTrie()
		root1 := tr.Root()
		//fmt.Printf("Root1: %x\n", tr.Root())
		v1, err = EncodeAsValue(v1)
		if err != nil {
			t.Errorf("Could not encode value: %v", err)
		}
		err = db.Put(bucket, key, v1)
		require.Nil(t, err)
		t1 := New(common.BytesToHash(root1))
		_ = t1.Rebuild(db, 0)
	}
}

// Put 1 embedded entry into the database and try to resolve it
func TestResolve1(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	err := db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes(""))
	require.Nil(t, err)

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("bfb355c9a7c26a9c173a9c30e1fb2895fd9908726a8d3dd097203b207d852cf5").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	err = r.ResolveWithDb(db, 0)
	require.Nil(t, err)

	_, ok := tr.Get(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	assert.True(t, ok)
}

func TestResolve2(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	err := db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes(""))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaccccccccccccccccccccccccccc"), common.Hex2Bytes(""))
	require.Nil(t, err)

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("38eb1d28b717978c8cb21b6939dc69ba445d5dea67ca0e948bbf0aef9f1bc2fb").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	err = r.ResolveWithDb(db, 0)
	require.Nil(t, err)

	_, ok := tr.Get(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	assert.True(t, ok)
}

func TestResolve2Keep(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	err := db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes(""))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaccccccccccccccccccccccccccc"), common.Hex2Bytes(""))
	require.Nil(t, err)

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("38eb1d28b717978c8cb21b6939dc69ba445d5dea67ca0e948bbf0aef9f1bc2fb").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	err = r.ResolveWithDb(db, 0)
	require.Nil(t, err)

	_, ok := tr.Get(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	assert.True(t, ok)
}

func TestResolve3Keep(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	err := db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes(""))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaabbbbbbbbbbbbbbbbbbbbbbbbbbb"), common.Hex2Bytes(""))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaccccccccccccccccccccccccccc"), common.Hex2Bytes(""))
	require.Nil(t, err)

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("b780e7d2bc3b7ab7f85084edb2fff42facefa0df9dd1e8190470f277d8183e7c").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	err = r.ResolveWithDb(db, 0)
	require.Nil(t, err, "resolve error")

	_, ok := tr.Get(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	assert.True(t, ok)
}

func TestTrieResolver(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	err := db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes(""))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaccccccccccccccccccccccccccc"), common.Hex2Bytes(""))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes(""))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes(""))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("bbaaaccccccccccccccccccccccccccc"), common.Hex2Bytes(""))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), common.Hex2Bytes(""))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("bccccccccccccccccccccccccccccccc"), common.Hex2Bytes(""))
	require.Nil(t, err)

	req1 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("38eb1d28b717978c8cb21b6939dc69ba445d5dea67ca0e948bbf0aef9f1bc2fb").Bytes()),
	}
	req2 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  2, // 1 bytes is 2 nibbles
		resolveHash: hashNode(common.HexToHash("dc2332366fcf65ad75d09901e199e3dd52a5389ad85ff1d853803c5f40cbde56").Bytes()),
	}
	req3 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("bbbaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  2, // 1 bytes is 2 nibbles
		resolveHash: hashNode(common.HexToHash("df6fd126d62ec79182d9ab6f879b63dfacb9ce2e1cb765b17b9752de9c2cbaa7").Bytes()),
	}
	resolver := NewResolver(0, false, 0)
	resolver.AddRequest(req3)
	resolver.AddRequest(req2)
	resolver.AddRequest(req1)

	err = resolver.ResolveWithDb(db, 0)
	require.Nil(t, err, "resolve error")

	_, ok := tr.Get(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	assert.True(t, ok)
}

func TestTwoStorageItems(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})

	key1 := common.Hex2Bytes("d7b6990105719101dabeb77144f2a3385c8033acd3af97e9423a695e81ad1eb5")
	key2 := common.Hex2Bytes("df6966c971051c3d54ec59162606531493a51404a002842f56009d7e5cf4a8c7")
	val1 := common.Hex2Bytes("02")
	val2 := common.Hex2Bytes("03")

	err := db.Put(dbutils.StorageBucket, key1, val1)
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, key2, val2)
	require.Nil(t, err)

	leaf1 := shortNode{Key: keybytesToHex(key1[1:]), Val: valueNode(val1)}
	leaf2 := shortNode{Key: keybytesToHex(key2[1:]), Val: valueNode(val2)}
	var branch fullNode
	branch.Children[0x7] = &leaf1
	branch.Children[0xf] = &leaf2
	branch.flags.dirty = true
	root := shortNode{Key: []byte{0xd}, Val: &branch}

	hasher := newHasher(false)
	defer returnHasherToPool(hasher)
	rootRlp, err := hasher.hashChildren(&root, 0)
	require.Nil(t, err, "failed ot hash children")

	// Resolve the root node

	rootHash := common.HexToHash("d06f3adc0b0624495478b857a37950d308d6840b349fe2c9eb6dcb813e0ccfb8")
	assert.Equal(t, rootHash, crypto.Keccak256Hash(rootRlp))

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  []byte{},
		resolvePos:  0,
		resolveHash: hashNode(rootHash.Bytes()),
	}
	resolver := NewResolver(0, false, 0)
	resolver.AddRequest(req)

	err = resolver.ResolveWithDb(db, 0)
	require.Nil(t, err, "resolve error")

	assert.Equal(t, rootHash.String(), tr.Hash().String())

	// Resolve the branch node

	branchRlp, err := hasher.hashChildren(&branch, 0)
	if err != nil {
		t.Errorf("failed ot hash children: %v", err)
	}

	req2 := &ResolveRequest{
		t:           tr,
		resolveHex:  []byte{0xd},
		resolvePos:  1,
		resolveHash: hashNode(crypto.Keccak256(branchRlp)),
	}
	resolver2 := NewResolver(0, false, 0)
	resolver2.AddRequest(req2)

	err = resolver2.ResolveWithDb(db, 0)
	require.Nil(t, err, "resolve error")

	assert.Equal(t, rootHash.String(), tr.Hash().String())

	_, ok := tr.Get(key1)
	assert.True(t, ok)
}

func TestTwoAccounts(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	key1 := common.Hex2Bytes("03601462093b5945d1676df093446790fd31b20e7b12a2e8e5e09d068109616b")
	err := db.Put(dbutils.AccountsBucket, key1, common.Hex2Bytes("020502540be400"))
	require.Nil(t, err)
	err = db.Put(dbutils.AccountsBucket, common.Hex2Bytes("0fbc62ba90dec43ec1d6016f9dd39dc324e967f2a3459a78281d1f4b2ba962a6"), common.Hex2Bytes("120164204f1593970e8f030c0a2c39758181a447774eae7c65653c4e6440e8c18dad69bc"))
	require.Nil(t, err)

	expect := common.HexToHash("925002c3260b44e44c3edebad1cc442142b03020209df1ab8bb86752edbd2cd7")

	buf := pool.GetBuffer(64)
	err = DecompressNibbles(common.Hex2Bytes("03601462093b5945d1676df093446790fd31b20e7b12a2e8e5e09d068109616b"), buf)
	require.Nil(t, err)

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  buf.Bytes(),
		resolvePos:  0,
		resolveHash: hashNode(expect.Bytes()),
	}

	resolver := NewResolver(0, true, 0)
	resolver.AddRequest(req)
	err = resolver.ResolveWithDb(db, 0)
	require.Nil(t, err, "resolve error")

	assert.Equal(t, expect.String(), tr.Hash().String())

	_, ok := tr.GetAccount(key1)
	assert.True(t, ok)
}

func TestTwoAccountsNegative(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	err := db.Put(dbutils.AccountsBucket, common.Hex2Bytes("03601462093b5945d1676df093446790fd31b20e7b12a2e8e5e09d068109616b"), common.Hex2Bytes("020502540be400"))
	require.Nil(t, err)
	err = db.Put(dbutils.AccountsBucket, common.Hex2Bytes("0fbc62ba90dec43ec1d6016f9dd39dc324e967f2a3459a78281d1f4b2ba962a6"), common.Hex2Bytes("120164204f1593970e8f030c0a2c39758181a447774eae7c65653c4e6440e8c18dad69bc"))
	require.Nil(t, err)

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  []byte{},
		resolvePos:  0,
		resolveHash: hashNode(common.HexToHash("wrong hash").Bytes()),
	}
	resolver := NewResolver(0, true, 0)
	resolver.AddRequest(req)
	err = resolver.ResolveWithDb(db, 0)
	require.NotNil(t, err)
}

func TestApiDetails(t *testing.T) {
	db := ethdb.NewMemDatabase()
	useCash := true
	tr := New(common.Hash{})

	// Generate accounts with various prefixes
	// Test will work with accounts: 01010000...

	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			k := fmt.Sprintf("%02x%02x%060x", i, j, 0)

			a := accounts.Account{
				// Using Nonce field as an ID of account.
				// Will check later if value which we .Get() from Trie has expected ID.
				Nonce: uint64(i*10 + j),
			}
			v := make([]byte, a.EncodingLengthForStorage())
			a.EncodeForStorage(v)

			err := db.Put(dbutils.AccountsBucket, common.Hex2Bytes(k), v)
			require.Nil(t, err)
		}
	}

	if useCash {
		err := db.Put(dbutils.IntermediateTrieHashesBucket, common.Hex2Bytes("00"), common.Hex2Bytes("c505b15def1161a1a2604c3ae9bc75424a828ff3b6ee5ab391a7af75654372f6"))
		require.Nil(t, err)
	}

	expectRootHash := common.HexToHash("ea6ea05b1e4e43e3402b73a4a2b0a9ccee69585de3d8d22598e46366a14ddc82")
	req1 := &ResolveRequest{
		t:           tr,
		resolveHex:  common.Hex2Bytes(fmt.Sprintf("00010001%0120x", 0)), // nibbles format
		resolvePos:  0,
		resolveHash: hashNode(expectRootHash.Bytes()),
	}
	req2 := &ResolveRequest{
		t:           tr,
		resolveHex:  common.Hex2Bytes("0f00"), // nibbles format
		resolvePos:  0,
		resolveHash: hashNode(expectRootHash.Bytes()),
	}

	resolver := NewResolver(0, true, 0)
	resolver.AddRequest(req1)
	resolver.AddRequest(req2)
	err := resolver.ResolveWithDb(db, 0)
	require.Nil(t, err, "resolve error")
	assert.Equal(t, expectRootHash.String(), tr.Hash().String())

	// Can find resolved accounts. Can't find non-resolved

	_, found := tr.GetAccount(common.Hex2Bytes(fmt.Sprintf("0000%060x", 0)))
	assert.False(t, found) // because not resolved, there is hashNode

	storage, found := tr.Get(common.Hex2Bytes(fmt.Sprintf("0101%060x", 0)))
	assert.True(t, found)
	assert.Nil(t, storage)

	acc, found := tr.GetAccount(common.Hex2Bytes(fmt.Sprintf("0101%060x", 0)))
	assert.True(t, found)
	require.NotNil(t, acc)
	assert.Equal(t, int(acc.Nonce), 11) // i * 10 + j

	// must return "found=true" if no such account in tree (false only if tree has hashNode)

	storage, found = tr.Get(common.Hex2Bytes(fmt.Sprintf("0501%060x", 0)))
	assert.True(t, found)
	assert.Nil(t, storage)

	acc, found = tr.GetAccount(common.Hex2Bytes(fmt.Sprintf("0501%060x", 0)))
	assert.True(t, found)
	assert.Nil(t, acc)

	// ToDo: Add storage bucket and resolve it
}
