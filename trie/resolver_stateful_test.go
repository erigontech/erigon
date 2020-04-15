package trie

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Put 1 embedded entry into the database and try to resolve it
func TestResolve1(t *testing.T) {
	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.StorageBucket, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("bfb355c9a7c26a9c173a9c30e1fb2895fd9908726a8d3dd097203b207d852cf5").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	err := r.ResolveWithDb(db, 0, false)
	require.NoError(err)

	_, ok := tr.Get(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	assert.True(ok)
}

func TestResolve2(t *testing.T) {
	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.StorageBucket, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("aaaaaccccccccccccccccccccccccccc", "")

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("38eb1d28b717978c8cb21b6939dc69ba445d5dea67ca0e948bbf0aef9f1bc2fb").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	err := r.ResolveWithDb(db, 0, false)
	require.NoError(err)

	_, ok := tr.Get(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	assert.True(ok)
}

func TestResolve2Keep(t *testing.T) {
	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.StorageBucket, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("aaaaaccccccccccccccccccccccccccc", "")

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("38eb1d28b717978c8cb21b6939dc69ba445d5dea67ca0e948bbf0aef9f1bc2fb").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	err := r.ResolveWithDb(db, 0, false)
	require.NoError(err)

	_, ok := tr.Get(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	assert.True(ok)
}

func TestResolve3Keep(t *testing.T) {
	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.StorageBucket, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("aaaaabbbbbbbbbbbbbbbbbbbbbbbbbbb", "")
	putStorage("aaaaaccccccccccccccccccccccccccc", "")

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("b780e7d2bc3b7ab7f85084edb2fff42facefa0df9dd1e8190470f277d8183e7c").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	err := r.ResolveWithDb(db, 0, false)
	require.NoError(err, "resolve error")

	_, ok := tr.Get(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	assert.True(ok)
}

func TestTrieResolver(t *testing.T) {
	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.StorageBucket, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("aaaaaccccccccccccccccccccccccccc", "")
	putStorage("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("bbaaaccccccccccccccccccccccccccc", "")
	putStorage("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "")
	putStorage("bccccccccccccccccccccccccccccccc", "")

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

	err := resolver.ResolveWithDb(db, 0, false)
	require.NoError(err, "resolve error")

	_, ok := tr.Get(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	assert.True(ok)
}

func TestTwoStorageItems(t *testing.T) {
	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	tr := New(common.Hash{})

	key1 := common.Hex2Bytes("d7b6990105719101dabeb77144f2a3385c8033acd3af97e9423a695e81ad1eb5")
	key2 := common.Hex2Bytes("df6966c971051c3d54ec59162606531493a51404a002842f56009d7e5cf4a8c7")
	val1 := common.Hex2Bytes("02")
	val2 := common.Hex2Bytes("03")

	require.NoError(db.Put(dbutils.StorageBucket, key1, val1))
	require.NoError(db.Put(dbutils.StorageBucket, key2, val2))

	leaf1 := shortNode{Key: keybytesToHex(key1[1:]), Val: valueNode(val1)}
	leaf2 := shortNode{Key: keybytesToHex(key2[1:]), Val: valueNode(val2)}
	var branch fullNode
	branch.Children[0x7] = &leaf1
	branch.Children[0xf] = &leaf2
	root := shortNode{Key: []byte{0xd}, Val: &branch}

	hasher := newHasher(false)
	defer returnHasherToPool(hasher)
	rootRlp, err := hasher.hashChildren(&root, 0)
	require.NoError(err, "failed ot hash children")

	// Resolve the root node

	rootHash := common.HexToHash("d06f3adc0b0624495478b857a37950d308d6840b349fe2c9eb6dcb813e0ccfb8")
	assert.Equal(rootHash, crypto.Keccak256Hash(rootRlp))

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  []byte{},
		resolvePos:  0,
		resolveHash: hashNode(rootHash.Bytes()),
	}
	resolver := NewResolver(0, false, 0)
	resolver.AddRequest(req)

	err = resolver.ResolveWithDb(db, 0, false)
	require.NoError(err, "resolve error")

	assert.Equal(rootHash.String(), tr.Hash().String())

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

	err = resolver2.ResolveWithDb(db, 0, false)
	require.NoError(err, "resolve error")

	assert.Equal(rootHash.String(), tr.Hash().String())

	_, ok := tr.Get(key1)
	assert.True(ok)
}

func TestTwoAccounts(t *testing.T) {
	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	key1 := common.Hex2Bytes("03601462093b5945d1676df093446790fd31b20e7b12a2e8e5e09d068109616b")
	err := db.Put(dbutils.AccountsBucket, key1, common.Hex2Bytes("020502540be400"))
	require.NoError(err)
	err = db.Put(dbutils.AccountsBucket, common.Hex2Bytes("0fbc62ba90dec43ec1d6016f9dd39dc324e967f2a3459a78281d1f4b2ba962a6"), common.Hex2Bytes("120164204f1593970e8f030c0a2c39758181a447774eae7c65653c4e6440e8c18dad69bc"))
	require.NoError(err)

	expect := common.HexToHash("925002c3260b44e44c3edebad1cc442142b03020209df1ab8bb86752edbd2cd7")

	buf := pool.GetBuffer(64)
	buf.Reset()
	defer pool.PutBuffer(buf)

	DecompressNibbles(common.Hex2Bytes("03601462093b5945d1676df093446790fd31b20e7b12a2e8e5e09d068109616b"), &buf.B)

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  buf.Bytes(),
		resolvePos:  0,
		resolveHash: hashNode(expect.Bytes()),
	}

	resolver := NewResolver(0, true, 0)
	resolver.AddRequest(req)
	err = resolver.ResolveWithDb(db, 0, false)
	require.NoError(err, "resolve error")

	assert.Equal(expect.String(), tr.Hash().String())

	_, ok := tr.GetAccount(key1)
	assert.True(ok)
}

func TestReturnErrOnWrongRootHash(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	putAccount := func(k string) {
		a := accounts.Account{}
		v := make([]byte, a.EncodingLengthForStorage())
		a.EncodeForStorage(v)
		err := db.Put(dbutils.AccountsBucket, common.Hex2Bytes(k), v)
		require.NoError(err)
	}

	putAccount("0000000000000000000000000000000000000000000000000000000000000000")

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  []byte{},
		resolvePos:  0,
		resolveHash: hashNode(common.HexToHash("wrong hash").Bytes()),
	}
	resolver := NewResolver(0, true, 0)
	resolver.AddRequest(req)
	err := resolver.ResolveWithDb(db, 0, false)
	require.NotNil(t, err)
}

func TestApiDetails(t *testing.T) {
	const Stateful = "stateful"
	const StatefulCached = "stateful_cached"

	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()

	storageKey := func(incarnation uint64, k string) []byte {
		return dbutils.GenerateCompositeStorageKey(common.HexToHash(k), incarnation, common.HexToHash(k))
	}
	putIH := func(k string, v string) {
		require.NoError(db.Put(dbutils.IntermediateTrieHashBucket, common.Hex2Bytes(k), common.Hex2Bytes(v)))
	}

	// Test attempt handle cases when: Trie root hash is same for Cached and non-Cached Resolvers
	// Test works with keys like: {base}{i}{j}{zeroes}
	// base = 0 or f - it covers edge cases - first/last subtrees
	//
	// i=0 - has data, has IntermediateHash, no resolve. Tree must have Hash.
	// i=1 - has values with incarnation=1. Tree must have Nil.
	// i=2 - has accounts and storage, no IntermediateHash. Tree must have Account nodes.
	// i>2 - no data, no IntermediateHash, no resolve.
	// i=f - has data, has IntermediateHash, no resolve. Edge case - last subtree.
	for _, base := range []string{"0", "f"} {
		for _, i := range []int{0, 1, 2, 15} {
			for _, j := range []int{0, 1, 2, 15} {
				k := fmt.Sprintf(base+"%x%x%061x", i, j, 0)
				storageV := common.Hex2Bytes(fmt.Sprintf("%x%x", i, j))
				incarnation := uint64(2)
				if i == 1 {
					storageV = []byte{}
					incarnation = 1
				}

				a := accounts.Account{
					// Using Nonce field as an ID of account.
					// Will check later if value which we .Get() from Trie has expected ID.
					Nonce:          uint64(i*10 + j),
					Initialised:    true,
					Root:           EmptyRoot,
					CodeHash:       EmptyCodeHash,
					Balance:        *big.NewInt(0),
					StorageSize:    uint64(len(storageV)),
					HasStorageSize: len(storageV) > 0,
				}
				v := make([]byte, a.EncodingLengthForStorage())
				a.EncodeForStorage(v)

				require.NoError(db.Put(dbutils.AccountsBucket, common.Hex2Bytes(k), v))
				require.NoError(db.Put(dbutils.StorageBucket, storageKey(incarnation, k), storageV))
			}
		}
	}

	putIH("00", "06e98f77330d54fa691a724018df5b2c5689596c03413ca59717ea9bd8a98893")
	putIH("ff", "ad4f92ca84a5980e14a356667eaf0db5d9ff78063630ebaa3d00a6634cd2a3fe")

	// this IntermediateHash key must not be used, because such key is in ResolveRequest
	putIH("01", "0000000000000000000000000000000000000000000000000000000000000000")

	t.Run("account resolver from scratch", func(t *testing.T) {
		tries := []*Trie{New(common.Hash{}), New(common.Hash{})}
		for i, resolverName := range []string{Stateful, StatefulCached} {
			resolver := NewResolver(5, true, 0)
			expectRootHash := common.HexToHash("1af5daf4281e4e5552e79069d0688492de8684c11b1e983f9c3bbac500ad694a")

			resolver.AddRequest(tries[i].NewResolveRequest(nil, nil, 0, expectRootHash.Bytes()))

			if resolverName == Stateful {
				err := resolver.ResolveStateful(db, 0)
				//fmt.Printf("%x\n", tries[i].root.(*fullNode).Children[15].(*fullNode).Children[15].hash())
				assert.NoError(err)
			} else {
				err := resolver.ResolveStatefulCached(db, 0, false)
				//fmt.Printf("%x\n", tries[i].root.(*shortNode).Val.(*fullNode).Children[15].hash())
				assert.NoError(err)
			}
			assert.Equal(expectRootHash.String(), tries[i].Hash().String(), resolverName)
		}
	})

	t.Run("account resolver", func(t *testing.T) {
		for _, resolverName := range []string{Stateful, StatefulCached} {
			tr := New(common.Hash{})
			resolver := NewResolver(0, true, 0)
			expectRootHash := common.HexToHash("1af5daf4281e4e5552e79069d0688492de8684c11b1e983f9c3bbac500ad694a")

			resolver.AddRequest(tr.NewResolveRequest(nil, append(common.Hex2Bytes(fmt.Sprintf("000101%0122x", 0)), 16), 0, expectRootHash.Bytes()))
			resolver.AddRequest(tr.NewResolveRequest(nil, common.Hex2Bytes("000202"), 0, expectRootHash.Bytes()))
			resolver.AddRequest(tr.NewResolveRequest(nil, common.Hex2Bytes("0f"), 0, expectRootHash.Bytes()))

			if resolverName == Stateful {
				err := resolver.ResolveStateful(db, 0)
				assert.NoError(err)
			} else {
				err := resolver.ResolveStatefulCached(db, 0, false)
				assert.NoError(err)
			}

			assert.Equal(expectRootHash.String(), tr.Hash().String(), resolverName)

			_, found := tr.GetAccount(common.Hex2Bytes(fmt.Sprintf("000%061x", 0)))
			assert.False(found) // exists in DB but resolved, there is hashNode

			acc, found := tr.GetAccount(common.Hex2Bytes(fmt.Sprintf("011%061x", 0)))
			assert.True(found)
			require.NotNil(acc)              // cache bucket has empty value, but self-destructed Account still available
			assert.Equal(int(acc.Nonce), 11) // i * 10 + j

			acc, found = tr.GetAccount(common.Hex2Bytes(fmt.Sprintf("021%061x", 0)))
			assert.True(found)
			require.NotNil(acc)              // exists in db and resolved
			assert.Equal(int(acc.Nonce), 21) // i * 10 + j

			acc, found = tr.GetAccount(common.Hex2Bytes(fmt.Sprintf("051%061x", 0)))
			assert.True(found)
			assert.Nil(acc) // not exists in DB

			assert.Panics(func() {
				tr.UpdateAccount(common.Hex2Bytes(fmt.Sprintf("001%061x", 0)), &accounts.Account{})
			})
			assert.NotPanics(func() {
				tr.UpdateAccount(common.Hex2Bytes(fmt.Sprintf("011%061x", 0)), &accounts.Account{})
				tr.UpdateAccount(common.Hex2Bytes(fmt.Sprintf("021%061x", 0)), &accounts.Account{})
				tr.UpdateAccount(common.Hex2Bytes(fmt.Sprintf("051%061x", 0)), &accounts.Account{})
			})
		}
	})

	t.Run("storage resolver", func(t *testing.T) {
		putIH("00", "c733727d362a5c5b05dc90d95aa48a9f8a564907ef5360849bf4b338bf85f7a4")
		putIH("ff", "087b59df7d243bf3c0d3e2886b774120c73ee90980a0b10e60908ca6e401c7f4")

		for _, resolverName := range []string{Stateful, StatefulCached} {
			tr, resolver := New(common.Hash{}), NewResolver(1, false, 0)
			expectRootHash := common.HexToHash("c3fd3ccd887c47c57d3f9c8eb84689e82a65c26ec13a3bf64a0d635f0df9cdf6")

			resolver.AddRequest(tr.NewResolveRequest(nil, append(common.Hex2Bytes(fmt.Sprintf("000101%0122x", 0)), 16), 0, expectRootHash.Bytes()))
			resolver.AddRequest(tr.NewResolveRequest(nil, common.Hex2Bytes("00020100"), 0, expectRootHash.Bytes()))
			resolver.AddRequest(tr.NewResolveRequest(nil, common.Hex2Bytes("0f"), 0, expectRootHash.Bytes()))

			if resolverName == Stateful {
				err := resolver.ResolveStateful(db, 0)
				require.NoError(err)
			} else {
				err := resolver.ResolveStatefulCached(db, 0, false)
				require.NoError(err)
			}
			assert.Equal(expectRootHash.String(), tr.Hash().String())

			_, found := tr.Get(storageKey(2, fmt.Sprintf("000%061x", 0)))
			assert.False(found) // exists in DB but not resolved, there is hashNode

			storage, found := tr.Get(storageKey(2, fmt.Sprintf("011%061x", 0)))
			assert.True(found)
			require.Nil(storage) // deleted by empty value in cache bucket

			storage, found = tr.Get(storageKey(2, fmt.Sprintf("021%061x", 0)))
			assert.True(found)
			require.Equal(storage, common.Hex2Bytes("21"))

			storage, found = tr.Get(storageKey(2, fmt.Sprintf("051%061x", 0)))
			assert.True(found)
			assert.Nil(storage) // not exists in DB

			assert.Panics(func() {
				tr.Update(storageKey(2, fmt.Sprintf("001%061x", 0)), nil)
			})
			assert.NotPanics(func() {
				tr.Update(storageKey(2, fmt.Sprintf("011%061x", 0)), nil)
				tr.Update(storageKey(2, fmt.Sprintf("021%061x", 0)), nil)
				tr.Update(storageKey(2, fmt.Sprintf("051%061x", 0)), nil)
			})
		}
	})
}

func TestIsBefore(t *testing.T) {
	assert := assert.New(t)

	is, minKey := keyIsBefore([]byte("a"), []byte("b"))
	assert.Equal(true, is)
	assert.Equal("a", fmt.Sprintf("%s", minKey))

	is, minKey = keyIsBefore([]byte("b"), []byte("a"))
	assert.Equal(false, is)
	assert.Equal("a", fmt.Sprintf("%s", minKey))

	is, minKey = keyIsBefore([]byte("b"), []byte(""))
	assert.Equal(false, is)
	assert.Equal("", fmt.Sprintf("%s", minKey))

	is, minKey = keyIsBefore(nil, []byte("b"))
	assert.Equal(false, is)
	assert.Equal("b", fmt.Sprintf("%s", minKey))

	is, minKey = keyIsBefore([]byte("b"), nil)
	assert.Equal(true, is)
	assert.Equal("b", fmt.Sprintf("%s", minKey))

	contract := fmt.Sprintf("2%063x", 0)
	storageKey := common.Hex2Bytes(contract + "ffffffff" + fmt.Sprintf("10%062x", 0))
	cacheKey := common.Hex2Bytes(contract + "ffffffff" + "20")
	is, minKey = keyIsBefore(cacheKey, storageKey)
	assert.False(is)
	assert.Equal(fmt.Sprintf("%x", storageKey), fmt.Sprintf("%x", minKey))

	storageKey = common.Hex2Bytes(contract + "ffffffffffffffff" + fmt.Sprintf("20%062x", 0))
	cacheKey = common.Hex2Bytes(contract + "ffffffffffffffff" + "10")
	is, minKey = keyIsBefore(cacheKey, storageKey)
	assert.True(is)
	assert.Equal(fmt.Sprintf("%x", cacheKey), fmt.Sprintf("%x", minKey))
}
