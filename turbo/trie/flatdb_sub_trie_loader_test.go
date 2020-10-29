package trie

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Put 1 embedded entry into the database and try to resolve it
func TestResolve1(t *testing.T) {
	t.Skip("weird case of abandoned storage, will handle it later")

	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.CurrentStateBucket, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	r := NewSubTrieLoader(0)
	rs := NewRetainList(0)
	rs.AddKey(common.Hex2Bytes("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa"))
	subTries, err := r.LoadSubTries(db, 0, rs, nil /* HashCollector */, [][]byte{common.Hex2Bytes("aaaaabbbbb")}, []int{40}, false)
	require.NoError(err)
	tr := New(common.Hash{})
	assert.NoError(tr.HookSubTries(subTries, [][]byte{nil})) // hook up to the root of the trie
	x, ok := tr.Get(common.Hex2Bytes("aaaaabbbbbaaaaabbbbbaa"))
	assert.True(ok)
	assert.NotNil(x)
}

func TestResolve2(t *testing.T) {
	t.Skip("weird case of abandoned storage, will handle it later")

	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.CurrentStateBucket, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("aaaaaccccccccccccccccccccccccccc", "")

	r := NewSubTrieLoader(0)
	rs := NewRetainList(0)
	rs.AddKey(common.Hex2Bytes("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa"))
	subTries, err := r.LoadSubTries(db, 0, rs, nil /* HashCollector */, [][]byte{common.Hex2Bytes("aaaaaaaaaa")}, []int{40}, false)
	require.NoError(err)

	tr := New(common.Hash{})
	assert.NoError(tr.HookSubTries(subTries, [][]byte{nil})) // hook up to the root of the trie
	x, ok := tr.Get(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	assert.True(ok)
	assert.NotNil(x)
}

func TestResolve2Keep(t *testing.T) {
	t.Skip("weird case of abandoned storage, will handle it later")

	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.CurrentStateBucket, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("aaaaaccccccccccccccccccccccccccc", "")

	r := NewSubTrieLoader(0)
	rs := NewRetainList(0)
	rs.AddKey(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	subTries, err := r.LoadSubTries(db, 0, rs, nil /* HashCollector */, [][]byte{common.Hex2Bytes("aaaaaaaaaa")}, []int{40}, false)
	require.NoError(err)

	tr := New(common.Hash{})
	assert.NoError(tr.HookSubTries(subTries, [][]byte{nil})) // hook up to the root of the trie
	x, ok := tr.Get(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	assert.True(ok)
	assert.NotNil(x)
}

func TestResolve3Keep(t *testing.T) {
	t.Skip("weird case of abandoned storage, will handle it later")

	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.CurrentStateBucket, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("aaaaabbbbbbbbbbbbbbbbbbbbbbbbbbb", "")
	putStorage("aaaaaccccccccccccccccccccccccccc", "")

	r := NewSubTrieLoader(0)
	rs := NewRetainList(0)
	rs.AddKey(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	subTries, err := r.LoadSubTries(db, 0, rs, nil /* HashCollector */, [][]byte{common.Hex2Bytes("aaaaaaaaaa")}, []int{40}, false)
	require.NoError(err)

	tr := New(common.Hash{})
	assert.NoError(tr.HookSubTries(subTries, [][]byte{nil})) // hook up to the root of the trie
	x, ok := tr.Get(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	assert.True(ok)
	assert.NotNil(x)
}

func TestTrieSubTrieLoader(t *testing.T) {
	t.Skip("weird case of abandoned storage, will handle it later")

	require, _, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.CurrentStateBucket, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("aaaaaccccccccccccccccccccccccccc", "")
	putStorage("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("bbaaaccccccccccccccccccccccccccc", "")
	putStorage("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "")
	putStorage("bccccccccccccccccccccccccccccccc", "")

	resolver := NewSubTrieLoader(0)
	rs := NewRetainList(0)
	rs.AddKey(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	rs.AddKey(common.Hex2Bytes("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	rs.AddKey(common.Hex2Bytes("bbbaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	_, err := resolver.LoadSubTries(db, 0, rs, nil, /* HashCollector */
		[][]byte{common.Hex2Bytes("aaaaa"), common.Hex2Bytes("bb")}, []int{40, 8}, false)
	require.NoError(err, "resolve error")
}

func TestTwoStorageItems(t *testing.T) {
	t.Skip("weird case of abandoned storage, will handle it later")

	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()

	key1 := common.Hex2Bytes("d7b6990105719101dabeb77144f2a3385c8033acd3af97e9423a695e81ad1eb5f5")
	key2 := common.Hex2Bytes("df6966c971051c3d54ec59162606531493a51404a002842f56009d7e5cf4a8c7f5")
	val1 := common.Hex2Bytes("02")
	val2 := common.Hex2Bytes("03")

	require.NoError(db.Put(dbutils.CurrentStateBucket, key1, val1))
	require.NoError(db.Put(dbutils.CurrentStateBucket, key2, val2))
	var branch fullNode
	branch.Children[0x7] = NewShortNode(keybytesToHex(key1[1:]), valueNode(val1))
	branch.Children[0xf] = NewShortNode(keybytesToHex(key2[1:]), valueNode(val2))
	root := NewShortNode([]byte{0xd}, &branch)

	hasher := newHasher(false)
	defer returnHasherToPool(hasher)
	rootRlp, err := hasher.hashChildren(root, 0)
	require.NoError(err, "failed ot hash children")

	// Resolve the root node

	rootHash := common.HexToHash("85737b049107f866fedbd6d787077fc2c245f4748e28896a3e8ee82c377ecdcf")
	assert.Equal(rootHash, crypto.Keccak256Hash(rootRlp))

	resolver := NewSubTrieLoader(0)
	rs := NewRetainList(0)
	subTries, err1 := resolver.LoadSubTries(db, 0, rs, nil /* HashCollector */, [][]byte{nil}, []int{0}, false)
	require.NoError(err1, "resolve error")

	assert.Equal(rootHash.String(), subTries.Hashes[0].String())
	tr := New(common.Hash{})
	err = tr.HookSubTries(subTries, [][]byte{nil}) // hook up to the root of the trie
	assert.NoError(err)

	// Resolve the branch node

	//branchRlp, err := hasher.hashChildren(&branch, 0)
	//if err != nil {
	//	t.Errorf("failed ot hash children: %v", err)
	//}

	resolver2 := NewSubTrieLoader(0)
	rs2 := NewRetainList(0)
	rs2.AddHex([]byte{0xd})
	subTries, err = resolver2.LoadSubTries(db, 0, rs2, nil /* HashCollector */, [][]byte{{0xd0}}, []int{4}, false)
	require.NoError(err, "resolve error")

	err = tr.HookSubTries(subTries, [][]byte{{0xd}}) // hook up to the prefix 0xd
	assert.NoError(err)

	x, ok := tr.Get(key1)
	assert.True(ok)
	assert.NotNil(x)
}

func TestTwoAccounts(t *testing.T) {
	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	key1 := common.Hex2Bytes("03601462093b5945d1676df093446790fd31b20e7b12a2e8e5e09d068109616b")
	acc := accounts.NewAccount()
	acc.Initialised = true
	acc.Balance.SetUint64(10000000000)
	acc.CodeHash.SetBytes(common.Hex2Bytes("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"))
	err := writeAccount(db, common.BytesToHash(key1), acc)
	require.NoError(err)

	key2 := common.Hex2Bytes("0fbc62ba90dec43ec1d6016f9dd39dc324e967f2a3459a78281d1f4b2ba962a6")
	acc2 := accounts.NewAccount()
	acc2.Initialised = true
	acc2.Balance.SetUint64(100)
	acc2.CodeHash.SetBytes(common.Hex2Bytes("4f1593970e8f030c0a2c39758181a447774eae7c65653c4e6440e8c18dad69bc"))
	err = writeAccount(db, common.BytesToHash(key2), acc2)
	require.NoError(err)

	expect := common.HexToHash("925002c3260b44e44c3edebad1cc442142b03020209df1ab8bb86752edbd2cd7")

	resolver := NewSubTrieLoader(0)
	rs := NewRetainList(0)
	rs.AddKey(key1)
	subTries, err1 := resolver.LoadSubTries(db, 0, rs, nil /* HashCollector */, [][]byte{nil}, []int{0}, false)
	require.NoError(err1, "resolve error")
	assert.Equal(expect.String(), subTries.Hashes[0].String())

	tr := New(common.Hash{})
	err = tr.HookSubTries(subTries, [][]byte{nil}) // hook up to the root
	assert.NoError(err)

	x, ok := tr.GetAccount(key1)
	assert.True(ok)
	assert.NotNil(x)
}

func TestReturnErrOnWrongRootHash(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()
	putAccount := func(k string) {
		a := accounts.Account{}
		err := writeAccount(db, common.BytesToHash(common.Hex2Bytes(k)), a)
		require.NoError(err)
	}

	putAccount("0000000000000000000000000000000000000000000000000000000000000000")

	rs := NewRetainList(0)
	resolver := NewSubTrieLoader(0)
	_, err := resolver.LoadSubTries(db, 0, rs, nil /* HashCollector */, [][]byte{nil}, []int{0}, false)
	require.NotNil(t, err)
}

func TestApiDetails(t *testing.T) {
	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()

	storageKey := func(incarnation uint64, k string) []byte {
		return dbutils.GenerateCompositeStorageKey(common.HexToHash(k), incarnation, common.HexToHash("0000000000000000000000000000000000000000000000000000000000000000"))
	}
	putIH := func(k string, v string) {
		require.NoError(db.Put(dbutils.IntermediateTrieHashBucket, common.Hex2Bytes(k), common.Hex2Bytes(v)))
	}

	// Test attempt handle cases when: Trie root hash is same for Cached and non-Cached SubTrieLoaders
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
				incarnation := uint64(2)
				if j == 1 {
					incarnation = 1
				}
				storageV := []byte{uint8(incarnation)}

				a := accounts.Account{
					// Using Nonce field as an ID of account.
					// Will check later if value which we .Get() from Trie has expected ID.
					Nonce:       uint64(i*10 + j),
					Initialised: true,
					CodeHash:    EmptyCodeHash,
					Balance:     *uint256.NewInt(),
					Incarnation: 2, // all acc have 2nd inc, but some storage are on 1st inc
				}
				require.NoError(writeAccount(db, common.BytesToHash(common.Hex2Bytes(k)), a))
				require.NoError(db.Put(dbutils.CurrentStateBucket, storageKey(incarnation, k), storageV))
			}
		}
	}

	/*
		Next IH's calculated by next logic:
		var root common.Hash
		_, err = tr.getHasher().hash(tr.root.(*fullNode).Children[0].(*fullNode).Children[0], true, root[:])
		require.NoError(err)
		fmt.Printf("%x\n", root)
	*/
	putIH("00", "7e099756ba801779e6ac78da0c8f0272a2033e92314f02fbf7ec5158ab57017b")
	putIH("ff", "73e9eaef7cbb0b824f964669ee2ebff9ed7a4cd2c672b521e44f9b33cab8aa55")

	// this IntermediateHash key must not be used, because such key is in ResolveRequest
	// putIH("01", "0000000000000000000000000000000000000000000000000000000000000000")

	tr := New(common.Hash{})

	{
		expectRootHash := common.HexToHash("8c34d9b522547741fefe2363762026cf821e6b6a9358a56c9af26182403d20d9")

		loader := NewSubTrieLoader(0)
		rs := NewRetainList(0)
		rs.AddHex(hexf("000101%0122x", 0))
		rs.AddHex(common.Hex2Bytes("000202"))
		rs.AddHex(common.Hex2Bytes("0f"))
		subTries, err := loader.LoadSubTries(db, 0, rs, nil /* HashCollector */, [][]byte{nil}, []int{0}, false)
		assert.NoError(err)

		err = tr.HookSubTries(subTries, [][]byte{nil}) // hook up to the root
		assert.NoError(err)

		assert.Equal(expectRootHash.String(), tr.Hash().String())

		//fmt.Printf("%x\n", tr.root.(*fullNode).Children[0].(*fullNode).Children[0].reference())
		//fmt.Printf("%x\n", tr.root.(*fullNode).Children[15].(*fullNode).Children[15].reference())
		//fmt.Printf("%d\n", tr.root.(*fullNode).Children[0].(*fullNode).Children[0].witnessSize())
		//fmt.Printf("%d\n", tr.root.(*fullNode).Children[15].(*fullNode).Children[15].witnessSize())

		_, found := tr.GetAccount(hexf("000%061x", 0))
		assert.False(found) // exists in DB but resolved, there is hashNode

		acc, found := tr.GetAccount(hexf("011%061x", 0))
		assert.True(found)
		require.NotNil(acc)              // cache bucket has empty value, but self-destructed Account still available
		assert.Equal(int(acc.Nonce), 11) // i * 10 + j

		acc, found = tr.GetAccount(hexf("021%061x", 0))
		assert.True(found)
		require.NotNil(acc)              // exists in db and resolved
		assert.Equal(int(acc.Nonce), 21) // i * 10 + j

		acc, found = tr.GetAccount(hexf("051%061x", 0))
		assert.True(found)
		assert.Nil(acc) // not exists in DB

		//assert.Panics(func() {
		//	tr.UpdateAccount(hexf("001%061x", 0), &accounts.Account{})
		//})
		//assert.NotPanics(func() {
		//	tr.UpdateAccount(hexf("011%061x", 0), &accounts.Account{})
		//	tr.UpdateAccount(hexf("021%061x", 0), &accounts.Account{})
		//	tr.UpdateAccount(hexf("051%061x", 0), &accounts.Account{})
		//})
	}

	{ // storage loader
		loader := NewSubTrieLoader(0)
		rl := NewRetainList(0)
		binary.BigEndian.PutUint64(bytes8[:], uint64(2))
		for i, b := range bytes8[:] {
			bytes16[i*2] = b / 16
			bytes16[i*2+1] = b % 16
		}
		rl.AddHex(append(append(hexf("000101%0122x", 0), bytes16[:]...), hexf("%0128x", 0)...))
		rl.AddHex(append(append(hexf("000201%0122x", 0), bytes16[:]...), hexf("%0128x", 0)...))
		rl.AddHex(append(append(hexf("000202%0122x", 0), bytes16[:]...), hexf("%0128x", 0)...))
		rl.AddHex(append(append(hexf("0f0f0f%0122x", 0), bytes16[:]...), hexf("%0128x", 0)...))
		dbPrefixes, fixedbits, hooks := tr.FindSubTriesToLoad(rl)
		rl.Rewind()
		subTries, err := loader.LoadSubTries(db, 0, rl, nil /* HashCollector */, dbPrefixes, fixedbits, false)
		require.NoError(err)

		err = tr.HookSubTries(subTries, hooks) // hook up to the root
		assert.NoError(err)

		_, found := tr.Get(hexf("000%061x", 0))
		assert.False(found) // exists in DB but not resolved, there is hashNode

		storage, found := tr.Get(hexf("011%061x", 0))
		assert.True(found)
		require.Nil(storage) // deleted by empty value in cache bucket

		storage, found = tr.Get(hexf("021%0125x", 0))
		assert.True(found)
		require.Nil(storage) // this storage has inc=1

		storage, found = tr.Get(hexf("022%0125x", 0))
		assert.True(found)
		require.Equal(storage, []byte{2})

		storage, found = tr.Get(hexf("051%0125x", 0))
		assert.True(found)
		assert.Nil(storage) // not exists in DB

		//assert.Panics(func() {
		//	tr.Update(hexf("001%0125x", 0), nil)
		//})
		//assert.NotPanics(func() {
		//	tr.Update(hexf("011%0125x", 0), nil)
		//	tr.Update(hexf("021%0125x", 0), nil)
		//	tr.Update(hexf("051%0125x", 0), nil)
		//})
	}
}

func hexf(format string, a ...interface{}) []byte {
	return common.FromHex(fmt.Sprintf(format, a...))
}

func TestStorageSubTrieLoader2(t *testing.T) {
	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()

	kAcc1 := common.FromHex("0000cf1ce0664746d39af9f6db99dc3370282f1d9d48df7f804b7e6499558c83")
	a1 := accounts.Account{
		Nonce:       uint64(1),
		Initialised: true,
		CodeHash:    EmptyCodeHash,
		Balance:     *uint256.NewInt(),
		Incarnation: 1,
		Root:        EmptyRoot,
	}

	if err := writeAccount(db, common.BytesToHash(kAcc1), a1); err != nil {
		panic(err)
	}

	kAcc2 := common.FromHex("0001cf1ce0664746d39af9f6db99dc3370282f1d9d48df7f804b7e6499558c83")
	k2 := "290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563"
	ks2 := dbutils.GenerateCompositeStorageKey(common.BytesToHash(kAcc2), 1, common.HexToHash(k2))
	require.NoError(db.Put(dbutils.CurrentStateBucket, ks2, common.FromHex("7a381122bada791a7ab1f6037dac80432753baad")))

	expectedAccStorageRoot := "28d28aa6f1d0179248560a25a1a4ad69be1cdeab9e2b24bc9f9c70608e3a7ec0"
	expectedAccRoot2 := expectedAccStorageRoot
	a2 := accounts.Account{
		Nonce:       uint64(1),
		Initialised: true,
		CodeHash:    EmptyCodeHash,
		Balance:     *uint256.NewInt(),
		Incarnation: 1,
		Root:        common.HexToHash(expectedAccRoot2),
	}

	if err := writeAccount(db, common.BytesToHash(kAcc2), a2); err != nil {
		panic(err)
	}

	kAcc3 := common.FromHex("0002cf1ce0664746d39af9f6db99dc3370282f1d9d48df7f804b7e6499558c83")
	k3 := k2
	ks3 := dbutils.GenerateCompositeStorageKey(common.BytesToHash(kAcc3), 2, common.HexToHash(k3))
	ks3OldIncarnation := dbutils.GenerateCompositeStorageKey(common.BytesToHash(kAcc3), 1, common.HexToHash(k3))
	require.NoError(db.Put(dbutils.CurrentStateBucket, ks3, common.FromHex("7a381122bada791a7ab1f6037dac80432753baad")))
	require.NoError(db.Put(dbutils.CurrentStateBucket, ks3OldIncarnation, common.FromHex("9999999999999999")))

	expectedAccRoot3 := expectedAccStorageRoot
	a3 := accounts.Account{
		Nonce:       uint64(1),
		Initialised: true,
		CodeHash:    EmptyCodeHash,
		Balance:     *uint256.NewInt(),
		Incarnation: 2,
		Root:        common.HexToHash(expectedAccRoot3),
	}

	if err := writeAccount(db, common.BytesToHash(kAcc3), a3); err != nil {
		panic(err)
	}

	//expectedRoot := "3a9dc9c90290be8d88abea1c01d408e2a4173b4e295863942f0980e49bfbf375"

	// abandoned storage - account was deleted, but storage still exists
	kAcc4 := common.FromHex("0004cf1ce0664746d39af9f6db99dc3370282f1d9d48df7f804b7e6499558c83") // don't write it to db
	ks4 := dbutils.GenerateCompositeStorageKey(common.BytesToHash(kAcc4), 1, common.HexToHash(k2))
	require.NoError(db.Put(dbutils.CurrentStateBucket, ks4, common.FromHex("7a381122bada791a7ab1f6037dac80432753baad")))

	{
		resolver := NewSubTrieLoader(0)
		rs := NewRetainList(0)
		rs.AddHex(common.FromHex("00000001"))
		_, err := resolver.LoadSubTries(db, 0, rs, nil /* HashCollector */, [][]byte{nil}, []int{0}, false)
		assert.NoError(err)
	}
	{
		resolver := NewSubTrieLoader(0)
		rs := NewRetainList(0)
		rs.AddKey(dbutils.GenerateStoragePrefix(kAcc2, a2.Incarnation))
		_, err := resolver.LoadSubTries(db, 0, rs, nil /* HashCollector */, [][]byte{nil}, []int{0}, false)
		assert.NoError(err)
	}
}

func TestCreateLoadingPrefixes(t *testing.T) {
	assert := assert.New(t)

	tr := New(common.Hash{})
	kAcc1 := common.FromHex("0001cf1ce0664746d39af9f6db99dc3370282f1d9d48df7f804b7e6499558c83")
	kInc := make([]byte, 8)
	binary.BigEndian.PutUint64(kInc, uint64(1))
	ks1 := common.FromHex("0000000000000000000000000000000000000000000000000000000000000001")
	acc1 := accounts.NewAccount()
	acc1.Balance.SetUint64(12345)
	acc1.Incarnation = 1
	acc1.Initialised = true
	tr.UpdateAccount(kAcc1, &acc1)
	tr.Update(concat(kAcc1, ks1...), []byte{1, 2, 3})

	kAcc2 := common.FromHex("0002cf1ce0664746d39af9f6db99dc3370282f1d9d48df7f804b7e6499558c83")
	ks2 := common.FromHex("0000000000000000000000000000000000000000000000000000000000000001")
	ks22 := common.FromHex("0000000000000000000000000000000000000000000000000000000000000002")
	acc2 := accounts.NewAccount()
	acc2.Balance.SetUint64(6789)
	acc2.Incarnation = 1
	acc2.Initialised = true
	tr.UpdateAccount(kAcc2, &acc2)
	tr.Update(concat(kAcc2, ks2...), []byte{4, 5, 6})
	tr.Update(concat(kAcc2, ks22...), []byte{7, 8, 9})
	tr.Hash()

	// Evict accounts only
	tr.EvictNode(keybytesToHex(kAcc1))
	tr.EvictNode(keybytesToHex(kAcc2))
	rs := NewRetainList(0)
	rs.AddKey(concat(concat(kAcc1, kInc...), ks1...))
	rs.AddKey(concat(concat(kAcc2, kInc...), ks2...))
	rs.AddKey(concat(concat(kAcc2, kInc...), ks22...))
	dbPrefixes, fixedbits, hooks := tr.FindSubTriesToLoad(rs)
	assert.Equal("[0001cf1ce0664746d39af9f6db99dc3370282f1d9d48df7f804b7e6499558c830000000000000001 0002cf1ce0664746d39af9f6db99dc3370282f1d9d48df7f804b7e6499558c830000000000000001]", fmt.Sprintf("%x", dbPrefixes))
	assert.Equal("[320 320]", fmt.Sprintf("%d", fixedbits))
	assert.Equal("[000000010c0f010c0e000606040704060d03090a0f090f060d0b09090d0c030307000208020f010d090d04080d0f070f0800040b070e060409090505080c0803 000000020c0f010c0e000606040704060d03090a0f090f060d0b09090d0c030307000208020f010d090d04080d0f070f0800040b070e060409090505080c0803]", fmt.Sprintf("%x", hooks))

	// Evict everytning
	tr.EvictNode([]byte{})
	// if resolve only accounts
	rs = NewRetainList(0)
	rs.AddKey(kAcc1)
	rs.AddKey(kAcc2)
	dbPrefixes, fixedbits, hooks = tr.FindSubTriesToLoad(rs)
	assert.Equal("[]", fmt.Sprintf("%x", dbPrefixes))
	assert.Equal("[0]", fmt.Sprintf("%d", fixedbits))
	assert.Equal("[]", fmt.Sprintf("%x", hooks))
}

func TestIsBefore(t *testing.T) {
	assert := assert.New(t)

	is := keyIsBefore([]byte("a"), []byte("b"))
	assert.Equal(true, is)

	is = keyIsBefore([]byte("b"), []byte("a"))
	assert.Equal(false, is)

	is = keyIsBefore([]byte("b"), []byte(""))
	assert.Equal(false, is)

	is = keyIsBefore(nil, []byte("b"))
	assert.Equal(false, is)

	is = keyIsBefore([]byte("b"), nil)
	assert.Equal(true, is)

	contract := fmt.Sprintf("2%063x", 0)
	storageKey := common.Hex2Bytes(contract + "ffffffff" + fmt.Sprintf("10%062x", 0))
	cacheKey := common.Hex2Bytes(contract + "ffffffff" + "20")
	is = keyIsBefore(cacheKey, storageKey)
	assert.False(is)

	storageKey = common.Hex2Bytes(contract + "ffffffffffffffff" + fmt.Sprintf("20%062x", 0))
	cacheKey = common.Hex2Bytes(contract + "ffffffffffffffff" + "10")
	is = keyIsBefore(cacheKey, storageKey)
	assert.True(is)
}

func TestIsSequence(t *testing.T) {
	assert := assert.New(t)

	type tc struct {
		prev, next string
		expect     bool
	}

	cases := []tc{
		{prev: "1234", next: "1235", expect: true},
		{prev: "12ff", next: "13", expect: true},
		{prev: "12ff", next: "13000000", expect: true},
		{prev: "1234", next: "5678", expect: false},
	}
	for _, tc := range cases {
		next, _ := dbutils.NextSubtree(common.FromHex(tc.prev))
		res := isSequence(next, common.FromHex(tc.next))
		assert.Equal(tc.expect, res, "%s, %s", tc.prev, tc.next)
	}

}

func writeAccount(db ethdb.Putter, addrHash common.Hash, acc accounts.Account) error {
	value := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(value)
	if err := db.Put(dbutils.CurrentStateBucket, addrHash[:], value); err != nil {
		return err
	}
	return nil
}
