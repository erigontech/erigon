package trie

import (
	"encoding/binary"
	"fmt"
	"testing"

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

	require, db := require.New(t), ethdb.NewMemDatabase()
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.CurrentStateBucketOld2, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	rs := NewRetainList(0)
	rs.AddKey(common.Hex2Bytes("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa"))
	loader := NewFlatDBTrieLoader("checkRoots")
	if err := loader.Reset(rs, nil, nil, false); err != nil {
		panic(err)
	}
	_, err := loader.CalcTrieRoot(db, common.Hex2Bytes("aaaaabbbbb"), nil)
	if err != nil {
		panic(err)
	}
}

func TestResolve2(t *testing.T) {
	t.Skip("weird case of abandoned storage, will handle it later")

	require, db := require.New(t), ethdb.NewMemDatabase()
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.CurrentStateBucketOld2, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("aaaaaccccccccccccccccccccccccccc", "")

	rs := NewRetainList(0)
	rs.AddKey(common.Hex2Bytes("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa"))
	loader := NewFlatDBTrieLoader("checkRoots")
	if err := loader.Reset(rs, nil, nil, false); err != nil {
		panic(err)
	}
	_, err := loader.CalcTrieRoot(db, common.Hex2Bytes("aaaaaaaaaa"), nil)
	if err != nil {
		panic(err)
	}
}

func TestResolve2Keep(t *testing.T) {
	t.Skip("weird case of abandoned storage, will handle it later")

	require, db := require.New(t), ethdb.NewMemDatabase()
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.CurrentStateBucketOld2, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("aaaaaccccccccccccccccccccccccccc", "")

	rs := NewRetainList(0)
	rs.AddKey(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	loader := NewFlatDBTrieLoader("checkRoots")
	if err := loader.Reset(rs, nil, nil, false); err != nil {
		panic(err)
	}
	_, err := loader.CalcTrieRoot(db, common.Hex2Bytes("aaaaaaaaaa"), nil)
	if err != nil {
		panic(err)
	}
}

func TestResolve3Keep(t *testing.T) {
	t.Skip("weird case of abandoned storage, will handle it later")

	require, db := require.New(t), ethdb.NewMemDatabase()
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.CurrentStateBucketOld2, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("aaaaabbbbbbbbbbbbbbbbbbbbbbbbbbb", "")
	putStorage("aaaaaccccccccccccccccccccccccccc", "")

	rs := NewRetainList(0)
	rs.AddKey(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	loader := NewFlatDBTrieLoader("checkRoots")
	if err := loader.Reset(rs, nil, nil, false); err != nil {
		panic(err)
	}
	_, err := loader.CalcTrieRoot(db, common.Hex2Bytes("aaaaaaaaaa"), nil)
	if err != nil {
		panic(err)
	}
}

func TestTrieSubTrieLoader(t *testing.T) {
	t.Skip("weird case of abandoned storage, will handle it later")

	require, _, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()
	putStorage := func(k string, v string) {
		err := db.Put(dbutils.CurrentStateBucketOld2, common.Hex2Bytes(k), common.Hex2Bytes(v))
		require.NoError(err)
	}
	putStorage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("aaaaaccccccccccccccccccccccccccc", "")
	putStorage("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "")
	putStorage("bbaaaccccccccccccccccccccccccccc", "")
	putStorage("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "")
	putStorage("bccccccccccccccccccccccccccccccc", "")

	rs := NewRetainList(0)
	rs.AddKey(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	rs.AddKey(common.Hex2Bytes("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	rs.AddKey(common.Hex2Bytes("bbbaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	loader := NewFlatDBTrieLoader("checkRoots")
	if err := loader.Reset(rs, nil, nil, false); err != nil {
		panic(err)
	}
	for _, prefix := range [][]byte{common.Hex2Bytes("aaaaa"), common.Hex2Bytes("bb")} {
		_, err := loader.CalcSubTrieRootOnCache(db, prefix, nil, nil)
		require.NoError(err, "resolve error")
	}
}

func TestTwoStorageItems(t *testing.T) {
	t.Skip("weird case of abandoned storage, will handle it later")

	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()

	key1 := common.Hex2Bytes("d7b6990105719101dabeb77144f2a3385c8033acd3af97e9423a695e81ad1eb5f5")
	key2 := common.Hex2Bytes("df6966c971051c3d54ec59162606531493a51404a002842f56009d7e5cf4a8c7f5")
	val1 := common.Hex2Bytes("02")
	val2 := common.Hex2Bytes("03")

	require.NoError(db.Put(dbutils.CurrentStateBucketOld2, key1, val1))
	require.NoError(db.Put(dbutils.CurrentStateBucketOld2, key2, val2))
	var branch fullNode
	branch.Children[0x7] = NewShortNode(keybytesToHex(key1[1:]), valueNode(val1))
	branch.Children[0xf] = NewShortNode(keybytesToHex(key2[1:]), valueNode(val2))
	root := NewShortNode([]byte{0xd}, &branch)

	hasher := newHasher(false)
	defer returnHasherToPool(hasher)
	rootRlp, err := hasher.hashChildren(root, 0)
	require.NoError(err, "failed ot hash hasState")

	// Resolve the root node

	rootHash := common.HexToHash("85737b049107f866fedbd6d787077fc2c245f4748e28896a3e8ee82c377ecdcf")
	assert.Equal(rootHash, crypto.Keccak256Hash(rootRlp))

	rs := NewRetainList(0)
	loader := NewFlatDBTrieLoader("checkRoots")
	if err := loader.Reset(rs, nil, nil, false); err != nil {
		panic(err)
	}
	got, err := loader.CalcTrieRoot(db, nil, nil)
	require.NoError(err, "resolve error")
	assert.Equal(rootHash.String(), got)

	rs2 := NewRetainList(0)
	rs2.AddHex([]byte{0xd})
	loader = NewFlatDBTrieLoader("checkRoots")
	if err := loader.Reset(rs2, nil, nil, false); err != nil {
		panic(err)
	}
	got, err = loader.CalcTrieRoot(db, []byte{0xd0}, nil)
	require.NoError(err, "resolve error")
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

	rs := NewRetainList(0)
	rs.AddKey(key1)
	loader := NewFlatDBTrieLoader("checkRoots")
	if err := loader.Reset(rs, nil, nil, false); err != nil {
		panic(err)
	}
	got, err := loader.CalcTrieRoot(db, nil, nil)
	require.NoError(err, "resolve error")
	assert.Equal(expect.String(), got)
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
	loader := NewFlatDBTrieLoader("checkRoots")
	if err := loader.Reset(rs, nil, nil, false); err != nil {
		panic(err)
	}
	_, err := loader.CalcTrieRoot(db, nil, nil)
	require.NotNil(t, err)
}

func TestCreateLoadingPrefixes(t *testing.T) {
	t.Skip("TG doesn't use this function")
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
		res := isSequenceOld(next, common.FromHex(tc.next))
		assert.Equal(tc.expect, res, "%s, %s", tc.prev, tc.next)
	}

}

func writeAccount(db ethdb.Putter, addrHash common.Hash, acc accounts.Account) error {
	value := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(value)
	if err := db.Put(dbutils.CurrentStateBucketOld2, addrHash[:], value); err != nil {
		return err
	}
	return nil
}
