// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/big"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

func init() {
	spew.Config.Indent = "    "
	spew.Config.DisableMethods = false
}

// Used for testing
func newEmpty() *Trie {
	trie := New(common.Hash{})
	return trie
}

func TestEmptyTrie(t *testing.T) {
	var trie Trie
	res := trie.Hash()
	exp := EmptyRoot
	if res != common.Hash(exp) {
		t.Errorf("expected %x got %x", exp, res)
	}
}

func TestNull(t *testing.T) {
	var trie Trie
	key := make([]byte, 32)
	value := []byte("test")
	trie.Update(key, value)
	v, _ := trie.Get(key)
	if !bytes.Equal(v, value) {
		t.Fatal("wrong value")
	}
}

func TestInsert(t *testing.T) {
	t.Skip("we don't support different key length")
	trie := newEmpty()

	updateString(trie, "doe", "reindeer")
	updateString(trie, "dog", "puppy")
	updateString(trie, "dogglesworth", "cat")

	fmt.Printf("\n\n%s\n\n", trie.root.fstring(""))
	exp := common.HexToHash("8aad789dff2f538bca5d8ea56e8abe10f4c7ba3a5dea95fea4cd6e7c3a1168d3")
	root := trie.Hash()
	if root != exp {
		t.Errorf("case 1: exp %x got %x", exp, root)
	}

	trie = newEmpty()
	updateString(trie, "A", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	exp = common.HexToHash("d23786fb4a010da3ce639d66d5e904a11dbc02746d1ce25029e53290cabf28ab")
	root = trie.Hash()
	if root != exp {
		t.Errorf("case 2: exp %x got %x", exp, root)
	}
}

func TestGet(t *testing.T) {
	t.Skip("different length of key is not supported")
	trie := newEmpty()

	updateString(trie, "doe", "reindeer")
	updateString(trie, "dog", "puppy")
	updateString(trie, "dogglesworth", "cat")

	for i := 0; i < 2; i++ {
		res := getString(trie, "dog")
		if !bytes.Equal(res, []byte("puppy")) {
			t.Errorf("expected puppy got %x", res)
		}

		unknown := getString(trie, "unknown")
		if unknown != nil {
			t.Errorf("expected nil got %x", unknown)
		}

		if i == 1 {
			return
		}
	}
}

func TestDelete(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth")

	trie := newEmpty()
	vals := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"ether", ""},
		{"dog", "puppy"},
		{"shaman", ""},
	}
	for _, val := range vals {
		if val.v != "" {
			updateString(trie, val.k, val.v)
		} else {
			deleteString(trie, val.k)
		}
	}

	hash := trie.Hash()
	exp := common.HexToHash("5991bb8c6514148a29db676a14ac506cd2cd5775ace63c30a4fe457715e9ac84")
	if hash != exp {
		t.Errorf("expected %x got %x", exp, hash)
	}
}

func TestEmptyValues(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth")
	trie := newEmpty()

	vals := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"ether", ""},
		{"dog", "puppy"},
		{"shaman", ""},
	}
	for _, val := range vals {
		updateString(trie, val.k, val.v)
	}

	hash := trie.Hash()
	exp := common.HexToHash("5991bb8c6514148a29db676a14ac506cd2cd5775ace63c30a4fe457715e9ac84")
	if hash != exp {
		t.Errorf("expected %x got %x", exp, hash)
	}
}

func TestReplication(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth")

	trie := newEmpty()
	vals := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"dog", "puppy"},
		{"somethingveryoddindeedthis is", "myothernodedata"},
	}
	for _, val := range vals {
		updateString(trie, val.k, val.v)
	}
	exp := trie.Hash()

	// create a new trie on top of the database and check that lookups work.
	trie2 := New(exp)
	for _, kv := range vals {
		if string(getString(trie2, kv.k)) != kv.v {
			t.Errorf("trie2 doesn't have %q => %q", kv.k, kv.v)
		}
	}
	hash := trie2.Hash()
	if hash != exp {
		t.Errorf("root failure. expected %x got %x", exp, hash)
	}

	// perform some insertions on the new trie.
	vals2 := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		// {"shaman", "horse"},
		// {"doge", "coin"},
		// {"ether", ""},
		// {"dog", "puppy"},
		// {"somethingveryoddindeedthis is", "myothernodedata"},
		// {"shaman", ""},
	}
	for _, val := range vals2 {
		updateString(trie2, val.k, val.v)
	}
	if hash := trie2.Hash(); hash != exp {
		t.Errorf("root failure. expected %x got %x", exp, hash)
	}
}

func TestLargeValue(t *testing.T) {
	trie := newEmpty()
	trie.Update([]byte("key1"), []byte{99, 99, 99, 99})
	trie.Update([]byte("key2"), bytes.Repeat([]byte{1}, 32))
	trie.Hash()
}

type countingDB struct {
	ethdb.Database
	gets map[string]int
}

func (db *countingDB) Get(bucket string, key []byte) ([]byte, error) {
	db.gets[string(key)]++
	return db.Database.Get(bucket, key)
}

// TestRandomCases tests som cases that were found via random fuzzing
func TestRandomCases(t *testing.T) {
	var rt []randTestStep = []randTestStep{
		{op: 6, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 0
		{op: 6, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 1
		{op: 0, key: common.Hex2Bytes("d51b182b95d677e5f1c82508c0228de96b73092d78ce78b2230cd948674f66fd1483bd"), value: common.Hex2Bytes("0000000000000002")},   // step 2
		{op: 2, key: common.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: common.Hex2Bytes("")},                 // step 3
		{op: 3, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 4
		{op: 3, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 5
		{op: 6, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 6
		{op: 3, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 7
		{op: 0, key: common.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: common.Hex2Bytes("0000000000000008")}, // step 8
		{op: 0, key: common.Hex2Bytes("d51b182b95d677e5f1c82508c0228de96b73092d78ce78b2230cd948674f66fd1483bd"), value: common.Hex2Bytes("0000000000000009")},   // step 9
		{op: 2, key: common.Hex2Bytes("fd"), value: common.Hex2Bytes("")},                                                                                       // step 10
		{op: 6, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 11
		{op: 6, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 12
		{op: 0, key: common.Hex2Bytes("fd"), value: common.Hex2Bytes("000000000000000d")},                                                                       // step 13
		{op: 6, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 14
		{op: 1, key: common.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: common.Hex2Bytes("")},                 // step 15
		{op: 3, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 16
		{op: 0, key: common.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: common.Hex2Bytes("0000000000000011")}, // step 17
		{op: 5, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 18
		{op: 3, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 19
		// FIXME: fix these testcases for turbo-geth
		//{op: 0, key: common.Hex2Bytes("d51b182b95d677e5f1c82508c0228de96b73092d78ce78b2230cd948674f66fd1483bd"), value: common.Hex2Bytes("0000000000000014")},           // step 20
		//{op: 0, key: common.Hex2Bytes("d51b182b95d677e5f1c82508c0228de96b73092d78ce78b2230cd948674f66fd1483bd"), value: common.Hex2Bytes("0000000000000015")},           // step 21
		//{op: 0, key: common.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: common.Hex2Bytes("0000000000000016")},         // step 22
		{op: 5, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")}, // step 23
		//{op: 1, key: common.Hex2Bytes("980c393656413a15c8da01978ed9f89feb80b502f58f2d640e3a2f5f7a99a7018f1b573befd92053ac6f78fca4a87268"), value: common.Hex2Bytes("")}, // step 24
		//{op: 1, key: common.Hex2Bytes("fd"), value: common.Hex2Bytes("")}, // step 25
	}
	runRandTest(rt)

}

// randTest performs random trie operations.
// Instances of this test are created by Generate.
type randTest []randTestStep

type randTestStep struct {
	op    int
	key   []byte // for opUpdate, opDelete, opGet
	value []byte // for opUpdate
	err   error  // for debugging
}

const (
	opUpdate = iota
	opDelete
	opGet
	opCommit
	opHash
	opReset
	opItercheckhash
	opCheckCacheInvariant
	opMax // boundary value, not an actual op
)

func (randTest) Generate(r *rand.Rand, size int) reflect.Value {
	var allKeys [][]byte
	genKey := func() []byte {
		if len(allKeys) < 2 || r.Intn(100) < 10 {
			// new key
			key := make([]byte, r.Intn(50))
			r.Read(key)
			allKeys = append(allKeys, key)
			return key
		}
		// use existing key
		return allKeys[r.Intn(len(allKeys))]
	}

	var steps randTest
	for i := 0; i < size; i++ {
		step := randTestStep{op: r.Intn(opMax)}
		switch step.op {
		case opUpdate:
			step.key = genKey()
			step.value = make([]byte, 8)
			binary.BigEndian.PutUint64(step.value, uint64(i))
		case opGet, opDelete:
			step.key = genKey()
		}
		steps = append(steps, step)
	}
	return reflect.ValueOf(steps)
}

func runRandTest(rt randTest) bool {
	tr := New(common.Hash{})
	values := make(map[string]string) // tracks content of the trie

	for i, step := range rt {
		fmt.Printf("{op: %d, key: common.Hex2Bytes(\"%x\"), value: common.Hex2Bytes(\"%x\")}, // step %d\n",
			step.op, step.key, step.value, i)
		switch step.op {
		case opUpdate:
			tr.Update(step.key, step.value)
			values[string(step.key)] = string(step.value)
		case opDelete:
			tr.Delete(step.key)
			delete(values, string(step.key))
		case opGet:
			v, _ := tr.Get(step.key)
			want := values[string(step.key)]
			if string(v) != want {
				rt[i].err = fmt.Errorf("mismatch for key 0x%x, got 0x%x want 0x%x", step.key, v, want)
			}
		case opCommit:
		case opHash:
			tr.Hash()
		case opReset:
			hash := tr.Hash()
			newtr := New(hash)
			tr = newtr
		case opItercheckhash:
			// FIXME: restore for turbo-geth
			/*
				checktr := New(common.Hash{})
				it := NewIterator(tr.NodeIterator(nil))
				for it.Next() {
					checktr.Update(it.Key, it.Value)
				}
				if tr.Hash() != checktr.Hash() {
					rt[i].err = fmt.Errorf("hash mismatch in opItercheckhash")
				}
			*/
		case opCheckCacheInvariant:
		}
		// Abort the test on error.
		if rt[i].err != nil {
			return false
		}
	}
	return true
}

func TestRandom(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth")

	if err := quick.Check(runRandTest, nil); err != nil {
		if cerr, ok := err.(*quick.CheckError); ok {
			t.Fatalf("random test iteration %d failed: %s", cerr.Count, spew.Sdump(cerr.In))
		}
		t.Fatal(err)
	}
}

func BenchmarkGet(b *testing.B)      { benchGet(b, false) }
func BenchmarkGetDB(b *testing.B)    { benchGet(b, true) }
func BenchmarkUpdateBE(b *testing.B) { benchUpdate(b, binary.BigEndian) }
func BenchmarkUpdateLE(b *testing.B) { benchUpdate(b, binary.LittleEndian) }

const benchElemCount = 20000

func benchGet(b *testing.B, commit bool) {
	trie := new(Trie)
	var tmpdir string
	var tmpdb ethdb.Database
	if commit {
		tmpdir, tmpdb = tempDB()
		trie = New(common.Hash{})
	}
	k := make([]byte, 32)
	for i := 0; i < benchElemCount; i++ {
		binary.LittleEndian.PutUint64(k, uint64(i))
		trie.Update(k, k)
	}
	binary.LittleEndian.PutUint64(k, benchElemCount/2)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		trie.Get(k)
	}
	b.StopTimer()

	if commit {
		tmpdb.Close()
		os.RemoveAll(tmpdir)
	}
}

func benchUpdate(b *testing.B, e binary.ByteOrder) *Trie {
	trie := newEmpty()
	k := make([]byte, 32)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		e.PutUint64(k, uint64(i))
		trie.Update(k, k)
	}
	return trie
}

// Benchmarks the trie hashing. Since the trie caches the result of any operation,
// we cannot use b.N as the number of hashing rouns, since all rounds apart from
// the first one will be NOOP. As such, we'll use b.N as the number of account to
// insert into the trie before measuring the hashing.
func BenchmarkHash(b *testing.B) {
	// Make the random benchmark deterministic
	random := rand.New(rand.NewSource(0))

	// Create a realistic account trie to hash
	addresses := make([][20]byte, b.N)
	for i := 0; i < len(addresses); i++ {
		for j := 0; j < len(addresses[i]); j++ {
			addresses[i][j] = byte(random.Intn(256))
		}
	}
	accounts := make([][]byte, len(addresses))
	for i := 0; i < len(accounts); i++ {
		var (
			nonce   = uint64(random.Int63())
			balance = new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
			root    = EmptyRoot
			code    = crypto.Keccak256(nil)
		)
		accounts[i], _ = rlp.EncodeToBytes([]interface{}{nonce, balance, root, code})
	}
	// Insert the accounts into the trie and hash it
	trie := newEmpty()
	for i := 0; i < len(addresses); i++ {
		trie.Update(crypto.Keccak256(addresses[i][:]), accounts[i])
	}
	b.ResetTimer()
	b.ReportAllocs()
	trie.Hash()
}

func tempDB() (string, ethdb.Database) {
	dir, err := ioutil.TempDir("", "trie-bench")
	if err != nil {
		panic(fmt.Sprintf("can't create temporary directory: %v", err))
	}
	return dir, ethdb.MustOpen(dir)
}

func getString(trie *Trie, k string) []byte {
	v, _ := trie.Get([]byte(k))
	return v
}

func updateString(trie *Trie, k, v string) {
	trie.Update([]byte(k), []byte(v))
}

func deleteString(trie *Trie, k string) {
	trie.Delete([]byte(k))
}

func TestDeepHash(t *testing.T) {
	acc := accounts.NewAccount()
	prefix := "prefix"
	var testdata = [][]struct {
		key   string
		value string
	}{
		{{"key1", "value1"}},
		{{"key1", "value1"}, {"key2", "value2"}},
		{{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}},
		{{"key1", "value1"}, {"key2", "value2"}, {"\xffek3", "value3"}},
	}
	for i, keyVals := range testdata {
		fmt.Println("Test", i)
		trie := New(common.Hash{})
		for _, keyVal := range keyVals {
			trie.Update([]byte(keyVal.key), []byte(keyVal.value))
		}
		trie.PrintTrie()
		hash1 := trie.Hash()

		prefixTrie := New(common.Hash{})
		prefixTrie.UpdateAccount([]byte(prefix), &acc)
		for _, keyVal := range keyVals {
			// Add a prefix to every key
			prefixTrie.Update([]byte(prefix+keyVal.key), []byte(keyVal.value))
		}

		got2, hash2 := prefixTrie.DeepHash([]byte(prefix))
		if !got2 {
			t.Errorf("Expected DeepHash returning true, got false, testcase %d", i)
		}
		if hash1 != hash2 {
			t.Errorf("DeepHash mistmatch: %x, expected %x, testcase %d", hash2, hash1, i)
		}
	}
}

func TestHashMapLeak(t *testing.T) {
	debug.OverrideGetNodeData(true)
	defer debug.OverrideGetNodeData(false)
	// freeze the randomness
	random := rand.New(rand.NewSource(794656320434))

	// now create a trie with some small and some big leaves
	trie := newEmpty()
	nTouches := 256 * 10

	var key [1]byte
	var val [8]byte
	for i := 0; i < nTouches; i++ {
		key[0] = byte(random.Intn(256))
		binary.BigEndian.PutUint64(val[:], random.Uint64())

		option := random.Intn(3)
		if option == 0 {
			// small leaf node
			trie.Update(key[:], val[:])
		} else if option == 1 {
			// big leaf node
			trie.Update(key[:], crypto.Keccak256(val[:]))
		} else {
			// test delete as well
			trie.Delete(key[:])
		}
	}

	// check the size of trie's hash map
	trie.Hash()

	nHashes := trie.HashMapSize()
	nExpected := 1 + 16 + 256/3

	assert.GreaterOrEqual(t, nHashes, nExpected*7/8)
	assert.LessOrEqual(t, nHashes, nExpected*9/8)
}

func genRandomByteArrayOfLen(length uint) []byte {
	array := make([]byte, length)
	for i := uint(0); i < length; i++ {
		array[i] = byte(rand.Intn(256))
	}
	return array
}

func getAddressForIndex(index int) [20]byte {
	var address [20]byte
	binary.BigEndian.PutUint32(address[:], uint32(index))
	return address
}

func TestCodeNodeValid(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	numberOfAccounts := 20

	addresses := make([][20]byte, numberOfAccounts)
	for i := 0; i < len(addresses); i++ {
		addresses[i] = getAddressForIndex(i)
	}
	codeValues := make([][]byte, len(addresses))
	for i := 0; i < len(addresses); i++ {
		codeValues[i] = genRandomByteArrayOfLen(128)
		codeHash := common.BytesToHash(crypto.Keccak256(codeValues[i]))
		balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
		acc := accounts.NewAccount()
		acc.Nonce = uint64(random.Int63())
		acc.Balance.SetFromBig(balance)
		acc.Root = EmptyRoot
		acc.CodeHash = codeHash

		trie.UpdateAccount(crypto.Keccak256(addresses[i][:]), &acc)
		err := trie.UpdateAccountCode(crypto.Keccak256(addresses[i][:]), codeValues[i])
		assert.Nil(t, err, "should successfully insert code")
	}

	for i := 0; i < len(addresses); i++ {
		value, gotValue := trie.GetAccountCode(crypto.Keccak256(addresses[i][:]))
		assert.True(t, gotValue, "should receive code value")
		assert.True(t, bytes.Equal(value, codeValues[i]), "should receive the right code")
	}
}

func TestCodeNodeUpdateNotExisting(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)
	codeValue := genRandomByteArrayOfLen(128)

	codeHash := common.BytesToHash(crypto.Keccak256(codeValue))
	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue)
	assert.Nil(t, err, "should successfully insert code")

	nonExistingAddress := getAddressForIndex(9999)
	codeValue2 := genRandomByteArrayOfLen(128)

	err = trie.UpdateAccountCode(crypto.Keccak256(nonExistingAddress[:]), codeValue2)
	assert.Error(t, err, "should return an error for non existing acc")
}

func TestCodeNodeGetNotExistingAccount(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)
	codeValue := genRandomByteArrayOfLen(128)

	codeHash := common.BytesToHash(crypto.Keccak256(codeValue))
	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue)
	assert.Nil(t, err, "should successfully insert code")

	nonExistingAddress := getAddressForIndex(9999)

	value, gotValue := trie.GetAccountCode(crypto.Keccak256(nonExistingAddress[:]))
	assert.True(t, gotValue, "should indicate that account doesn't exist at all (not just hashed)")
	assert.Nil(t, value, "the value should be nil")
}

func TestCodeNodeGetHashedAccount(t *testing.T) {
	trie := newEmpty()

	address := getAddressForIndex(0)

	fakeAccount := genRandomByteArrayOfLen(50)
	fakeAccountHash := common.BytesToHash(crypto.Keccak256(fakeAccount))

	hex := keybytesToHex(crypto.Keccak256(address[:]))

	_, trie.root = trie.insert(trie.root, hex, hashNode{hash: fakeAccountHash[:]})

	value, gotValue := trie.GetAccountCode(crypto.Keccak256(address[:]))
	assert.False(t, gotValue, "should indicate that account exists but hashed")
	assert.Nil(t, value, "the value should be nil")
}

func TestCodeNodeGetExistingAccountNoCodeNotEmpty(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)
	codeValue := genRandomByteArrayOfLen(128)

	codeHash := common.BytesToHash(crypto.Keccak256(codeValue))
	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)

	value, gotValue := trie.GetAccountCode(crypto.Keccak256(address[:]))
	assert.False(t, gotValue, "should indicate that account exists with code but the code isn't in cache")
	assert.Nil(t, value, "the value should be nil")
}

func TestCodeNodeGetExistingAccountEmptyCode(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)

	codeHash := EmptyCodeHash
	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)

	value, gotValue := trie.GetAccountCode(crypto.Keccak256(address[:]))
	assert.True(t, gotValue, "should indicate that account exists with empty code")
	assert.Nil(t, value, "the value should be nil")
}

func TestCodeNodeWrongHash(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)

	codeValue1 := genRandomByteArrayOfLen(128)
	codeHash1 := common.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash1

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)

	codeValue2 := genRandomByteArrayOfLen(128)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue2)
	assert.Error(t, err, "should NOT be able to insert code with wrong hash")
}

func TestCodeNodeUpdateAccountAndCodeValidHash(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)

	codeValue1 := genRandomByteArrayOfLen(128)
	codeHash1 := common.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash1

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue1)
	assert.Nil(t, err, "should successfully insert code")

	codeValue2 := genRandomByteArrayOfLen(128)
	codeHash2 := common.BytesToHash(crypto.Keccak256(codeValue2))

	acc.CodeHash = codeHash2

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err = trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue2)
	assert.Nil(t, err, "should successfully insert code")
}

func TestCodeNodeUpdateAccountAndCodeInvalidHash(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)

	codeValue1 := genRandomByteArrayOfLen(128)
	codeHash1 := common.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash1

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue1)
	assert.Nil(t, err, "should successfully insert code")

	codeValue2 := genRandomByteArrayOfLen(128)
	codeHash2 := common.BytesToHash(crypto.Keccak256(codeValue2))

	codeValue3 := genRandomByteArrayOfLen(128)

	acc.CodeHash = codeHash2

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err = trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue3)
	assert.Error(t, err, "should NOT be able to insert code with wrong hash")
}

func TestCodeNodeUpdateAccountChangeCodeHash(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)

	codeValue1 := genRandomByteArrayOfLen(128)
	codeHash1 := common.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash1

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue1)
	assert.Nil(t, err, "should successfully insert code")

	codeValue2 := genRandomByteArrayOfLen(128)
	codeHash2 := common.BytesToHash(crypto.Keccak256(codeValue2))

	acc.CodeHash = codeHash2

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	value, gotValue := trie.GetAccountCode(crypto.Keccak256(address[:]))
	assert.Nil(t, value, "the value should reset after the code change happen")
	assert.False(t, gotValue, "should indicate that the code isn't in the cache")
}

func TestCodeNodeUpdateAccountNoChangeCodeHash(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)

	codeValue1 := genRandomByteArrayOfLen(128)
	codeHash1 := common.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash1

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue1)
	assert.Nil(t, err, "should successfully insert code")

	acc.Nonce = uint64(random.Int63())
	balance = new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
	acc.Balance.SetFromBig(balance)

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	value, gotValue := trie.GetAccountCode(crypto.Keccak256(address[:]))
	assert.Equal(t, codeValue1, value, "the value should NOT reset after account's non codehash had changed")
	assert.True(t, gotValue, "should indicate that the code is still in the cache")
}

func TestNextSubtreeHex(t *testing.T) {
	assert := assert.New(t)

	type tc struct {
		prev, next string
		expect     bool
	}

	cases := []tc{
		{prev: "", next: "00", expect: true},
		{prev: "", next: "0000", expect: true},
		{prev: "", next: "01", expect: false},
		{prev: "00", next: "01", expect: true},
		{prev: "01020304", next: "01020305", expect: true},
		{prev: "01020f0f", next: "0103", expect: true},
		{prev: "01020f0f", next: "0103000000000000", expect: true},
		{prev: "01020304", next: "05060708", expect: false},
		{prev: "0f0f0d", next: "0f0f0e", expect: true},
		{prev: "0f", next: "", expect: true},
		{prev: "0f01", next: "", expect: false},
	}

	for _, tc := range cases {
		res := isDenseSequence(common.FromHex(tc.prev), common.FromHex(tc.next))
		assert.Equal(tc.expect, res, "%s, %s", tc.prev, tc.next)
	}
}

func TestIHCursorCanUseNextParent(t *testing.T) {
	db, assert := ethdb.NewMemDatabase(), require.New(t)
	defer db.Close()
	hash := fmt.Sprintf("%064d", 0)

	ih := IH(nil, nil, nil, nil)

	ih.k[1], ih.v[1], ih.branches[1] = common.FromHex("00"), common.FromHex(hash+hash), 0b0000000000000110
	ih.k[2], ih.v[2], ih.branches[2] = common.FromHex("0001"), common.FromHex(hash), 0b1000000000000000
	ih.lvl = 2
	ih.hashID[2], ih.maxHashID[2] = 1, 1
	ih.hashID[1], ih.maxHashID[1] = 0, 1
	assert.True(ih._nextSiblingOfParentInMem())
	assert.Equal(ih.k[ih.lvl], common.FromHex("00"))

	ih.k[1], ih.v[1], ih.branches[1] = common.FromHex("00"), common.FromHex(hash+hash), 0b0000000000000110
	ih.k[3], ih.v[3], ih.branches[3] = common.FromHex("000101"), common.FromHex(hash), 0b1000000000000000
	ih.lvl = 3
	ih.hashID[3], ih.maxHashID[3] = 1, 1
	ih.hashID[1], ih.maxHashID[1] = 0, 1
	assert.True(ih._nextSiblingOfParentInMem())
	assert.Equal(ih.k[ih.lvl], common.FromHex("00"))

}

func TestIHCursor(t *testing.T) {
	db, require := ethdb.NewMemDatabase(), require.New(t)
	defer db.Close()
	acc := fmt.Sprintf("010101%0122x", 0)
	storage := acc + fmt.Sprintf("%030x01", 0) + acc
	_ = storage
	hash := fmt.Sprintf("%064d", 0)

	_ = db.Put(dbutils.TrieOfAccountsBucket, common.FromHex("00"), common.FromHex(fmt.Sprintf("%04x", 0b0000000000000010)+fmt.Sprintf("%04x", 0b0000000000000010)+hash))
	_ = db.Put(dbutils.TrieOfAccountsBucket, common.FromHex("01"), common.FromHex(fmt.Sprintf("%04x", 0b0000000000000111)+fmt.Sprintf("%04x", 0b0000000000000111)+hash+hash+hash))
	_ = db.Put(dbutils.TrieOfAccountsBucket, common.FromHex("0101"), common.FromHex(fmt.Sprintf("%04x", 0b0000000000000111)+fmt.Sprintf("%04x", 0b0000000000000111)+hash+hash+hash))
	_ = db.Put(dbutils.TrieOfAccountsBucket, common.FromHex("02"), common.FromHex(fmt.Sprintf("%04x", 0b1000000000000000)+fmt.Sprintf("%04x", 0b1000000000000000)+hash))
	_ = db.Put(dbutils.TrieOfAccountsBucket, common.FromHex("0300"), common.FromHex(fmt.Sprintf("%04x", 0b0100000000000001)+fmt.Sprintf("%04x", 0b0000000000000001)+hash))
	_ = db.Put(dbutils.TrieOfAccountsBucket, common.FromHex("030000"), common.FromHex(fmt.Sprintf("%04x", 0b0000000000000001)+fmt.Sprintf("%04x", 0b0000000000000001)+hash))
	_ = db.Put(dbutils.TrieOfAccountsBucket, common.FromHex("03000e"), common.FromHex(fmt.Sprintf("%04x", 0b0100000000000000)+fmt.Sprintf("%04x", 0b1000000000000000)+hash))
	_ = db.Put(dbutils.TrieOfAccountsBucket, common.FromHex("050001"), common.FromHex(fmt.Sprintf("%04x", 0b0000000000000001)+fmt.Sprintf("%04x", 0b0000000000000001)+hash))
	_ = db.Put(dbutils.TrieOfAccountsBucket, common.FromHex("05000f"), common.FromHex(fmt.Sprintf("%04x", 0b0000000000000001)+fmt.Sprintf("%04x", 0b0000000000000001)+hash))
	_ = db.Put(dbutils.TrieOfAccountsBucket, common.FromHex("06"), common.FromHex(fmt.Sprintf("%04x", 0b0000000000000001)+fmt.Sprintf("%04x", 0b0000000000000001)+hash))
	//for _, k := range []string{"00", "0001", "01", "0100", "0101", "0102", "02"} {
	//	kk := common.FromHex(k)
	//	_ = db.Put(dbutils.TrieOfAccountsBucket, kk, kk)
	//}
	//for _, k := range []string{acc, acc + "00", acc + "01", acc + "02"} {
	//	kk := common.FromHex(k)
	//	_ = db.Put(dbutils.TrieOfStorageBucket, kk, kk)
	//}

	tx, err := db.KV().Begin(context.Background(), nil, ethdb.RW)
	require.NoError(err)
	defer tx.Rollback()

	cursor := tx.Cursor(dbutils.TrieOfAccountsBucket)
	rl := NewRetainList(0)
	//rl.AddHex(common.FromHex("0101"))
	rl.AddHex(common.FromHex(acc))
	rl.AddHex(common.FromHex(storage))
	rl.AddHex(common.FromHex(acc + "01"))
	rl.AddHex(common.FromHex("030000"))
	rl.AddHex(common.FromHex("03000e"))
	var filter = func(prefix []byte) bool { return !rl.Retain(prefix) }
	ih := IH(filter, func(keyHex []byte, branches, children uint16, hashes, rootHash []byte) error {
		return nil
	}, cursor, nil)
	k, _, _ := ih.AtPrefix([]byte{})
	require.Equal(common.FromHex("0001"), k)
	require.False(ih.skipState)
	require.Equal([]byte{}, ih.FirstNotCoveredPrefix())
	k, _, _ = ih.Next()
	require.Equal(common.FromHex("0100"), k)
	require.False(ih.skipState)
	require.Equal(common.FromHex("02"), ih.FirstNotCoveredPrefix())
	k, _, _ = ih.Next()
	require.Equal(common.FromHex("010100"), k)
	require.True(ih.skipState)
	k, _, _ = ih.Next()
	require.Equal(common.FromHex("010102"), k)
	require.False(ih.skipState)
	require.Equal(common.FromHex("1110"), ih.FirstNotCoveredPrefix())
	k, _, _ = ih.Next()
	require.Equal(common.FromHex("0102"), k)
	require.False(ih.skipState)
	require.Equal(common.FromHex("1130"), ih.FirstNotCoveredPrefix())
	k, _, _ = ih.Next()
	require.Equal(common.FromHex("020f"), k)
	require.False(ih.skipState)
	require.Equal(common.FromHex("13"), ih.FirstNotCoveredPrefix())
	k, _, _ = ih.Next()
	require.Equal(common.FromHex("03000000"), k)
	require.True(ih.skipState)
	k, _, _ = ih.Next()
	require.Equal(common.FromHex("03000e0e"), k)
	require.False(ih.skipState)
	require.Equal(common.FromHex("3001"), ih.FirstNotCoveredPrefix())
	k, _, _ = ih.Next()
	require.Equal(common.FromHex("05000100"), k)
	require.False(ih.skipState)
	k, _, _ = ih.Next()
	require.Equal(common.FromHex("05000f00"), k)
	require.False(ih.skipState)
	k, _, _ = ih.Next()
	require.Equal(common.FromHex("0600"), k)
	require.False(ih.skipState)
	k, _, _ = ih.Next()
	assert.Nil(t, k)

	//cursorS := tx.Cursor(dbutils.TrieOfStorageBucket)
	//ihStorage := IH(canUse, cursorS)
	//
	//k, _, _ = ihStorage.SeekToAccount(common.FromHex(acc))
	//require.Equal(common.FromHex(acc+"00"), k)
	//require.True(isDenseSequence(ihStorage.prev, k))
	//k, _, _ = ihStorage.Next()
	//require.Equal(common.FromHex(acc+"02"), k)
	//require.False(isDenseSequence(ihStorage.prev, k))
	//k, _, _ = ihStorage.Next()
	//assert.Nil(t, k)
	//require.False(isDenseSequence(ihStorage.prev, k))

}

func TestEmptyRoot(t *testing.T) {
	sc := shards.NewStateCache(32, 64*1024)

	sc.SetAccountHashesRead(common.FromHex("00"), 0b10, 0b111, []common.Hash{{}})
	sc.SetAccountHashesRead(common.FromHex("01"), 0b111, 0b111, []common.Hash{{}, {}, {}})
	sc.SetAccountHashesRead(common.FromHex("02"), 0b100, 0b111, []common.Hash{{}})

	rl := NewRetainList(0)
	rl.AddHex(common.FromHex("01"))
	rl.AddHex(common.FromHex("0101"))
	canUse := func(prefix []byte) bool { return !rl.Retain(prefix) }
	i := 0
	if err := sc.AccountHashesTree(canUse, []byte{}, func(ihK []byte, h common.Hash, skipState bool) error {
		i++
		switch i {
		case 1:
			assert.Equal(t, common.FromHex("0001"), ihK)
		case 2:
			assert.Equal(t, common.FromHex("0100"), ihK)
		case 3:
			assert.Equal(t, common.FromHex("0102"), ihK)
		case 4:
			assert.Equal(t, common.FromHex("0202"), ihK)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCollectRanges(t *testing.T) {
	sc := shards.NewStateCache(32, 64*1024)
	sc.SetAccountHashesRead(common.FromHex("00"), 0b1, 0b11, []common.Hash{{}})
	sc.SetAccountHashesRead(common.FromHex("02"), 0b10011, 0b10011, []common.Hash{{}, {}, {}})
	sc.SetAccountHashesRead(common.FromHex("0200"), 0b111, 0b1111, []common.Hash{{}, {}, {}})
	sc.SetAccountHashesRead(common.FromHex("0201"), 0b11, 0b11, []common.Hash{{}, {}})
	sc.SetAccountHashesRead(common.FromHex("03"), 0b1, 0b11, []common.Hash{{}})
	ranges, err := collectMissedAccIH(func(prefix []byte) bool {
		if bytes.Equal(prefix, common.FromHex("02")) {
			return false
		}
		if bytes.Equal(prefix, common.FromHex("0200")) {
			return false
		}
		if bytes.Equal(prefix, common.FromHex("020000")) {
			return false
		}
		return true
	}, []byte{}, sc, nil)
	require.NoError(t, err)
	for i := 0; i < len(ranges); i++ {
		fmt.Printf("ranges1: %x\n", ranges[i])
	}
}
