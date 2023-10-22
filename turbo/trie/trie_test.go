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
	"encoding/binary"
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/rlp"
)

func init() {
	spew.Config.Indent = "    "
	spew.Config.DisableMethods = false
}

// Used for testing
func newEmpty() *Trie {
	trie := New(libcommon.Hash{})
	return trie
}

func TestEmptyTrie(t *testing.T) {
	var trie Trie
	res := trie.Hash()
	exp := EmptyRoot
	if res != exp {
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

func TestLargeValue(t *testing.T) {
	trie := newEmpty()
	trie.Update([]byte("key1"), []byte{99, 99, 99, 99})
	trie.Update([]byte("key2"), bytes.Repeat([]byte{1}, 32))
	trie.Hash()
}

// TestRandomCases tests som cases that were found via random fuzzing
func TestRandomCases(t *testing.T) {
	var rt = []randTestStep{
		{op: 6, key: libcommon.Hex2Bytes(""), value: libcommon.Hex2Bytes("")},                                                                                         // step 0
		{op: 6, key: libcommon.Hex2Bytes(""), value: libcommon.Hex2Bytes("")},                                                                                         // step 1
		{op: 0, key: libcommon.Hex2Bytes("d51b182b95d677e5f1c82508c0228de96b73092d78ce78b2230cd948674f66fd1483bd"), value: libcommon.Hex2Bytes("0000000000000002")},   // step 2
		{op: 2, key: libcommon.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: libcommon.Hex2Bytes("")},                 // step 3
		{op: 3, key: libcommon.Hex2Bytes(""), value: libcommon.Hex2Bytes("")},                                                                                         // step 4
		{op: 3, key: libcommon.Hex2Bytes(""), value: libcommon.Hex2Bytes("")},                                                                                         // step 5
		{op: 6, key: libcommon.Hex2Bytes(""), value: libcommon.Hex2Bytes("")},                                                                                         // step 6
		{op: 3, key: libcommon.Hex2Bytes(""), value: libcommon.Hex2Bytes("")},                                                                                         // step 7
		{op: 0, key: libcommon.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: libcommon.Hex2Bytes("0000000000000008")}, // step 8
		{op: 0, key: libcommon.Hex2Bytes("d51b182b95d677e5f1c82508c0228de96b73092d78ce78b2230cd948674f66fd1483bd"), value: libcommon.Hex2Bytes("0000000000000009")},   // step 9
		{op: 2, key: libcommon.Hex2Bytes("fd"), value: libcommon.Hex2Bytes("")},                                                                                       // step 10
		{op: 6, key: libcommon.Hex2Bytes(""), value: libcommon.Hex2Bytes("")},                                                                                         // step 11
		{op: 6, key: libcommon.Hex2Bytes(""), value: libcommon.Hex2Bytes("")},                                                                                         // step 12
		{op: 0, key: libcommon.Hex2Bytes("fd"), value: libcommon.Hex2Bytes("000000000000000d")},                                                                       // step 13
		{op: 6, key: libcommon.Hex2Bytes(""), value: libcommon.Hex2Bytes("")},                                                                                         // step 14
		{op: 1, key: libcommon.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: libcommon.Hex2Bytes("")},                 // step 15
		{op: 3, key: libcommon.Hex2Bytes(""), value: libcommon.Hex2Bytes("")},                                                                                         // step 16
		{op: 0, key: libcommon.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: libcommon.Hex2Bytes("0000000000000011")}, // step 17
		{op: 5, key: libcommon.Hex2Bytes(""), value: libcommon.Hex2Bytes("")},                                                                                         // step 18
		{op: 3, key: libcommon.Hex2Bytes(""), value: libcommon.Hex2Bytes("")},                                                                                         // step 19
		// FIXME: fix these testcases for Erigon
		//{op: 0, key: common.Hex2Bytes("d51b182b95d677e5f1c82508c0228de96b73092d78ce78b2230cd948674f66fd1483bd"), value: common.Hex2Bytes("0000000000000014")},           // step 20
		//{op: 0, key: common.Hex2Bytes("d51b182b95d677e5f1c82508c0228de96b73092d78ce78b2230cd948674f66fd1483bd"), value: common.Hex2Bytes("0000000000000015")},           // step 21
		//{op: 0, key: common.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: common.Hex2Bytes("0000000000000016")},         // step 22
		{op: 5, key: libcommon.Hex2Bytes(""), value: libcommon.Hex2Bytes("")}, // step 23
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
	tr := New(libcommon.Hash{})
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
			// FIXME: restore for Erigon
			/*
				checktr := New(libcommon.Hash{})
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
			balance = new(big.Int).Rand(random, new(big.Int).Exp(libcommon.Big2, libcommon.Big256, nil))
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
		trie := New(libcommon.Hash{})
		for _, keyVal := range keyVals {
			trie.Update([]byte(keyVal.key), []byte(keyVal.value))
		}
		trie.PrintTrie()
		hash1 := trie.Hash()

		prefixTrie := New(libcommon.Hash{})
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
		codeHash := libcommon.BytesToHash(crypto.Keccak256(codeValues[i]))
		balance := new(big.Int).Rand(random, new(big.Int).Exp(libcommon.Big2, libcommon.Big256, nil))
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

	codeHash := libcommon.BytesToHash(crypto.Keccak256(codeValue))
	balance := new(big.Int).Rand(random, new(big.Int).Exp(libcommon.Big2, libcommon.Big256, nil))

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

	codeHash := libcommon.BytesToHash(crypto.Keccak256(codeValue))
	balance := new(big.Int).Rand(random, new(big.Int).Exp(libcommon.Big2, libcommon.Big256, nil))

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
	fakeAccountHash := libcommon.BytesToHash(crypto.Keccak256(fakeAccount))

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

	codeHash := libcommon.BytesToHash(crypto.Keccak256(codeValue))
	balance := new(big.Int).Rand(random, new(big.Int).Exp(libcommon.Big2, libcommon.Big256, nil))

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
	balance := new(big.Int).Rand(random, new(big.Int).Exp(libcommon.Big2, libcommon.Big256, nil))

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
	codeHash1 := libcommon.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(libcommon.Big2, libcommon.Big256, nil))

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
	codeHash1 := libcommon.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(libcommon.Big2, libcommon.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash1

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue1)
	assert.Nil(t, err, "should successfully insert code")

	codeValue2 := genRandomByteArrayOfLen(128)
	codeHash2 := libcommon.BytesToHash(crypto.Keccak256(codeValue2))

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
	codeHash1 := libcommon.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(libcommon.Big2, libcommon.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash1

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue1)
	assert.Nil(t, err, "should successfully insert code")

	codeValue2 := genRandomByteArrayOfLen(128)
	codeHash2 := libcommon.BytesToHash(crypto.Keccak256(codeValue2))

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
	codeHash1 := libcommon.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(libcommon.Big2, libcommon.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash1

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue1)
	assert.Nil(t, err, "should successfully insert code")

	codeValue2 := genRandomByteArrayOfLen(128)
	codeHash2 := libcommon.BytesToHash(crypto.Keccak256(codeValue2))

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
	codeHash1 := libcommon.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(libcommon.Big2, libcommon.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash1

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue1)
	assert.Nil(t, err, "should successfully insert code")

	acc.Nonce = uint64(random.Int63())
	balance = new(big.Int).Rand(random, new(big.Int).Exp(libcommon.Big2, libcommon.Big256, nil))
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

//func TestIHCursorCanUseNextParent(t *testing.T) {
//	db, assert := ethdb.NewMemDatabase(), require.New(t)
//	defer db.Close()
//	hash := fmt.Sprintf("%064d", 0)
//
//	ih := AccTrie(nil, nil, nil, nil)
//
//	ih.k[1], ih.v[1], ih.hasTree[1] = common.FromHex("00"), common.FromHex(hash+hash), 0b0000000000000110
//	ih.k[2], ih.v[2], ih.hasTree[2] = common.FromHex("0001"), common.FromHex(hash), 0b1000000000000000
//	ih.lvl = 2
//	ih.hashID[2] = 1
//	ih.hashID[1] = 0
//	assert.True(ih._nextSiblingOfParentInMem())
//	assert.Equal(ih.k[ih.lvl], common.FromHex("00"))
//
//	ih.k[1], ih.v[1], ih.hasTree[1] = common.FromHex("00"), common.FromHex(hash+hash), 0b0000000000000110
//	ih.k[3], ih.v[3], ih.hasTree[3] = common.FromHex("000101"), common.FromHex(hash), 0b1000000000000000
//	ih.lvl = 3
//	ih.hashID[3] = 1
//	ih.hashID[1] = 0
//	assert.True(ih._nextSiblingOfParentInMem())
//	assert.Equal(ih.k[ih.lvl], common.FromHex("00"))
//
//}
//
//func _TestEmptyRoot(t *testing.T) {
//	sc := shards.NewStateCache(32, 64*1024)
//
//	sc.SetAccountHashesRead(common.FromHex("00"), 0b111, 0b111, 0b111, []libcommon.Hash{{}, {}, {}})
//	sc.SetAccountHashesRead(common.FromHex("01"), 0b111, 0b111, 0b111, []libcommon.Hash{{}, {}, {}})
//	sc.SetAccountHashesRead(common.FromHex("02"), 0b111, 0b111, 0b111, []libcommon.Hash{{}, {}, {}})
//
//	rl := NewRetainList(0)
//	rl.AddHex(common.FromHex("01"))
//	rl.AddHex(common.FromHex("0101"))
//	canUse := func(prefix []byte) bool { return !rl.Retain(prefix) }
//	i := 0
//	if err := sc.AccountTree([]byte{}, func(ihK []byte, h libcommon.Hash, hasTree, skipState bool) (toChild bool, err error) {
//		i++
//		switch i {
//		case 1:
//			assert.Equal(t, common.FromHex("0001"), ihK)
//		case 2:
//			assert.Equal(t, common.FromHex("0100"), ihK)
//		case 3:
//			assert.Equal(t, common.FromHex("0102"), ihK)
//		case 4:
//			assert.Equal(t, common.FromHex("0202"), ihK)
//		}
//		if ok := canUse(ihK); ok {
//			return false, nil
//		}
//		return hasTree, nil
//	}, func(cur []byte) {
//		panic(fmt.Errorf("key %x not found in cache", cur))
//	}); err != nil {
//		t.Fatal(err)
//	}
//}
