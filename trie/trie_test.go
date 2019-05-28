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
	"io/ioutil"
	"math/big"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/davecgh/go-spew/spew"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
)

func init() {
	spew.Config.Indent = "    "
	spew.Config.DisableMethods = false
}

// Used for testing
func newEmpty() (ethdb.Database, *Trie) {
	diskdb := ethdb.NewMemDatabase()
	trie := New(common.Hash{}, testbucket, false)
	return diskdb, trie
}

func TestEmptyTrie(t *testing.T) {
	var trie Trie
	res := trie.Hash()
	exp := emptyRoot
	if res != common.Hash(exp) {
		t.Errorf("expected %x got %x", exp, res)
	}
}

func TestNull(t *testing.T) {
	var trie Trie
	key := make([]byte, 32)
	value := []byte("test")
	trie.Update(nil, key, value, 0)
	if !bytes.Equal(trie.Get(nil, key, 0), value) {
		t.Fatal("wrong value")
	}
}

func testMissingNodeDisk(t *testing.T)    { testMissingNode(t, false) }
func testMissingNodeMemonly(t *testing.T) { testMissingNode(t, true) }

func testMissingNode(t *testing.T, memonly bool) {
	diskdb := ethdb.NewMemDatabase()

	trie := New(common.Hash{}, testbucket, false)
	updateString(trie, diskdb, "120000", "qwerqwerqwerqwerqwerqwerqwerqwer")
	updateString(trie, diskdb, "123456", "asdfasdfasdfasdfasdfasdfasdfasdf")
	root := trie.Hash()

	trie = New(root, testbucket, false)
	_, _, err := trie.TryGet(diskdb, []byte("120000"), 0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	trie = New(root, testbucket, false)
	_, _, err = trie.TryGet(diskdb, []byte("120099"), 0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	trie = New(root, testbucket, false)
	_, _, err = trie.TryGet(diskdb, []byte("123456"), 0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	trie = New(root, testbucket, false)
	err = trie.TryUpdate(diskdb, []byte("120099"), []byte("zxcvzxcvzxcvzxcvzxcvzxcvzxcvzxcv"), 0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	trie = New(root, testbucket, false)
	err = trie.TryDelete(diskdb, []byte("123456"), 0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	hash := common.HexToHash("0xe1d943cc8f061a0c0b98162830b970395ac9315654824bf21b73b891365262f9")
	if !memonly {
		diskdb.Delete(testbucket, hash[:])
	}

	trie = New(root, testbucket, false)
	_, _, err = trie.TryGet(diskdb, []byte("120000"), 0)
	if _, ok := err.(*MissingNodeError); !ok {
		t.Errorf("Wrong error: %v", err)
	}
	trie = New(root, testbucket, false)
	_, _, err = trie.TryGet(diskdb, []byte("120099"), 0)
	if _, ok := err.(*MissingNodeError); !ok {
		t.Errorf("Wrong error: %v", err)
	}
	trie = New(root, testbucket, false)
	_, _, err = trie.TryGet(diskdb, []byte("123456"), 0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	trie = New(root, testbucket, false)
	err = trie.TryUpdate(diskdb, []byte("120099"), []byte("zxcv"), 0)
	if _, ok := err.(*MissingNodeError); !ok {
		t.Errorf("Wrong error: %v", err)
	}
	trie = New(root, testbucket, false)
	err = trie.TryDelete(diskdb, []byte("123456"), 0)
	if _, ok := err.(*MissingNodeError); !ok {
		t.Errorf("Wrong error: %v", err)
	}
}

func TestInsert(t *testing.T) {
	diskdb, trie := newEmpty()

	updateString(trie, diskdb, "doe", "reindeer")
	updateString(trie, diskdb, "dog", "puppy")
	updateString(trie, diskdb, "dogglesworth", "cat")

	fmt.Printf("\n\n%s\n\n", trie.root.fstring(""))
	exp := common.HexToHash("8aad789dff2f538bca5d8ea56e8abe10f4c7ba3a5dea95fea4cd6e7c3a1168d3")
	root := trie.Hash()
	if root != exp {
		t.Errorf("exp %x got %x", exp, root)
	}

	diskdb, trie = newEmpty()
	updateString(trie, diskdb, "A", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	exp = common.HexToHash("d23786fb4a010da3ce639d66d5e904a11dbc02746d1ce25029e53290cabf28ab")
	root = trie.Hash()
	if root != exp {
		t.Errorf("exp %x got %x", exp, root)
	}
}

func TestGet(t *testing.T) {
	diskdb, trie := newEmpty()
	updateString(trie, diskdb, "doe", "reindeer")
	updateString(trie, diskdb, "dog", "puppy")
	updateString(trie, diskdb, "dogglesworth", "cat")

	for i := 0; i < 2; i++ {
		res := getString(trie, diskdb, "dog")
		if !bytes.Equal(res, []byte("puppy")) {
			t.Errorf("expected puppy got %x", res)
		}

		unknown := getString(trie, diskdb, "unknown")
		if unknown != nil {
			t.Errorf("expected nil got %x", unknown)
		}

		if i == 1 {
			return
		}
	}
}

func TestDelete(t *testing.T) {
	diskdb, trie := newEmpty()
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
			updateString(trie, diskdb, val.k, val.v)
		} else {
			deleteString(trie, diskdb, val.k)
		}
	}

	hash := trie.Hash()
	exp := common.HexToHash("5991bb8c6514148a29db676a14ac506cd2cd5775ace63c30a4fe457715e9ac84")
	if hash != exp {
		t.Errorf("expected %x got %x", exp, hash)
	}
}

func TestEmptyValues(t *testing.T) {
	diskdb, trie := newEmpty()

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
		updateString(trie, diskdb, val.k, val.v)
	}

	hash := trie.Hash()
	exp := common.HexToHash("5991bb8c6514148a29db676a14ac506cd2cd5775ace63c30a4fe457715e9ac84")
	if hash != exp {
		t.Errorf("expected %x got %x", exp, hash)
	}
}

func testReplication(t *testing.T) {
	diskdb, trie := newEmpty()
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
		updateString(trie, diskdb, val.k, val.v)
	}
	exp := trie.Hash()

	// create a new trie on top of the database and check that lookups work.
	trie2 := New(exp, testbucket, false)
	for _, kv := range vals {
		if string(getString(trie2, diskdb, kv.k)) != kv.v {
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
		updateString(trie2, diskdb, val.k, val.v)
	}
	if hash := trie2.Hash(); hash != exp {
		t.Errorf("root failure. expected %x got %x", exp, hash)
	}
}

func TestLargeValue(t *testing.T) {
	diskdb, trie := newEmpty()
	trie.Update(diskdb, []byte("key1"), []byte{99, 99, 99, 99}, 0)
	trie.Update(diskdb, []byte("key2"), bytes.Repeat([]byte{1}, 32), 0)
	trie.Hash()
}

type countingDB struct {
	ethdb.Database
	gets map[string]int
}

func (db *countingDB) Get(bucket []byte, key []byte) ([]byte, error) {
	db.gets[string(key)]++
	return db.Database.Get(bucket, key)
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
	diskdb := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, testbucket, false)
	values := make(map[string]string) // tracks content of the trie

	for i, step := range rt {
		switch step.op {
		case opUpdate:
			tr.Update(diskdb, step.key, step.value, 0)
			values[string(step.key)] = string(step.value)
		case opDelete:
			tr.Delete(diskdb, step.key, 0)
			delete(values, string(step.key))
		case opGet:
			v := tr.Get(diskdb, step.key, 0)
			want := values[string(step.key)]
			if string(v) != want {
				rt[i].err = fmt.Errorf("mismatch for key 0x%x, got 0x%x want 0x%x", step.key, v, want)
			}
		case opCommit:
		case opHash:
			tr.Hash()
		case opReset:
			hash := tr.Hash()
			newtr := New(hash, testbucket, false)
			tr = newtr
		case opItercheckhash:
			checktr := New(common.Hash{}, testbucket, false)
			it := NewIterator(tr.NodeIterator(diskdb, nil, 0))
			for it.Next() {
				checktr.Update(diskdb, it.Key, it.Value, 0)
			}
			if tr.Hash() != checktr.Hash() {
				rt[i].err = fmt.Errorf("hash mismatch in opItercheckhash")
			}
		case opCheckCacheInvariant:
		}
		// Abort the test on error.
		if rt[i].err != nil {
			return false
		}
	}
	return true
}

func testRandom(t *testing.T) {
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
	var tmpdb ethdb.Database
	if commit {
		_, tmpdb = tempDB()
		trie = New(common.Hash{}, testbucket, false)
	}
	k := make([]byte, 32)
	for i := 0; i < benchElemCount; i++ {
		binary.LittleEndian.PutUint64(k, uint64(i))
		trie.Update(tmpdb, k, k, 0)
	}
	binary.LittleEndian.PutUint64(k, benchElemCount/2)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		trie.Get(tmpdb, k, 0)
	}
	b.StopTimer()

	if commit {
		ldb := tmpdb.(*ethdb.LDBDatabase)
		ldb.Close()
		os.RemoveAll(ldb.Path())
	}
}

func benchUpdate(b *testing.B, e binary.ByteOrder) *Trie {
	diskdb, trie := newEmpty()
	k := make([]byte, 32)
	for i := 0; i < b.N; i++ {
		e.PutUint64(k, uint64(i))
		trie.Update(diskdb, k, k, 0)
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
			root    = emptyRoot
			code    = crypto.Keccak256(nil)
		)
		accounts[i], _ = rlp.EncodeToBytes([]interface{}{nonce, balance, root, code})
	}
	// Insert the accounts into the trie and hash it
	diskdb, trie := newEmpty()
	for i := 0; i < len(addresses); i++ {
		trie.Update(diskdb, crypto.Keccak256(addresses[i][:]), accounts[i], 0)
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
	diskdb, err := ethdb.NewLDBDatabase(dir, 256)
	if err != nil {
		panic(fmt.Sprintf("can't create temporary database: %v", err))
	}
	return dir, diskdb
}

func getString(trie *Trie, db ethdb.Database, k string) []byte {
	return trie.Get(db, []byte(k), 0)
}

func updateString(trie *Trie, db ethdb.Database, k, v string) {
	trie.Update(db, []byte(k), []byte(v), 0)
}

func deleteString(trie *Trie, db ethdb.Database, k string) {
	trie.Delete(db, []byte(k), 0)
}
