// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"crypto/ecdsa"
	"reflect"
	"testing"

	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestGetAccount(t *testing.T) {
	acc1 := &accounts.Account{
		Nonce:       1,
		Incarnation: 1,
		Balance:     *uint256.NewInt(100),
		Root:        EmptyRoot,
	}
	acc2 := &accounts.Account{
		Nonce:       2,
		Incarnation: 2,
		Balance:     *uint256.NewInt(200),
		Root:        common.BytesToHash([]byte("0x1")),
		CodeHash:    common.BytesToHash([]byte("0x01")),
	}
	trie := newEmpty()
	key1 := []byte("acc1")
	key2 := []byte("acc2")
	key3 := []byte("unknown_acc")
	trie.UpdateAccount(key1, acc1)
	trie.UpdateAccount(key2, acc2)

	accRes1, _ := trie.GetAccount(key1)
	if reflect.DeepEqual(acc1, accRes1) == false {
		t.Fatal("not equal", key1)
	}
	accRes2, _ := trie.GetAccount(key2)
	if reflect.DeepEqual(acc2, accRes2) == false {
		t.Fatal("not equal", key2)
	}

	accRes3, _ := trie.GetAccount(key3)
	if accRes3 != nil {
		t.Fatal("Should be false", key3, accRes3)
	}
}

func TestAddSomeValuesToAccountAndCheckDeepHashForThem(t *testing.T) {
	acc := &accounts.Account{
		Nonce:       2,
		Incarnation: 2,
		Balance:     *uint256.NewInt(200),
		Root:        EmptyRoot,
		CodeHash:    emptyState,
	}

	_, _, addrHash, err := generateAcc()
	if err != nil {
		t.Fatal(err)
	}

	trie := newEmpty()
	keyAcc := addrHash[:]
	trie.UpdateAccount(addrHash[:], acc)

	accRes1, _ := trie.GetAccount(keyAcc)
	if reflect.DeepEqual(acc, accRes1) == false {
		t.Fatal("not equal", keyAcc)
	}

	value1 := common.HexToHash("0x3").Bytes()
	value2 := common.HexToHash("0x5").Bytes()

	storageKey1 := common.HexToHash("0x1")
	storageKey2 := common.HexToHash("0x5")

	fullStorageKey1 := dbutils.GenerateCompositeTrieKey(addrHash, storageKey1)
	fullStorageKey2 := dbutils.GenerateCompositeTrieKey(addrHash, storageKey2)

	trie.Update(fullStorageKey1, value1)
	trie.Update(fullStorageKey2, value2)

	expectedTrie := newEmpty()
	expectedTrie.Update(storageKey1.Bytes(), value1)
	expectedTrie.Update(storageKey2.Bytes(), value2)

	_, h1 := trie.DeepHash(addrHash.Bytes())
	h2 := expectedTrie.Hash()
	if h1 != h2 {
		t.Fatal("not equals", h1.String(), h2.String())
	}
}

func TestHash(t *testing.T) {
	addr1 := common.HexToAddress("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b")
	acc1 := &accounts.Account{
		Nonce:    1,
		Balance:  *uint256.NewInt(209488),
		Root:     common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"),
		CodeHash: common.HexToHash("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"),
	}

	addr2 := common.HexToAddress("0xb94f5374fce5edbc8e2a8697c15331677e6ebf0b")
	acc2 := &accounts.Account{
		Nonce:    0,
		Balance:  *uint256.NewInt(0),
		Root:     common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"),
		CodeHash: common.HexToHash("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"),
	}

	addr3 := common.HexToAddress("0xc94f5374fce5edbc8e2a8697c15331677e6ebf0b")
	acc3 := &accounts.Account{
		Nonce:    0,
		Balance:  *uint256.NewInt(1010),
		Root:     common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"),
		CodeHash: common.HexToHash("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"),
	}

	trie := New(common.Hash{})
	trie2 := NewTestRLPTrie(common.Hash{})

	trie.UpdateAccount(addr1.Bytes(), acc1)
	trie.UpdateAccount(addr2.Bytes(), acc2)
	trie.UpdateAccount(addr3.Bytes(), acc3)

	b1 := make([]byte, acc1.EncodingLengthForHashing())
	b2 := make([]byte, acc2.EncodingLengthForHashing())
	b3 := make([]byte, acc3.EncodingLengthForHashing())
	acc1.EncodeForHashing(b1)
	acc2.EncodeForHashing(b2)
	acc3.EncodeForHashing(b3)
	trie2.Update(addr1.Bytes(), b1)
	trie2.Update(addr2.Bytes(), b2)
	trie2.Update(addr3.Bytes(), b3)

	if trie.Hash().String() != trie2.Hash().String() {
		t.FailNow()
	}
}

func generateAcc() (*ecdsa.PrivateKey, common.Address, common.Hash, error) {
	key, err := crypto.GenerateKey()
	if err != nil {
		return nil, common.Address{}, common.Hash{}, err
	}

	addr := crypto.PubkeyToAddress(key.PublicKey)
	hash, err := hashVal(addr[:])
	if err != nil {
		return nil, common.Address{}, common.Hash{}, err
	}
	return key, addr, hash, nil
}

func hashVal(v []byte) (common.Hash, error) {
	sha := sha3.NewLegacyKeccak256().(keccakState)
	sha.Reset()
	_, err := sha.Write(v)
	if err != nil {
		return common.Hash{}, err
	}

	var hash common.Hash
	_, err = sha.Read(hash[:])
	if err != nil {
		return common.Hash{}, err
	}
	return hash, nil
}
