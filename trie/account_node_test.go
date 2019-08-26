package trie

import (
	"crypto/ecdsa"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"golang.org/x/crypto/sha3"
	"math/big"
	"reflect"
	"testing"
)

func TestGetAccount(t *testing.T) {
	acc1 := &accounts.Account{
		Nonce:       1,
		Incarnation: 1,
		Balance:     *big.NewInt(100),
	}
	acc2 := &accounts.Account{
		Nonce:       2,
		Incarnation: 2,
		Balance:     *big.NewInt(200),
		Root:        common.BytesToHash([]byte("0x1")),
		CodeHash:    common.BytesToHash([]byte("0x01")),
	}
	trie := newEmpty()
	key1 := []byte("acc1")
	key2 := []byte("acc2")
	key3 := []byte("unknown_acc")
	trie.UpdateAccount(key1, acc1, 0)
	trie.UpdateAccount(key2, acc2, 0)

	accRes1, _ := trie.GetAccount(key1, 0)
	if reflect.DeepEqual(acc1, accRes1) == false {
		t.Fatal("not equal", key1)
	}
	accRes2, _ := trie.GetAccount(key2, 0)
	if reflect.DeepEqual(acc2, accRes2) == false {
		t.Fatal("not equal", key2)
	}

	accRes3, _ := trie.GetAccount(key3, 0)
	if accRes3 !=nil {
		t.Fatal("Should be false", key3, accRes3)
	}
}

func TestAddSomeValuesToAccountAndCheckDeepHashForThem(t *testing.T) {
	acc := &accounts.Account{
		Nonce:       2,
		Incarnation: 2,
		Balance:     *big.NewInt(200),
		Root:        common.BytesToHash([]byte("0x1")),
		CodeHash:    common.BytesToHash([]byte("0x01")),
	}

	_,_,addrHash,err:=generateAcc()
	if err!=nil {
		t.Fatal(err)
	}

	trie := newEmpty()
	keyAcc:=addrHash[:]
	trie.UpdateAccount(addrHash[:], acc, 0)

	accRes1, _ := trie.GetAccount(keyAcc, 0)
	if reflect.DeepEqual(acc, accRes1) == false {
		t.Fatal("not equal", keyAcc)
	}

	value1:=common.HexToHash("0x3").Bytes()
	value2:=common.HexToHash("0x5").Bytes()

	storageKey1:=common.HexToHash("0x1").Bytes()
	storageKey2:=common.HexToHash("0x5").Bytes()

	fullStorageKey1:=append(addrHash.Bytes(), storageKey1...)
	fullStorageKey2:=append(addrHash.Bytes(), storageKey2...)

	trie.Update(fullStorageKey1, value1,0)
	trie.Update(fullStorageKey2, value2,0)

	expectedTrie := newEmpty()
	expectedTrie.Update(storageKey1, value1,0)
	expectedTrie.Update(storageKey2, value2,0)

	ok,h1:=trie.DeepHash(addrHash.Bytes())
	t.Log(ok)
	t.Log(h1.String())
	t.Log(expectedTrie.Hash().String())

	//accRes3, _ := trie.GetAccount(key3, 0)
	//if accRes3 !=nil {
	//	t.Fatal("Should be false", key3, accRes3)
	//}
}

func generateAcc() (*ecdsa.PrivateKey, common.Address, common.Hash, error)  {
	key,err:=crypto.GenerateKey()
	if err!=nil {
		return nil,common.Address{},common.Hash{}, err
	}

	addr:=crypto.PubkeyToAddress(key.PublicKey)
	hash,err:=hashVal(addr[:])
	if err!=nil {
		return nil,common.Address{},common.Hash{}, err
	}
	return key,addr, hash, nil
}

func hashVal(v []byte) (common.Hash, error) {
	sha:=sha3.NewLegacyKeccak256().(keccakState)
	sha.Reset()
	_,err:=sha.Write(v)
	if err!=nil {
		return common.Hash{}, err
	}

	var hash common.Hash
	_,err=sha.Read(hash[:])
	if err!=nil {
		return common.Hash{}, err
	}
	return hash, nil
}