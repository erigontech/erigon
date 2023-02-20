package trie

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
)

func TestTrieDeleteSubtree_ShortNode(t *testing.T) {
	trie := newEmpty()
	key := []byte{uint8(1)}
	val := []byte("1")

	trie.Update(key, val)
	v, ok := trie.Get(key)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}
	//remove unknown
	trie.DeleteSubtree([]byte{uint8(2)})
	v, ok = trie.Get(key)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}

	//remove by key
	trie.DeleteSubtree(key)

	v, _ = trie.Get(key)
	if v != nil {
		t.Fatal("must be false")
	}
}

func TestTrieDeleteSubtree_ShortNode_Debug(t *testing.T) {
	trie := newEmpty()
	addr1 := libcommon.HexToAddress("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f")
	addr2 := libcommon.HexToAddress("0xfc597da4849c0d854629216d9e297bbca7bb4616")

	key := []byte{uint8(1)}
	val := []byte{uint8(1)}

	keyHash, err := common.HashData(key)
	if err != nil {
		t.Fatal(err)
	}

	addrHash1, err := common.HashData(addr1[:])
	if err != nil {
		t.Fatal(err)
	}

	addrHash2, err := common.HashData(addr2[:])
	if err != nil {
		t.Fatal(err)
	}

	key1 := GenerateCompositeTrieKey(addrHash1, keyHash)
	key2 := GenerateCompositeTrieKey(addrHash2, keyHash)
	trie.Update(key1, val)
	trie.Update(key2, val)

	trie.PrintTrie()
	trie.DeleteSubtree(addrHash1.Bytes())
	trie.PrintTrie()

	v, ok := trie.Get(key2)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}

	v, _ = trie.Get(key1)
	if v != nil {
		t.Fatal("must be nil")
	}
}

func GenerateCompositeTrieKey(addressHash libcommon.Hash, seckey libcommon.Hash) []byte {
	compositeKey := make([]byte, 0, length.Hash+length.Hash)
	compositeKey = append(compositeKey, addressHash[:]...)
	compositeKey = append(compositeKey, seckey[:]...)
	return compositeKey
}

func TestTrieDeleteSubtree_ShortNode_LongPrefix(t *testing.T) {
	trie := newEmpty()
	key := []byte{uint8(1), uint8(1)}
	prefix := []byte{uint8(1)}
	val := []byte("1")

	trie.Update(key, val)
	v, ok := trie.Get(key)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}
	//remove unknown
	trie.Delete([]byte{uint8(2)})
	v, ok = trie.Get(key)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}

	//remove by key
	trie.DeleteSubtree(prefix)
	v, _ = trie.Get(key)
	t.Log(v)
	if v != nil {
		t.Fatal("must be false")
	}
}

func TestTrieDeleteSubtree_DuoNode(t *testing.T) {
	trie := newEmpty()
	key := []byte{uint8(1)}
	val := []byte{uint8(1)}
	keyExist := []byte{uint8(2)}
	valExist := []byte{uint8(2)}

	trie.Update(key, val)
	trie.Update(keyExist, valExist)
	v, ok := trie.Get(key)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}

	trie.DeleteSubtree(key)

	v, _ = trie.Get(key)
	if v != nil {
		t.Fatal("must be nil")
	}
	v, _ = trie.Get(keyExist)
	if bytes.Equal(v, valExist) == false {
		t.Fatal("must be false")
	}
}

func TestTrieDeleteSubtree_TwoDuoNode_FullMatch(t *testing.T) {
	trie := newEmpty()
	key1 := []byte{uint8(1), uint8(1), uint8(1)}
	key2 := []byte{uint8(1), uint8(1), uint8(2)}
	key3 := []byte{uint8(1), uint8(2), uint8(3)}

	val1 := []byte{uint8(1)}
	val2 := []byte{uint8(2)}
	val3 := []byte{uint8(3)}

	trie.Update(key1, val1)
	trie.Update(key2, val2)
	trie.Update(key3, val3)

	trie.DeleteSubtree(key1)
	trie.DeleteSubtree(key2)

	v, _ := trie.Get(key1)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key2)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key3)
	if bytes.Equal(v, val3) != true {
		t.Fatal("must be equals", v)
	}
}

func TestTrieDeleteSubtree_DuoNode_PartialMatch(t *testing.T) {
	trie := newEmpty()

	key1 := []byte{uint8(1), uint8(1), uint8(1)}
	key2 := []byte{uint8(1), uint8(1), uint8(2)}
	key3 := []byte{uint8(1), uint8(2), uint8(3)}
	partialKey := []byte{uint8(1), uint8(1)}

	val1 := []byte{uint8(1)}
	val2 := []byte{uint8(2)}
	val3 := []byte{uint8(3)}

	trie.Update(key1, val1)
	trie.Update(key2, val2)
	trie.Update(key3, val3)

	trie.DeleteSubtree(partialKey)
	v, _ := trie.Get(key1)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key2)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key3)
	if bytes.Equal(v, val3) != true {
		t.Fatal("must be equals", v)
	}
}

func TestTrieDeleteSubtree_FromFullNode_PartialMatch(t *testing.T) {
	trie := newEmpty()
	key1 := []byte{uint8(1), uint8(1), uint8(1)}
	key2 := []byte{uint8(1), uint8(1), uint8(2)}
	key3 := []byte{uint8(1), uint8(1), uint8(3)}
	key4 := []byte{uint8(1), uint8(2), uint8(4)}
	partialKey := []byte{uint8(1), uint8(1)}

	val1 := []byte{uint8(1)}
	val2 := []byte{uint8(2)}
	val3 := []byte{uint8(3)}
	val4 := []byte{uint8(4)}

	trie.Update(key1, val1)
	trie.Update(key2, val2)
	trie.Update(key3, val3)
	trie.Update(key4, val4)

	trie.DeleteSubtree(partialKey)

	v, _ := trie.Get(key1)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key2)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key3)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key4)
	if bytes.Equal(v, val4) != true {
		t.Fatal("must be equals", v)
	}
}

func TestTrieDeleteSubtree_RemoveFullNode_PartialMatch(t *testing.T) {
	trie := newEmpty()
	key1 := []byte{uint8(1), uint8(1), uint8(1), uint8(1)}
	key2 := []byte{uint8(1), uint8(1), uint8(2), uint8(1)}
	key3 := []byte{uint8(1), uint8(1), uint8(3), uint8(1)}
	key4 := []byte{uint8(1), uint8(2), uint8(4)}
	partialKey := []byte{uint8(1), uint8(1)}

	val1 := []byte{uint8(1)}
	val2 := []byte{uint8(2)}
	val3 := []byte{uint8(3)}
	val4 := []byte{uint8(4)}

	trie.Update(key1, val1)
	trie.Update(key2, val2)
	trie.Update(key3, val3)
	trie.Update(key4, val4)

	trie.DeleteSubtree(partialKey)

	v, _ := trie.Get(key1)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key2)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key3)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key4)
	if bytes.Equal(v, val4) != true {
		t.Fatal("must be equals", v)
	}
}

func TestTrieDeleteSubtree_FullNode_FullMatch(t *testing.T) {
	trie := newEmpty()
	key1 := []byte{uint8(1), uint8(1), uint8(1)}
	key2 := []byte{uint8(1), uint8(1), uint8(2)}
	key3 := []byte{uint8(1), uint8(1), uint8(3)}
	key4 := []byte{uint8(1), uint8(2), uint8(4)}

	val1 := []byte{uint8(1)}
	val2 := []byte{uint8(2)}
	val3 := []byte{uint8(3)}
	val4 := []byte{uint8(4)}

	trie.Update(key1, val1)
	trie.Update(key2, val2)
	trie.Update(key3, val3)
	trie.Update(key4, val4)

	trie.DeleteSubtree(key1)
	trie.DeleteSubtree(key2)

	v, _ := trie.Get(key1)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key2)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key3)
	if bytes.Equal(v, val3) != true {
		t.Fatal("must be equals", v)
	}
}

func TestTrieDeleteSubtree_ValueNode_PartialMatch(t *testing.T) {
	trie := newEmpty()
	key := []byte{uint8(1)}
	val := []byte{uint8(1)}
	keyExist := []byte{uint8(2)}
	valExist := []byte{uint8(2)}
	trie.Update(key, val)
	trie.Update(keyExist, valExist)
	v, ok := trie.Get(key)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}
	trie.DeleteSubtree(key)
	v, _ = trie.Get(key)
	if v != nil {
		t.Fatal("must be empty")
	}
	v, ok = trie.Get(keyExist)
	if ok == false || bytes.Equal(v, valExist) == false {
		t.Fatal("must be true")
	}
}

func TestAccountNotRemovedAfterRemovingSubtrieAfterAccount(t *testing.T) {
	acc := &accounts.Account{
		Nonce:       2,
		Incarnation: 2,
		Balance:     *uint256.NewInt(200),
		Root:        EmptyRoot,
		CodeHash:    emptyState,
	}

	trie := newEmpty()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	addrHash, err := common.HashData(crypto.PubkeyToAddress(key.PublicKey).Bytes())
	if err != nil {
		t.Fatal(err)
	}
	trie.UpdateAccount(addrHash.Bytes(), acc)

	accRes1, _ := trie.GetAccount(addrHash.Bytes())
	if reflect.DeepEqual(acc, accRes1) == false {
		t.Fatal("not equal", addrHash)
	}

	val1 := []byte("1")
	dataKey1, err := common.HashData([]byte("1"))
	if err != nil {
		t.Fatal(err)
	}

	val2 := []byte("2")
	dataKey2, err := common.HashData([]byte("2"))
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("=============================")
	trie.PrintTrie()
	fmt.Println("=============================")
	ck1 := GenerateCompositeTrieKey(addrHash, dataKey1)
	ck2 := GenerateCompositeTrieKey(addrHash, dataKey2)
	trie.Update(ck1, val1)
	trie.Update(ck2, val2)

	trie.DeleteSubtree(addrHash.Bytes())

	accRes2, _ := trie.GetAccount(addrHash.Bytes())
	if reflect.DeepEqual(acc, accRes2) == false {
		t.Fatal("account was deleted", addrHash)
	}

}
