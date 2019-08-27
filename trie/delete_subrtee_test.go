package trie

import (
	"bytes"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"testing"
)

func TestTrieDeleteSubtree_ShortNode(t *testing.T) {
	trie := newEmpty()
	key := []byte{uint8(1)}
	val := []byte("1")

	trie.Update(key, val, 0)
	v, ok := trie.Get(key, 0)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}
	//remove unknown
	trie.DeleteSubtree([]byte{uint8(2)}, 0)
	v, ok = trie.Get(key, 0)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}

	//remove by key
	trie.DeleteSubtree(key, 0)

	v, _ = trie.Get(key, 0)
	t.Log(v)
	if v != nil {
		t.Fatal("must be false")
	}
}

func TestTrieDeleteSubtree_ShortNode_Debug(t *testing.T) {
	trie := newEmpty()
	addr1:=common.HexToAddress("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f")
	addr2:=common.HexToAddress("0xfc597da4849c0d854629216d9e297bbca7bb4616")

	key := []byte{uint8(1)}
	val := []byte{uint8(1)}

	h := newHasher(false)
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(key[:])
	var keyHash common.Hash
	h.sha.Read(keyHash[:])

	h.sha.Reset()
	h.sha.Write(addr1[:])
	var buf1 common.Hash
	h.sha.Read(buf1[:])

	h.sha.Reset()
	h.sha.Write(addr2[:])
	var buf2 common.Hash
	h.sha.Read(buf2[:])



	key1:=GenerateCompositeTrieKey(buf1, keyHash)
	key2:=GenerateCompositeTrieKey(buf2, keyHash)
	trie.Update(key1, val, 0)
	trie.Update(key2, val, 0)

	trie.PrintTrie()
	trie.DeleteSubtree(buf1.Bytes(),0)
	trie.PrintTrie()

	v, ok := trie.Get(key2)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}
	v, _ = trie.Get(key1)
	if v!=nil {
		t.Fatal("must be nil")
	}
	////remove unknown
	//trie.DeleteSubtree([]byte{uint8(2)}, 0)
	//v, ok = trie.Get(key)
	//if ok == false || bytes.Equal(v, val) == false {
	//	t.Fatal("incorrect")
	//}
	//
	////remove by key
	//trie.DeleteSubtree(key, 0)
	//
	//v, _ = trie.Get(key)
	//t.Log(v)
	//if v != nil {
	//	t.Fatal("must be false")
	//}
}

func GenerateCompositeTrieKey(addressHash common.Hash, seckey common.Hash) []byte {
	compositeKey := make([]byte, 0, common.HashLength+common.HashLength)
	compositeKey = append(compositeKey, addressHash[:]...)
	compositeKey = append(compositeKey, seckey[:]...)
	return compositeKey
}

func TestTrieDeleteSubtree_ShortNode_LongPrefix(t *testing.T) {
	trie := newEmpty()
	key := []byte{uint8(1), uint8(1)}
	prefix := []byte{uint8(1)}
	val := []byte("1")

	trie.Update(key, val, 0)
	v, ok := trie.Get(key, 0)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}
	//remove unknown
	trie.DeleteSubtree([]byte{uint8(2)}, 0)
	v, ok = trie.Get(key, 0)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}

	//remove by key
	fmt.Println("+Print")
	trie.PrintTrie()
	fmt.Println("-Print")
	fmt.Println("+Delete")
	trie.DeleteSubtree(prefix, 0)
	fmt.Println("-Delete")
	fmt.Println("+Print")
	trie.PrintTrie()
	fmt.Println("-Print")

	v, _ = trie.Get(key, 0)
	t.Log(v)
	if v != nil {
		t.Fatal("must be false")
	}
}

func _TestTrieDeleteSubtree_ValueNode2(t *testing.T) {
	trie := newEmpty()
	key := common.Hex2Bytes("0x01")
	val := []byte("1")

	trie.Update(key, val, 0)
	v, ok := trie.Get(key, 0)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}

	trie.PrintTrie()
	//trie.DeleteSubtree([],0)
	v, ok = trie.Get(key, 0)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}

	v, ok = trie.Get(key, 0)
	if ok == true {
		t.Fatal("must be false")
	}
}
func TestTrieDeleteSubtree_DuoNode(t *testing.T) {
	trie := newEmpty()
	key := []byte{uint8(1)}
	val := []byte{uint8(1)}
	key_exist := []byte{uint8(2)}
	val_exist := []byte{uint8(2)}

	trie.Update(key, val, 0)
	trie.Update(key_exist, val_exist, 0)
	v, ok := trie.Get(key, 0)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}

	trie.DeleteSubtree(key, 0)

	v, ok = trie.Get(key, 0)
	fmt.Println(ok, v)
	if v != nil {
		t.Fatal("must be nil")
	}
	v, ok = trie.Get(key_exist, 0)
	fmt.Println(v, ok)
	if bytes.Equal(v, val_exist) == false {
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

	trie.Update(key1, val1, 0)
	trie.Update(key2, val2, 0)
	trie.Update(key3, val3, 0)

	fmt.Println("+Print")
	trie.PrintTrie()
	fmt.Println("-Print")

	fmt.Println("+Delete")
	//trie.DeleteSubtree(partialKey,0)
	trie.DeleteSubtree(key1, 0)
	fmt.Println("+Print")
	trie.PrintTrie()
	fmt.Println("-Print")
	trie.DeleteSubtree(key2, 0)
	fmt.Println("-Delete")

	fmt.Println("+Print")
	trie.PrintTrie()
	fmt.Println("-Print")

	v, _ := trie.Get(key1, 0)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key2, 0)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key3, 0)
	fmt.Println("")
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

	trie.Update(key1, val1, 0)
	trie.Update(key2, val2, 0)
	trie.Update(key3, val3, 0)

	fmt.Println("+Print")
	trie.PrintTrie()
	fmt.Println("-Print")

	fmt.Println("+Delete")
	trie.DeleteSubtree(partialKey, 0)
	fmt.Println("-Delete")

	fmt.Println("+Print")
	trie.PrintTrie()
	fmt.Println("-Print")
	return
	v, _ := trie.Get(key1, 0)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key2, 0)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key3, 0)
	fmt.Println("")
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

	trie.Update(key1, val1, 0)
	trie.Update(key2, val2, 0)
	trie.Update(key3, val3, 0)
	trie.Update(key4, val4, 0)

	fmt.Println("+Print")
	trie.PrintTrie()
	fmt.Println("-Print")

	fmt.Println("+Delete")
	trie.DeleteSubtree(partialKey, 0)
	fmt.Println("-Delete")

	fmt.Println("+Print")
	trie.PrintTrie()
	fmt.Println("-Print")

	v, _ := trie.Get(key1, 0)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key2, 0)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key3, 0)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key4, 0)
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

	trie.Update(key1, val1, 0)
	trie.Update(key2, val2, 0)
	trie.Update(key3, val3, 0)
	trie.Update(key4, val4, 0)

	fmt.Println("+Print")
	trie.PrintTrie()
	fmt.Println("-Print")

	fmt.Println("+Delete")
	trie.DeleteSubtree(partialKey, 0)
	fmt.Println("-Delete")

	fmt.Println("+Print")
	trie.PrintTrie()
	fmt.Println("-Print")

	v, _ := trie.Get(key1, 0)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key2, 0)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key3, 0)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key4, 0)
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

	trie.Update(key1, val1, 0)
	trie.Update(key2, val2, 0)
	trie.Update(key3, val3, 0)
	trie.Update(key4, val4, 0)

	fmt.Println("+Print")
	trie.PrintTrie()
	fmt.Println("-Print")

	fmt.Println("+Delete")
	trie.DeleteSubtree(key1, 0)
	fmt.Println("+Print")
	trie.PrintTrie()
	fmt.Println("-Print")

	trie.DeleteSubtree(key2, 0)
	fmt.Println("-Delete")

	fmt.Println("+Print")
	trie.PrintTrie()
	fmt.Println("-Print")

	v, _ := trie.Get(key1, 0)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key2, 0)
	if v != nil {
		t.Fatal("must be nil", v)
	}

	v, _ = trie.Get(key3, 0)
	fmt.Println("")
	if bytes.Equal(v, val3) != true {
		t.Fatal("must be equals", v)
	}
}

func TestTrieDeleteSubtree_ValueNode_PartialMatch(t *testing.T) {
	trie := newEmpty()
	key := []byte{uint8(1)}
	val := []byte{uint8(1)}
	key_exist := []byte{uint8(2)}
	val_exisg := []byte{uint8(2)}
	trie.Update(key, val, 0)
	trie.Update(key_exist, val_exisg, 0)
	v, ok := trie.Get(key, 0)
	if ok == false || bytes.Equal(v, val) == false {
		t.Fatal("incorrect")
	}
	trie.DeleteSubtree(key, 0)
	v, ok = trie.Get(key, 0)
	t.Log(v, ok, v == nil, len(v))
	if v != nil {
		t.Fatal("must be empty")
	}
	v, ok = trie.Get(key_exist, 0)
	t.Log("key_exist", v, ok)
	if ok == false || bytes.Equal(v, val_exisg) == false {
		t.Fatal("must be true")
	}
}
