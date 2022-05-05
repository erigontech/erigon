package commitment

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func Test_Update(t *testing.T) {

	tests := []struct {
		key, value []byte
	}{
		{key: []byte{12}, value: []byte("notorious")},
		{key: []byte{14}, value: []byte("2pac")},
		{key: []byte{15}, value: []byte("eminem")},
		{key: []byte{11}, value: []byte("big pun")},
		{key: []byte{20}, value: []byte("method-man")},
		{key: []byte{18}, value: []byte("fat-joe")},
		{key: []byte{30}, value: []byte("jay-z")},
		{key: []byte{5}, value: []byte("redman")},
	}

	bt := NewBinaryPatriciaTrie()
	for _, test := range tests {
		bt.Update(test.key, test.value)
	}

	require.NotNil(t, bt.root.Node)

	stack := make([]*Node, 0)
	var stackPtr int

	stack = append(stack, bt.root.Node)
	stackPtr++
	visited := make(map[*Node]struct{})

	antipaths := make(map[*Node]string)
	antipaths[bt.root.Node] = bitstring(bt.root.CommonPrefix).String()

	for len(stack) > 0 {
		next := stack[stackPtr-1]
		_, seen := visited[next]
		if seen {
			stack = stack[:stackPtr-1]
			stackPtr--
			continue
		}
		visited[next] = struct{}{}

		if next.Value == nil {
			//require.Truef(t, next.L != nil || next.R != nil, "if node is not a leaf, at least one child should present")
			if next.P != nil {
				require.True(t, next.R != nil && next.L != nil, "bot child should exist L: %p, R: %p", next.L, next.R)
			}
		}
		if next.L != nil || next.R != nil {
			require.Truef(t, next.Value == nil, "if node has childs, node value should be nil, got %v", next.Value)
		}
		if next.L != nil {
			stack = append(stack, next.L)
			stackPtr++

			curp := antipaths[next]
			antipaths[next.L] = curp + bitstring(next.LPrefix).String()

			require.Truef(t, bytes.HasPrefix(next.LPrefix, []byte{0}), "left prefix always begins with 0, got %v", next.LPrefix)
		}
		if next.R != nil {
			stack = append(stack, next.R)
			stackPtr++

			curp := antipaths[next]
			antipaths[next.R] = curp + bitstring(next.RPrefix).String()

			require.Truef(t, bytes.HasPrefix(next.RPrefix, []byte{1}), "right prefix always begins with 1, got %v", next.RPrefix)
		}

		if next.Value != nil {
			// leaf, go back
			stack = stack[:stackPtr-1]
			stackPtr--
			continue
		}
	}

	for node, path := range antipaths {
		if node.Value == nil {
			continue
		}

		if newBitstring(node.Key).String() != path {
			t.Fatalf("node key %v- %v, path %v", node.Key, newBitstring(node.Key).String(), path)
		}
	}

	t.Logf("tree total nodes: %d", len(visited))
}

func Test_Get(t *testing.T) {
	bt := NewBinaryPatriciaTrie()

	tests := []struct {
		key, value []byte
	}{
		{key: []byte{12}, value: []byte("notorious")},
		{key: []byte{14}, value: []byte("2pac")},
		{key: []byte{15}, value: []byte("eminem")},
		{key: []byte{11}, value: []byte("big pun")},
		{key: []byte{20}, value: []byte("method-man")},
		{key: []byte{18}, value: []byte("fat-joe")},
		{key: []byte{30}, value: []byte("jay-z")},
		{key: []byte{5}, value: []byte("redman")},
	}

	for _, test := range tests {
		bt.Update(test.key, test.value)
	}

	require.NotNil(t, bt.root.Node)

	for _, test := range tests {
		buf, ok := bt.Get(test.key)
		require.Truef(t, ok, "key %v not found", test.key)
		require.EqualValues(t, test.value, buf)
	}

}

func Test_BinaryPatriciaTrie_ProcessUpdates(t *testing.T) {
	bt := NewBinaryPatriciaTrie()

	builder := NewUpdateBuilder().
		Balance("9a", 100000).
		Balance("e8", 200000).
		Balance("a2", 300000).
		Balance("f0", 400000).
		Balance("af", 500000).
		Balance("33", 600000).
		Nonce("aa", 184)

	plainKeys, hashedKeys, updates := builder.Build()
	bt.SetTrace(true)
	_, err := bt.ProcessUpdates(plainKeys, hashedKeys, updates)
	require.NoError(t, err)

	require.NotNil(t, bt.root.Node)

	stack := make([]*Node, 0)
	var stackPtr int

	stack = append(stack, bt.root.Node)
	stackPtr++
	visited := make(map[*Node]struct{})

	// validity check
	for len(stack) > 0 {
		next := stack[stackPtr-1]
		_, seen := visited[next]
		if seen {
			stack = stack[:stackPtr-1]
			stackPtr--
			continue
		}
		visited[next] = struct{}{}

		if next.Value == nil {
			require.Truef(t, next.L != nil || next.R != nil, "if node is not a leaf, at least one child should present")
			if next.P != nil {
				require.True(t, next.R != nil && next.L != nil, "bot child should exist L: %p, R: %p", next.L, next.R)
			}
		}
		if next.L != nil || next.R != nil {
			require.Truef(t, next.Value == nil, "if node has childs, node value should be nil, got %v", next.Value)
		}
		if next.L != nil {
			stack = append(stack, next.L)
			stackPtr++

			require.Truef(t, bytes.HasPrefix(next.LPrefix, []byte{0}), "left prefix always begins with 0, got %v", next.LPrefix)
		}
		if next.R != nil {
			stack = append(stack, next.R)
			stackPtr++

			require.Truef(t, bytes.HasPrefix(next.RPrefix, []byte{1}), "right prefix always begins with 1, got %v", next.RPrefix)
		}

		if next.Value != nil {
			// leaf, go back
			stack = stack[:stackPtr-1]
			stackPtr--
			continue
		}
	}
	rootHash, _ := bt.RootHash()
	require.Len(t, rootHash, 32)
	fmt.Printf("%+v\n", hex.EncodeToString(rootHash))
	t.Logf("tree total nodes: %d", len(visited))
}

func Test_BinaryPatriciaTrie_UniqueRepresentation(t *testing.T) {
	trieSequential := NewBinaryPatriciaTrie()

	builder := NewUpdateBuilder().
		Balance("9a", 100000).
		Balance("e8", 200000).
		Balance("a2", 300000).
		Balance("f0", 400000).
		Balance("af", 500000).
		Balance("33", 600000).
		Nonce("aa", 184)

	plainKeys, hashedKeys, updates := builder.Build()

	emptyHash, _ := trieSequential.RootHash()
	require.EqualValues(t, EmptyRootHash, emptyHash)

	for i := 0; i < len(plainKeys); i++ {
		trieSequential.ProcessUpdates(plainKeys[i:i+1], hashedKeys[i:i+1], updates[i:i+1])
		sequentialHash, _ := trieSequential.RootHash()
		require.Len(t, sequentialHash, 32)
	}

	trieBatch := NewBinaryPatriciaTrie()
	trieBatch.SetTrace(true)
	trieBatch.ProcessUpdates(plainKeys, hashedKeys, updates)

	sequentialHash, _ := trieSequential.RootHash()
	batchHash, _ := trieBatch.RootHash()

	require.EqualValues(t, batchHash, sequentialHash)
}

func Test_BinaryPatriciaTrie_BranchEncoding(t *testing.T) {
	builder := NewUpdateBuilder().
		Balance("9a", 100000).
		Balance("e8", 200000).
		Balance("a2", 300000).
		Balance("f0", 400000).
		Balance("af", 500000).
		Balance("33", 600000).
		Nonce("aa", 184)

	plainKeys, hashedKeys, updates := builder.Build()

	trie := NewBinaryPatriciaTrie()

	emptyHash, _ := trie.RootHash()
	require.EqualValues(t, EmptyRootHash, emptyHash)

	trie.SetTrace(true)

	branchUpdates, err := trie.ProcessUpdates(plainKeys, hashedKeys, updates)
	require.NoError(t, err)
	require.NotEmpty(t, branchUpdates)

	//sequentialHash, _ := trie.RootHash()
	//expectedRoot, _ := hex.DecodeString("87809bbb5282c01ac13cac744db5fee083882e93f781d6af2ad028455d5bdaac")
	//
	//require.EqualValues(t, expectedRoot, sequentialHash)

	for pref, update := range branchUpdates {
		account, _, _ := ExtractBinPlainKeys(update)
		t.Logf("pref %v: accounts:", pref)
		for _, acc := range account {
			t.Logf("\t%s\n", hex.EncodeToString(acc))
		}
	}
}

func Test_ReplaceBinPlainKeys(t *testing.T) {
	v, err := hex.DecodeString("0000000310ea0300000000000001010000000000000001000100000000010001010000000000010101000000000101000100000000000101000000000000010001000000000100010000000000010000000000000000010000000000000100010000000000000100010000000001000000000000000100000100000000000000010000000000000101000000000100010000000000010001000000000001000000000000000101010000000000010101000000000000000101000000000000000000000000000000000000000001000000000000000001010100000000000100000000000000010001000000000000000000000000010101010000000001010100000000000000000100000000010000010000000001010101000000000100010000000000000101010000000001010000000000000000010100000000010100010000000000000100000000000000010000000000000000000000000001000101000000000101010100000000000001000000000001000001000000000101010000000000010001010000000001010101000000000101010000000000010001000000000000000001000000000001010000000000000001010000000001000100000000000100010000000000010101000000000000010100000000000101010100000000000000000000000001010000ea03010000000000010001000000000000010101000000000000000100000000010001010000000000010101000000000000000000000000010000010000000001010101000000000101010000000000010000000000000001000101000000000000010100000000000101010000000000010100000000000000010100000000010100000000000001010100000000000100010000000000010101010000000001010101000000000001000000000000010000000000000000000001000000000000000100000000000100000000000000000101000000000101000100000000000100000000000000010100000000000001000000000000000001000000000000010101000000000001000100000000000001010000000001010101000000000100000000000000010100010000000001000101000000000101010100000000010001000000000000010100000000000101010100000000000101000000000000000100000000000000000100000000010100010000000000010100000000000000010000000000000001000000000001010100000000000000000000000000010000010000000001010000000000000100000000000000010001010000000000010000000000000100000000000000000100000000000001000100000000000001010000000000010001010214b910f4453d5fa062f828caf3b0e2adff3824407c24e68080a000000000000000000000000000000000000000000000000000000000000000000214d2798468b343c4b104ecc2585e1c1b57b7c1807424e68080a00000000000000000000000000000000000000000000000000000000000000000")
	require.NoError(t, err)

	accountPlainKeys := make([][]byte, 0)
	accountPlainKeys = append(accountPlainKeys, []byte("1a"))
	accountPlainKeys = append(accountPlainKeys, []byte("b1"))

	storageKeys := make([][]byte, 0)
	buf := make([]byte, 0)
	fin, err := ReplaceBinPlainKeys(v, accountPlainKeys, storageKeys, buf)
	require.NoError(t, err)
	require.NotEmpty(t, fin)
}

func Test_ReplaceBinPlainKeys2(t *testing.T) {
	//rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	key := make([]byte, 52)

	_, err := rand.Read(key)
	require.NoError(t, err)

	L := &Node{Key: key, Value: []byte("aa"), isStorage: true}
	buf := encodeNodeUpdate(L, 1, 1, nil)

	accountPlainKeys, storageKeys, err := ExtractBinPlainKeys(buf)
	require.NoError(t, err)
	require.Empty(t, accountPlainKeys)
	require.Contains(t, storageKeys, key)

	newStorageKeys := make([][]byte, 0)
	newStorageKeys = append(newStorageKeys, []byte("1aa0"))
	//fin, err := ReplacePlainKeys(v, accountPlainKeys, storageKeys, buf)

	fin := make([]byte, 0)
	fin, err = ReplaceBinPlainKeys(buf, accountPlainKeys, newStorageKeys, fin)
	require.NoError(t, err)
	require.NotEmpty(t, fin)
	require.NotEqualValues(t, fin, buf)

	accountPlainKeys, storageKeys, err = ExtractBinPlainKeys(fin)
	require.NoError(t, err)
	require.Empty(t, accountPlainKeys)
	require.Contains(t, storageKeys, newStorageKeys[0])
}

func Test_EncodeUpdate_Storage(t *testing.T) {
	L := &Node{Key: []byte("1aa0"), Value: []byte("aa"), isStorage: true}

	buf := encodeNodeUpdate(L, 1, 1, nil)

	accountPlainKeys, storagePlainKeys, err := ExtractBinPlainKeys(buf)
	require.NoError(t, err)
	require.Len(t, storagePlainKeys, 1)
	require.Len(t, accountPlainKeys, 0)
	require.Contains(t, storagePlainKeys, L.Key)

	newAccountPlainKeys := make([][]byte, 0)
	newAccountPlainKeys = append(newAccountPlainKeys, []byte("11a"))
	newStorageKeys := make([][]byte, 0)
	newStorageKeys = append(newStorageKeys, []byte("7770"))

	fin, err := ReplaceBinPlainKeys(buf, newAccountPlainKeys, newStorageKeys, []byte{})
	require.NoError(t, err)
	require.NotEmpty(t, fin)

	accountPlainKeys, storagePlainKeys, err = ExtractBinPlainKeys(fin)
	require.NoError(t, err)
	require.Len(t, storagePlainKeys, 1)
	require.Len(t, accountPlainKeys, 0)
	require.Contains(t, storagePlainKeys, newStorageKeys[0])

	// ====================== replace account plain key

	R := &Node{Key: []byte("1a"), Value: []byte("bb")}
	buf = encodeNodeUpdate(R, 2, 2, nil)

	accountPlainKeys, storagePlainKeys, err = ExtractBinPlainKeys(buf)
	require.NoError(t, err)
	require.Len(t, storagePlainKeys, 0)
	require.Len(t, accountPlainKeys, 1)
	require.Contains(t, accountPlainKeys, R.Key)

	fin, err = ReplaceBinPlainKeys(buf, newAccountPlainKeys, newStorageKeys, []byte{})
	require.NoError(t, err)
	require.NotEmpty(t, fin)

	accountPlainKeys, storagePlainKeys, err = ExtractBinPlainKeys(fin)
	require.NoError(t, err)
	require.Len(t, storagePlainKeys, 0)
	require.Len(t, accountPlainKeys, 1)
	require.Contains(t, accountPlainKeys, newAccountPlainKeys[0])
}

func Test_bitstring_encode_decode_padding(t *testing.T) {
	key, err := hex.DecodeString("db3164534fec08b5a86ae5dda0a997a63f2ee408")
	require.NoError(t, err)

	bs := newBitstring(key)
	re, padding := bs.reconstructHex()
	require.Zerof(t, padding, "padding should be zero")
	require.EqualValues(t, key, re)
}

func Test_bitstring_encode_decode_empty(t *testing.T) {
	re, pad := bitstring{}.reconstructHex()
	require.EqualValues(t, bitstring{}, re)
	require.EqualValues(t, 0, pad)
}

func Test_bitstring_encode_decode_one_padding(t *testing.T) {
	bs := bitstring{1}
	re, pad := bs.reconstructHex()
	require.EqualValues(t, 7, pad)
	require.EqualValues(t, []byte{1 << 7, byte(0xf0 | pad)}, re)

	bs2 := bitstringWithPadding(re, pad)
	require.EqualValues(t, bs, bs2)
}

func Test_bitstring_encode_decode_padding_notzero(t *testing.T) {
	t.Skip("Failing")
	key, err := hex.DecodeString("db3164534fec08b5a86ae5dda0a997a63f2ee408")
	require.NoError(t, err)

	bs := newBitstring(key)
	offt := 3 // last byte is 08 => 1000, chop last three zeros

	chop := bs[len(bs)-offt:]
	bs = bs[:len(bs)-offt]
	_ = chop
	re, padding := bs.reconstructHex() // during reconstruction padding will be applied - add 3 chopped zero
	require.EqualValues(t, offt, padding)
	require.EqualValues(t, key, re)
}

func Test_BinaryPatriciaTrie_ProcessUpdatesDelete(t *testing.T) {
	bt := NewBinaryPatriciaTrie()

	builder := NewUpdateBuilder().
		Balance("ffff", 200000).
		Balance("feff", 300000).
		Balance("ffdf", 400000).
		Balance("fedf", 400000)

	plainKeys, hashedKeys, updates := builder.Build()

	bt.SetTrace(true)
	_, err := bt.ProcessUpdates(plainKeys, hashedKeys, updates)
	require.NotNil(t, bt.root.Node)
	require.NoError(t, err)
	t.Logf("trie stat: %s", bt.StatString())

	builder = NewUpdateBuilder().
		Delete("fedf").
		Delete("ffff")

	plainKeys, hashedKeys, updates = builder.Build()
	require.NoError(t, err)

	_, err = bt.ProcessUpdates(plainKeys, hashedKeys, updates)
	require.NotNil(t, bt.root.Node)
	require.NoError(t, err)

	for i, key := range hashedKeys {
		v, ok := bt.Get(key) // keys "af" and "e8" should be deleted
		if len(v) > 0 {
			t.Logf("key %x: %v", hashedKeys[i], new(Account).decode(v).String())
		}
		require.Emptyf(t, v, "value for key %x should be not empty", plainKeys[i])
		require.False(t, ok)
	}

	stack := make([]*Node, 0)
	var stackPtr int

	stack = append(stack, bt.root.Node)
	stackPtr++
	visited := make(map[*Node]struct{})

	// validity check
	for len(stack) > 0 {
		next := stack[stackPtr-1]
		_, seen := visited[next]
		if seen {
			stack = stack[:stackPtr-1]
			stackPtr--
			continue
		}
		visited[next] = struct{}{}

		if next.Value == nil {
			require.Truef(t, next.L != nil || next.R != nil, "if node is not a leaf, at least one child should present")
			if next.P != nil {
				require.True(t, next.R != nil && next.L != nil, "bot child should exist L: %p, R: %p", next.L, next.R)
			}
		}
		if next.L != nil || next.R != nil {
			require.Truef(t, next.Value == nil, "if node has childs, node value should be nil, got %v", next.Value)
		}
		if next.L != nil {
			stack = append(stack, next.L)
			stackPtr++

			require.Truef(t, bytes.HasPrefix(next.LPrefix, []byte{0}), "left prefix always begins with 0, got %v", next.LPrefix)
		}
		if next.R != nil {
			stack = append(stack, next.R)
			stackPtr++

			require.Truef(t, bytes.HasPrefix(next.RPrefix, []byte{1}), "right prefix always begins with 1, got %v", next.RPrefix)
		}

		if next.Value != nil {
			// leaf, go back
			stack = stack[:stackPtr-1]
			stackPtr--
			continue
		}
	}
	rootHash, _ := bt.RootHash()
	require.Len(t, rootHash, 32)
	fmt.Printf("%+v\n", hex.EncodeToString(rootHash))
	t.Logf("tree total nodes: %d", len(visited))
}

func Test_BinaryPatriciaTrie_ProcessStorageUpdates(t *testing.T) {
	bt := NewBinaryPatriciaTrie()

	builder := NewUpdateBuilder().
		Storage("e8", "02", "98").
		Balance("e8", 1337)

	plainKeys, hashedKeys, updates := builder.Build()

	bt.SetTrace(true)
	_, err := bt.ProcessUpdates(plainKeys, hashedKeys, updates)
	require.NoError(t, err)
	require.NotNil(t, bt.root.Node)

	checkPlainKeys, checkHashedKeys, _ := NewUpdateBuilder().Delete("e8").Build()

	av, exist := bt.Get(checkHashedKeys[0])
	require.Truef(t, exist, "key %x should exist", checkPlainKeys[0])
	acc := new(Account).decode(av)
	require.Truef(t, acc.Balance.Eq(uint256.NewInt(1337)), "balance should be 1337, got %v", acc.Balance)

	accountPlainKey, accountHashedKey, upd := NewUpdateBuilder().DeleteStorage("e8", "02").Build()
	_, err = bt.ProcessUpdates(accountPlainKey, accountHashedKey, upd)
	require.NoError(t, err)

	for i, key := range accountHashedKey {
		v, ok := bt.Get(key)
		require.Emptyf(t, v, "value for key %x should be empty", accountPlainKey[i])
		require.False(t, ok)
	}

	stack := make([]*Node, 0)
	var stackPtr int

	stack = append(stack, bt.root.Node)
	stackPtr++
	visited := make(map[*Node]struct{})

	// validity check
	for len(stack) > 0 {
		next := stack[stackPtr-1]
		_, seen := visited[next]
		if seen {
			stack = stack[:stackPtr-1]
			stackPtr--
			continue
		}
		visited[next] = struct{}{}

		if next.Value == nil {
			require.Truef(t, next.L != nil || next.R != nil, "if node is not a leaf, at least one child should present")
			if next.P != nil {
				require.True(t, next.R != nil && next.L != nil, "bot child should exist L: %p, R: %p", next.L, next.R)
			}
		}
		if next.L != nil || next.R != nil {
			require.Truef(t, next.Value == nil, "if node has childs, node value should be nil, got %v", next.Value)
		}
		if next.L != nil {
			stack = append(stack, next.L)
			stackPtr++
			if len(next.LPrefix) == 0 {
				require.NotNilf(t, next.L.Value, "if left prefix is empty, left child MUST be leaf and there MUST be another child down on path, got branch")
			} else {
				require.Truef(t, bytes.HasPrefix(next.LPrefix, []byte{0}), "left prefix always begins with 0, got %v", next.LPrefix)
			}
		}
		if next.R != nil {
			stack = append(stack, next.R)
			stackPtr++
			if len(next.RPrefix) == 0 {
				require.NotNilf(t, next.R.Value, "if right prefix is nil, right child MUST be leaf, got branch")
			} else {
				require.Truef(t, bytes.HasPrefix(next.RPrefix, []byte{1}), "right prefix always begins with 1, got %v", next.RPrefix)
			}
		}

		if next.Value != nil {
			// leaf, go back
			stack = stack[:stackPtr-1]
			stackPtr--
			continue
		}
	}
	rootHash, _ := bt.RootHash()
	require.Len(t, rootHash, 32)
	fmt.Printf("%+v\n", hex.EncodeToString(rootHash))
	t.Logf("tree total nodes: %d", len(visited))
}

func Test_encodeNode(t *testing.T) {
	builder := NewUpdateBuilder().
		Balance("ff", 255).
		Balance("fd", 253).
		Balance("fe", 254)

	apk, _, upd := builder.Build()
	trie := NewBinaryPatriciaTrie()
	trie.trace = true
	for i := 0; i < len(upd); i++ {
		updates, err := trie.ProcessUpdates(apk[i:i+1], apk[i:i+1], upd[i:i+1])
		require.NoError(t, err)
		require.NotEmpty(t, updates)
		fmt.Printf("-----\n")
	}
}
