package trie

import (
	"bytes"
	//"fmt"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

func testRebuild(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	bucket := []byte("AT")
	tr := New(common.Hash{}, bucket, nil, false)

	keys := []string{
		"FIRSTFIRSTFIRSTFIRSTFIRSTFIRSTFI",
		"SECONDSECONDSECONDSECONDSECONDSE",
		"FISECONDSECONDSECONDSECONDSECOND",
		"FISECONDSECONDSECONDSECONDSECONB",
		"THIRDTHIRDTHIRDTHIRDTHIRDTHIRDTH",
	}
	values := []string{
		"FIRST",
		"SECOND",
		"THIRD",
		"FORTH",
		"FIRTH",
	}

	for i := 0; i < len(keys); i++ {
		key := []byte(keys[i])
		value := []byte(values[i])
		v1, err := rlp.EncodeToBytes(bytes.TrimLeft(value, "\x00"))
		if err != nil {
			t.Errorf("Could not encode value: %v", err)
		}
		tr.Update(key, v1, 0)
		tr.PrintTrie()
		root1 := tr.Root()
		//fmt.Printf("Root1: %x\n", tr.Root())
		v1, err = EncodeAsValue(v1)
		if err != nil {
			t.Errorf("Could not encode value: %v", err)
		}
		db.Put(bucket, key, v1)
		t1 := New(common.BytesToHash(root1), bucket, nil, false)
		t1.Rebuild(db, 0)
	}
}

// Put 1 embedded entry into the database and try to resolve it
func TestResolve1Embedded(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, testbucket, nil, false)
	db.PutS(testbucket, []byte("abcdefghijklmnopqrstuvwxyz012345"), []byte("a"), 0)
	tc := &TrieContinuation{
		t:           tr,
		value:       nil,
		resolveHex:  keybytesToHex([]byte("abcdefghijklmnopqrstuvwxyz012345")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: nil,
		resolved:    nil,
		n:           nil,
	}
	r := NewResolver(false, false, 0)
	r.AddContinuation(tc)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
}

// Put 1 embedded entry into the database and try to resolve it
func TestResolve1(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, testbucket, nil, false)
	db.Put(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	tc := &TrieContinuation{
		t:           tr,
		value:       nil,
		resolveHex:  keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("741326629cbf4ba5d5afebd56dd714ba4a531ddb6b07b829aa85dee4d97d34a4").Bytes()),
		resolved:    nil,
		n:           nil,
	}
	r := NewResolver(false, false, 0)
	r.AddContinuation(tc)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve1 resolved:\n%s\n", tc.resolved.fstring(""))
}

func TestResolve2(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, testbucket, nil, false)
	db.Put(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	tc := &TrieContinuation{
		t:           tr,
		value:       nil,
		resolveHex:  keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("c9f98a7d966d37c7231d11910c72f01a213057111b8171f5f137269bb73e45e4").Bytes()),
		resolved:    nil,
		n:           nil,
	}
	r := NewResolver(false, false, 0)
	r.AddContinuation(tc)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve2 resolved:\n%s\n", tc.resolved.fstring(""))
}

func TestResolve2Keep(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, testbucket, nil, false)
	db.Put(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	tc := &TrieContinuation{
		t:           tr,
		value:       nil,
		resolveHex:  keybytesToHex([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("c9f98a7d966d37c7231d11910c72f01a213057111b8171f5f137269bb73e45e4").Bytes()),
		resolved:    nil,
		n:           nil,
	}
	r := NewResolver(false, false, 0)
	r.AddContinuation(tc)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve2Keep resolved:\n%s\n", tc.resolved.fstring(""))
}

func TestResolve3Keep(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, testbucket, nil, false)
	db.Put(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("aaaaabbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	tc := &TrieContinuation{
		t:           tr,
		value:       nil,
		resolveHex:  keybytesToHex([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("03e27bd9cc47c0a03a8480035f765a4ba242c40ae4badfd1628af5a1ca5fd57a").Bytes()),
		resolved:    nil,
		n:           nil,
	}
	r := NewResolver(false, false, 0)
	r.AddContinuation(tc)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve3Keep resolved:\n%s\n", tc.resolved.fstring(""))
}

func TestTrieResolver(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, testbucket, nil, false)
	db.Put(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))

	db.Put(testbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("bbaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	tc1 := &TrieContinuation{
		t:           tr,
		value:       nil,
		resolveHex:  keybytesToHex([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("c9f98a7d966d37c7231d11910c72f01a213057111b8171f5f137269bb73e45e4").Bytes()),
		resolved:    nil,
		n:           nil,
	}
	tc2 := &TrieContinuation{
		t:           tr,
		value:       nil,
		resolveHex:  keybytesToHex([]byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  2, // 2 bytes is 4 nibbles
		resolveHash: hashNode(common.HexToHash("b183c6dd36a92675ab74e32008a41735f485d20df283be0f349a412c769fe6c9").Bytes()),
		resolved:    nil,
		n:           nil,
	}
	tc3 := &TrieContinuation{
		t:           tr,
		value:       nil,
		resolveHex:  keybytesToHex([]byte("bbbaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  2, // 3 bytes is 6 nibbles
		resolveHash: hashNode(common.HexToHash("b183c6dd36a92675ab74e32008a41735f485d20df283be0f349a412c769fe6c9").Bytes()),
		resolved:    nil,
		n:           nil,
	}
	resolver := NewResolver(false, false, 0)
	resolver.AddContinuation(tc3)
	resolver.AddContinuation(tc2)
	resolver.AddContinuation(tc1)
	if err := resolver.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Resolve error: %v", err)
	}
	//t.Errorf("TestTrieResolver resolved:\n%s\n", tc3.resolved.fstring(""))
}
