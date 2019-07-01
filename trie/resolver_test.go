package trie

import (
	"bytes"
	//"fmt"
	"testing"

	"context"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

var testbucket = []byte("test")

func testRebuild(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	bucket := []byte("AT")
	tr := New(common.Hash{}, false)

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
		t1 := New(common.BytesToHash(root1), false)
		t1.Rebuild(context.Background(), db, 0)
	}
}

// Put 1 embedded entry into the database and try to resolve it
func TestResolve1Embedded(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, false)
	db.PutS(testbucket, []byte("abcdefghijklmnopqrstuvwxyz012345"), []byte("a"), 0)
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("abcdefghijklmnopqrstuvwxyz012345")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: nil,
	}
	r := NewResolver(context.Background(), false, false, 0)
	r.AddRequest(req)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
}

// Put 1 embedded entry into the database and try to resolve it
func TestResolve1(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, false)
	db.Put(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("741326629cbf4ba5d5afebd56dd714ba4a531ddb6b07b829aa85dee4d97d34a4").Bytes()),
	}
	r := NewResolver(context.Background(), false, false, 0)
	r.AddRequest(req)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve1 resolved:\n%s\n", req.resolved.fstring(""))
}

func TestResolve2(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, false)
	db.Put(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("c9f98a7d966d37c7231d11910c72f01a213057111b8171f5f137269bb73e45e4").Bytes()),
	}
	r := NewResolver(context.Background(), false, false, 0)
	r.AddRequest(req)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve2 resolved:\n%s\n", req.resolved.fstring(""))
}

func TestResolve2Keep(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, false)
	db.Put(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("c9f98a7d966d37c7231d11910c72f01a213057111b8171f5f137269bb73e45e4").Bytes()),
	}
	r := NewResolver(context.Background(), false, false, 0)
	r.AddRequest(req)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve2Keep resolved:\n%s\n", tc.resolved.fstring(""))
}

func TestResolve3Keep(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, false)
	db.Put(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("aaaaabbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("03e27bd9cc47c0a03a8480035f765a4ba242c40ae4badfd1628af5a1ca5fd57a").Bytes()),
	}
	r := NewResolver(context.Background(), false, false, 0)
	r.AddRequest(req)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve3Keep resolved:\n%s\n", tc.resolved.fstring(""))
}

func TestTrieResolver(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, false)
	db.Put(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))

	db.Put(testbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("bbaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	db.Put(testbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	req1 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("c9f98a7d966d37c7231d11910c72f01a213057111b8171f5f137269bb73e45e4").Bytes()),
	}
	req2 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  2, // 2 bytes is 4 nibbles
		resolveHash: hashNode(common.HexToHash("b183c6dd36a92675ab74e32008a41735f485d20df283be0f349a412c769fe6c9").Bytes()),
	}
	req3 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("bbbaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  2, // 3 bytes is 6 nibbles
		resolveHash: hashNode(common.HexToHash("b183c6dd36a92675ab74e32008a41735f485d20df283be0f349a412c769fe6c9").Bytes()),
	}
	resolver := NewResolver(context.Background(), false, false, 0)
	resolver.AddRequest(req3)
	resolver.AddRequest(req2)
	resolver.AddRequest(req1)
	if err := resolver.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Resolve error: %v", err)
	}
	//t.Errorf("TestTrieResolver resolved:\n%s\n", req3.resolved.fstring(""))
}
