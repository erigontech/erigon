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
		_ = t1.Rebuild(context.Background(), db, 0)
	}
}

// Put 1 embedded entry into the database and try to resolve it
func TestResolve1(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, false)
	if err := db.Put([]byte("ST"), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("bfb355c9a7c26a9c173a9c30e1fb2895fd9908726a8d3dd097203b207d852cf5").Bytes()),
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
	if err := db.Put([]byte("ST"), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put([]byte("ST"), []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("84e743de8f2e0cae5bb148b1a90459cb6c8643bdfd154bbd6782746e4dda5657").Bytes()),
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
	if err := db.Put([]byte("ST"), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put([]byte("ST"), []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("84e743de8f2e0cae5bb148b1a90459cb6c8643bdfd154bbd6782746e4dda5657").Bytes()),
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
	if err := db.Put([]byte("ST"), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put([]byte("ST"), []byte("aaaaabbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put([]byte("ST"), []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("a9ac90d195f552b28a38097388523c463c36537decdc2c10b3eb0984db30fd0b").Bytes()),
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
	if err := db.Put([]byte("ST"), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put([]byte("ST"), []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}

	if err := db.Put([]byte("ST"), []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put([]byte("ST"), []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put([]byte("ST"), []byte("bbaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put([]byte("ST"), []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put([]byte("ST"), []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	req1 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("84e743de8f2e0cae5bb148b1a90459cb6c8643bdfd154bbd6782746e4dda5657").Bytes()),
	}
	req2 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  2, // 2 bytes is 4 nibbles
		resolveHash: hashNode(common.HexToHash("294cc6f7e68d82fbdfdabb59bca6e7970a84ad8cfd8e4a3182daedb0373b6004").Bytes()),
	}
	req3 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("bbbaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  2, // 3 bytes is 6 nibbles
		resolveHash: hashNode(common.HexToHash("294cc6f7e68d82fbdfdabb59bca6e7970a84ad8cfd8e4a3182daedb0373b6004").Bytes()),
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
