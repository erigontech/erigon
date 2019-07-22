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
	tr := New(common.Hash{})

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
		t1 := New(common.BytesToHash(root1))
		_ = t1.Rebuild(context.Background(), db, 0)
	}
}

// Put 1 embedded entry into the database and try to resolve it
func TestResolve1(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	if err := db.Put([]byte("ST"), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("6556dfaac213851c044228962a8dc179125d81e496805ef0f4b891e9109135e2").Bytes()),
	}
	r := NewResolver(context.Background(), 0, false, 0)
	r.AddRequest(req)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve1 resolved:\n%s\n", req.resolved.fstring(""))
}

func TestResolve2(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
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
		resolveHash: hashNode(common.HexToHash("002aa06ac2d513074996011586fe840be50432a8ea3f8b7938c20394ec9ee3cf").Bytes()),
	}
	r := NewResolver(context.Background(), 0, false, 0)
	r.AddRequest(req)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve2 resolved:\n%s\n", req.resolved.fstring(""))
}

func TestResolve2Keep(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
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
		resolveHash: hashNode(common.HexToHash("002aa06ac2d513074996011586fe840be50432a8ea3f8b7938c20394ec9ee3cf").Bytes()),
	}
	r := NewResolver(context.Background(), 0, false, 0)
	r.AddRequest(req)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve2Keep resolved:\n%s\n", tc.resolved.fstring(""))
}

func TestResolve3Keep(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
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
		resolveHash: hashNode(common.HexToHash("99f344aa0aea9a539dfac2f6404fd4506e0b274eac7aee7b6ed61eba73f98282").Bytes()),
	}
	r := NewResolver(context.Background(), 0, false, 0)
	r.AddRequest(req)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve3Keep resolved:\n%s\n", tc.resolved.fstring(""))
}

func TestTrieResolver(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
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
		resolveHash: hashNode(common.HexToHash("002aa06ac2d513074996011586fe840be50432a8ea3f8b7938c20394ec9ee3cf").Bytes()),
	}
	req2 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  2, // 2 bytes is 4 nibbles
		resolveHash: hashNode(common.HexToHash("dc2332366fcf65ad75d09901e199e3dd52a5389ad85ff1d853803c5f40cbde56").Bytes()),
	}
	req3 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("bbbaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  2, // 3 bytes is 6 nibbles
		resolveHash: hashNode(common.HexToHash("dc2332366fcf65ad75d09901e199e3dd52a5389ad85ff1d853803c5f40cbde56").Bytes()),
	}
	resolver := NewResolver(context.Background(), 0, false, 0)
	resolver.AddRequest(req3)
	resolver.AddRequest(req2)
	resolver.AddRequest(req1)
	if err := resolver.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Resolve error: %v", err)
	}
	//t.Errorf("TestTrieResolver resolved:\n%s\n", req3.resolved.fstring(""))
}
