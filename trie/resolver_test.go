package trie

import (
	"bytes"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/rlp"

	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestRebuild(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth")

	db := ethdb.NewMemDatabase()
	defer db.Close()
	bucket := dbutils.AccountsBucket
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
		_ = t1.Rebuild(db, 0)
	}
}

// Put 1 embedded entry into the database and try to resolve it
func TestResolve1(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	if err := db.Put(dbutils.StorageBucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("6556dfaac213851c044228962a8dc179125d81e496805ef0f4b891e9109135e2").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve1 resolved:\n%s\n", req.resolved.fstring(""))
}

func TestResolve2(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	if err := db.Put(dbutils.StorageBucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put(dbutils.StorageBucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("ca8155b4955b3723207ba30103f1759effbf87e5d8193fa215e5fe9818a00e2a").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve2 resolved:\n%s\n", req.resolved.fstring(""))
}

func TestResolve2Keep(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	if err := db.Put(dbutils.StorageBucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put(dbutils.StorageBucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("ca8155b4955b3723207ba30103f1759effbf87e5d8193fa215e5fe9818a00e2a").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve2Keep resolved:\n%s\n", tc.resolved.fstring(""))
}

func TestResolve3Keep(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	if err := db.Put(dbutils.StorageBucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put(dbutils.StorageBucket, []byte("aaaaabbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put(dbutils.StorageBucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("037d4f8cdf09ad062c866adefc24115c9e84e07399bd6ea058ed386b76dafde2").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	if err := r.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve3Keep resolved:\n%s\n", tc.resolved.fstring(""))
}

func TestTrieResolver(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	if err := db.Put(dbutils.StorageBucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put(dbutils.StorageBucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}

	if err := db.Put(dbutils.StorageBucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put(dbutils.StorageBucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put(dbutils.StorageBucket, []byte("bbaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put(dbutils.StorageBucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	if err := db.Put(dbutils.StorageBucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")); err != nil {
		t.Error(err)
	}
	req1 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("ca8155b4955b3723207ba30103f1759effbf87e5d8193fa215e5fe9818a00e2a").Bytes()),
	}
	req2 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  2, // 1 bytes is 2 nibbles
		resolveHash: hashNode(common.HexToHash("dc2332366fcf65ad75d09901e199e3dd52a5389ad85ff1d853803c5f40cbde56").Bytes()),
	}
	req3 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex([]byte("bbbaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  2, // 1 bytes is 2 nibbles
		resolveHash: hashNode(common.HexToHash("79d4d20420e467bc56adad82c454d68bc72ffbe7a26ad33028002bcbd1d41a05").Bytes()),
	}
	resolver := NewResolver(0, false, 0)
	resolver.AddRequest(req3)
	resolver.AddRequest(req2)
	resolver.AddRequest(req1)
	if err := resolver.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Resolve error: %v", err)
	}
	//t.Errorf("TestTrieResolver resolved:\n%s\n", req3.resolved.fstring(""))
}

func TestTwoStorageItems(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	if err := db.Put(dbutils.StorageBucket, common.Hex2Bytes("d7b6990105719101dabeb77144f2a3385c8033acd3af97e9423a695e81ad1eb5"), common.Hex2Bytes("02")); err != nil {
		t.Error(err)
	}
	if err := db.Put(dbutils.StorageBucket, common.Hex2Bytes("df6966c971051c3d54ec59162606531493a51404a002842f56009d7e5cf4a8c7"), common.Hex2Bytes("03")); err != nil {
		t.Error(err)
	}
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  []byte{},
		resolvePos:  0,
		resolveHash: hashNode(common.HexToHash("d06f3adc0b0624495478b857a37950d308d6840b349fe2c9eb6dcb813e0ccfb8").Bytes()),
	}
	resolver := NewResolver(0, false, 0)
	resolver.AddRequest(req)
	if err := resolver.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Resolve error: %v", err)
	}
}

func TestTwoAccounts(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	if err := db.Put(dbutils.AccountsBucket, common.Hex2Bytes("03601462093b5945d1676df093446790fd31b20e7b12a2e8e5e09d068109616b"), common.Hex2Bytes("c8808502540be40080")); err != nil {
		t.Error(err)
	}
	if err := db.Put(dbutils.AccountsBucket, common.Hex2Bytes("0fbc62ba90dec43ec1d6016f9dd39dc324e967f2a3459a78281d1f4b2ba962a6"), common.Hex2Bytes("f845806480a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a04f1593970e8f030c0a2c39758181a447774eae7c65653c4e6440e8c18dad69bc")); err != nil {
		t.Error(err)
	}
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  []byte{},
		resolvePos:  0,
		resolveHash: hashNode(common.HexToHash("925002c3260b44e44c3edebad1cc442142b03020209df1ab8bb86752edbd2cd7").Bytes()),
	}
	resolver := NewResolver(0, true, 0)
	resolver.AddRequest(req)
	if err := resolver.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Resolve error: %v", err)
	}
}
