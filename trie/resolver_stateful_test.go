package trie

import (
	"bytes"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	v := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	err := db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes(v))
	require.Nil(t, err)

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("bfb355c9a7c26a9c173a9c30e1fb2895fd9908726a8d3dd097203b207d852cf5").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	err = r.ResolveWithDb(db, 0)
	require.Nil(t, err)

	//t.Errorf("TestResolve1 resolved:\n%s\n", req.resolved.fstring(""))
	expectPrinted := "\n"
	printed := bytes.Buffer{}
	tr.Print(&printed)
	assert.Equal(t, expectPrinted, printed.String())
}

func TestResolve2(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	v := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	err := db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes(v))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaccccccccccccccccccccccccccc"), common.Hex2Bytes(v))
	require.Nil(t, err)

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("38eb1d28b717978c8cb21b6939dc69ba445d5dea67ca0e948bbf0aef9f1bc2fb").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	err = r.ResolveWithDb(db, 0)
	require.Nil(t, err)

	//t.Errorf("TestResolve2 resolved:\n%s\n", req.resolved.fstring(""))
	expectPrinted := "\n"
	printed := bytes.Buffer{}
	tr.Print(&printed)
	assert.Equal(t, expectPrinted, printed.String())
}

func TestResolve2Keep(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	v := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	err := db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes(v))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaccccccccccccccccccccccccccc"), common.Hex2Bytes(v))
	require.Nil(t, err)

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("38eb1d28b717978c8cb21b6939dc69ba445d5dea67ca0e948bbf0aef9f1bc2fb").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	err = r.ResolveWithDb(db, 0)
	require.Nil(t, err)

	// t.Errorf("TestResolve2Keep resolved:\n%s\n", tc.resolved.fstring(""))
	expectPrinted := "\n"
	printed := bytes.Buffer{}
	tr.Print(&printed)
	assert.Equal(t, expectPrinted, printed.String())
}

func TestResolve3Keep(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	v := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	err := db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes(v))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaabbbbbbbbbbbbbbbbbbbbbbbbbbb"), common.Hex2Bytes(v))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaccccccccccccccccccccccccccc"), common.Hex2Bytes(v))
	require.Nil(t, err)

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("b780e7d2bc3b7ab7f85084edb2fff42facefa0df9dd1e8190470f277d8183e7c").Bytes()),
	}
	r := NewResolver(0, false, 0)
	r.AddRequest(req)
	err = r.ResolveWithDb(db, 0)
	require.Nil(t, err, "resolve error")

	//t.Errorf("TestResolve3Keep resolved:\n%s\n", tc.resolved.fstring(""))

	expectPrinted := "\n"
	printed := bytes.Buffer{}
	tr.Print(&printed)
	assert.Equal(t, expectPrinted, printed.String())
}

func TestTrieResolver(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	v := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	err := db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes(v))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("aaaaaccccccccccccccccccccccccccc"), common.Hex2Bytes(v))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes(v))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes(v))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("bbaaaccccccccccccccccccccccccccc"), common.Hex2Bytes(v))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), common.Hex2Bytes(v))
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, common.Hex2Bytes("bccccccccccccccccccccccccccccccc"), common.Hex2Bytes(v))
	require.Nil(t, err)

	req1 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("38eb1d28b717978c8cb21b6939dc69ba445d5dea67ca0e948bbf0aef9f1bc2fb").Bytes()),
	}
	req2 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  2, // 1 bytes is 2 nibbles
		resolveHash: hashNode(common.HexToHash("dc2332366fcf65ad75d09901e199e3dd52a5389ad85ff1d853803c5f40cbde56").Bytes()),
	}
	req3 := &ResolveRequest{
		t:           tr,
		resolveHex:  keybytesToHex(common.Hex2Bytes("bbbaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos:  2, // 1 bytes is 2 nibbles
		resolveHash: hashNode(common.HexToHash("df6fd126d62ec79182d9ab6f879b63dfacb9ce2e1cb765b17b9752de9c2cbaa7").Bytes()),
	}
	resolver := NewResolver(0, false, 0)
	resolver.AddRequest(req3)
	resolver.AddRequest(req2)
	resolver.AddRequest(req1)

	err = resolver.ResolveWithDb(db, 0)
	require.Nil(t, err, "resolve error")

	expectPrinted := "\n"
	printed := bytes.Buffer{}
	tr.Print(&printed)
	assert.Equal(t, expectPrinted, printed.String())
}

func TestTwoStorageItems(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})

	key1 := common.Hex2Bytes("d7b6990105719101dabeb77144f2a3385c8033acd3af97e9423a695e81ad1eb5")
	key2 := common.Hex2Bytes("df6966c971051c3d54ec59162606531493a51404a002842f56009d7e5cf4a8c7")
	val1 := common.Hex2Bytes("02")
	val2 := common.Hex2Bytes("03")

	err := db.Put(dbutils.StorageBucket, key1, val1)
	require.Nil(t, err)
	err = db.Put(dbutils.StorageBucket, key2, val2)
	require.Nil(t, err)

	leaf1 := shortNode{Key: keybytesToHex(key1[1:]), Val: valueNode(val1)}
	leaf2 := shortNode{Key: keybytesToHex(key2[1:]), Val: valueNode(val2)}
	var branch fullNode
	branch.Children[0x7] = &leaf1
	branch.Children[0xf] = &leaf2
	branch.flags.dirty = true
	root := shortNode{Key: []byte{0xd}, Val: &branch}

	hasher := newHasher(false)
	defer returnHasherToPool(hasher)
	rootRlp, err := hasher.hashChildren(&root, 0)
	require.Nil(t, err, "failed ot hash children")

	// Resolve the root node

	rootHash := common.HexToHash("d06f3adc0b0624495478b857a37950d308d6840b349fe2c9eb6dcb813e0ccfb8")
	assert.Equal(t, rootHash, crypto.Keccak256Hash(rootRlp))

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  []byte{},
		resolvePos:  0,
		resolveHash: hashNode(rootHash.Bytes()),
	}
	resolver := NewResolver(0, false, 0)
	resolver.AddRequest(req)

	err = resolver.ResolveWithDb(db, 0)
	require.Nil(t, err, "resolve error")

	assert.Equal(t, rootHash.String(), tr.Hash().String())

	// Resolve the branch node

	branchRlp, err := hasher.hashChildren(&branch, 0)
	if err != nil {
		t.Errorf("failed ot hash children: %v", err)
	}

	req2 := &ResolveRequest{
		t:           tr,
		resolveHex:  []byte{0xd},
		resolvePos:  1,
		resolveHash: hashNode(crypto.Keccak256(branchRlp)),
	}
	resolver2 := NewResolver(0, false, 0)
	resolver2.AddRequest(req2)

	err = resolver2.ResolveWithDb(db, 0)
	require.Nil(t, err, "resolve error")

	assert.Equal(t, rootHash.String(), tr.Hash().String())

	expectPrinted := "s(0d:f(7:s(0b060909000100050701090100010d0a0b0e0b07070104040f020a030308050c080003030a0c0d030a0f09070e090402030a0609050e08010a0d010e0b0510:v(02))15:s(060906060c0907010005010c030d05040e0c05090106020600060503010409030a05010400040a0000020804020f05060000090d070e050c0f040a080c0710:v(03))))\n"
	printed := bytes.Buffer{}
	tr.Print(&printed)
	assert.Equal(t, expectPrinted, printed.String())
}

func TestTwoAccounts(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	err := db.Put(dbutils.AccountsBucket, common.Hex2Bytes("03601462093b5945d1676df093446790fd31b20e7b12a2e8e5e09d068109616b"), common.Hex2Bytes("020502540be400"))
	require.Nil(t, err)
	err = db.Put(dbutils.AccountsBucket, common.Hex2Bytes("0fbc62ba90dec43ec1d6016f9dd39dc324e967f2a3459a78281d1f4b2ba962a6"), common.Hex2Bytes("120164204f1593970e8f030c0a2c39758181a447774eae7c65653c4e6440e8c18dad69bc"))
	require.Nil(t, err)

	expect := common.HexToHash("925002c3260b44e44c3edebad1cc442142b03020209df1ab8bb86752edbd2cd7")
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  []byte{},
		resolvePos:  0,
		resolveHash: hashNode(expect.Bytes()),
	}
	resolver := NewResolver(0, true, 0)
	resolver.AddRequest(req)
	err = resolver.ResolveWithDb(db, 0)
	require.Nil(t, err, "resolve error")

	assert.Equal(t, expect.String(), tr.Hash().String())

	expectPrinted := "s(00:h(0dee9ab52e096dcc54037c30b3d183a0b06316cbcf49070a419b32ed2d2d64eb))\n"
	printed := bytes.Buffer{}
	tr.Print(&printed)
	assert.Equal(t, expectPrinted, printed.String())
}

func TestTwoAccountsNegative(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	err := db.Put(dbutils.AccountsBucket, common.Hex2Bytes("03601462093b5945d1676df093446790fd31b20e7b12a2e8e5e09d068109616b"), common.Hex2Bytes("020502540be400"))
	require.Nil(t, err)
	err = db.Put(dbutils.AccountsBucket, common.Hex2Bytes("0fbc62ba90dec43ec1d6016f9dd39dc324e967f2a3459a78281d1f4b2ba962a6"), common.Hex2Bytes("120164204f1593970e8f030c0a2c39758181a447774eae7c65653c4e6440e8c18dad69bc"))
	require.Nil(t, err)

	req := &ResolveRequest{
		t:           tr,
		resolveHex:  []byte{},
		resolvePos:  0,
		resolveHash: hashNode(common.HexToHash("wrong hash").Bytes()),
	}
	resolver := NewResolver(0, true, 0)
	resolver.AddRequest(req)
	err = resolver.ResolveWithDb(db, 0)
	require.NotNil(t, err)
}

func TestTwoAccounts_IntermediateCache(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{})
	err := db.Put(dbutils.AccountsBucket, common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"), common.Hex2Bytes("020502540be400"))
	require.Nil(t, err)
	err = db.Put(dbutils.AccountsBucket, common.Hex2Bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), common.Hex2Bytes("020502540be400"))
	require.Nil(t, err)
	err = db.Put(dbutils.AccountsBucket, common.Hex2Bytes("aabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), common.Hex2Bytes("120164204f1593970e8f030c0a2c39758181a447774eae7c65653c4e6440e8c18dad69bc"))
	require.Nil(t, err)
	err = db.Put(dbutils.AccountsBucket, common.Hex2Bytes("aabbcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"), common.Hex2Bytes("120164204f1593970e8f030c0a2c39758181a447774eae7c65653c4e6440e8c18dad69bc"))
	require.Nil(t, err)
	err = db.Put(dbutils.AccountsBucket, common.Hex2Bytes("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"), common.Hex2Bytes("120164204f1593970e8f030c0a2c39758181a447774eae7c65653c4e6440e8c18dad69bc"))
	require.Nil(t, err)

	expect := common.HexToHash("fbf273784067d17ba0e682653a14348bb66978345ff0fe2c75659dfdbdff5fc3")
	req := &ResolveRequest{
		t:           tr,
		resolveHex:  []byte{},
		resolvePos:  0,
		resolveHash: hashNode(expect.Bytes()),
	}
	resolver := NewResolver(0, true, 0)
	resolver.AddRequest(req)
	err = resolver.ResolveWithDb(db, 0)
	require.Nil(t, err, "resolve error")

	assert.Equal(t, expect.String(), tr.Hash().String())

	expectPrinted := "f(0:s(00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010:v(f849808502540be400a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470))10:s(0a:h(61b4ae1d35aa975e1951b4c67340e7bcceec1a9057617105b4f791b5b80aec3f))15:s(0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f10:v(f8448064a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a04f1593970e8f030c0a2c39758181a447774eae7c65653c4e6440e8c18dad69bc)))\n"
	printed := bytes.Buffer{}
	tr.Print(&printed)
	assert.Equal(t, expectPrinted, printed.String())
}
