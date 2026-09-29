# Geth blob vectors

These vectors were generated from geth-pbt ref `origin/pbt` at `793dedb`.
The export was made without changing the checkout:

```sh
git -C /Users/awskii/org/wrk/go-ethereum archive --format=tar --prefix=geth-793dedb/ 793dedb | tar -x -C /tmp/tandem-pbt-witness
```

The throwaway generator was saved as `trie/bintrie/blob_vectors_test.go` in
the export and run with:

```sh
GOCACHE=/tmp/tandem-pbt-witness/geth-gocache GOPROXY=off go test ./trie/bintrie/ -run '^TestGenerateBlobVectors$' -count=1
```

```go
package bintrie

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

type blobVector struct {
	Name     string     `json:"name"`
	Kind     string     `json:"kind"`
	Position int        `json:"position,omitempty"`
	Stem     string     `json:"stem,omitempty"`
	Leaves   []blobLeaf `json:"leaves,omitempty"`
	Prefix   string     `json:"prefix,omitempty"`
	Left     string     `json:"left,omitempty"`
	Right    string     `json:"right,omitempty"`
	Key      string     `json:"key,omitempty"`
	Value    string     `json:"value"`
	Blob     string     `json:"blob"`
	Hash     string     `json:"hash"`
}

type blobLeaf struct {
	Sub   byte   `json:"sub"`
	Value string `json:"value"`
}

func TestGenerateBlobVectors(t *testing.T) {
	value := func(seed byte) []byte {
		return append(make([]byte, 31), seed)
	}
	stem := func(zone byte, length int, seed byte) []byte {
		out := make([]byte, length)
		out[0] = zone
		for i := 1; i < len(out); i++ {
			out[i] = seed + byte(i)
		}
		return out
	}
	encode := func(b []byte) string { return "0x" + hex.EncodeToString(b) }
	encodeHash := func(h common.Hash) string { return encode(h[:]) }
	serialize := func(n binaryNode, position int) (string, string) {
		blob, err := serializeNodeErr(n, position)
		if err != nil {
			t.Fatal(err)
		}
		hash, err := DeserializeAndHash(blob)
		if err != nil {
			t.Fatal(err)
		}
		return encode(blob), encodeHash(hash)
	}
	var vectors []blobVector
	accountStem := stem(AccountZone, AccountKeyLength-1, 0x10)
	storageStem := stem(StorageZone, StorageKeyLength-1, 0x20)
	leaf := func(name string, leafStem []byte, sub, seed byte) {
		key := append(append([]byte{}, leafStem...), sub)
		blob, hash := serialize(&groupNode{stem: leafStem, subs: []byte{sub}, vals: [][]byte{value(seed)}}, 0)
		vectors = append(vectors, blobVector{Name: name, Kind: "leaf", Key: encode(key), Value: encode(value(seed)), Blob: blob, Hash: hash})
	}
	leaf("leaf-account-34-byte-key", accountStem, 7, 0x11)
	leaf("leaf-storage-66-byte-key", storageStem, 255, 0x12)

	var left, right common.Hash
	left[0] = 0x11
	right[0] = 0x22
	branch := func(name string, prefix bitstr) {
		blob, hash := serialize(&branchNode{prefix: prefix, left: hashedNode(left), right: hashedNode(right)}, 0)
		vectors = append(vectors, blobVector{Name: name, Kind: "branch", Prefix: encode(prefix.b), Left: encodeHash(left), Right: encodeHash(right), Blob: blob, Hash: hash})
	}
	branch("branch-empty-prefix", bitstr{})
	branch("branch-long-prefix", bitstr{b: []byte{0xa5, 0x80}, n: 10})

	group := func(name string, position int, leafStem []byte, subs []byte, seed byte) {
		vals := make([][]byte, len(subs))
		leaves := make([]blobLeaf, len(subs))
		for i, sub := range subs {
			vals[i] = value(seed + byte(i))
			leaves[i] = blobLeaf{Sub: sub, Value: encode(vals[i])}
		}
		blob, hash := serialize(&groupNode{stem: leafStem, subs: subs, vals: vals}, position)
		vectors = append(vectors, blobVector{Name: name, Kind: "group", Position: position, Stem: encode(leafStem), Leaves: leaves, Blob: blob, Hash: hash})
	}
	group("group-position-0-k2", 0, accountStem, []byte{0, 255}, 0x30)
	group("group-position-8-k2", 8, storageStem, []byte{1, 255}, 0x40)
	subs := make([]byte, 256)
	for i := range subs {
		subs[i] = byte(i)
	}
	group("group-position-0-k256", 0, storageStem, subs, 0x50)
	group("group-position-16-k256", 16, storageStem, subs, 0x60)

	data, err := json.MarshalIndent(vectors, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("/tmp/tandem-pbt-witness/geth_blobs.json", append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
```
