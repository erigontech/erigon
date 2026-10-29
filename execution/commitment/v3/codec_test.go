// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.
package v3

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	randv2 "math/rand/v2"
	"slices"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest"
)

func TestPathCodec(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E5/PackPathRoundTrip", func(t *testing.T) {
			var packedBuffer [32]byte
			var unpackedBuffer [64]byte
			for count := 0; count <= 64; count++ {
				path := make([]byte, count)
				for i := range path {
					path[i] = byte((i*7 + count) & 0x0f)
				}

				packed := packPath(path, packedBuffer[:])
				require.Len(t, packed, packedLen(count))
				if count&1 == 1 {
					require.Zero(t, packed[len(packed)-1]&0x0f)
				}
				require.Equal(t, path, unpackPath(packed, count, unpackedBuffer[:]))
			}
		}},
		{"E6/PackPathReusesDestinationAndClearsOddPadding", func(t *testing.T) {
			dst := make([]byte, 3)
			packed := packPath([]byte{1, 2, 3, 4, 5}, dst)
			require.Equal(t, []byte{0x12, 0x34, 0x50}, packed)

			packed = packPath([]byte{6, 7, 8, 9}, packed)
			require.Equal(t, []byte{0x67, 0x89}, packed)
		}},
		{"E7/PackedPathSortOrder", func(t *testing.T) {
			makeKey := func(path []byte) []byte {
				key := []byte{0x40}
				key = append(key, packPath(path, nil)...)
				return append(key, byte(len(path)))
			}
			paths := [][]byte{
				{0x09, 0x0f},
				{0x0a, 0x00, 0x00},
				{0x0a},
				{0x0a, 0x00, 0x02},
				{0x0a, 0x0f, 0x01},
				{0x0b, 0x00},
			}
			keys := make([][]byte, len(paths))
			for i, path := range paths {
				keys[i] = makeKey(path)
			}
			slices.SortFunc(keys, bytes.Compare)

			require.Equal(t, [][]byte{{0x40, 0xa0, 0x00, 0x03}, {0x40, 0xa0, 0x01}, {0x40, 0xa0, 0x20, 0x03}}, keys[1:4])

			first, last := -1, -1
			for i, key := range keys {
				path := unpackPath(key[1:len(key)-1], int(key[len(key)-1]), nil)
				if path[0] == 0x0a {
					if first == -1 {
						first = i
					}
					last = i
				}
			}
			require.NotEqual(t, -1, first)
			for _, key := range keys[first : last+1] {
				path := unpackPath(key[1:len(key)-1], int(key[len(key)-1]), nil)
				require.Equal(t, byte(0x0a), path[0])
			}
		}},
		{"E6/PackPathIntoDirtyBufferMatchesFreshBuffer", func(t *testing.T) {
			for count := range 65 {
				nibbles := make([]byte, count)
				for i := range nibbles {
					nibbles[i] = byte((i*7 + 3) & 0x0f)
				}
				want := packPath(nibbles, nil)

				var scratch [32]byte
				for i := range scratch {
					scratch[i] = 0xff
				}
				got := packPath(nibbles, scratch[:0])
				require.True(t, bytes.Equal(want, got), "count %d: want %x got %x", count, want, got)

				reused := packPath(bytes.Repeat([]byte{0x0f}, 64), nil)
				got = packPath(nibbles, reused[:0])
				require.True(t, bytes.Equal(want, got), "count %d reused: want %x got %x", count, want, got)
			}
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

func TestNodeKeys(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E8/NodeKeyRoundTrip", func(t *testing.T) {
			var addrHash [32]byte
			for i := range addrHash {
				addrHash[i] = byte(i)
			}

			for count := 0; count <= 64; count++ {
				path := make([]byte, count)
				for i := range path {
					path[i] = byte((i + count) & 0x0f)
				}

				for _, tc := range []struct {
					name string
					key  []byte
					tag  byte
					addr []byte
				}{
					{name: "account", key: AccountNodeKey(path, nil), tag: tagAccountNode, addr: []byte{}},
					{name: "storage", key: StorageNodeKey(addrHash, path, nil), tag: tagStorageNode, addr: addrHash[:]},
				} {
					t.Run(tc.name, func(t *testing.T) {
						require.Equal(t, tc.tag, tc.key[0])
						require.Equal(t, tc.addr, tc.key[1:1+len(tc.addr)])
						require.Equal(t, byte(count), tc.key[len(tc.key)-1])
						require.Equal(t, path, unpackPath(tc.key[1+len(tc.addr):len(tc.key)-1], count, []byte{}))
					})
				}
			}
		}},
		{"E8/RootKeys", func(t *testing.T) {
			var addrHash [32]byte
			addrHash[0] = 0xab

			require.Equal(t, []byte{tagAccountNode, 0}, AccountNodeKey(nil, nil))
			require.Equal(t, append([]byte{tagStorageNode}, append(addrHash[:], 0)...), StorageNodeKey(addrHash, nil, nil))
		}},
		{"E8/NodeKeyUsesDestination", func(t *testing.T) {
			dst := []byte{0xaa, 0xbb}
			key := AccountNodeKey([]byte{1, 2, 3}, dst)
			require.Equal(t, []byte{0xaa, 0xbb, tagAccountNode, 0x12, 0x30, 3}, key)
			require.Equal(t, []byte{0xaa, 0xbb}, dst)
		}},
		{"E8/V1KeyedTagDisjointness", func(t *testing.T) {
			rng := randv2.New(randv2.NewPCG(0x6b657973, 0x7634))
			for range 10_000 {
				path := make([]byte, rng.IntN(65))
				for j := range path {
					path[j] = byte(rng.IntN(16))
				}
				compact := nibbles.HexToCompact(path)
				require.Less(t, compact[0], byte(tagAccountNode))
			}

			require.Equal(t, AccountNodeKey(nil, nil), nibbles.EncodeKeyV2([]byte{4, 0}))
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

func TestAccountCodec(t *testing.T) {
	codeHash, storageRoot := common.HexToHash("0x1234"), common.HexToHash("0x5678")
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E1-E2/account-values", func(t *testing.T) {
			for _, tt := range []struct {
				name string
				spec commitmenttest.AccountSpec
				root common.Hash
			}{
				{"eoa", commitmenttest.AccountSpec{CodeHash: empty.CodeHash}, empty.RootHash},
				{"contract with storage", commitmenttest.AccountSpec{Nonce: 3, Balance: 99, CodeHash: codeHash}, storageRoot},
				{"contract with code and no storage", commitmenttest.AccountSpec{Nonce: 7, CodeHash: codeHash}, empty.RootHash},
				{"elision/zero/code", commitmenttest.AccountSpec{CodeHash: codeHash}, empty.RootHash},
				{"elision/balance/empty-code", commitmenttest.AccountSpec{Balance: 99, CodeHash: empty.CodeHash}, empty.RootHash},
				{"elision/balance/code", commitmenttest.AccountSpec{Balance: 99, CodeHash: codeHash}, empty.RootHash},
			} {
				t.Run(tt.name, func(t *testing.T) {
					update := testAccountUpdate(commitmenttest.Account(tt.spec))
					update.Flags = 0
					encoded := encodeAccountLeaf(update, tt.root[:], nil)
					nonce, balance, gotCode, gotRoot, err := decodeAccountLeaf(encoded)
					require.NoError(t, err)
					require.Equal(t, tt.spec.Nonce, nonce)
					require.Equal(t, 0, balance.Cmp(uint256.NewInt(tt.spec.Balance)))
					require.Equal(t, tt.spec.Balance, balance.Uint64())
					require.Equal(t, tt.spec.CodeHash[:], gotCode)
					require.Equal(t, tt.root[:], gotRoot)
				})
			}
		}},
		{"E4/consensus", func(t *testing.T) {
			for _, balance := range []uint64{0, 99} {
				for _, code := range []common.Hash{empty.CodeHash, codeHash} {
					acc := accounts.NewAccount()
					acc.Nonce = 7
					acc.Balance.SetUint64(balance)
					acc.Root = storageRoot
					acc.CodeHash = accounts.InternCodeHash(code)
					require.Equal(t, acc.RLP(), accountConsensusRLP(acc.Nonce, &acc.Balance, acc.Root[:], code[:], nil))
				}
			}
			acc := accounts.NewAccount()
			got := accountConsensusRLP(0, &acc.Balance, empty.RootHash[:], empty.CodeHash[:], nil)
			acc.Root = empty.RootHash
			require.Equal(t, acc.RLP(), got)
		}},
		{"E2/defaults", func(t *testing.T) {
			for _, tt := range []struct {
				name   string
				update commitment.Update
				root   []byte
				want   []byte
			}{
				{"zero code", commitment.Update{}, empty.RootHash[:], []byte{0}},
				{"empty code", commitment.Update{CodeHash: empty.CodeHash}, empty.RootHash[:], []byte{0}},
				{"zero hashes", commitment.Update{CodeHash: common.Hash{}}, make([]byte, length.Hash), []byte{0}},
				{"nonce only", commitment.Update{Nonce: 1, CodeHash: empty.CodeHash}, empty.RootHash[:], []byte{accountHasNonce, 1}},
				{"balance only", commitment.Update{Balance: *uint256.NewInt(99), CodeHash: empty.CodeHash}, empty.RootHash[:], []byte{0, 99}},
			} {
				t.Run(tt.name, func(t *testing.T) { require.Equal(t, tt.want, encodeAccountLeaf(&tt.update, tt.root, nil)) })
			}
			full := encodeAccountLeaf(&commitment.Update{Nonce: 1, Balance: *uint256.NewInt(99), CodeHash: codeHash}, empty.RootHash[:], nil)
			require.Len(t, full, 1+1+length.Hash+1)
		}},
		{"E3/malformed", func(t *testing.T) {
			encoded := append([]byte{accountHasNonce | accountHasCodeHash, 1}, append(make([]byte, 30), 0x12, 0x34)...)
			for i := range encoded {
				_, _, _, _, err := decodeAccountLeaf(encoded[:i])
				require.Error(t, err, "truncation at %d", i)
			}
			for _, tt := range []struct {
				name string
				data []byte
			}{
				{"unknown flags", []byte{0x80, 0}},
				{"zero code", append([]byte{accountHasCodeHash, 0}, make([]byte, length.Hash)...)},
				{"zero nonce", []byte{accountHasNonce, 0}},
				{"oversized balance", append([]byte{0}, make([]byte, length.Hash+1)...)},
				{"leading zero balance", []byte{0, 0x00, 0x01}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					_, _, _, _, err := decodeAccountLeaf(tt.data)
					require.ErrorIs(t, err, errAccountLeafFlags)
				})
			}
		}},
		{"E1-E101/seeded-decode", func(t *testing.T) {
			for _, share := range []int{12, 0} {
				t.Run(fmt.Sprintf("contracts=%d", share), func(t *testing.T) {
					rnd := rand.New(rand.NewSource(7))
					for i := range 200000 {
						value, root := commitmenttest.SizedAccount(i, i%100 < share, rnd)
						encoded := encodeAccountLeaf(testAccountUpdate(value), root, nil)
						nonce, balance, code, storage, err := decodeAccountLeaf(encoded)
						require.NoError(t, err, "account=%d", i)
						require.Equal(t, value.Nonce, nonce)
						require.Equal(t, value.Balance, balance)
						require.Equal(t, value.CodeHash[:], code)
						require.Equal(t, root, storage)
						_ = accountConsensusRLP(nonce, &balance, storage, code, nil)
					}
				})
			}
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

func TestStateCodec(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E44/StateRoundTrip", func(t *testing.T) {
			trie := &Trie{root: bytes.Repeat([]byte{0x37}, 32)}
			encoded, err := trie.EncodeState(91, 73, []byte{0xaa})
			require.NoError(t, err)
			require.Equal(t, commitment.CommitmentV3StateMarker, encoded[1])

			restored := &Trie{}
			blockNum, txNum, err := restored.RestoreState(encoded[1:])
			require.NoError(t, err)
			require.Equal(t, uint64(91), blockNum)
			require.Equal(t, uint64(73), txNum)
			root, err := restored.RootHash()
			require.NoError(t, err)
			require.Equal(t, trie.root, root)
		}},
		{"E44/StateRoundTripEmptyRoot", func(t *testing.T) {
			trie := &Trie{}
			encoded, err := trie.EncodeState(1, 2, nil)
			require.NoError(t, err)

			restored := &Trie{root: bytes.Repeat([]byte{0x55}, 32)}
			_, _, err = restored.RestoreState(encoded)
			require.NoError(t, err)
			root, err := restored.RootHash()
			require.NoError(t, err)
			require.Equal(t, empty.RootHash[:], root)
		}},
		{"E44/StateRejectsLegacyAndMalformedBlobs", func(t *testing.T) {
			trie := &Trie{}
			legacy := make([]byte, commitment.CommitmentV3StateSize)
			legacy[0] = commitment.CommitmentV3StateMarker - 1
			_, _, err := trie.RestoreState(legacy)
			require.ErrorIs(t, err, commitment.ErrCommitmentV3StateMarker)

			for _, value := range [][]byte{{commitment.CommitmentV3StateMarker}, make([]byte, commitment.CommitmentV3StateSize-1), make([]byte, commitment.CommitmentV3StateSize+1)} {
				_, _, err = trie.RestoreState(value)
				require.ErrorIs(t, err, commitment.ErrCommitmentV3StateSize)
			}

			badMarker := make([]byte, commitment.CommitmentV3StateSize)
			badMarker[0] = commitment.CommitmentV3StateMarker + 1
			_, _, err = trie.RestoreState(badMarker)
			require.ErrorIs(t, err, commitment.ErrCommitmentV3StateMarker)
		}},
		{"E44/StateEncodeRejectsInvalidRoot", func(t *testing.T) {
			_, err := (&Trie{root: []byte{1}}).EncodeState(0, 0, nil)
			require.ErrorIs(t, err, commitment.ErrCommitmentV3StateSize)
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

func TestRecordCodec(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E24/RecordDecodeHeaderForms", func(t *testing.T) {
			suffix := packPath(bytes.Repeat([]byte{1}, 60), nil)
			tests := []struct {
				name  string
				flags byte
				data  []byte
				depth int
			}{
				{
					name:  "branch",
					flags: 0,
					data:  commitmenttest.Record(&commitmenttest.RecordSpec{ChildMask: 0x0003}),
					depth: 3,
				},
				{
					name:  "child extension",
					flags: hdrHasChildExt,
					data:  commitmenttest.Record(&commitmenttest.RecordSpec{Flags: hdrHasChildExt, ChildMask: 0x0001, ExtensionMask: 0x0001, Extensions: [16][]byte{0: extFixture([]byte{1, 2})}}),
					depth: 3,
				},
				{
					name:  "leaf child",
					flags: 0,
					data:  commitmenttest.Record(&commitmenttest.RecordSpec{ChildMask: 0x0001, LeafMask: 0x0001, Leaves: [16]commitmenttest.Leaf{0: {Suffix: suffix, Value: []byte{0x42}}}}),
					depth: 3,
				},
				{
					name:  "self extension",
					flags: hdrHasSelfExt,
					data:  commitmenttest.Record(&commitmenttest.RecordSpec{Flags: hdrHasSelfExt, ChildMask: 0x0001, SelfExtension: extFixture([]byte{3, 4, 5})}),
					depth: 0,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					require.NoError(t, Validate(tt.data, tt.depth))
					r := Record{data: tt.data, depth: tt.depth}
					require.Equal(t, byte(recordFormat), r.data[0]&hdrFormatMask)
				})
			}
		}},
		{"E24-E25/RecordDecodeHeaderBitCombinations", func(t *testing.T) {
			for combination := range 16 {
				flags := byte(combination << 4)
				depth := 3
				if flags&(hdrHasSelfExt|hdrIsLeafRoot) != 0 {
					depth = 0
				}
				if flags&hdrIsLeafRoot != 0 {
					data := append([]byte{flags}, make([]byte, 32)...)
					data = append(data, 0)
					wantErr := combination != 8
					t.Run(string(rune('a'+combination)), func(t *testing.T) {
						err := Validate(data, depth)
						require.Equal(t, wantErr, err != nil)
					})
					continue
				}

				child := uint16(1)
				ext := uint16(0)
				var extAt [16][]byte
				if flags&hdrHasChildExt != 0 {
					ext |= 1
					extAt[0] = extFixture([]byte{1})
				}
				self := []byte(nil)
				if flags&hdrHasSelfExt != 0 {
					self = extFixture([]byte{1})
				}
				data := commitmenttest.Record(&commitmenttest.RecordSpec{Flags: flags, ChildMask: child, ExtensionMask: ext, SelfExtension: self, Extensions: extAt})
				wantErr := flags&hdrHasEmb != 0
				t.Run(string(rune('a'+combination)), func(t *testing.T) {
					err := Validate(data, depth)
					require.Equal(t, wantErr, err != nil)
				})
			}
		}},
		{"E26/RecordDecodeAccessors", func(t *testing.T) {
			depth := 3
			suffix := packPath(bytes.Repeat([]byte{1}, 60), nil)
			extension := extFixture([]byte{6, 7, 8})
			second := extFixture([]byte{9})
			data := commitmenttest.Record(&commitmenttest.RecordSpec{
				Flags: hdrHasChildExt, ChildMask: 0x000f, LeafMask: 0x000a, ExtensionMask: 0x0005,
				Extensions: [16][]byte{0: extension, 2: second},
				Leaves: [16]commitmenttest.Leaf{
					1: {Suffix: suffix, Value: []byte{0x11, 0x12}},
					3: {Suffix: suffix, Value: []byte{0x31}},
				},
			})
			require.NoError(t, Validate(data, depth))
			r := Record{data: data, depth: depth}
			l := r.layout()
			require.Equal(t, uint16(0x000f), l.child)
			require.Equal(t, uint16(0x000a), l.leaf)
			require.Equal(t, uint16(0x0005), l.ext)
			for _, nib := range []int{0, 2} {
				require.Len(t, r.slotAt(l, nib), 32)
				require.Equal(t, byte(nib), r.slotAt(l, nib)[0])
			}
			for _, nib := range []int{1, 3} {
				require.Nil(t, r.slotAt(l, nib))
			}
			require.Equal(t, extension, r.extAt(l, 0))
			require.Equal(t, second, r.extAt(l, 2))
			require.Nil(t, r.extAt(l, 1))
			for _, tc := range []struct {
				nib   int
				value []byte
			}{{1, []byte{0x11, 0x12}}, {3, []byte{0x31}}} {
				gotSuffix, gotValue := r.leafAt(l, tc.nib)
				require.Equal(t, suffix, gotSuffix)
				require.Equal(t, tc.value, gotValue)
			}

			root := commitmenttest.Record(&commitmenttest.RecordSpec{Flags: hdrHasSelfExt, ChildMask: 1, SelfExtension: extFixture([]byte{1, 2})})
			require.Equal(t, extFixture([]byte{1, 2}), (Record{data: root, depth: 0}).SelfExt())
		}},
		{"E28/RecordDecodeLeafRoot", func(t *testing.T) {
			key := bytes.Repeat([]byte{0xab}, 32)
			value := []byte{1, 2, 3}
			data := append([]byte{hdrIsLeafRoot}, key...)
			data = append(data, byte(len(value)))
			data = append(data, value...)
			require.NoError(t, Validate(data, 0))
			gotKey, gotValue := data[1:33], data[34:]
			require.Equal(t, key, gotKey)
			require.Equal(t, value, gotValue)
		}},
		{"E27/RecordDecodeNoAllocations", func(t *testing.T) {
			data := commitmenttest.Record(&commitmenttest.RecordSpec{
				Flags: hdrHasChildExt, ChildMask: 0x000f, LeafMask: 0x000a, ExtensionMask: 0x0001,
				Extensions: [16][]byte{0: extFixture([]byte{6, 7})},
				Leaves: [16]commitmenttest.Leaf{
					1: {Suffix: packPath(bytes.Repeat([]byte{1}, 60), nil), Value: []byte{1}},
					3: {Suffix: packPath(bytes.Repeat([]byte{1}, 60), nil), Value: []byte{3}},
				},
			})
			r := Record{data: data, depth: 3}
			require.Zero(t, testing.AllocsPerRun(100, func() {
				l := r.layout()
				_ = r.slotAt(l, 0)
				_ = r.extAt(l, 0)
				_, _ = r.leafAt(l, 1)
			}))
		}},
		{"E25/RecordDecodeRejectsMalformedRecords", func(t *testing.T) {
			valid := commitmenttest.Record(&commitmenttest.RecordSpec{ChildMask: 1})
			tests := []struct {
				name  string
				data  []byte
				depth int
				want  error
			}{
				{name: "truncated body", data: valid[:len(valid)-1], depth: 3, want: ErrRecordTruncated},
				{name: "leaf outside child mask", data: commitmenttest.Record(&commitmenttest.RecordSpec{ChildMask: 1, LeafMask: 2}), depth: 3, want: ErrRecordMasks},
				{name: "extension overlaps leaf", data: commitmenttest.Record(&commitmenttest.RecordSpec{Flags: hdrHasChildExt, ChildMask: 1, LeafMask: 1, ExtensionMask: 1, Extensions: [16][]byte{0: extFixture([]byte{1})}}), depth: 3, want: ErrRecordMasks},
				{name: "leaf root and self extension", data: append([]byte{hdrIsLeafRoot | hdrHasSelfExt}, make([]byte, 33)...), depth: 0, want: ErrRecordRoot},
				{name: "self extension has two children", data: commitmenttest.Record(&commitmenttest.RecordSpec{Flags: hdrHasSelfExt, ChildMask: 3, SelfExtension: extFixture([]byte{1})}), depth: 0, want: ErrRecordRoot},
				{name: "short extension trailer", data: commitmenttest.Record(&commitmenttest.RecordSpec{Flags: hdrHasChildExt, ChildMask: 1, ExtensionMask: 1, Extensions: [16][]byte{0: {3, 0x12}}}), depth: 3, want: ErrRecordTruncated},
				{name: "embedded flag", data: commitmenttest.Record(&commitmenttest.RecordSpec{Flags: hdrHasEmb, ChildMask: 1}), depth: 3, want: ErrRecordFormat},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					require.ErrorIs(t, Validate(tt.data, tt.depth), tt.want)
				})
			}
		}},
		{"E26/EncodeRecordRoundTrip", func(t *testing.T) {
			suffix := packPath(bytes.Repeat([]byte{1}, 60), nil)
			hash := bytes.Repeat([]byte{0xab}, 32)
			tests := []struct {
				name  string
				depth int
				path  []byte
				spec  commitmenttest.RecordSpec
				check func(*testing.T, Record)
			}{
				{
					name:  "branch with leaf",
					depth: 3,
					spec:  commitmenttest.RecordSpec{ChildMask: 3, LeafMask: 2, Hashes: [16][]byte{0: hash}, Leaves: [16]commitmenttest.Leaf{1: {Suffix: suffix, Value: []byte{0x42}}}},
					check: func(t *testing.T, r Record) {
						l := r.layout()
						require.Equal(t, uint16(3), l.child)
						require.Equal(t, uint16(2), l.leaf)
						require.Equal(t, hash, r.slotAt(l, 0))
						gotSuffix, gotValue := r.leafAt(l, 1)
						require.Equal(t, suffix, gotSuffix)
						require.Equal(t, []byte{0x42}, gotValue)
					},
				},
				{
					name:  "child extension",
					depth: 3,
					spec:  commitmenttest.RecordSpec{ChildMask: 1 << 4, Hashes: [16][]byte{4: hash}, Extensions: [16][]byte{4: {3, 0x23, 0x40}}},
					check: func(t *testing.T, r Record) {
						l := r.layout()
						require.Equal(t, uint16(1<<4), l.ext)
						require.Equal(t, []byte{3, 0x23, 0x40}, r.extAt(l, 4))
						require.Equal(t, hash, r.slotAt(l, 4))
					},
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					n := materializeNode(tt.path, planeAccount, &tt.spec)
					data := encodeRecord(n, tt.depth, bytes.Repeat([]byte{0xff}, 8))
					require.NoError(t, Validate(data, tt.depth))
					tt.check(t, Record{data: data, depth: tt.depth})
				})
			}
		}},
		{"E28/EncodeRecordRootForms", func(t *testing.T) {
			hash := bytes.Repeat([]byte{0x11}, 32)
			tests := []struct {
				name  string
				path  []byte
				spec  commitmenttest.RecordSpec
				check func(*testing.T, []byte)
			}{
				{
					name: "leaf root",
					spec: commitmenttest.RecordSpec{ChildMask: 1 << 4, LeafMask: 1 << 4, Leaves: [16]commitmenttest.Leaf{4: {Suffix: packPath(bytes.Repeat([]byte{2}, 63), nil), Value: []byte{1, 2}}}},
					check: func(t *testing.T, data []byte) {
						require.Equal(t, byte(hdrIsLeafRoot), data[0])
						require.NoError(t, Validate(data, 0))
						key, value := data[1:33], data[34:]
						require.Equal(t, packPath(append([]byte{4}, bytes.Repeat([]byte{2}, 63)...), nil), key)
						require.Equal(t, []byte{1, 2}, value)
					},
				},
				{
					name: "extension root",
					path: []byte{1, 2},
					spec: commitmenttest.RecordSpec{ChildMask: 1 << 3, Hashes: [16][]byte{3: hash}},
					check: func(t *testing.T, data []byte) {
						require.Equal(t, byte(hdrHasSelfExt), data[0])
						require.NoError(t, Validate(data, 0))
						r := Record{data: data, depth: 0}
						require.Equal(t, []byte{2, 0x12}, r.SelfExt())
						require.Equal(t, hash, r.slotAt(r.layout(), 3))
					},
				},
				{
					name: "branch root",
					spec: commitmenttest.RecordSpec{ChildMask: 1<<3 | 1<<9, Hashes: [16][]byte{3: hash, 9: bytes.Repeat([]byte{0x22}, 32)}},
					check: func(t *testing.T, data []byte) {
						require.Equal(t, byte(recordFormat), data[0])
						require.NoError(t, Validate(data, 0))
						require.Equal(t, uint16(1<<3|1<<9), Record{data: data, depth: 0}.layout().child)
					},
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					tt.check(t, encodeRecord(materializeNode(tt.path, planeAccount, &tt.spec), 0, nil))
				})
			}
		}},
		{"E28/EncodeRecordTombstone", func(t *testing.T) {
			dst := []byte{1, 2, 3}
			require.Empty(t, encodeRecord(fork(nil), 7, dst))
		}},
		{"E27-E28/EncodeRecordReusesDestination", func(t *testing.T) {
			n := fork(nil)
			n.setStoredChild(2, bytes.Repeat([]byte{0x33}, 32), nil)
			dst := make([]byte, 0, 256)
			data := encodeRecord(n, 7, dst)
			require.NoError(t, Validate(data, 7))
			data = encodeRecord(fork(nil), 7, data)
			require.Empty(t, data)
		}},
		{"E28/EncodeRecordRejectsUnrepresentableRoots", func(t *testing.T) {
			require.Panics(t, func() {
				n := fork(nil)
				n.setStoredChild(1, bytes.Repeat([]byte{1}, 32), nil)
				encodeRecord(n, 0, nil)
			})
			require.Panics(t, func() {
				n := fork([]byte{1})
				n.setStoredChild(1, bytes.Repeat([]byte{1}, 32), []byte{2})
				encodeRecord(n, 0, nil)
			})
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

func extFixture(path []byte) []byte {
	return append([]byte{byte(len(path))}, packPath(path, nil)...)
}

func TestConsensusHash(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E20-E21/LeafRefVectors", func(t *testing.T) {
			key := nibbles.HexToCompact([]byte{1, 2})
			payload := bytes.Repeat([]byte{0x11}, 32)
			got := leafRef(key, payload, nil)
			require.Equal(t, mustDecodeHex(t, "e2d0b037b975d0e18dfaad291e1c147eeab6f0547d7056bdf3ea0b4fbeeca973"), got)

			inline := leafRef(key, []byte{1}, nil)
			require.Equal(t, []byte{0xc4, 0x82, 0x00, 0x12, 0x01}, inline)
		}},
		{"E21/LeafRefHashesAtBoundary", func(t *testing.T) {
			key := nibbles.HexToCompact([]byte{1, 2})
			below := leafRef(key, bytes.Repeat([]byte{0x22}, 26), nil)
			at := leafRef(key, bytes.Repeat([]byte{0x22}, 27), nil)
			require.Less(t, len(below), 32)
			require.Len(t, at, 32)
		}},
		{"E20/ExtensionRefVector", func(t *testing.T) {
			child := mustDecodeHex(t, "c7c2cc5c8eba62b7ff0b998ee46c6cc41075ee8d2dbefa0fa4b26bdb2530f272")
			got := extensionRef([]byte{1, 2}, child)
			require.Equal(t, mustDecodeHex(t, "c5ab940b6ad7c39b31afef79e182f2d568c79617ad4bca225ceb3e9d1af4cd1d"), got[:])
		}},
		{"E20/BranchRefVector", func(t *testing.T) {
			var refs [16][]byte
			refs[1] = bytes.Repeat([]byte{0x11}, 32)
			refs[14] = bytes.Repeat([]byte{0x22}, 32)
			got := branchRef(&refs)
			require.Equal(t, mustDecodeHex(t, "c7c2cc5c8eba62b7ff0b998ee46c6cc41075ee8d2dbefa0fa4b26bdb2530f272"), got[:])
		}},
		{"E22/BranchRefRejectsInlinableBranch", func(t *testing.T) {
			var refs [16][]byte
			refs[0] = []byte{0xc1, 0x01}
			refs[1] = []byte{0xc1, 0x02}
			require.PanicsWithValue(t, "commitment v3: inlinable branch child", func() {
				branchRef(&refs)
			})
		}},
		{"E22/BranchRefAllowsOrdinaryBranches", func(t *testing.T) {
			for childCount := 2; childCount <= 16; childCount++ {
				t.Run(string(rune('a'+childCount)), func(t *testing.T) {
					var refs [16][]byte
					for i := range childCount {
						refs[i] = bytes.Repeat([]byte{byte(i + 1)}, 32)
					}
					require.NotPanics(t, func() { branchRef(&refs) })
				})
			}
		}},
		{"E23/StorageLeafRefMatchesRLP", func(t *testing.T) {
			payloads := [][]byte{{}, {0x00}, {0x7f}, {0x80}, {0xff}}
			for n := 2; n <= 32; n++ {
				payloads = append(payloads, bytes.Repeat([]byte{0xaa}, n))
			}
			for _, keyLen := range []int{0, 1, 2, 30, 62, 63, 64} {
				suffix := nibbles.HexToCompact(append(bytes.Repeat([]byte{0x0c}, keyLen), nibbles.Terminator))
				for _, payload := range payloads {
					inner, err := rlp.EncodeToBytes(payload)
					require.NoError(t, err)
					want, err := rlp.EncodeToBytes([][]byte{suffix, inner})
					require.NoError(t, err)
					if len(want) >= 32 {
						hash := keccak.Sum256(want)
						want = hash[:]
					}
					require.Equal(t, want, storageLeafRef(suffix, payload, nil), "key %d payload %x", keyLen, payload)
				}
			}
		}},
		{"E23/StorageLeafRefRandom", func(t *testing.T) {
			rnd := rand.New(rand.NewSource(11))
			for _, suffixLen := range []int{0, 1, 2, 17, 33} {
				suffix := make([]byte, suffixLen)
				for l := 1; l <= 32; l++ {
					for trial := range 400 {
						v := make([]byte, l)
						rnd.Read(v)
						if trial < 256 && l == 1 {
							v[0] = byte(trial)
						}
						if v[0] == 0 && l > 1 {
							v[0] = 1
						}
						rnd.Read(suffix)
						want := storageLeafRefBuffered(suffix, v, nil)
						require.Equal(t, want, storageLeafRef(suffix, v, nil), "suffixLen=%d l=%d v=%x", suffixLen, l, v)
					}
				}
			}
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	require.NoError(t, err)
	return decoded
}

func storageLeafRefBuffered(suffix []byte, payload []byte, dst []byte) []byte {
	var encoded bytes.Buffer
	var prefix [8]byte
	if err := rlp.RlpSerializableBytes(payload).ToDoubleRLP(&encoded, prefix[:]); err != nil {
		panic(err)
	}
	contentLen := rlp.StringLen(suffix) + encoded.Len()
	start := len(dst)
	dst = append(dst, make([]byte, rlp.ListLen(contentLen))...)
	pos := start + rlp.EncodeListPrefixToBuf(contentLen, dst[start:])
	pos += rlp.EncodeStringToBuf(suffix, dst[pos:])
	pos += copy(dst[pos:], encoded.Bytes())
	encodedBytes := dst[start:pos]
	if len(encodedBytes) < 32 {
		return encodedBytes
	}
	hash := keccak.Sum256(encodedBytes)
	return append(dst[:start], hash[:]...)
}

func TestUnfoldRejectsInvalidRecords(t *testing.T) {
	branch := func(path []byte) []byte {
		n := fork(path)
		n.plane = planeAccount
		n.setStoredChild(0, bytes.Repeat([]byte{1}, 32), nil)
		n.setStoredChild(1, bytes.Repeat([]byte{2}, 32), nil)
		data := encodeRecord(n, len(path), nil)
		return data[:len(data)-10]
	}
	for _, tc := range []struct {
		name string
		path []byte
		data []byte
	}{
		{"self-extension flag without an extension", nil, []byte{0x90}},
		{"bare header at the root", nil, []byte{0x10}},
		{"bare header below the root", []byte{3}, []byte{0x10}},
		{"short leaf root", nil, []byte{0x80, 1, 2, 3}},
		{"truncated slot at the root", nil, branch(nil)},
		{"truncated slot below the root", []byte{3}, branch([]byte{3})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newShardedContext()
			require.NoError(t, ctx.PutBranch(nodeKey(planeAccount, nil, tc.path, nil), tc.data, nil))
			var err error
			require.NotPanics(t, func() { _, err = unfold(ctx, tc.path, planeAccount, nil) })
			require.Error(t, err)
		})
	}
}
