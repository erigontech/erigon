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

package artifact

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"os"
	"sort"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type goldenArtifact struct {
	Bytes          string `json:"bytes"`
	SnapshotDigest string `json:"snapshotDigest"`
}

func TestWriterReproducesHandWrittenGolden(t *testing.T) {
	golden := readGolden(t)
	want, err := hex.DecodeString(golden.Bytes)
	require.NoError(t, err)
	root := common.BytesToHash([]byte("\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f"))
	leaves := goldenLeaves(t)
	var got bytes.Buffer
	digest, err := WriteSnapshot(&got, root, func(emit func([]byte, []byte) error) error {
		for _, leaf := range leaves {
			if err := emit(leaf.Key, leaf.Value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, want, got.Bytes(), "the writer must reproduce the hand-written artifact")
	require.Equal(t, golden.SnapshotDigest, hex.EncodeToString(digest[:]))
}

func TestReaderAcceptsHandWrittenGolden(t *testing.T) {
	golden := readGolden(t)
	want, err := hex.DecodeString(golden.Bytes)
	require.NoError(t, err)
	snapshot, err := ReadSnapshot(bytes.NewReader(want))
	require.NoError(t, err)
	require.Len(t, snapshot.Headers, 3)
	require.Len(t, snapshot.CodeGroups, 1)
	require.Len(t, snapshot.StorageGroups, 1)
	require.Equal(t, byte(0), snapshot.Headers[0].Kind)
	require.Equal(t, byte(1), snapshot.Headers[1].Kind)
	require.Equal(t, byte(2), snapshot.Headers[2].Kind)
	require.Equal(t, golden.SnapshotDigest, hex.EncodeToString(snapshot.SnapshotDigest[:]))
}

func TestArtifactReaderRejectsMalformedInputs(t *testing.T) {
	golden := readGolden(t)
	data, err := hex.DecodeString(golden.Bytes)
	require.NoError(t, err)
	tests := []struct {
		name string
		data func() []byte
	}{
		{name: "leading zero integer", data: func() []byte {
			broken := bytes.Clone(data)
			broken[72] = 2
			broken[73] = 0
			return broken
		}},
		{name: "header slot is not below 64", data: func() []byte {
			broken := bytes.Clone(data)
			index := bytes.Index(broken, []byte{2, 0, 1, 0x11})
			require.NotEqual(t, -1, index)
			broken[index] = 1
			broken[index+1] = 64
			return broken
		}},
		{name: "trailing byte", data: func() []byte {
			return append(bytes.Clone(data), 0)
		}},
	}
	for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
			_, err := ReadSnapshot(bytes.NewReader(test.data()))
			if test.name == "header slot is not below 64" {
				require.ErrorContains(t, err, "header slot")
				return
			}
			require.Error(t, err, "the reader must reject %s", test.name)
		})
	}
}

func TestArtifactRoundTripAndEmptySnapshot(t *testing.T) {
	root := common.Hash{1}
	leaves := goldenLeaves(t)
	for _, n := range []int{0, len(leaves)} {
		var encoded bytes.Buffer
		_, err := WriteSnapshot(&encoded, root, func(emit func([]byte, []byte) error) error {
			for _, leaf := range leaves[:n] {
				if err := emit(leaf.Key, leaf.Value); err != nil {
					return err
				}
			}
			return nil
		})
		if n == 0 {
			require.NoError(t, err)
			snapshot, readErr := ReadSnapshot(bytes.NewReader(encoded.Bytes()))
			require.NoError(t, readErr)
			require.Empty(t, snapshot.Headers)
			require.Equal(t, root, snapshot.Root)
			continue
		}
		require.NoError(t, err)
		_, err = ReadSnapshot(bytes.NewReader(encoded.Bytes()))
		require.NoError(t, err)
	}
	for seed := int64(0); seed < 5; seed++ {
		rng := rand.New(rand.NewSource(seed))
		basic, err := eip8297.EncodeBasicData(uint64(seed+1), newBalance(uint64(seed+1)), 0)
		require.NoError(t, err)
		key := eip8297.TreeKey(eip8297.AccountZone, bytes.Repeat([]byte{byte(rng.Intn(255) + 1)}, 32), eip8297.BasicDataLeafKey)
		var encoded bytes.Buffer
		_, err = WriteSnapshot(&encoded, root, func(emit func([]byte, []byte) error) error { return emit(key, basic[:]) })
		require.NoError(t, err)
		_, err = ReadSnapshot(bytes.NewReader(encoded.Bytes()))
		require.NoError(t, err)
	}
}

func TestWriterRejectsEmptyAccountAndZeroSizeCode(t *testing.T) {
	position := append(bytes.Repeat([]byte{0}, 31), 1)
	zeroBasic, err := eip8297.EncodeBasicData(0, uint256.NewInt(0), 0)
	require.NoError(t, err)
	write := func(leaves []testLeaf) error {
		var output bytes.Buffer
		_, err := WriteSnapshot(&output, common.Hash{}, func(emit func([]byte, []byte) error) error {
			for _, leaf := range leaves {
				if err := emit(leaf.Key, leaf.Value); err != nil {
					return err
				}
			}
			return nil
		})
		return err
	}
	require.Error(t, write([]testLeaf{{eip8297.TreeKey(eip8297.AccountZone, position, 0), zeroBasic[:]}}), "an empty kind-0 account must be refused")
	codeBasic, err := eip8297.EncodeBasicData(1, uint256.NewInt(0), 0)
	require.NoError(t, err)
	require.Error(t, write([]testLeaf{
		{eip8297.TreeKey(eip8297.AccountZone, position, 0), codeBasic[:]},
		{eip8297.TreeKey(eip8297.AccountZone, position, 1), bytes.Repeat([]byte{1}, 32)},
	}), "kind-1 code of size zero must be refused")
}

func TestPreimageReaderRejectsUnsortedDuplicateAndTruncatedRecords(t *testing.T) {
	addressA := common.Address{1}
	records := []Preimage{{Address: addressA}}
	var encoded bytes.Buffer
	require.NoError(t, WritePreimages(&encoded, records))
	_, err := ReadPreimages(bytes.NewReader(append(encoded.Bytes(), 1)))
	require.Error(t, err, "a trailing byte must not be accepted as a preimage record")
	duplicate := append(bytes.Clone(encoded.Bytes()), encoded.Bytes()...)
	_, err = ReadPreimages(bytes.NewReader(duplicate))
	require.Error(t, err, "duplicate addresses must be rejected")
	addressRecords := []Preimage{{Address: common.Address{1}}, {Address: common.Address{2}}}
	sort.Slice(addressRecords, func(i, j int) bool {
		left := keccak.Sum256(addressRecords[i].Address[:])
		right := keccak.Sum256(addressRecords[j].Address[:])
		return bytes.Compare(left[:], right[:]) < 0
	})
	encoded.Reset()
	require.NoError(t, WritePreimages(&encoded, addressRecords))
	unsortedAddresses := bytes.Clone(encoded.Bytes())
	firstAddress := bytes.Clone(unsortedAddresses[:20])
	copy(unsortedAddresses[:20], unsortedAddresses[24:44])
	copy(unsortedAddresses[24:44], firstAddress)
	_, err = ReadPreimages(bytes.NewReader(unsortedAddresses))
	require.Error(t, err, "unsorted addresses must be rejected")
	slots := [][32]byte{{1}, {2}}
	sort.Slice(slots, func(i, j int) bool {
		left := keccak.Sum256(slots[i][:])
		right := keccak.Sum256(slots[j][:])
		return bytes.Compare(left[:], right[:]) < 0
	})
	records = []Preimage{{Address: addressA, Slots: slots}}
	encoded.Reset()
	require.NoError(t, WritePreimages(&encoded, records))
	unsortedSlots := bytes.Clone(encoded.Bytes())
	firstSlot := bytes.Clone(unsortedSlots[24:56])
	copy(unsortedSlots[24:56], unsortedSlots[56:88])
	copy(unsortedSlots[56:88], firstSlot)
	_, err = ReadPreimages(bytes.NewReader(unsortedSlots))
	require.Error(t, err, "unsorted slots must be rejected")
	duplicateSlot := bytes.Clone(encoded.Bytes())
	copy(duplicateSlot[56:88], duplicateSlot[24:56])
	_, err = ReadPreimages(bytes.NewReader(duplicateSlot))
	require.Error(t, err, "duplicate slots must be rejected")
}

func TestPreimageJoinRejectsMissingAndSurplusAddress(t *testing.T) {
	address := common.Address{1}
	snapshot := Snapshot{Headers: []Header{{AddressHash: common.BytesToHash(eip8297.TreeKeyAccount(address[:], 0)[1:33])}}}
	var encoded bytes.Buffer
	require.NoError(t, WritePreimages(&encoded, []Preimage{{Address: common.Address{2}}}))
	records, err := ReadPreimages(bytes.NewReader(encoded.Bytes()))
	require.NoError(t, err)
	require.Error(t, Join(snapshot, records, eip8297.HashBytes), "a missing address must be rejected")
	encoded.Reset()
	records = []Preimage{{Address: common.Address{1}}, {Address: common.Address{2}}}
	sort.Slice(records, func(i, j int) bool {
		left := keccak.Sum256(records[i].Address[:])
		right := keccak.Sum256(records[j].Address[:])
		return bytes.Compare(left[:], right[:]) < 0
	})
	require.NoError(t, WritePreimages(&encoded, records))
	records, err = ReadPreimages(bytes.NewReader(encoded.Bytes()))
	require.NoError(t, err)
	require.Error(t, Join(snapshot, records, eip8297.HashBytes), "a surplus address must be rejected")
}

type testLeaf struct {
	Key   []byte
	Value []byte
}

func goldenLeaves(t *testing.T) []testLeaf {
	t.Helper()
	account := func(position []byte, sub byte) []byte { return eip8297.TreeKey(eip8297.AccountZone, position, sub) }
	code := func(position []byte, sub byte) []byte { return eip8297.TreeKey(eip8297.CodeZone, position, sub) }
	storage := func(address, stem []byte, sub byte) []byte {
		return eip8297.TreeKey(eip8297.StorageZone, append(bytes.Clone(address), stem...), sub)
	}
	position := func(value byte) []byte { return append(bytes.Repeat([]byte{0}, 31), value) }
	basic := func(nonce, balance, codeSize uint64) []byte {
		value, err := eip8297.EncodeBasicData(nonce, newBalance(balance), codeSize)
		require.NoError(t, err)
		return value[:]
	}
	delegation := make([]byte, 32)
	copy(delegation, append([]byte{0xef, 0x01, 0x00}, bytes.Repeat([]byte{0xbb}, 20)...))
	return []testLeaf{
		{account(position(1), 0), basic(1, 2, 0)},
		{account(position(1), 64), paddedValue(0x11)},
		{account(position(1), 67), paddedValue(0x2233)},
		{account(position(2), 0), basic(2, 0, 31)},
		{account(position(2), 1), bytes.Repeat([]byte{0xaa}, 32)},
		{account(position(2), 65), paddedValue(0x44)},
		{account(position(3), 0), basic(3, 4, eip8297.DelegationCodeLength)},
		{account(position(3), 2), delegation},
		{code(bytes.Repeat([]byte{0xcc}, 32), 0), leftPaddedValue(0x55)},
		{code(bytes.Repeat([]byte{0xcc}, 32), 7), leftPaddedBytes(0x66, 0x77)},
		{storage(append(bytes.Repeat([]byte{0}, 31), 1), bytes.Repeat([]byte{0xdd}, 32), 64), paddedValue(0x88)},
	}
}

func paddedValue(value uint64) []byte {
	result := make([]byte, 32)
	for i := uint(0); i < 8 && value != 0; i++ {
		result[31-i] = byte(value)
		value >>= 8
	}
	return result
}

func leftPaddedValue(value byte) []byte {
	result := make([]byte, 32)
	result[31] = value
	return result
}

func leftPaddedBytes(values ...byte) []byte {
	result := make([]byte, 32)
	copy(result[len(result)-len(values):], values)
	return result
}

func newBalance(value uint64) *uint256.Int {
	return uint256.NewInt(value)
}

func readGolden(t *testing.T) goldenArtifact {
	t.Helper()
	data, err := os.ReadFile("testdata/golden.json")
	require.NoError(t, err)
	var golden goldenArtifact
	require.NoError(t, json.Unmarshal(data, &golden))
	return golden
}
