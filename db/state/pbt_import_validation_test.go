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

package state

import (
	"bytes"
	"sort"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/artifact"
)

func TestPBTImportRejectsWrongChunk(t *testing.T) {
	hash := common.Hash{1}
	stem := pbtCodeGroupStem(eip8297.HashBytes, hash, 0)
	groups := map[common.Hash]map[byte][]byte{stem: {0: bytes.Repeat([]byte{4}, eip8297.ValueLength)}}
	err := validatePBTImportCode(t, artifact.Header{Kind: 1, CodeHash: hash, CodeSize: []byte{3}}, groups)
	require.ErrorContains(t, err, "code chunks disagree")
}

func TestPBTImportRejectsCodeSizeDisagreement(t *testing.T) {
	hash := common.Hash{1}
	address1 := common.Address{1}
	address2 := common.Address{2}
	addressBytes1 := eip8297.RightAlign32(address1[:])
	addressBytes2 := eip8297.RightAlign32(address2[:])
	addressHash1 := eip8297.HashBytes(addressBytes1[:])
	addressHash2 := eip8297.HashBytes(addressBytes2[:])
	state := &pbtImportState{
		headers: map[common.Hash]artifact.Header{
			addressHash1: {Kind: 1, AddressHash: addressHash1, CodeHash: hash, CodeSize: []byte{1}},
			addressHash2: {Kind: 1, AddressHash: addressHash2, CodeHash: hash, CodeSize: []byte{2}},
		},
		addresses: map[common.Hash]common.Address{addressHash1: address1, addressHash2: address2},
		code:      map[common.Hash]map[byte][]byte{hash: {0: bytes.Repeat([]byte{1}, eip8297.ValueLength)}},
	}
	require.ErrorContains(t, validatePBTImportState(state, eip8297.HashBytes), "code size disagrees")
}

func TestPBTImportRejectsKindOneDesignator(t *testing.T) {
	code := append([]byte{0xef, 0x01, 0x00}, bytes.Repeat([]byte{1}, 20)...)
	hash := common.Hash(keccak.Sum256(code))
	address := common.Address{1}
	addressBytes := eip8297.RightAlign32(address[:])
	addressHash := eip8297.HashBytes(addressBytes[:])
	chunks := eip8297.ChunkifyCode(code)
	stem := pbtCodeGroupStem(eip8297.HashBytes, hash, 0)
	groups := map[common.Hash]map[byte][]byte{stem: {0: chunks[0][:]}}
	state := &pbtImportState{
		headers:   map[common.Hash]artifact.Header{addressHash: {Kind: 1, AddressHash: addressHash, CodeHash: hash, CodeSize: []byte{23}}},
		addresses: map[common.Hash]common.Address{addressHash: address},
		code:      groups,
	}
	require.ErrorContains(t, validatePBTImportState(state, eip8297.HashBytes), "designator")
}

func TestPBTImportRejectsSurplusCodeGroup(t *testing.T) {
	address := common.Address{1}
	addressBytes := eip8297.RightAlign32(address[:])
	addressHash := eip8297.HashBytes(addressBytes[:])
	state := &pbtImportState{
		headers:   map[common.Hash]artifact.Header{addressHash: {Kind: 0, AddressHash: addressHash}},
		addresses: map[common.Hash]common.Address{addressHash: address},
		code:      map[common.Hash]map[byte][]byte{{2}: {0: bytes.Repeat([]byte{1}, eip8297.ValueLength)}},
	}
	require.ErrorContains(t, validatePBTImportState(state, eip8297.HashBytes), "surplus code group")
}

func TestPBTImportRejectsCodeChunkOutsideCodeSize(t *testing.T) {
	code := bytes.Repeat([]byte{1}, eip8297.ChunkDataLen)
	hash := common.Hash(keccak.Sum256(code))
	chunks := eip8297.ChunkifyCode(code)
	stem := pbtCodeGroupStem(eip8297.HashBytes, hash, 0)
	groups := map[common.Hash]map[byte][]byte{stem: {1: chunks[0][:]}}
	err := validatePBTImportCode(t, artifact.Header{Kind: 1, CodeHash: hash, CodeSize: []byte{1}}, groups)
	require.ErrorContains(t, err, "surplus code chunk")
}

func TestPBTImportEmptyStateHasZeroRoot(t *testing.T) {
	state := &pbtImportState{headers: map[common.Hash]artifact.Header{}, addresses: map[common.Hash]common.Address{}, code: map[common.Hash]map[byte][]byte{}}
	require.NoError(t, validatePBTImportState(state, eip8297.HashBytes))
	require.Equal(t, eip8297.EmptyTreeHash, eip8297.StateRootWithHash(nil, eip8297.HashBytes))
}

func TestPBTImportRejectsMissingAndSurplusPreimages(t *testing.T) {
	address := common.Address{1}
	address32 := eip8297.RightAlign32(address[:])
	position := eip8297.HashBytes(address32[:])
	expected := func(yield func([]byte) error) error {
		return yield(eip8297.TreeKey(eip8297.AccountZone, position[:], eip8297.BasicDataLeafKey))
	}
	var missing bytes.Buffer
	require.NoError(t, artifact.WritePreimages(&missing, []artifact.Preimage{}))
	require.Error(t, artifact.CheckPreimageSetAt(bytes.NewReader(missing.Bytes()), int64(missing.Len()), expected, eip8297.HashBytes))
	var surplus bytes.Buffer
	records := []artifact.Preimage{{Address: address}, {Address: common.Address{2}}}
	sort.Slice(records, func(i, j int) bool {
		left := keccak.Sum256(records[i].Address[:])
		right := keccak.Sum256(records[j].Address[:])
		return bytes.Compare(left[:], right[:]) < 0
	})
	require.NoError(t, artifact.WritePreimages(&surplus, records))
	require.Error(t, artifact.CheckPreimageSetAt(bytes.NewReader(surplus.Bytes()), int64(surplus.Len()), expected, eip8297.HashBytes))
}

func validatePBTImportCode(t *testing.T, header artifact.Header, groups map[common.Hash]map[byte][]byte) error {
	t.Helper()
	_, _, err := importCodeForHeader(header, groups, eip8297.HashBytes)
	return err
}
