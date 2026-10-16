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

package protocol

import (
	"math/big"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/execution/abi"
)

// TestSelectors pins the entrypoint selectors. A selector hashes the
// canonical signature, so it fixes the type and position of every field of
// the nested structs.
func TestSelectors(t *testing.T) {
	require.Equal(t, [4]byte{0xe4, 0xa4, 0x80, 0xe4}, PostAndVerifyBatchSelector)
	require.Equal(t, [4]byte{0xbf, 0x2e, 0xeb, 0xd3}, LoadExecutionTableSelector)
	require.Equal(t, [4]byte{0xc3, 0xfb, 0x5f, 0x3d}, ExecuteIncomingCrossChainCallSelector)
}

// TestStructLayout requires each struct to list its tuple's fields in order
// with matching tags: the decoder copies by position, the encoder by tag.
func TestStructLayout(t *testing.T) {
	for _, c := range []struct {
		value      any
		components []abi.ArgumentMarshaling
	}{
		{ExecutionEntry{}, executionEntryABI},
		{StaticExecutionEntry{}, staticExecutionEntryABI},
		{ProofSystemBatch{}, proofSystemBatchABI},
		{L2ExecutionEntry{}, l2ExecutionEntryABI},
		{L2StaticExecutionEntry{}, l2StaticExecutionEntryABI},
	} {
		checkLayout(t, reflect.TypeOf(c.value), c.components)
	}
}

func checkLayout(t *testing.T, typ reflect.Type, components []abi.ArgumentMarshaling) {
	t.Helper()
	require.Equal(t, len(components), typ.NumField(), typ.Name())
	for i, c := range components {
		f := typ.Field(i)
		require.Equal(t, c.Name, f.Tag.Get("abi"), "%s.%s", typ.Name(), f.Name)
		if c.Components != nil {
			checkLayout(t, f.Type.Elem(), c.Components)
		}
	}
}

// TestSolidityEntryHashes checks the entry hashes PublicInputsHashVectors.t.sol
// asserts for the entries it builds in _entry(1, 0x1111) and in
// testAllSharedInputDimensionsVector.
func TestSolidityEntryHashes(t *testing.T) {
	vectors, err := testfixtures.LoadPublicInputsVectors()
	require.NoError(t, err)
	byName := make(map[string]*testfixtures.PublicInputsVector)
	for i := range vectors.Vectors {
		byName[vectors.Vectors[i].Name] = &vectors.Vectors[i]
	}

	full := byName["full_entry_and_calldata_single_rollup_single_ps"]
	require.NotNil(t, full)
	entry := ExecutionEntry{
		RollupUpdates: []RollupUpdate{{
			RollupID:    1,
			EtherDelta:  new(big.Int),
			CurrentRoot: common.HexToHash("0x1111"),
			NewRoot:     common.HexToHash("0x2222"),
		}},
		ProxyEntryHash:      common.HexToHash("0x3333"),
		RollingHash:         common.HexToHash("0x4444"),
		DestinationRollupID: 1,
		Success:             true,
		ReturnData:          common.FromHex("0xdeadbeef"),
	}
	h, err := ExecutionEntryHash(&entry)
	require.NoError(t, err)
	require.Equal(t, full.EntryHashes, []common.Hash{h})

	all := byName["all_shared_input_dimensions"]
	require.NotNil(t, all)
	static := StaticExecutionEntry{
		ExpectedRoots:       []ExpectedRootPerRollup{{RollupID: 1, Root: common.HexToHash("0x1111")}},
		ProxyEntryHash:      common.HexToHash("0x5555"),
		RollingHash:         common.HexToHash("0x6666"),
		DestinationRollupID: 1,
		Success:             true,
		ReturnData:          common.FromHex("0xcafe"),
	}
	h, err = StaticExecutionEntryHash(&static)
	require.NoError(t, err)
	require.Equal(t, all.StaticEntryHashes, []common.Hash{h})
}

// TestEncodingMatchesReference compares execution/abi encodings of every
// struct with the reference encoder over generated values.
func TestEncodingMatchesReference(t *testing.T) {
	g := newValueGen(1)
	for range 200 {
		e := g.entry()
		enc, err := EncodeExecutionEntry(&e)
		require.NoError(t, err)
		require.Equal(t, refEncode(refEntry(&e)), enc)

		s := g.staticEntry()
		enc, err = EncodeStaticExecutionEntry(&s)
		require.NoError(t, err)
		require.Equal(t, refEncode(refStaticEntry(&s)), enc)

		b := g.batch()
		enc, err = postAndVerifyBatchMethod.Inputs.Pack(b)
		require.NoError(t, err)
		require.Equal(t, refEncode(refBatch(&b)), enc)

		l2, l2s := g.l2Entries(), g.l2StaticEntries()
		enc, err = l2TableArgs.Pack(l2, l2s)
		require.NoError(t, err)
		require.Equal(t, refEncode(refL2Entries(l2), refL2StaticEntries(l2s)), enc)
	}
}

func TestEncodeRejectsInvalidIntegers(t *testing.T) {
	pow2 := func(n uint) *big.Int { return new(big.Int).Lsh(big.NewInt(1), n) }
	minus1 := func(v *big.Int) *big.Int { return v.Sub(v, big.NewInt(1)) }
	for _, c := range []struct {
		name         string
		delta, value *big.Int
		err          error
	}{
		{"int192 min", new(big.Int).Neg(pow2(191)), new(big.Int), nil},
		{"int192 max", minus1(pow2(191)), new(big.Int), nil},
		{"uint256 max", new(big.Int), minus1(pow2(256)), nil},
		{"nil etherDelta", nil, new(big.Int), ErrNilInteger},
		{"etherDelta below int192", minus1(new(big.Int).Neg(pow2(191))), new(big.Int), ErrIntegerRange},
		{"etherDelta above int192", pow2(191), new(big.Int), ErrIntegerRange},
		{"nil value", new(big.Int), nil, ErrNilInteger},
		{"negative value", new(big.Int), big.NewInt(-1), ErrIntegerRange},
		{"value above uint256", new(big.Int), pow2(256), ErrIntegerRange},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := ExecutionEntry{
				RollupUpdates: []RollupUpdate{{EtherDelta: c.delta}},
				ExpectedL1ToL2Calls: []ExpectedCall{{
					Calls: []CrossChainCall{{Value: c.value}},
				}},
			}
			_, err := EncodeExecutionEntry(&e)
			if c.err == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, c.err)
		})
	}
	_, err := EncodeStaticExecutionEntry(&StaticExecutionEntry{L2ToL1Calls: []CrossChainCall{{}}})
	require.ErrorIs(t, err, ErrNilInteger)
	require.ErrorContains(t, err, "l2ToL1Calls[0].value")
}
