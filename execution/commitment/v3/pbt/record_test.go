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

package pbt

import (
	"bytes"
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestRecordKeys(t *testing.T) {
	path := eip8297.PathFromBits([]byte{0xa0}, 4)
	key, err := EncodeRowKey(&path)
	require.NoError(t, err)
	require.Equal(t, []byte{0xa0, 4}, key)

	_, err = EncodeRowKey(eip8297Path(5))
	require.ErrorIs(t, err, errorRule(KeyError))
	require.Equal(t, []byte{0x08}, GlobalRootKey())

	addr := bytes.Repeat([]byte{0x11}, 20)
	slot := make([]byte, 32)
	slot[31] = 64
	storageKey := eip8297.TreeKeyStorage(addr, slot)
	bucketPath := eip8297.PathFromBits(storageKey[:33], 264)
	want := eip8297.AppendBitPath(nil, &bucketPath)
	got, err := BucketRootKey(addr)
	require.NoError(t, err)
	require.Equal(t, want, got)

	_, err = BucketRootKey(bytes.Repeat([]byte{1}, 19))
	require.ErrorIs(t, err, errorRule(KeyError))
}

func TestRecordRoundTrips(t *testing.T) {
	addr := bytes.Repeat([]byte{0x22}, 20)
	globalKey := GlobalRootKey()
	account := accountKey(0, eip8297.BasicDataLeafKey)
	codeHash := [32]byte(empty.CodeHash)
	slot := make([]byte, 32)
	slot[31] = 64
	storage := eip8297.TreeKeyStorage(addr, slot)
	bucketKey, err := BucketRootKey(addr)
	require.NoError(t, err)

	tests := []struct {
		name string
		key  []byte
		rec  Record
	}{
		{
			name: "global leaf root",
			key:  globalKey,
			rec:  Record{Form: LeafRoot, Cells: [16]Cell{0: {Kind: LeafCell, Key: account, Value: basicValue()}}},
		},
		{
			name: "bucket leaf root",
			key:  bucketKey,
			rec:  Record{Form: LeafRoot, Cells: [16]Cell{0: {Kind: LeafCell, Key: storage, Value: storageValue()}}},
		},
		{
			name: "global extension root",
			key:  globalKey,
			rec: Record{
				Form:    ExtRoot,
				SelfExt: eip8297.PathFromBits([]byte{0x00}, 4),
				Left:    hash(0x31),
				Right:   hash(0x32),
			},
		},
		{
			name: "row branch and leaf",
			key:  rowKey(8),
			rec: Record{Form: RowRoot, Cells: [16]Cell{
				1: {Kind: LeafCell, Key: accountKey(1, eip8297.BasicDataLeafKey), Value: basicValue()},
				2: {Kind: BranchCell, Prefix: eip8297.PathFromBits([]byte{0xa0}, 4), Left: hash(0x41), Right: hash(0x42)},
			}},
		},
		{
			name: "sixteen account cells",
			key:  rowKey(8),
			rec:  sixteenCellRecord(),
		},
		{
			name: "account stem empty suffix",
			key:  rowKey(268),
			rec: Record{Form: RowRoot, Cells: [16]Cell{
				0: {Kind: LeafCell, Key: accountKey(0, eip8297.BasicDataLeafKey), Value: basicValue()},
				1: {Kind: LeafCell, Key: accountKey(0, eip8297.CodeHashLeafKey), Value: codeHash},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := EncodeRecord(tt.key, &tt.rec)
			require.NoError(t, err)
			got, err := DecodeRecord(tt.key, data)
			require.NoError(t, err)
			require.Equal(t, tt.rec, got)
		})
	}
}

func TestRecordGlobalRowRootSupportsAccountAndStorage(t *testing.T) {
	storage := eip8297.TreeKeyStorage(bytes.Repeat([]byte{0x22}, 20), storageSlotKey())
	record := Record{Form: RowRoot, Cells: [16]Cell{
		0:  {Kind: LeafCell, Key: accountKey(0, eip8297.BasicDataLeafKey), Value: basicValue()},
		15: {Kind: LeafCell, Key: storage, Value: storageValue()},
	}}

	data, err := EncodeRecord(GlobalRootKey(), &record)
	require.NoError(t, err)
	got, err := DecodeRecord(GlobalRootKey(), data)
	require.NoError(t, err)
	require.Equal(t, record, got)
}

func TestRecordGlobalRowRootByteOracleForCodeAndStorage(t *testing.T) {
	code := make([]byte, 34)
	code[0] = eip8297.CodeZone
	code[33] = 7
	storage := eip8297.TreeKeyStorage(bytes.Repeat([]byte{0x33}, 20), storageSlotKey())
	codeValue := [32]byte{1}
	record := Record{Form: RowRoot, Cells: [16]Cell{
		0:  {Kind: LeafCell, Key: code, Value: codeValue},
		15: {Kind: LeafCell, Key: storage, Value: storageValue()},
	}}

	got, err := EncodeRecord(GlobalRootKey(), &record)
	require.NoError(t, err)
	require.Equal(t, globalCodeStorageOracle(code, storage), got)
}

func TestRecordRejectsInvalidRowKeys(t *testing.T) {
	branchRow := append([]byte{0, 0, 3, 0, 0}, make([]byte, 128)...)
	ordinaryEmpty := append([]byte{0, 0x80, 1, 0, 0}, make([]byte, 128)...)

	_, err := DecodeRecord([]byte{2, 0}, branchRow)
	require.ErrorIs(t, err, errorRule(ZoneError))

	_, err = DecodeRecord([]byte{0}, ordinaryEmpty)
	require.ErrorIs(t, err, errorRule(KeyError))
	_, err = DecodeRecord(GlobalRootKey(), ordinaryEmpty)
	require.NoError(t, err)
}

func TestRecordRejectsOldFormatBeforeKeyValidation(t *testing.T) {
	data := make([]byte, 0, 68)
	for range 2 {
		data = append(data, 0x12, 0)
		data = append(data, make([]byte, 32)...)
	}
	_, err := DecodeRecord([]byte{0, 0, 1}, data)
	var recordErr *RecordError
	require.ErrorAs(t, err, &recordErr)
	require.Equal(t, FormatError, recordErr.Rule)
	require.Contains(t, err.Error(), "rebuild")
}

func TestRecordRejectsExtensionsPastZoneKeyLength(t *testing.T) {
	for _, bitLen := range []int16{260, 261} {
		t.Run(string(rune('a'+bitLen-260)), func(t *testing.T) {
			record := Record{Form: RowRoot, Cells: [16]Cell{
				0: {Kind: BranchCell, Prefix: eip8297.PathFromBits(make([]byte, (int(bitLen)+7)/8), bitLen), Left: hash(1), Right: hash(2)},
				1: {Kind: BranchCell, Left: hash(3), Right: hash(4)},
			}}
			_, err := EncodeRecord(rowKey(8), &record)
			require.ErrorIs(t, err, errorRule(ExtensionLengthError))
		})
	}

	for _, bitLen := range []int16{272, 273} {
		t.Run(string(rune('a'+bitLen-272)), func(t *testing.T) {
			record := Record{Form: ExtRoot, SelfExt: eip8297.PathFromBits(make([]byte, (int(bitLen)+7)/8), bitLen), Left: hash(1), Right: hash(2)}
			_, err := EncodeRecord(GlobalRootKey(), &record)
			require.ErrorIs(t, err, errorRule(SelfExtensionLengthError))
		})
	}
}

func TestRecordRoundTripsLongestLegalExtensions(t *testing.T) {
	row := Record{Form: RowRoot, Cells: [16]Cell{
		0: {Kind: BranchCell, Prefix: eip8297.PathFromBits(make([]byte, 33), 259), Left: hash(1), Right: hash(2)},
		1: {Kind: BranchCell, Left: hash(3), Right: hash(4)},
	}}
	data, err := EncodeRecord(rowKey(8), &row)
	require.NoError(t, err)
	got, err := DecodeRecord(rowKey(8), data)
	require.NoError(t, err)
	require.Equal(t, row, got)

	ext := Record{Form: ExtRoot, SelfExt: eip8297.PathFromBits(make([]byte, 34), 271), Left: hash(1), Right: hash(2)}
	data, err = EncodeRecord(GlobalRootKey(), &ext)
	require.NoError(t, err)
	got, err = DecodeRecord(GlobalRootKey(), data)
	require.NoError(t, err)
	require.Equal(t, ext, got)
}

func TestRecordTombstone(t *testing.T) {
	key := rowKey(8)
	record := Record{}
	data, err := EncodeRecord(key, &record)
	require.NoError(t, err)
	require.Empty(t, data)
	got, err := DecodeRecord(key, nil)
	require.NoError(t, err)
	require.Equal(t, Record{}, got)
}

func TestRecordRoundTripsEveryRowCellCount(t *testing.T) {
	key := rowKey(8)
	for count := 2; count <= 16; count++ {
		t.Run(string(rune('a'+count)), func(t *testing.T) {
			var record Record
			record.Form = RowRoot
			for slot := 0; slot < count; slot++ {
				record.Cells[slot] = Cell{Kind: LeafCell, Key: accountKey(byte(slot), eip8297.BasicDataLeafKey), Value: basicValue()}
			}
			data, err := EncodeRecord(key, &record)
			require.NoError(t, err)
			got, err := DecodeRecord(key, data)
			require.NoError(t, err)
			require.Equal(t, record, got)
		})
	}
}

func TestRecordByteOracle(t *testing.T) {
	key := rowKey(8)
	rec := Record{Form: RowRoot, Cells: [16]Cell{
		1: {Kind: LeafCell, Key: accountKey(1, eip8297.BasicDataLeafKey), Value: basicValue()},
		2: {Kind: BranchCell, Prefix: eip8297.PathFromBits([]byte{0x80}, 1), Left: hash(0x51), Right: hash(0x52)},
	}}
	got, err := EncodeRecord(key, &rec)
	require.NoError(t, err)

	want := manualRowOracle()
	require.Equal(t, want, got)
}

func TestRecordRejectsNonCanonical(t *testing.T) {
	validKey := rowKey(8)

	selfExt := append([]byte{0x10, 0, 3, 0xe0}, make([]byte, 64)...)
	globalKey := GlobalRootKey()
	rootExtKey := rowKey(8)
	rootExt := append([]byte{0x10, 0, 4, 0x00}, make([]byte, 64)...)
	baseLeaf := manualRowOracle()
	baseLeaf = append([]byte(nil), baseLeaf...)

	tests := []struct {
		name string
		key  []byte
		data []byte
		want RecordErrorRule
	}{
		{name: "format", key: validKey, data: []byte{0x01}, want: FormatError},
		{name: "format 0x10", key: validKey, data: []byte{0x10}, want: FormatError},
		{name: "format leaf bit", key: validKey, data: []byte{0x81}, want: FormatError},
		{name: "format branch bit", key: validKey, data: []byte{0x02}, want: FormatError},
		{name: "reserved bit", key: validKey, data: []byte{0x40}, want: ReservedError},
		{name: "both root bits", key: globalKey, data: []byte{0x90}, want: RootBitsError},
		{name: "extension root key", key: rootExtKey, data: rootExt, want: RootKeyError},
		{name: "has extension without mask", key: validKey, data: append([]byte{0x20, 0, 6, 0, 2, 0, 0}, make([]byte, 64)...), want: HasExtError},
		{name: "leaf outside child mask", key: validKey, data: append([]byte{0, 0, 2, 0, 4}, make([]byte, 64)...), want: MaskError},
		{name: "extension overlaps leaf", key: validKey, data: append([]byte{0x20, 0, 2, 0, 2, 0, 2}, make([]byte, 64)...), want: MaskError},
		{name: "one cell", key: validKey, data: validSingleLeafRow(), want: CellCountError},
		{name: "extension length", key: validKey, data: append(append([]byte{0x20, 0, 3, 0, 2, 0, 1}, make([]byte, 64)...), 0, 0), want: ExtensionLengthError},
		{name: "self extension length", key: globalKey, data: selfExt, want: SelfExtensionLengthError},
		{name: "self extension padding", key: globalKey, data: append([]byte{0x10, 0, 4, 0xaf}, make([]byte, 64)...), want: PaddingError},
		{name: "extension padding", key: validKey, data: extensionPadRow(), want: PaddingError},
		{name: "suffix padding", key: validKey, data: suffixPadRow(), want: PaddingError},
		{name: "exact length", key: validKey, data: append(baseLeaf, 0), want: LengthError},
		{name: "compact value", key: validKey, data: compactValueRow(), want: CompactValueError},
		{name: "suffix length", key: rowKey(528), data: append([]byte{0, 0, 3, 0, 2}, make([]byte, 64)...), want: SuffixLengthError},
		{name: "reserved zone", key: []byte{0x02, 0}, data: reservedZoneRow(), want: ZoneError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got *RecordError
			err := func() error {
				_, err := DecodeRecord(tt.key, tt.data)
				return err
			}()
			require.ErrorAs(t, err, &got)
			require.Equal(t, tt.want, got.Rule)
			if tt.want == FormatError {
				require.Contains(t, err.Error(), "rebuild")
			}
		})
	}
}

var fuzzRecordBodies atomic.Uint64

func FuzzRecordDecodeCanonical(f *testing.F) {
	for _, seed := range [][]byte{manualRowOracle(), {0}, {0x80}, {0x10, 0, 4, 0x00}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runRecordFuzzBody(data)
	})
}

func TestRecordFuzzSeeds(t *testing.T) {
	fuzzRecordBodies.Store(0)
	for _, seed := range [][]byte{manualRowOracle(), {0}, {0x80}, {0x10, 0, 4, 0x00}} {
		runRecordFuzzBody(seed)
	}
	require.Equal(t, uint64(4), fuzzRecordBodies.Load())
}

func sixteenCellRecord() Record {
	var rec Record
	rec.Form = RowRoot
	for slot := range 16 {
		rec.Cells[slot] = Cell{Kind: LeafCell, Key: accountKey(byte(slot), eip8297.BasicDataLeafKey), Value: basicValue()}
	}
	return rec
}

func basicValue() [32]byte {
	value, err := eip8297.EncodeBasicData(7, uint256.NewInt(9), 3)
	if err != nil {
		panic(err)
	}
	return value
}

func storageValue() [32]byte {
	return eip8297.EncodeStorageValue([]byte{0x42})
}

func storageSlotKey() []byte {
	slot := make([]byte, 32)
	slot[31] = 64
	return slot
}

func globalCodeStorageOracle(code, storage []byte) []byte {
	out := []byte{0, 0x80, 1, 0x80, 1}
	codePath := eip8297.PathFromBits(code, 272)
	out = append(out, manualPackedRange(&codePath, 4, 272)...)
	out = append(out, 1, 1)
	storagePath := eip8297.PathFromBits(storage, 528)
	out = append(out, manualPackedRange(&storagePath, 4, 528)...)
	return append(out, 1, 0x42)
}

func accountKey(slot, subIndex byte) []byte {
	key := make([]byte, 34)
	key[0] = eip8297.AccountZone
	key[1] = slot << 4
	key[33] = subIndex
	return key
}

func runRecordFuzzBody(data []byte) {
	fuzzRecordBodies.Add(1)
	_, _ = DecodeRecord(rowKey(8), data)
}

func hash(v byte) common.Hash {
	return common.BytesToHash(bytes.Repeat([]byte{v}, 32))
}

func rowKey(bitLen int16) []byte {
	path := eip8297.PathFromBits(make([]byte, (int(bitLen)+7)/8), bitLen)
	if bitLen == 8 {
		path.SetBitAt(0, 0)
	}
	return eip8297.AppendBitPath(nil, &path)
}

func eip8297Path(bitLen int16) *eip8297.Bitpath {
	path := eip8297.PathFromBits(nil, bitLen)
	return &path
}

func errorRule(rule RecordErrorRule) error {
	return &RecordError{Rule: rule}
}

func manualRowOracle() []byte {
	path := eip8297.PathFromBits([]byte{0}, 8)
	leafKey := accountKey(1, eip8297.BasicDataLeafKey)
	leafPath := eip8297.PathFromBits(leafKey, 272)
	want := []byte{0x20, 0, 6, 0, 2, 0, 4}
	left, right := hash(0x51), hash(0x52)
	want = append(want, left[:]...)
	want = append(want, right[:]...)
	want = append(want, 0, 1, 0x80)
	suffix := manualPackedRange(&leafPath, 12, 272)
	want = append(want, suffix...)
	want = append(want, 5, 0x02, 0x21, 0x03, 0x07, 0x09)
	_ = path
	return want
}

func manualPackedRange(path *eip8297.Bitpath, from, to int16) []byte {
	out := make([]byte, (int(to-from)+7)/8)
	for i := from; i < to; i++ {
		if path.Bit(i) != 0 {
			j := i - from
			out[j/8] |= 1 << (7 - uint(j%8))
		}
	}
	return out
}

func validSingleLeafRow() []byte {
	key := accountKey(1, eip8297.BasicDataLeafKey)
	path := eip8297.PathFromBits(key, 272)
	out := []byte{0, 0, 2, 0, 2}
	out = append(out, manualPackedRange(&path, 12, 272)...)
	out = append(out, 5, 0x02, 0x21, 0x03, 0x07, 0x09)
	return out
}

func extensionPadRow() []byte {
	out := []byte{0x20, 0, 3, 0, 2, 0, 1}
	out = append(out, make([]byte, 64)...)
	out = append(out, 0, 1, 0x01)
	return out
}

func suffixPadRow() []byte {
	out := []byte{0, 0, 3, 0, 2}
	out = append(out, make([]byte, 64)...)
	out = append(out, make([]byte, 33)...)
	out = append(out, 5, 0x02, 0x21, 0x03, 0x07, 0x09)
	out[5+64+33-1] = 0x01
	return out
}

func compactValueRow() []byte {
	out := manualRowOracle()
	out = append([]byte(nil), out[:len(out)-6]...)
	out = append(out, 2, 0, 1)
	return out
}

func reservedZoneRow() []byte {
	out := []byte{0, 0, 5, 0, 4}
	out = append(out, make([]byte, 64)...)
	return out
}
