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

package commitment

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplacePlainKeysRejectsEdgeRecord(t *testing.T) {
	d := recordTestData("branch", nil)
	record := EncodeBranchChild(0, &d)

	_, err := BranchData(record).ReplacePlainKeysForFormat(nil, func([]byte, bool) ([]byte, error) {
		return nil, nil
	}, true)
	require.ErrorIs(t, err, ErrEdgeRecord, "ReplacePlainKeys accepted a %d-byte edge record: %x", len(record), record)
}

func TestLegacyRowParsersRejectEdgeRecord(t *testing.T) {
	d := recordTestData("branch", nil)
	record := BranchData(EncodeBranchChild(0, &d))
	require.True(t, record.IsEdgeRecord())

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "decodeCells",
			call: func() error {
				_, _, _, err := record.decodeCellsForFormat(true)
				return err
			},
		},
		{
			name: "Validate",
			call: func() error { return record.ValidateForFormat(nil, true) },
		},
		{
			name: "IsComplete",
			call: func() error {
				_, err := record.IsCompleteForFormat(true)
				return err
			},
		},
		{
			name: "ChildCount",
			call: func() error {
				_, err := record.ChildCountForFormat(true)
				return err
			},
		},
		{
			name: "VerifyBranchHashes",
			call: func() error { return VerifyBranchHashesForFormat(nil, record, nil, nil, true) },
		},
		{
			name: "DecodeBranchAndCollectStat",
			call: func() error {
				_, err := DecodeBranchAndCollectStatForFormat([]byte{0x01}, record, VariantHexPatriciaTrie, true)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.ErrorIs(t, test.call(), ErrEdgeRecord, "%s accepted an edge record: %x", test.name, record)
		})
	}
}

func TestLegacyRowWithEdgeShapedPrefixIsParsedAsLegacy(t *testing.T) {
	var cells [16]cellEncodeData
	cells[12] = recordTestData("branch", nil)
	legacy, err := NewBranchEncoder(1024).EncodeBranch(1<<12, 1<<12, 1<<12, &cells)
	require.NoError(t, err)
	require.True(t, BranchData(legacy).IsEdgeRecord())

	count, err := BranchData(legacy).ChildCount()
	require.NoError(t, err)
	require.Equal(t, 1, count)
}
