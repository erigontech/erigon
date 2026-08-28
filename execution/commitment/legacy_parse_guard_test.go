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

	_, err := BranchData(record).ReplacePlainKeys(nil, func([]byte, bool) ([]byte, error) {
		return nil, nil
	})
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
				_, _, _, err := record.decodeCells()
				return err
			},
		},
		{
			name: "Validate",
			call: func() error { return record.Validate(nil) },
		},
		{
			name: "IsComplete",
			call: func() error {
				_, err := record.IsComplete()
				return err
			},
		},
		{
			name: "ChildCount",
			call: func() error {
				_, err := record.ChildCount()
				return err
			},
		},
		{
			name: "VerifyBranchHashes",
			call: func() error { return VerifyBranchHashes(nil, record, nil, nil) },
		},
		{
			name: "DecodeBranchAndCollectStat",
			call: func() error {
				_, err := DecodeBranchAndCollectStat([]byte{0x01}, record, VariantHexPatriciaTrie)
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
