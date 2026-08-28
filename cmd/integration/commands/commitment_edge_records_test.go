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

package commands

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func TestFormatCommitmentEdgeRecords(t *testing.T) {
	nodeKey := nibbles.EncodeKeyV3([]byte{1, 2})
	record := make([]byte, 1+2+32)
	record[0] = 0x10
	record[2] = 1
	record[3] = 1
	var records [16][]byte
	records[3] = record
	records[7] = []byte{}

	rendered, err := formatCommitmentEdgeRecords(nodeKey, records, 1<<3|1<<7)
	require.NoError(t, err)
	require.Contains(t, rendered, "child 3")
	require.Contains(t, rendered, hex.EncodeToString(nibbles.ChildKeyV3(nodeKey, 3)))
	require.Contains(t, rendered, hex.EncodeToString(record))
	require.Contains(t, rendered, "child 7")
	require.Contains(t, rendered, "<tombstone>")
}

func TestFormatCommitmentEdgeRecordsRejectsLegacyValue(t *testing.T) {
	var records [16][]byte
	records[1] = []byte{0, 1, 0, 1}

	_, err := formatCommitmentEdgeRecords(nibbles.EncodeKeyV3(nil), records, 1<<1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid encoding")
}
