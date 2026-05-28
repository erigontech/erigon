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

package types

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// Our code assumed no struct would ever require RLP encoding of more than
// 16_777_216 bytes — but at high mgas large receipts can, and the
// incorrect RLP prefix length then led to incorrect trie roots.
func TestDeriveSha_ReceiptOver16MiB(t *testing.T) {
	const (
		nLogs           = 300_000
		logAddrHex      = "ab8e934ae169d0c87025f6cd25eef65050641d60"
		cumGas          = uint64(0xd915b58) // 227,629,912
		expectedRootHex = "8133caae197e6049123faab206e79972cd392bc749d403321280fc0c0e726763"
	)

	addrBytes, err := hex.DecodeString(logAddrHex)
	require.NoError(t, err)
	addr := common.Address(addrBytes)

	var topic common.Hash
	for i := range topic {
		topic[i] = 0xff
	}

	logs := make(Logs, nLogs)
	for i := range logs {
		logs[i] = &Log{Address: addr, Topics: []common.Hash{topic}, Data: nil}
	}

	r := &Receipt{
		Type:              LegacyTxType,
		Status:            ReceiptStatusSuccessful,
		CumulativeGasUsed: cumGas,
		Logs:              logs,
	}
	receipts := Receipts{r}
	r.Bloom = CreateBloom(receipts)

	var buf bytes.Buffer
	receipts.EncodeIndex(0, &buf)
	require.Greater(t, buf.Len(), 1<<24, "test only exercises the bug if the receipt RLP exceeds 16 MiB")

	expectedBytes, err := hex.DecodeString(expectedRootHex)
	require.NoError(t, err)
	require.Equal(t, common.Hash(expectedBytes), DeriveSha(receipts))
}
