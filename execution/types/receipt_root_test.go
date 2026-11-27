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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/rlp"
)

func TestSetReceiptBloomsMatchesCreateBloom(t *testing.T) {
	t.Parallel()
	a := common.HexToAddress("0x1111111111111111111111111111111111111111")
	topic := common.HexToHash("0x01")
	receipts := Receipts{
		{Logs: Logs{
			{Address: a, Topics: []common.Hash{topic, common.HexToHash("0x02")}},
			{Address: common.HexToAddress("0x2222222222222222222222222222222222222222")},
		}},
		{Bloom: Bloom{0xff}},
		{Logs: Logs{
			{Address: a, Topics: []common.Hash{topic}},
			// A topic equal to the left-padded address must not reuse the address bits.
			{Address: a, Topics: []common.Hash{common.BytesToHash(a[:])}},
		}},
	}
	var want Bloom
	wantPerReceipt := make([]Bloom, len(receipts))
	for i, r := range receipts {
		wantPerReceipt[i] = CreateBloom(Receipts{r})
		want.Or(&wantPerReceipt[i])
	}

	require.Equal(t, want, SetReceiptBlooms(receipts))
	for i, r := range receipts {
		require.Equal(t, wantPerReceipt[i], r.Bloom, "receipt %d", i)
	}
}

func TestReceiptConsensusEncodingMatchesReflection(t *testing.T) {
	t.Parallel()
	addr := common.HexToAddress("0x3333333333333333333333333333333333333333")
	postState := common.HexToHash("0xabcdef")
	manyTopics := []common.Hash{{1}, {2}, {3}, {4}}
	cases := map[string]*Receipt{
		"legacy post-state": {Type: LegacyTxType, PostState: postState[:], CumulativeGasUsed: 21_000},
		"legacy failed":     {Type: LegacyTxType, Status: ReceiptStatusFailed},
		"typed no logs":     {Type: DynamicFeeTxType, Status: ReceiptStatusSuccessful, CumulativeGasUsed: 1},
		"typed small gas":   {Type: AccessListTxType, Status: ReceiptStatusSuccessful, CumulativeGasUsed: 0x7f},
		"typed logs": {Type: BlobTxType, Status: ReceiptStatusSuccessful, CumulativeGasUsed: 30_000_000, Logs: Logs{
			{Address: addr},
			{Address: addr, Topics: manyTopics, Data: []byte{0x01}},
			{Address: addr, Topics: manyTopics[:1], Data: bytes.Repeat([]byte{0xaa}, 300)},
		}},
		"typed max gas": {Type: SetCodeTxType, Status: ReceiptStatusSuccessful, CumulativeGasUsed: ^uint64(0), Logs: Logs{{Address: addr, Data: []byte{0x80}}}},
	}
	for name, r := range cases {
		t.Run(name, func(t *testing.T) {
			r.Bloom = CreateBloom(Receipts{r})
			want, err := rlp.EncodeToBytes(&receiptRLP{r.statusEncoding(), r.CumulativeGasUsed, r.Bloom, r.Logs})
			require.NoError(t, err)
			if r.Type != LegacyTxType {
				want = append([]byte{r.Type}, want...)
			}

			var got bytes.Buffer
			Receipts{r}.EncodeIndex(0, &got)
			require.Equal(t, want, got.Bytes())

			binary, err := r.MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, want, binary)
		})
	}
}

func TestTransferReceiptsRootPinned(t *testing.T) {
	t.Parallel()
	receipts := transferReceipts(300)
	bloom := SetReceiptBlooms(receipts)
	require.Equal(t, common.HexToHash("0xcdc4c9b414960ad3e190f4ac206ad843de3c389129c26008b3689e59d8698070"), crypto.Keccak256Hash(bloom[:]))
	require.Equal(t, common.HexToHash("0x62118ac0682f69efc486213e9303a5233e6f1195674769ea2005da3b62157b55"), DeriveSha(receipts))
}
