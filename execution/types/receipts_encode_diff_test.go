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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
)

// Value-typed mirrors of the consensus encodings. They deliberately carry no
// EncodeRLP method, so rlp.Encode reaches them through the reflection encoder
// and they stay an oracle independent of the hand-written one under test.
type refLog struct {
	Address common.Address
	Topics  []common.Hash
	Data    []byte
}

type refReceipt struct {
	PostStateOrStatus []byte
	CumulativeGasUsed uint64
	Bloom             Bloom
	Logs              []refLog
}

func referenceEncodeIndex(t *testing.T, r *Receipt) []byte {
	t.Helper()
	logs := make([]refLog, len(r.Logs))
	for i, l := range r.Logs {
		logs[i] = refLog{Address: l.Address, Topics: l.Topics, Data: l.Data}
	}
	var buf bytes.Buffer
	if r.Type != LegacyTxType {
		buf.WriteByte(r.Type)
	}
	require.NoError(t, rlp.Encode(&buf, &refReceipt{
		PostStateOrStatus: r.statusEncoding(),
		CumulativeGasUsed: r.CumulativeGasUsed,
		Bloom:             r.Bloom,
		Logs:              logs,
	}))
	return buf.Bytes()
}

// TestEncodeIndexMatchesReflection pins the hand-written consensus encoder against
// the reflection encoder across receipt types, status vs. post-state, and the string
// and list length-prefix boundaries.
func TestEncodeIndexMatchesReflection(t *testing.T) {
	rnd := rand.New(rand.NewSource(42)) //nolint:gosec
	randBytes := func(n int) []byte {
		b := make([]byte, n)
		rnd.Read(b)
		return b
	}
	randTopics := func(n int) []common.Hash {
		if n == 0 {
			return nil
		}
		topics := make([]common.Hash, n)
		for i := range topics {
			rnd.Read(topics[i][:])
		}
		return topics
	}

	// Sizes straddling the 55/56-byte short-vs-long string boundary and the
	// 55/56-byte list boundary that the payload length prefix switches on.
	dataSizes := []int{0, 1, 2, 31, 32, 55, 56, 57, 1024}
	topicCounts := []int{0, 1, 2, 4}
	types := []uint8{LegacyTxType, AccessListTxType, DynamicFeeTxType, BlobTxType, SetCodeTxType}

	for _, txType := range types {
		for _, nTopics := range topicCounts {
			for _, dataLen := range dataSizes {
				for _, nLogs := range []int{0, 1, 3} {
					for _, failed := range []bool{false, true} {
						for _, postState := range []bool{false, true} {
							logs := make(Logs, nLogs)
							for i := range logs {
								var addr common.Address
								rnd.Read(addr[:])
								logs[i] = &Log{
									Address: addr,
									Topics:  randTopics(nTopics),
									Data:    randBytes(dataLen),
								}
							}
							r := &Receipt{
								Type:              txType,
								CumulativeGasUsed: rnd.Uint64(),
								Logs:              logs,
							}
							if failed {
								r.Status = ReceiptStatusFailed
							} else {
								r.Status = ReceiptStatusSuccessful
							}
							if postState {
								r.PostState = randBytes(32)
							}
							r.Bloom = CreateBloom(Receipts{r})

							var got bytes.Buffer
							Receipts{r}.EncodeIndex(0, &got)
							require.Equal(t, referenceEncodeIndex(t, r), got.Bytes(),
								"type=%d topics=%d dataLen=%d logs=%d failed=%v postState=%v",
								txType, nTopics, dataLen, nLogs, failed, postState)
						}
					}
				}
			}
		}
	}
}

// TestEncodeIndexSingleByteCumulativeGas covers the RLP quirk that a value below
// 0x80 is its own encoding, which the hand-written size calculation must agree with.
func TestEncodeIndexSingleByteCumulativeGas(t *testing.T) {
	for _, gas := range []uint64{0, 1, 127, 128, 255, 256, 1 << 32, 1<<64 - 1} {
		r := &Receipt{Type: LegacyTxType, Status: ReceiptStatusSuccessful, CumulativeGasUsed: gas}
		var got bytes.Buffer
		Receipts{r}.EncodeIndex(0, &got)
		require.Equal(t, referenceEncodeIndex(t, r), got.Bytes(), "gas=%d", gas)
	}
}

// TestEncodeIndexNilLog pins that a nil *Log encodes as an empty list rather than
// panicking, matching the reflect-based encoder it replaced.
func TestEncodeIndexNilLog(t *testing.T) {
	r := &Receipt{
		Type:              LegacyTxType,
		Status:            ReceiptStatusSuccessful,
		CumulativeGasUsed: 1,
		Logs:              Logs{nil, {Address: common.Address{1}, Topics: []common.Hash{{2}}}, nil},
	}
	var want bytes.Buffer
	require.NoError(t, rlp.Encode(&want, &receiptRLP{r.statusEncoding(), r.CumulativeGasUsed, r.Bloom, r.Logs}))

	var got bytes.Buffer
	Receipts{r}.EncodeIndex(0, &got)
	require.Equal(t, want.Bytes(), got.Bytes())
}

// TestEncodeIndexUnsupportedType pins that an unrecognised receipt type writes
// nothing at all, leaving DeriveSha to fail on the root comparison instead.
func TestEncodeIndexUnsupportedType(t *testing.T) {
	r := &Receipt{Type: 0x7f, Status: ReceiptStatusSuccessful, CumulativeGasUsed: 1}
	var got bytes.Buffer
	Receipts{r}.EncodeIndex(0, &got)
	require.Empty(t, got.Bytes())
}
