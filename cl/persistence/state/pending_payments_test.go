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

package state_accessors

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

const testSlotsPerEpoch = uint64(4)

func pendingPayments() *solid.VectorSSZ[*cltypes.BuilderPendingPayment] {
	return solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](int(2 * testSlotsPerEpoch))
}

func pendingPayment(value uint64) *cltypes.BuilderPendingPayment {
	return &cltypes.BuilderPendingPayment{
		Weight: value,
		Withdrawal: &cltypes.BuilderPendingWithdrawal{
			FeeRecipient: common.Address{byte(value)},
			Amount:       value + 1,
			BuilderIndex: value + 2,
		},
		ProposerIndex: value + 3,
	}
}

func encodePendingPaymentsRow(t *testing.T, writer *PendingPaymentsWriter, slot uint64, post *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) []byte {
	t.Helper()
	var row bytes.Buffer
	require.NoError(t, writer.WriteRow(&row, slot, post))
	return bytes.Clone(row.Bytes())
}

func TestPendingPaymentsRowLayout(t *testing.T) {
	payments := pendingPayments()
	payments.Set(3, pendingPayment(11))
	encoded, err := payments.EncodeSSZ(nil)
	require.NoError(t, err)

	writer := NewPendingPaymentsWriter(testSlotsPerEpoch)
	base := encodePendingPaymentsRow(t, writer, 0, payments)
	expectedBase := append([]byte{0, 0, 0, 0, 0, 0, 0, 0, 'G', 'L', 'O', 'A', 1, 2}, encoded...)
	require.Equal(t, expectedBase, base)

	pre := pendingPayments()
	require.NoError(t, writer.Initialize(pre))
	delta := encodePendingPaymentsRow(t, writer, 1, payments)
	expectedDelta := []byte{0, 0, 0, 0, 0, 0, 0, 0, 'G', 'L', 'O', 'A', 1, 3, 1, 0, 3, 0}
	entry, err := payments.Get(3).EncodeSSZ(nil)
	require.NoError(t, err)
	expectedDelta = append(expectedDelta, entry...)
	require.Equal(t, expectedDelta, delta)
}

func TestPendingPaymentsWriterLifecycle(t *testing.T) {
	pre := pendingPayments()
	post := pendingPayments()
	post.Set(1, pendingPayment(10))
	writer := NewPendingPaymentsWriter(testSlotsPerEpoch)

	require.NoError(t, writer.Initialize(pre))
	first := encodePendingPaymentsRow(t, writer, 1, post)
	require.Equal(t, byte(2), first[13])

	require.NoError(t, writer.Initialize(post))
	post.Get(1).Weight++
	delta := encodePendingPaymentsRow(t, writer, 2, post)
	require.Equal(t, byte(3), delta[13])
	require.Equal(t, uint16(1), binary.LittleEndian.Uint16(delta[14:]))

	require.NoError(t, writer.Initialize(post))
	epochBase := encodePendingPaymentsRow(t, writer, testSlotsPerEpoch, post)
	require.Equal(t, byte(2), epochBase[13])

	writer = NewPendingPaymentsWriter(testSlotsPerEpoch)
	missingBaselineBase := encodePendingPaymentsRow(t, writer, testSlotsPerEpoch+1, post)
	require.Equal(t, byte(2), missingBaselineBase[13])
	stillMissingBaselineBase := encodePendingPaymentsRow(t, writer, testSlotsPerEpoch+2, post)
	require.Equal(t, byte(2), stillMissingBaselineBase[13])

	require.NoError(t, writer.Initialize(post))
	noChange := encodePendingPaymentsRow(t, writer, testSlotsPerEpoch+3, post)
	require.Equal(t, byte(3), noChange[13])
	require.Equal(t, uint16(0), binary.LittleEndian.Uint16(noChange[14:]))
}

func TestPendingPaymentsWriterCopiesBaselineValues(t *testing.T) {
	pre := pendingPayments()
	pre.Set(5, pendingPayment(20))
	writer := NewPendingPaymentsWriter(testSlotsPerEpoch)
	require.NoError(t, writer.Initialize(pre))
	encodePendingPaymentsRow(t, writer, 0, pre)
	require.NoError(t, writer.Initialize(pre))

	pre.Get(5).Weight++
	pre.Get(5).Withdrawal.Amount++
	row := encodePendingPaymentsRow(t, writer, 1, pre)
	require.Equal(t, byte(3), row[13])
	require.Equal(t, uint16(1), binary.LittleEndian.Uint16(row[14:]))
	require.Equal(t, uint16(5), binary.LittleEndian.Uint16(row[16:]))
}

func clonePendingPayments(t *testing.T, src *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) *solid.VectorSSZ[*cltypes.BuilderPendingPayment] {
	t.Helper()
	encoded, err := src.EncodeSSZ(nil)
	require.NoError(t, err)
	dst := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](src.Length())
	require.NoError(t, dst.DecodeSSZ(encoded, 0))
	return dst
}

func assertPendingPaymentsEqual(t *testing.T, expected, actual *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) {
	t.Helper()
	expectedSSZ, err := expected.EncodeSSZ(nil)
	require.NoError(t, err)
	actualSSZ, err := actual.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, expectedSSZ, actualSSZ)
}

func TestPendingPaymentsMutationRoundTrip(t *testing.T) {
	base := pendingPayments()
	for index := range base.Length() {
		payment := pendingPayment(uint64(index + 1))
		payment.ProposerIndex = uint64(index % 3)
		base.Set(index, payment)
	}

	tests := []struct {
		name     string
		slot     uint64
		wantKind byte
		mutate   func(*solid.VectorSSZ[*cltypes.BuilderPendingPayment])
	}{
		{name: "bid insertion", slot: 2, wantKind: PendingPaymentsDeltaKind, mutate: func(v *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) {
			v.Set(3, pendingPayment(90))
		}},
		{name: "parent settlement", slot: 2, wantKind: PendingPaymentsDeltaKind, mutate: func(v *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) {
			v.Set(2, &cltypes.BuilderPendingPayment{Withdrawal: &cltypes.BuilderPendingWithdrawal{}})
		}},
		{name: "attestation weights", slot: 2, wantKind: PendingPaymentsDeltaKind, mutate: func(v *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) {
			v.Get(1).Weight += 10
			v.Get(4).Weight += 20
			v.Get(7).Weight += 30
		}},
		{name: "epoch rotation", slot: testSlotsPerEpoch, wantKind: PendingPaymentsBaseKind, mutate: func(v *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) {
			half := v.Length() / 2
			for index := range half {
				v.Set(index, v.Get(index+half))
				v.Set(index+half, &cltypes.BuilderPendingPayment{Withdrawal: &cltypes.BuilderPendingWithdrawal{}})
			}
		}},
		{name: "proposer slashing", slot: 2, wantKind: PendingPaymentsDeltaKind, mutate: func(v *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) {
			for index := range v.Length() {
				if v.Get(index).ProposerIndex == 1 {
					v.Set(index, &cltypes.BuilderPendingPayment{Withdrawal: &cltypes.BuilderPendingWithdrawal{}})
				}
			}
		}},
		{name: "combined", slot: 2, wantKind: PendingPaymentsDeltaKind, mutate: func(v *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) {
			v.Set(0, pendingPayment(100))
			v.Set(3, &cltypes.BuilderPendingPayment{Withdrawal: &cltypes.BuilderPendingWithdrawal{}})
			v.Get(6).Weight += 50
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer := NewPendingPaymentsWriter(testSlotsPerEpoch)
			baseRow := encodePendingPaymentsRow(t, writer, 1, base)
			require.NoError(t, writer.Initialize(base))
			post := clonePendingPayments(t, base)
			test.mutate(post)
			row := encodePendingPaymentsRow(t, writer, test.slot, post)
			kind, err := PendingPaymentsRowKind(row)
			require.NoError(t, err)
			require.Equal(t, test.wantKind, kind)

			got := pendingPayments()
			require.NoError(t, ApplyPendingPaymentsRow(baseRow, got))
			require.NoError(t, ApplyPendingPaymentsRow(row, got))
			assertPendingPaymentsEqual(t, post, got)
		})
	}
}

func TestPendingPaymentsDecodersRejectInvalidRowsWithoutMutation(t *testing.T) {
	writer := NewPendingPaymentsWriter(testSlotsPerEpoch)
	baseState := pendingPayments()
	baseState.Set(1, pendingPayment(10))
	validBase := encodePendingPaymentsRow(t, writer, 1, baseState)
	require.NoError(t, writer.Initialize(baseState))
	post := clonePendingPayments(t, baseState)
	post.Set(1, pendingPayment(20))
	post.Set(3, pendingPayment(30))
	validDelta := encodePendingPaymentsRow(t, writer, 2, post)
	legacy := legacyPendingPaymentsRow(t, baseState)

	tests := []struct {
		name string
		row  func() []byte
	}{
		{name: "legacy zstd", row: func() []byte { return bytes.Clone(legacy) }},
		{name: "short preamble", row: func() []byte { return bytes.Clone(validBase[:13]) }},
		{name: "wrong magic", row: func() []byte { row := bytes.Clone(validBase); row[8] = 'X'; return row }},
		{name: "nonzero first word", row: func() []byte { row := bytes.Clone(validBase); row[0] = 1; return row }},
		{name: "nonzero second word", row: func() []byte { row := bytes.Clone(validBase); row[4] = 1; return row }},
		{name: "unknown version", row: func() []byte { row := bytes.Clone(validBase); row[12] = 2; return row }},
		{name: "unknown kind", row: func() []byte { row := bytes.Clone(validBase); row[13] = 4; return row }},
		{name: "builders kind", row: func() []byte { row := bytes.Clone(validBase); row[13] = 1; return row }},
		{name: "base truncated", row: func() []byte { return bytes.Clone(validBase[:len(validBase)-1]) }},
		{name: "base trailing", row: func() []byte { return append(bytes.Clone(validBase), 0) }},
		{name: "delta without count", row: func() []byte { return bytes.Clone(validDelta[:14]) }},
		{name: "delta partial count", row: func() []byte { return bytes.Clone(validDelta[:15]) }},
		{name: "delta partial index", row: func() []byte { return bytes.Clone(validDelta[:17]) }},
		{name: "delta without entry", row: func() []byte { return bytes.Clone(validDelta[:18]) }},
		{name: "delta partial entry", row: func() []byte { return bytes.Clone(validDelta[:30]) }},
		{name: "delta truncated final entry", row: func() []byte { return bytes.Clone(validDelta[:len(validDelta)-1]) }},
		{name: "delta trailing", row: func() []byte { return append(bytes.Clone(validDelta), 0) }},
		{name: "duplicate index", row: func() []byte {
			row := bytes.Clone(validDelta)
			copy(row[16+pendingPaymentDeltaSize:18+pendingPaymentDeltaSize], row[16:18])
			return row
		}},
		{name: "decreasing index", row: func() []byte {
			row := bytes.Clone(validDelta)
			binary.LittleEndian.PutUint16(row[16:], 3)
			binary.LittleEndian.PutUint16(row[16+pendingPaymentDeltaSize:], 1)
			return row
		}},
		{name: "index at vector length", row: func() []byte {
			row := bytes.Clone(validDelta)
			binary.LittleEndian.PutUint16(row[16:], uint16(baseState.Length()))
			return row
		}},
		{name: "max index", row: func() []byte {
			row := bytes.Clone(validDelta)
			binary.LittleEndian.PutUint16(row[16:], math.MaxUint16)
			return row
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out := clonePendingPayments(t, baseState)
			before, err := out.EncodeSSZ(nil)
			require.NoError(t, err)
			beforePointer := out.Get(0)
			err = ApplyPendingPaymentsRow(test.row(), out)
			require.ErrorIs(t, err, ErrHistoryRebuildRequired)
			after, encodeErr := out.EncodeSSZ(nil)
			require.NoError(t, encodeErr)
			require.Equal(t, before, after)
			require.Same(t, beforePointer, out.Get(0))
		})
	}
}

func legacyPendingPaymentsRow(t *testing.T, payments *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) []byte {
	t.Helper()
	encoded, err := payments.EncodeSSZ(nil)
	require.NoError(t, err)
	var compressed bytes.Buffer
	encoder, err := zstd.NewWriter(&compressed)
	require.NoError(t, err)
	_, err = encoder.Write(encoded)
	require.NoError(t, err)
	require.NoError(t, encoder.Close())
	return bytes.Clone(compressed.Bytes())
}

func TestPendingPaymentsWriterRejectsVectorTooLongForDelta(t *testing.T) {
	payments := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](math.MaxUint16 + 1)
	for i := range payments.Length() {
		payments.Set(i, pendingPayment(0))
	}
	writer := NewPendingPaymentsWriter(math.MaxUint16/2 + 1)
	var row bytes.Buffer
	require.Error(t, writer.WriteRow(&row, 0, payments))
	require.Zero(t, row.Len())
}
