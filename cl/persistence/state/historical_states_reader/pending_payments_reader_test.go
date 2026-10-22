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

package historical_states_reader

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/db/kv"
)

const readerSlotsPerEpoch = uint64(4)

func readerPendingPayments(values ...uint64) *solid.VectorSSZ[*cltypes.BuilderPendingPayment] {
	payments := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](int(2 * readerSlotsPerEpoch))
	for index, value := range values {
		payments.Set(index, &cltypes.BuilderPendingPayment{
			Weight: value,
			Withdrawal: &cltypes.BuilderPendingWithdrawal{
				Amount:       value + 1,
				BuilderIndex: value + 2,
			},
			ProposerIndex: value + 3,
		})
	}
	return payments
}

func pendingPaymentsBaseRow(t *testing.T, slot uint64, payments *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) []byte {
	return pendingPaymentsBaseRowWithSlots(t, slot, readerSlotsPerEpoch, payments)
}

func pendingPaymentsBaseRowWithSlots(t *testing.T, slot, slotsPerEpoch uint64, payments *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) []byte {
	t.Helper()
	writer := state_accessors.NewPendingPaymentsWriter(slotsPerEpoch)
	var row bytes.Buffer
	require.NoError(t, writer.WriteRow(&row, slot, payments))
	return bytes.Clone(row.Bytes())
}

func pendingPaymentsDeltaRow(t *testing.T, slot uint64, pre, post *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) []byte {
	t.Helper()
	writer := state_accessors.NewPendingPaymentsWriter(readerSlotsPerEpoch)
	require.NoError(t, writer.WriteRow(bytes.NewBuffer(nil), slot, pre))
	require.NoError(t, writer.Initialize(pre))
	var row bytes.Buffer
	require.NoError(t, writer.WriteRow(&row, slot, post))
	return bytes.Clone(row.Bytes())
}

func pendingPaymentsReaderData(rows map[uint64][]byte, slotData ...uint64) map[string]map[string][]byte {
	data := map[string]map[string][]byte{
		kv.BuilderPendingPaymentsTable: {},
		kv.SlotData:                    {},
	}
	for slot, row := range rows {
		data[kv.BuilderPendingPaymentsTable][string(base_encoding.Encode64ToBytes4(slot))] = row
	}
	for _, slot := range slotData {
		data[kv.SlotData][string(base_encoding.Encode64ToBytes4(slot))] = []byte{1}
	}
	return data
}

func assertReaderPendingPayments(t *testing.T, expected, actual *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) {
	t.Helper()
	expectedSSZ, err := expected.EncodeSSZ(nil)
	require.NoError(t, err)
	actualSSZ, err := actual.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, expectedSSZ, actualSSZ)
}

func TestReadBuilderPendingPaymentsScansEpoch(t *testing.T) {
	base := readerPendingPayments(10, 20)
	afterFirst := readerPendingPayments(10, 21)
	afterSecond := readerPendingPayments(30, 21)
	restart := readerPendingPayments(40, 50)
	afterRestart := readerPendingPayments(41, 50)

	tests := []struct {
		name      string
		target    uint64
		rows      map[uint64][]byte
		slotData  []uint64
		expected  *solid.VectorSSZ[*cltypes.BuilderPendingPayment]
		wantError bool
	}{
		{
			name:     "base at target",
			target:   4,
			rows:     map[uint64][]byte{4: pendingPaymentsBaseRow(t, 4, base)},
			slotData: []uint64{4}, expected: base,
		},
		{
			name:   "base and several deltas",
			target: 6,
			rows: map[uint64][]byte{
				4: pendingPaymentsBaseRow(t, 4, base),
				5: pendingPaymentsDeltaRow(t, 5, base, afterFirst),
				6: pendingPaymentsDeltaRow(t, 6, afterFirst, afterSecond),
			},
			slotData: []uint64{4, 5, 6}, expected: afterSecond,
		},
		{
			name:   "missed slot between deltas",
			target: 7,
			rows: map[uint64][]byte{
				4: pendingPaymentsBaseRow(t, 4, base),
				5: pendingPaymentsDeltaRow(t, 5, base, afterFirst),
				7: pendingPaymentsDeltaRow(t, 7, afterFirst, afterSecond),
			},
			slotData: []uint64{4, 5, 7}, expected: afterSecond,
		},
		{
			name:   "processed block hole",
			target: 6,
			rows: map[uint64][]byte{
				4: pendingPaymentsBaseRow(t, 4, base),
				6: pendingPaymentsDeltaRow(t, 6, afterFirst, afterSecond),
			},
			slotData: []uint64{4, 5, 6}, wantError: true,
		},
		{
			name:   "malformed later delta",
			target: 6,
			rows: map[uint64][]byte{
				4: pendingPaymentsBaseRow(t, 4, base),
				5: pendingPaymentsDeltaRow(t, 5, base, afterFirst),
				6: append(pendingPaymentsDeltaRow(t, 6, afterFirst, afterSecond), 0),
			},
			slotData: []uint64{4, 5, 6}, wantError: true,
		},
		{
			name:      "delta without base",
			target:    5,
			rows:      map[uint64][]byte{5: pendingPaymentsDeltaRow(t, 5, base, afterFirst)},
			slotData:  []uint64{5},
			wantError: true,
		},
		{
			name:   "base in previous epoch",
			target: 5,
			rows: map[uint64][]byte{
				3: pendingPaymentsBaseRow(t, 3, base),
				5: pendingPaymentsDeltaRow(t, 5, base, afterFirst),
			},
			slotData: []uint64{3, 5}, wantError: true,
		},
		{
			name:   "latest base after restart",
			target: 7,
			rows: map[uint64][]byte{
				4: pendingPaymentsBaseRow(t, 4, base),
				5: pendingPaymentsDeltaRow(t, 5, base, afterFirst),
				6: pendingPaymentsBaseRow(t, 6, restart),
				7: pendingPaymentsDeltaRow(t, 7, restart, afterRestart),
			},
			slotData: []uint64{4, 5, 6, 7}, expected: afterRestart,
		},
		{
			name:     "exact fork slot",
			target:   32,
			rows:     map[uint64][]byte{32: pendingPaymentsBaseRow(t, 32, base)},
			slotData: []uint64{32}, expected: base,
		},
		{
			name:     "missed fork slot",
			target:   33,
			rows:     map[uint64][]byte{33: pendingPaymentsBaseRow(t, 33, base)},
			slotData: []uint64{33}, expected: base,
		},
		{
			name:     "missed epoch start",
			target:   35,
			rows:     map[uint64][]byte{35: pendingPaymentsBaseRow(t, 35, base)},
			slotData: []uint64{35}, expected: base,
		},
		{
			name:     "fully missed previous epoch",
			target:   64,
			rows:     map[uint64][]byte{64: pendingPaymentsBaseRow(t, 64, base)},
			slotData: []uint64{64}, expected: base,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := readerPendingPayments(99)
			before, err := got.EncodeSSZ(nil)
			require.NoError(t, err)
			err = ReadBuilderPendingPayments(mockGetValFn(pendingPaymentsReaderData(test.rows, test.slotData...)), test.target, readerSlotsPerEpoch, got)
			if test.wantError {
				require.ErrorIs(t, err, state_accessors.ErrHistoryRebuildRequired)
				after, encodeErr := got.EncodeSSZ(nil)
				require.NoError(t, encodeErr)
				require.Equal(t, before, after)
				return
			}
			require.NoError(t, err)
			assertReaderPendingPayments(t, test.expected, got)
		})
	}
}

func TestReadCompressedSSZRejectsPendingPaymentsRowsWithoutMutation(t *testing.T) {
	slot := uint64(5)
	pre := readerPendingPayments(10)
	post := readerPendingPayments(20)
	rows := [][]byte{
		pendingPaymentsBaseRow(t, slot, pre),
		pendingPaymentsDeltaRow(t, slot, pre, post),
	}

	for _, row := range rows {
		out := readerPendingPayments(99)
		before, err := out.EncodeSSZ(nil)
		require.NoError(t, err)
		getter := mockGetValFn(map[string]map[string][]byte{
			kv.BuilderPendingPaymentsTable: {
				string(base_encoding.Encode64ToBytes4(slot)): row,
			},
		})
		err = readCompressedSSZ(getter, slot, kv.BuilderPendingPaymentsTable, out, int(clparams.GloasVersion))
		require.Error(t, err)
		after, encodeErr := out.EncodeSSZ(nil)
		require.NoError(t, encodeErr)
		require.Equal(t, before, after)
	}
}
