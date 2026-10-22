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
	"fmt"
	"io"
	"math"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

const (
	PendingPaymentsBaseKind  = byte(2)
	PendingPaymentsDeltaKind = byte(3)
	pendingPaymentSSZSize    = 52
	pendingPaymentDeltaSize  = 2 + pendingPaymentSSZSize
)

type PendingPaymentsWriter struct {
	slotsPerEpoch uint64
	baseline      []byte
	scratch       []byte
	needBase      bool
	lastEpoch     uint64
}

func NewPendingPaymentsWriter(slotsPerEpoch uint64) *PendingPaymentsWriter {
	return &PendingPaymentsWriter{slotsPerEpoch: slotsPerEpoch, needBase: true}
}

func (w *PendingPaymentsWriter) Initialize(pre *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) error {
	encoded, err := pre.EncodeSSZ(w.scratch[:0])
	if err != nil {
		return err
	}
	w.baseline, w.scratch = encoded, w.baseline[:0]
	return nil
}

func (w *PendingPaymentsWriter) WriteRow(dst io.Writer, slot uint64, post *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) error {
	if post.Length() > math.MaxUint16 {
		return fmt.Errorf("pending payments vector length %d does not fit a delta row", post.Length())
	}
	encoded, err := post.EncodeSSZ(w.scratch[:0])
	if err != nil {
		return err
	}
	w.scratch = encoded
	epoch := slot / w.slotsPerEpoch
	writeBase := w.needBase || len(w.baseline) != len(encoded) || epoch != w.lastEpoch
	var row []byte
	if writeBase {
		row = appendHistoryRowPreamble(make([]byte, 0, historyRowPreambleSize+len(encoded)), PendingPaymentsBaseKind)
		row = append(row, encoded...)
	} else {
		row = appendHistoryRowPreamble(make([]byte, 0, historyRowPreambleSize+2), PendingPaymentsDeltaKind)
		row = append(row, 0, 0)
		count := uint16(0)
		for index := range post.Length() {
			start := index * pendingPaymentSSZSize
			if bytes.Equal(w.baseline[start:start+pendingPaymentSSZSize], encoded[start:start+pendingPaymentSSZSize]) {
				continue
			}
			count++
			row = binary.LittleEndian.AppendUint16(row, uint16(index))
			row = append(row, encoded[start:start+pendingPaymentSSZSize]...)
		}
		binary.LittleEndian.PutUint16(row[historyRowPreambleSize:], count)
	}
	if _, err := dst.Write(row); err != nil {
		return err
	}
	w.needBase = false
	w.lastEpoch = epoch
	return nil
}

func PendingPaymentsRowKind(row []byte) (byte, error) {
	kind, _, err := parseHistoryRow(row, "pending payments row")
	if err != nil {
		return 0, err
	}
	if kind != PendingPaymentsBaseKind && kind != PendingPaymentsDeltaKind {
		return 0, rebuildRequired("unsupported pending payments row kind %d", kind)
	}
	return kind, nil
}

func ApplyPendingPaymentsRow(row []byte, out *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) error {
	kind, body, err := parseHistoryRow(row, "pending payments row")
	if err != nil {
		return err
	}
	switch kind {
	case PendingPaymentsBaseKind:
		expectedLength := out.Length() * pendingPaymentSSZSize
		if len(body) != expectedLength {
			return rebuildRequired("pending payments base length is %d, want %d", len(body), expectedLength)
		}
		decoded := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](out.Length())
		if err := decoded.DecodeSSZ(body, 0); err != nil {
			return rebuildRequired("decode pending payments base: %v", err)
		}
		decoded.CopyTo(out)
		return nil
	case PendingPaymentsDeltaKind:
		if len(body) < 2 {
			return rebuildRequired("truncated pending payments delta count")
		}
		count := binary.LittleEndian.Uint16(body)
		expectedLength := uint64(2) + uint64(count)*pendingPaymentDeltaSize
		if uint64(len(body)) != expectedLength {
			return rebuildRequired("pending payments delta length is %d, want %d", len(body), expectedLength)
		}

		type update struct {
			index   uint16
			payment *cltypes.BuilderPendingPayment
		}
		updates := make([]update, 0, count)
		var previous uint16
		for entry := range count {
			offset := 2 + int(entry)*pendingPaymentDeltaSize
			index := binary.LittleEndian.Uint16(body[offset:])
			if entry > 0 && index <= previous {
				return rebuildRequired("pending payment indices are not strictly increasing")
			}
			if int(index) >= out.Length() {
				return rebuildRequired("pending payment index %d is outside vector length %d", index, out.Length())
			}
			payment := new(cltypes.BuilderPendingPayment)
			if err := payment.DecodeSSZ(body[offset+2:offset+pendingPaymentDeltaSize], 0); err != nil {
				return rebuildRequired("decode pending payment at index %d: %v", index, err)
			}
			updates = append(updates, update{index: index, payment: payment})
			previous = index
		}
		for _, update := range updates {
			out.Set(int(update.index), update.payment)
		}
		return nil
	default:
		return rebuildRequired("unsupported pending payments row kind %d", kind)
	}
}
