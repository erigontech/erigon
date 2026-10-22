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
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

const benchSlotsPerEpoch = uint64(32)

func benchPendingPayment(index uint64) *cltypes.BuilderPendingPayment {
	digest := crypto.Keccak256(binary.LittleEndian.AppendUint64(nil, index))
	var feeRecipient common.Address
	copy(feeRecipient[:], digest)
	return &cltypes.BuilderPendingPayment{
		Weight: 1_000_000_000_000_000 + index*137_438_953_481,
		Withdrawal: &cltypes.BuilderPendingWithdrawal{
			FeeRecipient: feeRecipient,
			Amount:       10_000_000 + index*12_345_679,
			BuilderIndex: index % 1_000,
		},
		ProposerIndex: 1_000_000 + index,
	}
}

func benchPendingPayments(filled int) *solid.VectorSSZ[*cltypes.BuilderPendingPayment] {
	payments := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](int(2 * benchSlotsPerEpoch))
	for index := range filled {
		payments.Set(index, benchPendingPayment(uint64(index)))
	}
	return payments
}

func cloneBenchPendingPayments(b *testing.B, src *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) *solid.VectorSSZ[*cltypes.BuilderPendingPayment] {
	encoded, err := src.EncodeSSZ(nil)
	if err != nil {
		b.Fatal(err)
	}
	dst := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](src.Length())
	if err := dst.DecodeSSZ(encoded, 0); err != nil {
		b.Fatal(err)
	}
	return dst
}

func benchLegacyZstdRowSize(b *testing.B, payments *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) {
	encoded, err := payments.EncodeSSZ(nil)
	if err != nil {
		b.Fatal(err)
	}
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		b.Fatal(err)
	}
	defer encoder.Close()
	var row []byte
	for b.Loop() {
		row = encoder.EncodeAll(encoded, row[:0])
	}
	b.ReportMetric(float64(len(row)), "bytes/row")
}

func benchBaseRowSize(b *testing.B, payments *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) {
	writer := NewPendingPaymentsWriter(benchSlotsPerEpoch)
	var row bytes.Buffer
	for b.Loop() {
		row.Reset()
		if err := writer.WriteRow(&row, 0, payments); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(row.Len()), "bytes/row")
}

func benchDeltaRowSize(b *testing.B, pre, post *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) {
	writer := NewPendingPaymentsWriter(benchSlotsPerEpoch)
	if err := writer.WriteRow(&bytes.Buffer{}, 0, pre); err != nil {
		b.Fatal(err)
	}
	if err := writer.Initialize(pre); err != nil {
		b.Fatal(err)
	}
	var row bytes.Buffer
	for b.Loop() {
		row.Reset()
		if err := writer.WriteRow(&row, 1, post); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(row.Len()), "bytes/row")
}

func BenchmarkPendingPaymentsRowSize(b *testing.B) {
	cases := []struct {
		name   string
		filled int
	}{
		{"dense", int(2 * benchSlotsPerEpoch)},
		{"sparse_empty", 0},
		{"sparse_one", 1},
	}
	for _, c := range cases {
		payments := benchPendingPayments(c.filled)
		b.Run(c.name+"/legacy_zstd", func(b *testing.B) { benchLegacyZstdRowSize(b, payments) })
		b.Run(c.name+"/base", func(b *testing.B) { benchBaseRowSize(b, payments) })
		b.Run(c.name+"/no_change_delta", func(b *testing.B) { benchDeltaRowSize(b, payments, payments) })
	}

	pre := benchPendingPayments(int(2 * benchSlotsPerEpoch))
	post := cloneBenchPendingPayments(b, pre)
	post.Set(5, &cltypes.BuilderPendingPayment{Withdrawal: &cltypes.BuilderPendingWithdrawal{}})
	post.Set(17, benchPendingPayment(64))
	post.Get(7).Weight += 32_000_000_000
	post.Get(19).Weight += 64_000_000_000
	post.Get(33).Weight += 96_000_000_000
	b.Run("dense/typical_delta", func(b *testing.B) { benchDeltaRowSize(b, pre, post) })
}
