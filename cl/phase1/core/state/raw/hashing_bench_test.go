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

package raw

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func newStateWithQueues(version clparams.StateVersion) *BeaconState {
	s := newTestState(version)
	for i := range 7600 {
		s.AppendPendingDeposit(&solid.PendingDeposit{Amount: uint64(i), Slot: uint64(i)})
	}
	for i := range 100 {
		s.AppendPendingPartialWithdrawal(&solid.PendingPartialWithdrawal{ValidatorIndex: uint64(i), Amount: uint64(i)})
	}
	for i := range 50 {
		s.AppendPendingConsolidation(&solid.PendingConsolidation{SourceIndex: uint64(i), TargetIndex: uint64(i + 1)})
	}
	if version >= clparams.GloasVersion {
		for i := range 64 {
			s.builders.Append(&cltypes.Builder{Balance: uint64(i)})
			s.builderPendingWithdrawals.Append(&cltypes.BuilderPendingWithdrawal{BuilderIndex: uint64(i), Amount: uint64(i)})
		}
		for i := range 16 {
			s.payloadExpectedWithdrawals.Append(&cltypes.Withdrawal{Index: uint64(i), Amount: uint64(i)})
		}
	}
	return s
}

func BenchmarkHashSSZAfterCopyInto(b *testing.B) {
	for _, tc := range []struct {
		name    string
		version clparams.StateVersion
	}{
		{"fulu", clparams.FuluVersion},
		{"gloas", clparams.GloasVersion},
	} {
		b.Run(tc.name, func(b *testing.B) {
			src := newStateWithQueues(tc.version)
			if _, err := src.HashSSZ(); err != nil {
				b.Fatal(err)
			}
			dst := New(src.BeaconConfig())
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := src.CopyInto(dst); err != nil {
					b.Fatal(err)
				}
				if _, err := dst.HashSSZ(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
