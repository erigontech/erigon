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

package aggregation

import (
	"context"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
)

// BenchmarkAddAttestationElectra measures adding one committee's single attestations,
// one by one, with real BLS signature aggregation.
func BenchmarkAddAttestationElectra(b *testing.B) {
	const committeeSize = 425
	cfg := clparams.MainnetBeaconConfig
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(b))
	clock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
	clock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	committeeBits := solid.NewBitVector(int(cfg.MaxCommitteesPerSlot))
	if err := committeeBits.SetBitAt(5, true); err != nil {
		b.Fatal(err)
	}
	key, err := bls.GenerateKey()
	if err != nil {
		b.Fatal(err)
	}
	var sig [96]byte
	copy(sig[:], key.Sign([]byte("attestation")).Bytes())
	singles := make([]*solid.Attestation, committeeSize)
	for i := range singles {
		raw := make([]byte, committeeSize/8+1)
		raw[i/8] |= 1 << (i % 8)
		raw[committeeSize/8] |= 1 << (committeeSize % 8)
		bits := solid.BitlistFromBytes(raw, int(cfg.MaxValidatorsPerCommittee*cfg.MaxCommitteesPerSlot))
		singles[i] = &solid.Attestation{AggregationBits: bits, Data: &solid.AttestationData{Slot: 1}, Signature: sig, CommitteeBits: committeeBits}
	}
	b.ResetTimer()
	for b.Loop() {
		pool := NewAggregationPool(context.Background(), &cfg, nil, clock)
		for _, att := range singles {
			if err := pool.AddAttestation(att); err != nil {
				b.Fatal(err)
			}
		}
	}
}
