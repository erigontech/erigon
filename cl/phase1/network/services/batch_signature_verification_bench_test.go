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

package services

import (
	"errors"
	"sync"
	"testing"

	"github.com/erigontech/erigon/cl/utils/bls"
)

func BenchmarkBatchSignatureVerifierFallback(b *testing.B) {
	validRoot := make([]byte, 32)
	validRoot[0] = 1
	invalidRoot := make([]byte, 32)
	invalidRoot[0] = 2
	signatures := make([][]byte, batchSignatureVerificationThreshold)
	publicKeys := make([][]byte, batchSignatureVerificationThreshold)
	for i := range batchSignatureVerificationThreshold {
		key, err := bls.GenerateKey()
		if err != nil {
			b.Fatal(err)
		}
		signatures[i] = key.Sign(validRoot).Bytes()
		publicKeys[i] = bls.CompressPublicKey(key.PublicKey())
	}

	benchmarks := []struct {
		name         string
		invalidCount int
	}{
		{name: "all_valid"},
		{name: "one_invalid", invalidCount: 1},
		{name: "all_invalid", invalidCount: batchSignatureVerificationThreshold},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			entries := make([]*AggregateVerificationData, batchSignatureVerificationThreshold)
			for i := range entries {
				signRoot := validRoot
				if i < benchmark.invalidCount {
					signRoot = invalidRoot
				}
				entries[i] = &AggregateVerificationData{
					Signatures: [][]byte{signatures[i]},
					SignRoots:  [][]byte{signRoot},
					Pks:        [][]byte{publicKeys[i]},
				}
			}
			verifier := NewBatchSignatureVerifier(b.Context(), nil)
			callbacks, err := verifier.processSignatureVerification(entries)
			if benchmark.invalidCount == 0 && err != nil ||
				benchmark.invalidCount != 0 && !errors.Is(err, ErrInvalidBlsSignature) {
				b.Fatalf("unexpected verification result: %v", err)
			}
			if len(callbacks) != len(entries)-benchmark.invalidCount {
				b.Fatalf("got %d valid callbacks, want %d", len(callbacks), len(entries)-benchmark.invalidCount)
			}

			b.Run("batch", func(b *testing.B) {
				for range b.N {
					_, _ = verifier.processSignatureVerification(entries)
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(entries)), "ns/msg")
			})
			b.Run("individual", func(b *testing.B) {
				for range b.N {
					var group sync.WaitGroup
					for _, entry := range entries {
						group.Go(func() {
							_, _ = bls.VerifyAggregate(entry.Signatures[0], entry.SignRoots[0], [][]byte{entry.Pks[0]})
						})
					}
					group.Wait()
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(entries)), "ns/msg")
			})
		})
	}
}
