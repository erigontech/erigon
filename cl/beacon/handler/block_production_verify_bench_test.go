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

package handler

import (
	"testing"

	"github.com/erigontech/erigon/cl/utils/bls"
)

// BenchmarkVerifySelectedAttestations measures the signature checks of a full block: 8
// attestations of 64 committees of 512 signers, with decompressed public keys cached as Caplin
// runs them.
func BenchmarkVerifySelectedAttestations(b *testing.B) {
	const distinctKeys, signers, attestations = 512, 64 * 512, 8
	bls.SetEnabledCaching(true)
	b.Cleanup(func() { bls.SetEnabledCaching(false) })
	msg := make([]byte, 32)
	keys := make([][]byte, signers)
	sigs := make([][]byte, signers)
	for i := range distinctKeys {
		key, err := bls.GenerateKey()
		if err != nil {
			b.Fatal(err)
		}
		pk, sig := bls.CompressPublicKey(key.PublicKey()), key.Sign(msg).Bytes()
		for j := i; j < signers; j += distinctKeys {
			keys[j], sigs[j] = pk, sig
		}
	}
	aggregate, err := bls.AggregateSignatures(sigs)
	if err != nil {
		b.Fatal(err)
	}
	verify := func() {
		if ok, err := bls.VerifyAggregate(aggregate, msg, keys); err != nil || !ok {
			b.Fatal("verification failed", err)
		}
	}
	verify() // fill the public key cache

	for b.Loop() {
		for range attestations {
			verify()
		}
	}
}
