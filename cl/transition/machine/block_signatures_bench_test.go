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

package machine_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/machine"
	"github.com/erigontech/erigon/cl/utils/bls"
)

func BenchmarkProcessOperationsPayloadAttestationSignatures(b *testing.B) {
	s, keys, block := blockSignatureFixture(b, 512)
	bls.SetEnabledCaching(true)
	b.Cleanup(func() { bls.SetEnabledCaching(false) })
	for _, key := range keys.validators {
		if err := bls.LoadPublicKeyIntoCache(bls.CompressPublicKey(key.PublicKey()), true); err != nil {
			b.Fatal(err)
		}
	}

	body := block.Body
	body.PayloadAttestations = newPayloadAttestationList(s)
	for i := range 4 {
		body.PayloadAttestations.Append(newPayloadAttestation(b, s, keys.validators, i&1 != 0, i&2 != 0))
	}

	extraMessage := make([]byte, 32)
	extraMessage[0] = 1
	extraSignature := keys.validators[0].Sign(extraMessage).Bytes()
	extraPublicKey := bls.CompressPublicKey(keys.validators[0].PublicKey())
	parentSlot := s.LatestBlockHeader().Slot

	for b.Loop() {
		signatures, messages, publicKeys, err := machine.ProcessOperations(transition.ValidatingMachine, s, body, parentSlot)
		if err != nil {
			b.Fatal(err)
		}
		signatures = append(signatures, extraSignature)
		messages = append(messages, extraMessage)
		publicKeys = append(publicKeys, extraPublicKey)
		valid, err := bls.VerifyMultipleSignatures(signatures, messages, publicKeys)
		if err != nil {
			b.Fatal(err)
		}
		if !valid {
			b.Fatal("signature batch is invalid")
		}
	}
}
