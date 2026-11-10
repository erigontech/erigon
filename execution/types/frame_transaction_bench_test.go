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
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkFrameTransactionValidate(b *testing.B) {
	for name, scheme := range map[string]uint8{"secp256k1": FrameSignatureSecp256k1, "p256": FrameSignatureP256} {
		b.Run(name, func(b *testing.B) {
			tx, _ := signedFrameTransaction(b, scheme)
			b.Run("repeated", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if err := tx.Validate(); err != nil {
						b.Fatal(err)
					}
				}
			})
			var encoded bytes.Buffer
			require.NoError(b, tx.MarshalBinary(&encoded))
			b.Run("decoded", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					decoded, err := DecodeTransaction(encoded.Bytes())
					if err != nil {
						b.Fatal(err)
					}
					if err := decoded.(*FrameTransaction).Validate(); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
