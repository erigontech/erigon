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
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
)

func BenchmarkFrameTransactionEncodingSize(b *testing.B) {
	for _, size := range []int{0, 128 << 10} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			tx := FrameTransaction{
				NonceKeys:  []uint256.Int{{}},
				Frames:     []Frame{{Data: make([]byte, size)}},
				Signatures: []FrameSignature{{Scheme: FrameSignatureP256, Signature: make([]byte, 128)}},
			}
			b.ReportAllocs()
			for b.Loop() {
				tx.EncodingSize()
			}
		})
	}
}

func BenchmarkFrameTransactionGasLimits(b *testing.B) {
	for _, tc := range []struct {
		name string
		keys []uint256.Int
	}{
		{"legacy", []uint256.Int{{}}},
		{"keyed", []uint256.Int{{1}, {256}}},
		{"16 keys", frameNonceKeysAtBit(FrameMaxNonceKeys, 255)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			tx := FrameTransaction{NonceKeys: tc.keys, Nonce: 256, Frames: []Frame{{}}}
			b.ReportAllocs()
			for b.Loop() {
				if _, _, _, err := tx.GasLimits(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFrameTransactionValidate(b *testing.B) {
	for name, scheme := range map[string]uint8{"secp256k1": FrameSignatureSecp256k1, "p256": FrameSignatureP256} {
		b.Run(name, func(b *testing.B) {
			tx, _ := signedFrameTransaction(b, scheme, nil)
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

func BenchmarkFrameTransactionValidateArbitrary(b *testing.B) {
	for _, mixed := range []bool{false, true} {
		b.Run(fmt.Sprintf("mixed=%t", mixed), func(b *testing.B) {
			tx := &FrameTransaction{NonceKeys: []uint256.Int{{}}, Frames: []Frame{{}}}
			if mixed {
				tx, _ = signedFrameTransaction(b, FrameSignatureSecp256k1, nil)
				hash := tx.SigningHash(nil)
				tx.Signatures[0].Message = hash[:]
			}
			tx.Signatures = append(tx.Signatures, FrameSignature{Scheme: FrameSignatureArbitrary, Signature: make([]byte, 128<<10)})
			require.NoError(b, tx.Validate())
			b.ReportAllocs()
			for b.Loop() {
				if err := tx.Validate(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFrameTransactionExecutionSetup(b *testing.B) {
	for _, size := range []int{0, 128 << 10} {
		for name, scheme := range map[string]uint8{"secp256k1": FrameSignatureSecp256k1, "p256": FrameSignatureP256} {
			b.Run(fmt.Sprintf("%s/%d", name, size), func(b *testing.B) {
				tx, _ := signedFrameTransaction(b, scheme, bytes.Repeat([]byte{1}, size))
				var encoded bytes.Buffer
				require.NoError(b, tx.MarshalBinary(&encoded))
				signer := LatestSignerForChainID(&tx.ChainID)
				rules := &chain.Rules{IsBogota: true, ChainID: &tx.ChainID}
				b.ReportAllocs()
				for b.Loop() {
					decoded, err := DecodeTransaction(encoded.Bytes())
					if err != nil {
						b.Fatal(err)
					}
					if _, err := decoded.Sender(*signer); err != nil {
						b.Fatal(err)
					}
					if _, err := decoded.AsMessage(*signer, nil, rules); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
