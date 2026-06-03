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

package engine_types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/hexutil"
)

func worstCaseBundleV2() BlobsBundleV2 {
	const blobs = 21
	bundle := make(BlobsBundleV2, blobs)
	for i := range bundle {
		blob := make(hexutil.Bytes, sszBlobBytes)
		for j := range blob {
			blob[j] = byte(i + j)
		}
		proofs := make([]hexutil.Bytes, sszCellsPerExtBlob)
		for c := range proofs {
			p := make(hexutil.Bytes, sszKZGBytes)
			for j := range p {
				p[j] = byte(c + j)
			}
			proofs[c] = p
		}
		bundle[i] = &BlobAndProofV2{Blob: blob, CellProofs: proofs}
	}
	return bundle
}

func TestBlobsBundleV2MarshalFastJSONMatchesReflection(t *testing.T) {
	full := worstCaseBundleV2()
	cases := map[string]BlobsBundleV2{
		"nil bundle":       nil,
		"empty bundle":     {},
		"full":             full,
		"with nil entry":   {full[0], nil, full[1]},
		"empty proofs":     {{Blob: hexutil.Bytes{0x01}, CellProofs: []hexutil.Bytes{}}},
		"nil proofs":       {{Blob: hexutil.Bytes{0x01}, CellProofs: nil}},
		"empty blob":       {{Blob: hexutil.Bytes{}, CellProofs: []hexutil.Bytes{{0x09}}}},
		"only nil entries": {nil, nil},
	}
	for name, bundle := range cases {
		t.Run(name, func(t *testing.T) {
			want, err := json.Marshal([]*BlobAndProofV2(bundle))
			require.NoError(t, err)
			got, err := bundle.MarshalFastJSON()
			require.NoError(t, err)
			require.Equal(t, string(want), string(got))
		})
	}
}

func TestBlobsBundleV1MarshalFastJSONMatchesReflection(t *testing.T) {
	cases := map[string]BlobsBundleV1{
		"nil bundle":     nil,
		"empty bundle":   {},
		"full":           {{Blob: hexutil.Bytes{0x01, 0x02}, Proof: hexutil.Bytes{0xaa}}},
		"with nil entry": {{Blob: hexutil.Bytes{0x01}, Proof: hexutil.Bytes{0x02}}, nil},
		"empty":          {{Blob: hexutil.Bytes{}, Proof: hexutil.Bytes{}}},
	}
	for name, bundle := range cases {
		t.Run(name, func(t *testing.T) {
			want, err := json.Marshal([]*BlobAndProofV1(bundle))
			require.NoError(t, err)
			got, err := bundle.MarshalFastJSON()
			require.NoError(t, err)
			require.Equal(t, string(want), string(got))
		})
	}
}

// BenchmarkBlobsBundleV2Marshal compares the worst-case getBlobsV3 response (21 blobs, 128 cell
// proofs each) encoded by stdlib reflection vs MarshalFastJSON.
func BenchmarkBlobsBundleV2Marshal(b *testing.B) {
	bundle := worstCaseBundleV2()
	enc, _ := bundle.MarshalFastJSON()
	size := int64(len(enc))

	b.Run("stdlib_reflect", func(b *testing.B) {
		slice := []*BlobAndProofV2(bundle)
		b.SetBytes(size)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := json.Marshal(slice); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fast", func(b *testing.B) {
		b.SetBytes(size)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := bundle.MarshalFastJSON(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
