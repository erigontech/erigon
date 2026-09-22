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
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

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
			got, err := jsonstream.Marshal(bundle)
			require.NoError(t, err)
			require.Equal(t, string(want), string(got))
		})
	}
}

func TestBlobsBundleV3MarshalFastJSONMatchesReflection(t *testing.T) {
	full := blobCellsAndProofsBundle(128, sszCellsPerExtBlob)
	var nilBytes hexutil.Bytes
	cases := map[string]BlobsBundleV3{
		"nil bundle":       nil,
		"empty bundle":     {},
		"full":             full,
		"with nil entry":   {full[0], nil, full[1]},
		"only nil entries": {nil, nil},
		"nil arrays":       {{}},
		"empty arrays":     {{BlobCells: []*hexutil.Bytes{}, Proofs: []*hexutil.Bytes{}}},
		"nil cells":        {{Proofs: []*hexutil.Bytes{{0x01}}}},
		"nil proofs":       {{BlobCells: []*hexutil.Bytes{{0x02}}}},
		"null entries":     {{BlobCells: []*hexutil.Bytes{{0x01}, nil, {0x02}}, Proofs: []*hexutil.Bytes{nil, {0x03}, nil}}},
		"empty bytes":      {{BlobCells: []*hexutil.Bytes{{}}, Proofs: []*hexutil.Bytes{{}}}},
		"nil bytes":        {{BlobCells: []*hexutil.Bytes{&nilBytes}, Proofs: []*hexutil.Bytes{&nilBytes}}},
	}
	for name, bundle := range cases {
		t.Run(name, func(t *testing.T) {
			want, err := json.Marshal([]*BlobCellsAndProofsV1(bundle))
			require.NoError(t, err)
			got, err := jsonstream.Marshal(bundle)
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
			got, err := jsonstream.Marshal(bundle)
			require.NoError(t, err)
			require.Equal(t, string(want), string(got))
		})
	}
}

func TestBlobCellsAndProofsV1NullCells(t *testing.T) {
	const input = `{"blob_cells":["0x0102",null],"proofs":["0x0304",null]}`
	var response BlobCellsAndProofsV1
	require.NoError(t, json.Unmarshal([]byte(input), &response))
	require.Equal(t, []*hexutil.Bytes{{1, 2}, nil}, response.BlobCells)
	require.Equal(t, []*hexutil.Bytes{{3, 4}, nil}, response.Proofs)
	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	require.JSONEq(t, input, string(encoded))
}

func blobCellsAndProofsBundle(blobs, cells int) []*BlobCellsAndProofsV1 {
	bundle := make([]*BlobCellsAndProofsV1, blobs)
	for i := range bundle {
		entry := &BlobCellsAndProofsV1{
			BlobCells: make([]*hexutil.Bytes, cells),
			Proofs:    make([]*hexutil.Bytes, cells),
		}
		for j := range cells {
			cell := make(hexutil.Bytes, params.BytesPerCell)
			for k := range cell {
				cell[k] = byte(i + j + k)
			}
			proof := make(hexutil.Bytes, sszKZGBytes)
			for k := range proof {
				proof[k] = byte(j + k)
			}
			entry.BlobCells[j] = &cell
			entry.Proofs[j] = &proof
		}
		bundle[i] = entry
	}
	return bundle
}

func worstCaseBundleV2() BlobsBundleV2 {
	// getBlobs rejects more than 128 hashes per call (-38004), so 128 is the largest payload it serialises.
	const blobs = 128
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
