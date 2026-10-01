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
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// BlobsBundleV1, BlobsBundleV2, and BlobsBundleV3 are engine_getBlobs response slices.
// MarshalFastJSONTo streams them one blob at a time, matching json.Marshal of the underlying slice.
type (
	BlobsBundleV1 []*BlobAndProofV1
	BlobsBundleV2 []*BlobAndProofV2
	BlobsBundleV3 []*BlobCellsAndProofsV1
)

func (bundle BlobsBundleV1) MarshalFastJSONTo(s *jsonstream.Stream) error {
	jsonstream.ArrayValue(s, bundle, writeMarshaler[*BlobAndProofV1])
	return nil
}

func (bundle BlobsBundleV2) MarshalFastJSONTo(s *jsonstream.Stream) error {
	jsonstream.ArrayValue(s, bundle, writeMarshaler[*BlobAndProofV2])
	return nil
}

func (bundle BlobsBundleV3) MarshalFastJSONTo(s *jsonstream.Stream) error {
	jsonstream.ArrayValue(s, bundle, writeBlobCellsV1)
	return nil
}

func writeBlobCellsV1(s *jsonstream.Stream, bp **BlobCellsAndProofsV1) {
	b := *bp
	if b == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	s.Field("blob_cells")
	jsonstream.ArrayValue(s, b.BlobCells, writeHexPtr)
	s.Field("proofs")
	jsonstream.ArrayValue(s, b.Proofs, writeHexPtr)
	s.WriteObjectEnd()
}

func writeHexPtr(s *jsonstream.Stream, b **hexutil.Bytes) {
	if *b == nil {
		s.WriteNil()
		return
	}
	s.WriteHex(**b)
}

// writeMarshaler writes one element of a slice whose elements write themselves.
func writeMarshaler[M jsonstream.Marshaler](s *jsonstream.Stream, m *M) {
	_ = (*m).MarshalFastJSONTo(s)
}
