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

func (bundle BlobsBundleV1) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	jsonstream.ArrayValue(s, bundle, writeBlobV1)
	return nil
}

func (bundle BlobsBundleV2) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	jsonstream.ArrayValue(s, bundle, writeBlobV2)
	return nil
}

func (bundle BlobsBundleV3) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	jsonstream.ArrayValue(s, bundle, writeBlobCellsV1)
	return nil
}

func writeBlobV1(s *jsonstream.StackStream, bp **BlobAndProofV1) {
	b := *bp
	if b == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	s.Field("blob").WriteHex(b.Blob)
	s.Field("proof").WriteHex(b.Proof)
	s.WriteObjectEnd()
}

func writeBlobV2(s *jsonstream.StackStream, bp **BlobAndProofV2) {
	b := *bp
	if b == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	s.Field("blob").WriteHex(b.Blob)
	s.Field("proofs")
	jsonstream.ArrayValue(s, b.CellProofs, writeHex)
	s.WriteObjectEnd()
}

func writeBlobCellsV1(s *jsonstream.StackStream, bp **BlobCellsAndProofsV1) {
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

// writeHex writes one value per element, so a blob array flushes blob by blob instead of
// growing one buffer for all of them.
func writeHex(s *jsonstream.StackStream, b *hexutil.Bytes) { s.WriteHex(*b) }

func writeHexPtr(s *jsonstream.StackStream, b **hexutil.Bytes) {
	if *b == nil {
		s.WriteNil()
		return
	}
	s.WriteHex(**b)
}
