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

import "github.com/erigontech/erigon/common/hexutil"

// BlobsBundleV1 and BlobsBundleV2 are the engine_getBlobs response slices. Their MarshalFastJSONTo
// streams one blob at a time with direct hex encoding instead of reflection, byte-identical to
// json.Marshal of the underlying slice (see blob_response_test.go).
type (
	BlobsBundleV1 []*BlobAndProofV1
	BlobsBundleV2 []*BlobAndProofV2
)

func (bundle BlobsBundleV1) MarshalFastJSONTo(w hexutil.JSONWriter) error {
	if bundle == nil {
		w.WriteNil()
		return nil
	}
	w.WriteArrayStart()
	for i, b := range bundle {
		if i > 0 {
			w.WriteMore()
		}
		if b == nil {
			w.WriteNil()
			continue
		}
		w.WriteObjectStart()
		{
			w.WriteObjectField("blob")
			w.WriteHex(b.Blob)
			w.WriteMore()
			w.WriteObjectField("proof")
			w.WriteHex(b.Proof)
		}
		w.WriteObjectEnd()
	}
	w.WriteArrayEnd()
	return nil
}

func (bundle BlobsBundleV2) MarshalFastJSONTo(w hexutil.JSONWriter) error {
	if bundle == nil {
		w.WriteNil()
		return nil
	}
	w.WriteArrayStart()
	for i, b := range bundle {
		if i > 0 {
			w.WriteMore()
		}
		if b == nil {
			w.WriteNil()
			continue
		}
		w.WriteObjectStart()
		{
			w.WriteObjectField("blob")
			w.WriteHex(b.Blob)
			w.WriteMore()
			w.WriteObjectField("proofs")
			hexutil.MarshalFastJSONArrayTo(w, b.CellProofs)
		}
		w.WriteObjectEnd()
	}
	w.WriteArrayEnd()
	return nil
}
