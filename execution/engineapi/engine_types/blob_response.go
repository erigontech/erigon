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
	hexutil.MarshalFastJSONElemsTo(w, bundle, blobV1JSONLen, appendBlobV1JSON)
	return nil
}

func (bundle BlobsBundleV2) MarshalFastJSONTo(w hexutil.JSONWriter) error {
	hexutil.MarshalFastJSONElemsTo(w, bundle, blobV2JSONLen, appendBlobV2JSON)
	return nil
}

func appendBlobV1JSON(dst []byte, b *BlobAndProofV1) []byte {
	if b == nil {
		return append(dst, "null"...)
	}
	dst = append(dst, `{"blob":`...)
	dst = hexutil.AppendQuoted(dst, b.Blob)
	dst = append(dst, `,"proof":`...)
	dst = hexutil.AppendQuoted(dst, b.Proof)
	return append(dst, '}')
}

func blobV1JSONLen(b *BlobAndProofV1) int {
	if b == nil {
		return len("null")
	}
	return len(`{"blob":`) + hexutil.QuotedLen(len(b.Blob)) + len(`,"proof":`) + hexutil.QuotedLen(len(b.Proof)) + len("}")
}

func appendBlobV2JSON(dst []byte, b *BlobAndProofV2) []byte {
	if b == nil {
		return append(dst, "null"...)
	}
	dst = append(dst, `{"blob":`...)
	dst = hexutil.AppendQuoted(dst, b.Blob)
	dst = append(dst, `,"proofs":`...)
	if b.CellProofs == nil {
		dst = append(dst, "null"...)
	} else {
		dst = append(dst, '[')
		for i, p := range b.CellProofs {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = hexutil.AppendQuoted(dst, p)
		}
		dst = append(dst, ']')
	}
	return append(dst, '}')
}

func blobV2JSONLen(b *BlobAndProofV2) int {
	if b == nil {
		return len("null")
	}
	n := len(`{"blob":`) + hexutil.QuotedLen(len(b.Blob)) + len(`,"proofs":`) + len("}")
	if b.CellProofs == nil {
		n += len("null")
	} else {
		n += len("[]")
		for i, p := range b.CellProofs {
			if i > 0 {
				n++
			}
			n += hexutil.QuotedLen(len(p))
		}
	}
	return n
}
