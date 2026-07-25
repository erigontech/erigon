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

import "encoding/hex"

// BlobsBundleV1 and BlobsBundleV2 are the engine_getBlobs response slices. Their MarshalFastJSON
// serializes the whole response into one pre-sized buffer (direct hex encoding) instead of reflection,
// byte-identical to json.Marshal of the underlying slice (see blob_response_test.go).
type (
	BlobsBundleV1 []*BlobAndProofV1
	BlobsBundleV2 []*BlobAndProofV2
)

func (bundle BlobsBundleV1) MarshalFastJSON() ([]byte, error) {
	if bundle == nil {
		return jsonNull(), nil
	}
	size := len("[]")
	for i, b := range bundle {
		if i > 0 {
			size++
		}
		size += blobV1JSONLen(b)
	}
	out := make([]byte, 0, size)
	out = append(out, '[')
	for i, b := range bundle {
		if i > 0 {
			out = append(out, ',')
		}
		out = appendBlobV1JSON(out, b)
	}
	return append(out, ']'), nil
}

func (bundle BlobsBundleV2) MarshalFastJSON() ([]byte, error) {
	if bundle == nil {
		return jsonNull(), nil
	}
	size := len("[]")
	for i, b := range bundle {
		if i > 0 {
			size++
		}
		size += blobV2JSONLen(b)
	}
	out := make([]byte, 0, size)
	out = append(out, '[')
	for i, b := range bundle {
		if i > 0 {
			out = append(out, ',')
		}
		out = appendBlobV2JSON(out, b)
	}
	return append(out, ']'), nil
}

func appendBlobV1JSON(dst []byte, b *BlobAndProofV1) []byte {
	if b == nil {
		return append(dst, "null"...)
	}
	dst = append(dst, `{"blob":`...)
	dst = appendQuotedHex(dst, b.Blob)
	dst = append(dst, `,"proof":`...)
	dst = appendQuotedHex(dst, b.Proof)
	return append(dst, '}')
}

func blobV1JSONLen(b *BlobAndProofV1) int {
	if b == nil {
		return len("null")
	}
	return len(`{"blob":`) + quotedHexLen(len(b.Blob)) + len(`,"proof":`) + quotedHexLen(len(b.Proof)) + len("}")
}

func appendBlobV2JSON(dst []byte, b *BlobAndProofV2) []byte {
	if b == nil {
		return append(dst, "null"...)
	}
	dst = append(dst, `{"blob":`...)
	dst = appendQuotedHex(dst, b.Blob)
	dst = append(dst, `,"proofs":`...)
	if b.CellProofs == nil {
		dst = append(dst, "null"...)
	} else {
		dst = append(dst, '[')
		for i, p := range b.CellProofs {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendQuotedHex(dst, p)
		}
		dst = append(dst, ']')
	}
	return append(dst, '}')
}

func blobV2JSONLen(b *BlobAndProofV2) int {
	if b == nil {
		return len("null")
	}
	n := len(`{"blob":`) + quotedHexLen(len(b.Blob)) + len(`,"proofs":`) + len("}")
	if b.CellProofs == nil {
		n += len("null")
	} else {
		n += len("[]")
		for i, p := range b.CellProofs {
			if i > 0 {
				n++
			}
			n += quotedHexLen(len(p))
		}
	}
	return n
}

func quotedHexLen(n int) int { return len(`"0x`) + 2*n + len(`"`) }

func appendQuotedHex(dst, src []byte) []byte {
	dst = append(dst, '"', '0', 'x')
	dst = hex.AppendEncode(dst, src)
	return append(dst, '"')
}

func jsonNull() []byte { return []byte("null") }
