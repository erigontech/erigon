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
	"encoding/hex"

	"github.com/erigontech/erigon/common/hexutil"
)

// BlobsBundleV1, BlobsBundleV2, and BlobsBundleV3 are engine_getBlobs response slices.
// MarshalFastJSON encodes them into one buffer, matching json.Marshal of the underlying slice.
type (
	BlobsBundleV1 []*BlobAndProofV1
	BlobsBundleV2 []*BlobAndProofV2
	BlobsBundleV3 []*BlobCellsAndProofsV1
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

func (bundle BlobsBundleV3) MarshalFastJSON() ([]byte, error) {
	if bundle == nil {
		return jsonNull(), nil
	}
	size := len("[]")
	for i, b := range bundle {
		if i > 0 {
			size++
		}
		if b == nil {
			size += len("null")
			continue
		}
		size += len(`{"blob_cells":`) + hexPtrArrayLen(b.BlobCells) +
			len(`,"proofs":`) + hexPtrArrayLen(b.Proofs) + len("}")
	}
	out := make([]byte, 0, size)
	out = append(out, '[')
	for i, b := range bundle {
		if i > 0 {
			out = append(out, ',')
		}
		if b == nil {
			out = append(out, "null"...)
			continue
		}
		out = append(out, `{"blob_cells":`...)
		out = appendHexPtrArray(out, b.BlobCells)
		out = append(out, `,"proofs":`...)
		out = appendHexPtrArray(out, b.Proofs)
		out = append(out, '}')
	}
	return append(out, ']'), nil
}

func hexPtrArrayLen(arr []*hexutil.Bytes) int {
	if arr == nil {
		return len("null")
	}
	n := len("[]")
	for i, b := range arr {
		if i > 0 {
			n++
		}
		if b == nil {
			n += len("null")
		} else {
			n += quotedHexLen(len(*b))
		}
	}
	return n
}

func appendHexPtrArray(dst []byte, arr []*hexutil.Bytes) []byte {
	if arr == nil {
		return append(dst, "null"...)
	}
	dst = append(dst, '[')
	for i, b := range arr {
		if i > 0 {
			dst = append(dst, ',')
		}
		if b == nil {
			dst = append(dst, "null"...)
		} else {
			dst = appendQuotedHex(dst, *b)
		}
	}
	return append(dst, ']')
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
