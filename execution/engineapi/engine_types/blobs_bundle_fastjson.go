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

	"github.com/erigontech/erigon/common/hexutil"
)

// MarshalFastJSON serializes the getPayload blobs bundle into one pre-sized buffer (direct hex
// encoding) instead of reflection, byte-identical to json.Marshal of the bundle.
func (b *BlobsBundle) MarshalFastJSON() ([]byte, error) {
	if b == nil {
		return jsonNull(), nil
	}
	size := len(`{"commitments":`) + hexArrayLen(b.Commitments) +
		len(`,"proofs":`) + hexArrayLen(b.Proofs) +
		len(`,"blobs":`) + hexArrayLen(b.Blobs) + len("}")
	out := make([]byte, 0, size)
	out = append(out, `{"commitments":`...)
	out = appendHexArray(out, b.Commitments)
	out = append(out, `,"proofs":`...)
	out = appendHexArray(out, b.Proofs)
	out = append(out, `,"blobs":`...)
	out = appendHexArray(out, b.Blobs)
	return append(out, '}'), nil
}

func appendHexArray(dst []byte, arr []hexutil.Bytes) []byte {
	if arr == nil {
		return append(dst, "null"...)
	}
	dst = append(dst, '[')
	for i, b := range arr {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendQuotedHex(dst, b)
	}
	return append(dst, ']')
}

func hexArrayLen(arr []hexutil.Bytes) int {
	if arr == nil {
		return len("null")
	}
	n := len("[]")
	for i, b := range arr {
		if i > 0 {
			n++
		}
		n += quotedHexLen(len(b))
	}
	return n
}

// MarshalFastJSON assembles the getPayload envelope field-by-field, fast-marshaling the
// (reflection-heavy) BlobsBundle and deferring to json.Marshal for the smaller fields.
// Byte-identical to json.Marshal(r).
func (r *GetPayloadResponse) MarshalFastJSON() ([]byte, error) {
	if r == nil {
		return jsonNull(), nil
	}
	executionPayload, err := json.Marshal(r.ExecutionPayload)
	if err != nil {
		return nil, err
	}
	blockValue, err := json.Marshal(r.BlockValue)
	if err != nil {
		return nil, err
	}
	blobsBundle, err := r.BlobsBundle.MarshalFastJSON()
	if err != nil {
		return nil, err
	}
	executionRequests, err := json.Marshal(r.ExecutionRequests)
	if err != nil {
		return nil, err
	}
	shouldOverrideBuilder, err := json.Marshal(r.ShouldOverrideBuilder)
	if err != nil {
		return nil, err
	}
	size := len(`{"executionPayload":`) + len(executionPayload) +
		len(`,"blockValue":`) + len(blockValue) +
		len(`,"blobsBundle":`) + len(blobsBundle) +
		len(`,"executionRequests":`) + len(executionRequests) +
		len(`,"shouldOverrideBuilder":`) + len(shouldOverrideBuilder) + len("}")
	out := make([]byte, 0, size)
	out = append(out, `{"executionPayload":`...)
	out = append(out, executionPayload...)
	out = append(out, `,"blockValue":`...)
	out = append(out, blockValue...)
	out = append(out, `,"blobsBundle":`...)
	out = append(out, blobsBundle...)
	out = append(out, `,"executionRequests":`...)
	out = append(out, executionRequests...)
	out = append(out, `,"shouldOverrideBuilder":`...)
	out = append(out, shouldOverrideBuilder...)
	return append(out, '}'), nil
}
