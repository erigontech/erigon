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
	return b.appendJSON(make([]byte, 0, b.jsonLen())), nil
}

// MarshalFastJSONTo writes one array element at a time, so the blobs never sit in one buffer.
func (b *BlobsBundle) MarshalFastJSONTo(w hexutil.JSONWriter) error {
	if b == nil {
		hexutil.WriteRawJSON(w, "null")
		return nil
	}
	hexutil.WriteRawJSON(w, `{"commitments":`)
	hexutil.MarshalFastJSONArrayTo(w, b.Commitments)
	hexutil.WriteRawJSON(w, `,"proofs":`)
	hexutil.MarshalFastJSONArrayTo(w, b.Proofs)
	hexutil.WriteRawJSON(w, `,"blobs":`)
	hexutil.MarshalFastJSONArrayTo(w, b.Blobs)
	hexutil.WriteRawJSON(w, "}")
	return nil
}

func (b *BlobsBundle) jsonLen() int {
	if b == nil {
		return len("null")
	}
	return len(`{"commitments":`) + hexArrayLen(b.Commitments) +
		len(`,"proofs":`) + hexArrayLen(b.Proofs) +
		len(`,"blobs":`) + hexArrayLen(b.Blobs) + len("}")
}

func (b *BlobsBundle) appendJSON(dst []byte) []byte {
	if b == nil {
		return append(dst, "null"...)
	}
	dst = append(dst, `{"commitments":`...)
	dst = appendHexArray(dst, b.Commitments)
	dst = append(dst, `,"proofs":`...)
	dst = appendHexArray(dst, b.Proofs)
	dst = append(dst, `,"blobs":`...)
	dst = appendHexArray(dst, b.Blobs)
	return append(dst, '}')
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
	f, err := r.marshalFields()
	if err != nil {
		return nil, err
	}
	return f.appendJSON(make([]byte, 0, f.jsonLen()+r.BlobsBundle.jsonLen()), r.BlobsBundle), nil
}

func (r *GetPayloadResponse) MarshalFastJSONTo(w hexutil.JSONWriter) error {
	if r == nil {
		hexutil.WriteRawJSON(w, "null")
		return nil
	}
	f, err := r.marshalFields()
	if err != nil {
		return err
	}
	enc := append(w.AvailableBuffer(f.jsonLen()), `{"executionPayload":`...)
	enc = append(enc, f.executionPayload...)
	enc = append(enc, `,"blockValue":`...)
	enc = append(enc, f.blockValue...)
	w.WriteRawBytes(append(enc, `,"blobsBundle":`...))
	if err := r.BlobsBundle.MarshalFastJSONTo(w); err != nil {
		return err
	}
	enc = append(w.AvailableBuffer(f.jsonLen()), `,"executionRequests":`...)
	enc = append(enc, f.executionRequests...)
	enc = append(enc, `,"shouldOverrideBuilder":`...)
	enc = append(enc, f.shouldOverrideBuilder...)
	w.WriteRawBytes(append(enc, '}'))
	return nil
}

// getPayloadFields holds the fields json.Marshal encodes; the bundle is appended directly.
type getPayloadFields struct {
	executionPayload, blockValue, executionRequests, shouldOverrideBuilder []byte
}

func (r *GetPayloadResponse) marshalFields() (f getPayloadFields, err error) {
	if f.executionPayload, err = json.Marshal(r.ExecutionPayload); err != nil {
		return f, err
	}
	if f.blockValue, err = json.Marshal(r.BlockValue); err != nil {
		return f, err
	}
	if f.executionRequests, err = json.Marshal(r.ExecutionRequests); err != nil {
		return f, err
	}
	if f.shouldOverrideBuilder, err = json.Marshal(r.ShouldOverrideBuilder); err != nil {
		return f, err
	}
	return f, nil
}

func (f *getPayloadFields) jsonLen() int {
	return len(`{"executionPayload":`) + len(f.executionPayload) +
		len(`,"blockValue":`) + len(f.blockValue) +
		len(`,"blobsBundle":`) +
		len(`,"executionRequests":`) + len(f.executionRequests) +
		len(`,"shouldOverrideBuilder":`) + len(f.shouldOverrideBuilder) + len("}")
}

func (f *getPayloadFields) appendJSON(dst []byte, bundle *BlobsBundle) []byte {
	dst = append(dst, `{"executionPayload":`...)
	dst = append(dst, f.executionPayload...)
	dst = append(dst, `,"blockValue":`...)
	dst = append(dst, f.blockValue...)
	dst = append(dst, `,"blobsBundle":`...)
	dst = bundle.appendJSON(dst)
	dst = append(dst, `,"executionRequests":`...)
	dst = append(dst, f.executionRequests...)
	dst = append(dst, `,"shouldOverrideBuilder":`...)
	dst = append(dst, f.shouldOverrideBuilder...)
	return append(dst, '}')
}
