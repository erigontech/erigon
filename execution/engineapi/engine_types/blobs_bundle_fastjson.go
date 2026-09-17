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

// MarshalFastJSONTo writes one array element at a time, so the blobs never sit in one buffer.
func (b *BlobsBundle) MarshalFastJSONTo(w hexutil.JSONWriter) error {
	if b == nil {
		w.WriteNil()
		return nil
	}
	w.WriteObjectStart()
	{
		w.WriteObjectField("commitments")
		hexutil.MarshalFastJSONArrayTo(w, b.Commitments)
		w.WriteMore()
		w.WriteObjectField("proofs")
		hexutil.MarshalFastJSONArrayTo(w, b.Proofs)
		w.WriteMore()
		w.WriteObjectField("blobs")
		hexutil.MarshalFastJSONArrayTo(w, b.Blobs)
	}
	w.WriteObjectEnd()
	return nil
}

func (r *GetPayloadResponse) MarshalFastJSONTo(w hexutil.JSONWriter) error {
	f, err := r.marshalFields()
	if err != nil {
		return err
	}
	w.WriteObjectStart()
	{
		w.WriteObjectField("executionPayload")
		w.WriteRawBytes(f.executionPayload)
		w.WriteMore()
		w.WriteObjectField("blockValue")
		w.WriteRawBytes(f.blockValue)
		w.WriteMore()
		w.WriteObjectField("blobsBundle")
		if err := r.BlobsBundle.MarshalFastJSONTo(w); err != nil {
			return err
		}
		w.WriteMore()
		w.WriteObjectField("executionRequests")
		w.WriteRawBytes(f.executionRequests)
		w.WriteMore()
		w.WriteObjectField("shouldOverrideBuilder")
		w.WriteRawBytes(f.shouldOverrideBuilder)
	}
	w.WriteObjectEnd()
	return nil
}

// getPayloadFields holds the fields json.Marshal encodes, marshaled before anything is written.
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
