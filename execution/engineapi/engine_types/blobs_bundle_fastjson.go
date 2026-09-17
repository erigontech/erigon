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
	executionPayload, err := json.Marshal(r.ExecutionPayload)
	if err != nil {
		return err
	}
	blockValue, err := json.Marshal(r.BlockValue)
	if err != nil {
		return err
	}
	executionRequests, err := json.Marshal(r.ExecutionRequests)
	if err != nil {
		return err
	}
	shouldOverrideBuilder, err := json.Marshal(r.ShouldOverrideBuilder)
	if err != nil {
		return err
	}
	w.WriteObjectStart()
	{
		w.WriteObjectField("executionPayload")
		w.WriteRawBytes(executionPayload)
		w.WriteMore()
		w.WriteObjectField("blockValue")
		w.WriteRawBytes(blockValue)
		w.WriteMore()
		w.WriteObjectField("blobsBundle")
		if err := r.BlobsBundle.MarshalFastJSONTo(w); err != nil {
			return err
		}
		w.WriteMore()
		w.WriteObjectField("executionRequests")
		w.WriteRawBytes(executionRequests)
		w.WriteMore()
		w.WriteObjectField("shouldOverrideBuilder")
		w.WriteRawBytes(shouldOverrideBuilder)
	}
	w.WriteObjectEnd()
	return nil
}
