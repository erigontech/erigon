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

	"github.com/erigontech/erigon/rpc/jsonstream"
)

// MarshalFastJSONTo streams the getPayload blobs bundle blob by blob, byte-identical to
// json.Marshal of the bundle.
func (b *BlobsBundle) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	writeBlobsBundle(s, b)
	return nil
}

func writeBlobsBundle(s *jsonstream.StackStream, b *BlobsBundle) {
	if b == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	s.WriteObjectField("commitments")
	jsonstream.ArrayValue(s, b.Commitments, writeHex)
	jsonstream.Field(s, "proofs")
	jsonstream.ArrayValue(s, b.Proofs, writeHex)
	jsonstream.Field(s, "blobs")
	jsonstream.ArrayValue(s, b.Blobs, writeHex)
	s.WriteObjectEnd()
}

// MarshalFastJSONTo writes the getPayload envelope, byte-identical to json.Marshal(r).
// executionPayload and blockValue go through json.Marshal before the first write, so their
// errors leave nothing written; the other fields are streamed.
func (r *GetPayloadResponse) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	if r == nil {
		s.WriteNil()
		return nil
	}
	executionPayload, err := json.Marshal(r.ExecutionPayload)
	if err != nil {
		return err
	}
	blockValue, err := json.Marshal(r.BlockValue)
	if err != nil {
		return err
	}
	s.WriteObjectStart()
	s.WriteObjectField("executionPayload").WriteRawBytes(executionPayload)
	jsonstream.Field(s, "blockValue").WriteRawBytes(blockValue)
	jsonstream.Field(s, "blobsBundle")
	writeBlobsBundle(s, r.BlobsBundle)
	jsonstream.Field(s, "executionRequests")
	jsonstream.ArrayValue(s, r.ExecutionRequests, writeHex)
	jsonstream.Field(s, "shouldOverrideBuilder").WriteBool(r.ShouldOverrideBuilder)
	s.WriteObjectEnd()
	return nil
}
