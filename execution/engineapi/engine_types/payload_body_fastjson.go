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
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// ExecutionPayloadBodies and ExecutionPayloadBodiesV2 are the engine_getPayloadBodiesBy* answers.
// The RPC encoder only consults the top-level result for a fast marshaller, so a plain slice
// would take the reflection path.
type (
	ExecutionPayloadBodies   []*ExecutionPayloadBody
	ExecutionPayloadBodiesV2 []*ExecutionPayloadBodyV2
)

func (bs ExecutionPayloadBodies) MarshalFastJSONTo(s *jsonstream.Stream) error {
	jsonstream.ArrayValue(s, bs, writeMarshaler[*ExecutionPayloadBody])
	return nil
}

func (bs ExecutionPayloadBodiesV2) MarshalFastJSONTo(s *jsonstream.Stream) error {
	jsonstream.ArrayValue(s, bs, writeMarshaler[*ExecutionPayloadBodyV2])
	return nil
}
