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
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// ExecutionPayloadBodies and ExecutionPayloadBodiesV2 are the engine_getPayloadBodiesBy* answers.
// The RPC encoder only consults the top-level result for a fast marshaller, so a plain slice
// would take the reflection path.
type (
	ExecutionPayloadBodies   []*ExecutionPayloadBody
	ExecutionPayloadBodiesV2 []*ExecutionPayloadBodyV2
)

func (bs ExecutionPayloadBodies) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	jsonstream.ArrayValue(s, bs, func(s *jsonstream.StackStream, b **ExecutionPayloadBody) {
		if *b == nil {
			s.WriteNil()
			return
		}
		s.WriteObjectStart()
		writeBodyFields(s, (*b).Transactions, (*b).Withdrawals)
		s.WriteObjectEnd()
	})
	return nil
}

func (bs ExecutionPayloadBodiesV2) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	jsonstream.ArrayValue(s, bs, func(s *jsonstream.StackStream, b **ExecutionPayloadBodyV2) {
		if *b == nil {
			s.WriteNil()
			return
		}
		s.WriteObjectStart()
		writeBodyFields(s, (*b).Transactions, (*b).Withdrawals)
		s.WriteObjectField("blockAccessList")
		if bal := (*b).BlockAccessList; bal == nil {
			s.WriteNil()
		} else {
			s.WriteHex(*bal)
		}
		s.WriteObjectEnd()
	})
	return nil
}

// writeBodyFields writes the fields both body versions share, in their declaration order.
func writeBodyFields(s *jsonstream.StackStream, txs []hexutil.Bytes, withdrawals []*types.Withdrawal) {
	s.WriteObjectField("transactions")
	jsonstream.ArrayValue(s, txs, func(s *jsonstream.StackStream, txn *hexutil.Bytes) { s.WriteHex(*txn) })
	s.WriteObjectField("withdrawals")
	jsonstream.ArrayValue(s, withdrawals, func(s *jsonstream.StackStream, wp **types.Withdrawal) {
		w := *wp
		if w == nil {
			s.WriteNil()
			return
		}
		s.WriteObjectStart()
		s.WriteObjectField("index").WriteQuotedText(&w.Index)
		s.WriteObjectField("validatorIndex").WriteQuotedText(&w.Validator)
		s.WriteObjectField("address").WriteHex(w.Address[:])
		s.WriteObjectField("amount").WriteQuotedText(&w.Amount)
		s.WriteObjectEnd()
	})
}
