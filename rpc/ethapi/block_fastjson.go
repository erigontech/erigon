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

package ethapi

import (
	"encoding/json"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// MarshalFastJSONTo writes the whole block. It must exist: RPCBlock embeds RPCHeader, so
// without it the promoted header method would satisfy the fast-JSON interface and a block
// would serialise as a bare header, losing its transactions.
func (b *RPCBlock) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	if b == nil {
		s.WriteNil()
		return nil
	}

	// An `any` field holding a type with no fast path falls back to the reflection encoder.
	// That is done up front: the contract is that a marshaller reports failure before its
	// first write, never with half a result already streamed.
	hashes, hashesOK := b.Transactions.([]common.Hash)
	full, fullOK := b.Transactions.([]*RPCTransaction)
	var rawTxs []byte
	var err error
	if !hashesOK && !fullOK {
		if rawTxs, err = marshalIfSet(b.Transactions); err != nil {
			return err
		}
	}
	callErrs, err := marshalCallErrors(b.Calls)
	if err != nil {
		return err
	}

	s.WriteObjectStart()
	if err := b.RPCHeader.writeJSONFields(s); err != nil {
		return err
	}

	jsonstream.Text(s, "size", &b.Size)

	// omitempty on an `any` drops only a nil interface, so an empty list still shows.
	switch {
	case hashesOK:
		jsonstream.HexesField(s, "transactions", hashes)
	case fullOK:
		s.Field("transactions")
		jsonstream.ArrayValue(s, full, writeTxElem)
	case rawTxs != nil:
		s.Field("transactions").WriteRawBytes(rawTxs)
	}

	jsonstream.HexesField(s, "uncles", b.Uncles)

	if b.Withdrawals != nil {
		s.Field("withdrawals")
		if err := b.Withdrawals.MarshalFastJSONTo(s); err != nil {
			return err
		}
	}
	if b.TransactionCount != nil {
		s.Field("transactionCount").Uint(*b.TransactionCount)
	}
	if b.TotalDifficulty != nil {
		jsonstream.Text(s, "totalDifficulty", b.TotalDifficulty)
	}
	if b.Calls != nil {
		s.Field("calls").WriteArrayStart()
		for i := range b.Calls {
			b.Calls[i].writeTo(s, callErrs[i])
		}
		s.WriteArrayEnd()
	}
	s.WriteObjectEnd()
	return nil
}

// marshalCallErrors encodes the calls' errors up front, the one part of a call result that
// needs the reflection encoder, so a failure is reported before the block's first write.
func marshalCallErrors(calls []CallResult) ([][]byte, error) {
	if calls == nil {
		return nil, nil
	}
	errs := make([][]byte, len(calls))
	for i := range calls {
		var err error
		if errs[i], err = marshalIfSet(calls[i].Error); err != nil {
			return nil, err
		}
	}
	return errs, nil
}

func (r *CallResult) writeTo(s *jsonstream.StackStream, callErr []byte) {
	s.WriteObjectStart()
	s.Field("returnData").WriteString(r.ReturnData)
	s.Field("logs")
	jsonstream.ArrayValue(s, r.Logs, writeLogElem)
	jsonstream.Text(s, "gasUsed", &r.GasUsed)
	jsonstream.Text(s, "maxUsedGas", &r.MaxUsedGas)
	jsonstream.Text(s, "status", &r.Status)
	if callErr != nil {
		s.Field("error").WriteRawBytes(callErr)
	}
	s.WriteObjectEnd()
}

// writeLogElem never fails: RPCLog.MarshalFastJSONTo reports no error.
func writeLogElem(s *jsonstream.StackStream, l **types.RPCLog) { _ = (*l).MarshalFastJSONTo(s) }

// writeTxElem never fails: RPCTransaction.MarshalFastJSONTo reports no error.
func writeTxElem(s *jsonstream.StackStream, t **RPCTransaction) { _ = (*t).MarshalFastJSONTo(s) }

// marshalIfSet encodes v unless it is absent, so the caller states each field once.
func marshalIfSet(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}
