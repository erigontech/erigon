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

// MarshalFastJSONTo writes the header as its own object, for eth_getHeaderByNumber and
// eth_getHeaderByHash. RPCBlock flattens the same fields into its own object instead,
// through WriteFieldsTo.
func (h *RPCHeader) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	if h == nil {
		s.WriteNil()
		return nil
	}
	s.WriteObjectStart()
	h.WriteFieldsTo(s)
	s.WriteObjectEnd()
	return nil
}

// WriteFieldsTo writes the header's fields without the enclosing object, in the order the
// struct declares them so the bytes match reflection exactly. The caller owns the braces,
// which is how RPCBlock flattens the embedded header into its own object.
func (h *RPCHeader) WriteFieldsTo(s *jsonstream.StackStream) {
	s.WriteObjectField("number")
	if h.Number == nil {
		s.WriteNil()
	} else {
		s.WriteQuotedText(h.Number)
	}
	jsonstream.Field(s, "hash")
	if h.Hash == nil {
		s.WriteNil()
	} else {
		s.WriteHex(h.Hash[:])
	}
	jsonstream.Field(s, "parentHash").WriteHex(h.ParentHash[:])
	jsonstream.Field(s, "nonce")
	if h.Nonce == nil {
		s.WriteNil()
	} else {
		s.WriteHex(h.Nonce[:])
	}
	jsonstream.Field(s, "mixHash").WriteHex(h.MixHash[:])
	jsonstream.Field(s, "sha3Uncles").WriteHex(h.Sha3Uncles[:])
	jsonstream.Field(s, "logsBloom")
	if h.LogsBloom == nil {
		s.WriteNil()
	} else {
		s.WriteHex(h.LogsBloom[:])
	}
	jsonstream.Field(s, "stateRoot").WriteHex(h.StateRoot[:])
	jsonstream.Field(s, "miner")
	if h.Miner == nil {
		s.WriteNil()
	} else {
		s.WriteHex(h.Miner[:])
	}
	jsonstream.Text(s, "difficulty", h.Difficulty)
	jsonstream.Field(s, "extraData").WriteHex(h.ExtraData)
	jsonstream.Text(s, "gasLimit", &h.GasLimit)
	jsonstream.Text(s, "gasUsed", &h.GasUsed)
	jsonstream.Text(s, "timestamp", &h.Timestamp)
	jsonstream.Field(s, "transactionsRoot").WriteHex(h.TransactionsRoot[:])
	jsonstream.Field(s, "receiptsRoot").WriteHex(h.ReceiptsRoot[:])

	// omitempty: a nil pointer is left out entirely.
	if h.BaseFeePerGas != nil {
		jsonstream.Text(s, "baseFeePerGas", h.BaseFeePerGas)
	}
	if h.WithdrawalsRoot != nil {
		jsonstream.Field(s, "withdrawalsRoot").WriteHex(h.WithdrawalsRoot[:])
	}
	if h.BlobGasUsed != nil {
		jsonstream.Text(s, "blobGasUsed", h.BlobGasUsed)
	}
	if h.ExcessBlobGas != nil {
		jsonstream.Text(s, "excessBlobGas", h.ExcessBlobGas)
	}
	if h.ParentBeaconBlockRoot != nil {
		jsonstream.Field(s, "parentBeaconBlockRoot").WriteHex(h.ParentBeaconBlockRoot[:])
	}
	if h.RequestsHash != nil {
		jsonstream.Field(s, "requestsHash").WriteHex(h.RequestsHash[:])
	}
	if h.BlockAccessListHash != nil {
		jsonstream.Field(s, "blockAccessListHash").WriteHex(h.BlockAccessListHash[:])
	}
	if h.SlotNumber != nil {
		jsonstream.Text(s, "slotNumber", h.SlotNumber)
	}
	if h.AuraSeal != nil {
		jsonstream.Field(s, "auraSeal").WriteHex(*h.AuraSeal)
	}
	if h.AuraStep != nil {
		jsonstream.Text(s, "auraStep", h.AuraStep)
	}
}

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
	var rawTxs, txCount, calls []byte
	var err error
	if !hashesOK && !fullOK {
		if rawTxs, err = marshalIfSet(b.Transactions); err != nil {
			return err
		}
	}
	if txCount, err = marshalIfSet(b.TransactionCount); err != nil {
		return err
	}
	if calls, err = marshalIfSet(b.Calls); err != nil {
		return err
	}

	s.WriteObjectStart()
	b.RPCHeader.WriteFieldsTo(s)

	jsonstream.Text(s, "size", &b.Size)

	// omitempty on an `any` drops only a nil interface, so an empty list still shows.
	switch {
	case hashesOK:
		jsonstream.HexesField(s, "transactions", hashes)
	case fullOK:
		jsonstream.Field(s, "transactions")
		jsonstream.ArrayValue(s, full, writeTxElem)
	case rawTxs != nil:
		jsonstream.Field(s, "transactions").WriteRawBytes(rawTxs)
	}

	jsonstream.HexesField(s, "uncles", b.Uncles)

	if b.Withdrawals != nil {
		jsonstream.Field(s, "withdrawals")
		jsonstream.ArrayValue(s, *b.Withdrawals, writeWithdrawalElem)
	}
	if txCount != nil {
		jsonstream.Field(s, "transactionCount").WriteRawBytes(txCount)
	}
	if b.TotalDifficulty != nil {
		jsonstream.Text(s, "totalDifficulty", b.TotalDifficulty)
	}
	if calls != nil {
		jsonstream.Field(s, "calls").WriteRawBytes(calls)
	}
	s.WriteObjectEnd()
	return nil
}

// writeTxElem never fails: RPCTransaction.MarshalFastJSONTo reports no error.
func writeTxElem(s *jsonstream.StackStream, t **RPCTransaction) { _ = (*t).MarshalFastJSONTo(s) }

func writeWithdrawalElem(s *jsonstream.StackStream, wd **types.Withdrawal) {
	if *wd == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	s.WriteObjectField("index").WriteQuotedText(&(*wd).Index)
	s.WriteMore()
	s.WriteObjectField("validatorIndex").WriteQuotedText(&(*wd).Validator)
	s.WriteMore()
	s.WriteObjectField("address").WriteHex((*wd).Address[:])
	s.WriteMore()
	s.WriteObjectField("amount").WriteQuotedText(&(*wd).Amount)
	s.WriteObjectEnd()
}

// marshalIfSet encodes v unless it is absent, so the caller states each field once.
func marshalIfSet(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}
