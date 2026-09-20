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
	"github.com/erigontech/erigon/rpc/jsonstream"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
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
	jsonstream.Hex(s, "hash", hashOrNull(h.Hash))
	jsonstream.Hex(s, "parentHash", h.ParentHash[:])
	jsonstream.Hex(s, "nonce", nonceOrNull(h.Nonce))
	jsonstream.Hex(s, "mixHash", h.MixHash[:])
	jsonstream.Hex(s, "sha3Uncles", h.Sha3Uncles[:])
	jsonstream.Hex(s, "logsBloom", bloomOrNull(h.LogsBloom))
	jsonstream.Hex(s, "stateRoot", h.StateRoot[:])
	jsonstream.Hex(s, "miner", addrOrNull(h.Miner))
	jsonstream.Text(s, "difficulty", h.Difficulty)
	jsonstream.Field(s, "extraData").WriteHex(h.ExtraData)
	jsonstream.Text(s, "gasLimit", &h.GasLimit)
	jsonstream.Text(s, "gasUsed", &h.GasUsed)
	jsonstream.Text(s, "timestamp", &h.Timestamp)
	jsonstream.Hex(s, "transactionsRoot", h.TransactionsRoot[:])
	jsonstream.Hex(s, "receiptsRoot", h.ReceiptsRoot[:])

	// omitempty: a nil pointer is left out entirely.
	if h.BaseFeePerGas != nil {
		jsonstream.Text(s, "baseFeePerGas", h.BaseFeePerGas)
	}
	if h.WithdrawalsRoot != nil {
		jsonstream.Hex(s, "withdrawalsRoot", h.WithdrawalsRoot[:])
	}
	if h.BlobGasUsed != nil {
		jsonstream.Text(s, "blobGasUsed", h.BlobGasUsed)
	}
	if h.ExcessBlobGas != nil {
		jsonstream.Text(s, "excessBlobGas", h.ExcessBlobGas)
	}
	if h.ParentBeaconBlockRoot != nil {
		jsonstream.Hex(s, "parentBeaconBlockRoot", h.ParentBeaconBlockRoot[:])
	}
	if h.RequestsHash != nil {
		jsonstream.Hex(s, "requestsHash", h.RequestsHash[:])
	}
	if h.BlockAccessListHash != nil {
		jsonstream.Hex(s, "blockAccessListHash", h.BlockAccessListHash[:])
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

// The OrNull helpers turn a nil pointer into the nil slice jsonw.Hex renders as null.
func hashOrNull(h *common.Hash) []byte {
	if h == nil {
		return nil
	}
	return h[:]
}

func nonceOrNull(n *types.BlockNonce) []byte {
	if n == nil {
		return nil
	}
	return n[:]
}

func bloomOrNull(b *types.Bloom) []byte {
	if b == nil {
		return nil
	}
	return b[:]
}

func addrOrNull(a *common.Address) []byte {
	if a == nil {
		return nil
	}
	return a[:]
}

// MarshalFastJSONTo writes the whole block. It must exist: RPCBlock embeds RPCHeader, so
// without it the promoted header method would satisfy the fast-JSON interface and a block
// would serialise as a bare header, losing its transactions.
func (b *RPCBlock) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	if b == nil {
		s.WriteNil()
		return nil
	}

	// The `any` fields carry whatever concrete type their caller set, so they go through
	// the reflection encoder. That is done up front: the contract is that a marshaller
	// reports failure before its first write, never with half a result already streamed.
	hashes, hashesOK := b.Transactions.([]common.Hash)
	var fullTxs, txCount, calls []byte
	var err error
	if !hashesOK {
		if fullTxs, err = marshalIfSet(b.Transactions); err != nil {
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
		jsonstream.Array(s, "transactions", &hashes, writeHashElem)
	case fullTxs != nil:
		jsonstream.Field(s, "transactions").WriteRawBytes(fullTxs)
	}

	jsonstream.Array(s, "uncles", &b.Uncles, writeHashElem)

	jsonstream.Array(s, "withdrawals", b.Withdrawals, writeWithdrawalElem)
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

func writeHashElem(s *jsonstream.StackStream, h *common.Hash) { s.WriteHex(h[:]) }

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
