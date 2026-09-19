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
	"github.com/erigontech/erigon/rpc/jsonstream/jsonw"
)

// MarshalFastJSONTo writes the header as its own object, for eth_getHeaderByNumber and
// eth_getHeaderByHash. RPCBlock flattens the same fields into its own object instead,
// through WriteFieldsTo.
func (h *RPCHeader) MarshalFastJSONTo(w jsonw.JSONWriter) error {
	if h == nil {
		w.WriteNil()
		return nil
	}
	w.WriteObjectStart()
	h.WriteFieldsTo(w)
	w.WriteObjectEnd()
	return nil
}

// WriteFieldsTo writes the header's fields without the enclosing object, in the order the
// struct declares them so the bytes match reflection exactly. The caller owns the braces,
// which is how RPCBlock flattens the embedded header into its own object.
func (h *RPCHeader) WriteFieldsTo(w jsonw.JSONWriter) {
	w.WriteObjectField("number")
	if h.Number == nil {
		w.WriteNil()
	} else {
		w.WriteQuotedText(h.Number)
	}
	jsonw.Hex(w, "hash", hashOrNull(h.Hash))
	jsonw.Hex(w, "parentHash", h.ParentHash[:])
	jsonw.Hex(w, "nonce", nonceOrNull(h.Nonce))
	jsonw.Hex(w, "mixHash", h.MixHash[:])
	jsonw.Hex(w, "sha3Uncles", h.Sha3Uncles[:])
	jsonw.Hex(w, "logsBloom", bloomOrNull(h.LogsBloom))
	jsonw.Hex(w, "stateRoot", h.StateRoot[:])
	jsonw.Hex(w, "miner", addrOrNull(h.Miner))
	jsonw.Quoted(w, "difficulty", h.Difficulty)
	jsonw.Field(w, "extraData").WriteHex(h.ExtraData)
	jsonw.Quoted(w, "gasLimit", &h.GasLimit)
	jsonw.Quoted(w, "gasUsed", &h.GasUsed)
	jsonw.Quoted(w, "timestamp", &h.Timestamp)
	jsonw.Hex(w, "transactionsRoot", h.TransactionsRoot[:])
	jsonw.Hex(w, "receiptsRoot", h.ReceiptsRoot[:])

	// omitempty: a nil pointer is left out entirely.
	if h.BaseFeePerGas != nil {
		jsonw.Quoted(w, "baseFeePerGas", h.BaseFeePerGas)
	}
	if h.WithdrawalsRoot != nil {
		jsonw.Hex(w, "withdrawalsRoot", h.WithdrawalsRoot[:])
	}
	if h.BlobGasUsed != nil {
		jsonw.Quoted(w, "blobGasUsed", h.BlobGasUsed)
	}
	if h.ExcessBlobGas != nil {
		jsonw.Quoted(w, "excessBlobGas", h.ExcessBlobGas)
	}
	if h.ParentBeaconBlockRoot != nil {
		jsonw.Hex(w, "parentBeaconBlockRoot", h.ParentBeaconBlockRoot[:])
	}
	if h.RequestsHash != nil {
		jsonw.Hex(w, "requestsHash", h.RequestsHash[:])
	}
	if h.BlockAccessListHash != nil {
		jsonw.Hex(w, "blockAccessListHash", h.BlockAccessListHash[:])
	}
	if h.SlotNumber != nil {
		jsonw.Quoted(w, "slotNumber", h.SlotNumber)
	}
	if h.AuraSeal != nil {
		jsonw.Field(w, "auraSeal").WriteHex(*h.AuraSeal)
	}
	if h.AuraStep != nil {
		jsonw.Quoted(w, "auraStep", h.AuraStep)
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
func (b *RPCBlock) MarshalFastJSONTo(w jsonw.JSONWriter) error {
	if b == nil {
		w.WriteNil()
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

	w.WriteObjectStart()
	b.RPCHeader.WriteFieldsTo(w)

	jsonw.Quoted(w, "size", &b.Size)

	// omitempty on an `any` drops only a nil interface, so an empty list still shows.
	switch {
	case hashesOK:
		jsonw.Array(w, "transactions", &hashes, writeHashElem)
	case fullTxs != nil:
		jsonw.Field(w, "transactions").WriteRawBytes(fullTxs)
	}

	jsonw.Array(w, "uncles", &b.Uncles, writeHashElem)

	jsonw.Array(w, "withdrawals", b.Withdrawals, writeWithdrawalElem)
	if txCount != nil {
		jsonw.Field(w, "transactionCount").WriteRawBytes(txCount)
	}
	if b.TotalDifficulty != nil {
		jsonw.Quoted(w, "totalDifficulty", b.TotalDifficulty)
	}
	if calls != nil {
		jsonw.Field(w, "calls").WriteRawBytes(calls)
	}
	w.WriteObjectEnd()
	return nil
}

func writeHashElem(w jsonw.JSONWriter, h *common.Hash) { w.WriteHex(h[:]) }

func writeWithdrawalElem(w jsonw.JSONWriter, wd **types.Withdrawal) {
	if *wd == nil {
		w.WriteNil()
		return
	}
	w.WriteObjectStart()
	w.WriteObjectField("index").WriteQuotedText(&(*wd).Index)
	w.WriteMore()
	w.WriteObjectField("validatorIndex").WriteQuotedText(&(*wd).Validator)
	w.WriteMore()
	w.WriteObjectField("address").WriteHex((*wd).Address[:])
	w.WriteMore()
	w.WriteObjectField("amount").WriteQuotedText(&(*wd).Amount)
	w.WriteObjectEnd()
}

// marshalIfSet encodes v unless it is absent, so the caller states each field once.
func marshalIfSet(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}
