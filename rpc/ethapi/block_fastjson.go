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
	"github.com/erigontech/erigon/common/hexutil"
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

	// The first field of the object opens it without a comma; every helper below writes
	// the separator its field needs.
	w.WriteObjectField("number")
	if h.Number == nil {
		w.WriteNil()
	} else {
		w.WriteQuotedText(h.Number)
	}
	writeHex(w, "hash", hashBytes(h.Hash))
	writeHex(w, "parentHash", h.ParentHash[:])
	writeHex(w, "nonce", nonceBytes(h.Nonce))
	writeHex(w, "mixHash", h.MixHash[:])
	writeHex(w, "sha3Uncles", h.Sha3Uncles[:])
	writeHex(w, "logsBloom", bloomBytes(h.LogsBloom))
	writeHex(w, "stateRoot", h.StateRoot[:])
	writeHex(w, "miner", addrBytes(h.Miner))
	writeU256(w, "difficulty", h.Difficulty)
	writeHexBytes(w, "extraData", h.ExtraData)
	writeUint64(w, "gasLimit", &h.GasLimit)
	writeUint64(w, "gasUsed", &h.GasUsed)
	writeUint64(w, "timestamp", &h.Timestamp)
	writeHex(w, "transactionsRoot", h.TransactionsRoot[:])
	writeHex(w, "receiptsRoot", h.ReceiptsRoot[:])

	// omitempty: a nil pointer is left out entirely.
	if h.BaseFeePerGas != nil {
		writeU256(w, "baseFeePerGas", h.BaseFeePerGas)
	}
	if h.WithdrawalsRoot != nil {
		writeHex(w, "withdrawalsRoot", h.WithdrawalsRoot[:])
	}
	if h.BlobGasUsed != nil {
		writeUint64(w, "blobGasUsed", h.BlobGasUsed)
	}
	if h.ExcessBlobGas != nil {
		writeUint64(w, "excessBlobGas", h.ExcessBlobGas)
	}
	if h.ParentBeaconBlockRoot != nil {
		writeHex(w, "parentBeaconBlockRoot", h.ParentBeaconBlockRoot[:])
	}
	if h.RequestsHash != nil {
		writeHex(w, "requestsHash", h.RequestsHash[:])
	}
	if h.BlockAccessListHash != nil {
		writeHex(w, "blockAccessListHash", h.BlockAccessListHash[:])
	}
	if h.SlotNumber != nil {
		writeUint64(w, "slotNumber", h.SlotNumber)
	}
	if h.AuraSeal != nil {
		writeHexBytes(w, "auraSeal", *h.AuraSeal)
	}
	if h.AuraStep != nil {
		writeUint64(w, "auraStep", h.AuraStep)
	}
}

// field writes the comma a following field needs, then the field name. The first field of
// an object uses WriteObjectField directly.
func field(w jsonw.JSONWriter, name string) jsonw.JSONWriter {
	w.WriteMore()
	return w.WriteObjectField(name)
}

// writeHex writes b as hex, or null when it is nil, which is what a nil pointer field
// marshals to. Go cannot express one generic over arrays of different lengths, so the
// callers slice their own array and this takes the result.
func writeHex(w jsonw.JSONWriter, name string, b []byte) {
	if b == nil {
		field(w, name).WriteNil()
		return
	}
	field(w, name).WriteHex(b)
}

func hashBytes(h *common.Hash) []byte {
	if h == nil {
		return nil
	}
	return h[:]
}

func nonceBytes(n *types.BlockNonce) []byte {
	if n == nil {
		return nil
	}
	return n[:]
}

func bloomBytes(b *types.Bloom) []byte {
	if b == nil {
		return nil
	}
	return b[:]
}

func addrBytes(a *common.Address) []byte {
	if a == nil {
		return nil
	}
	return a[:]
}

// writeHexBytes writes a hexutil.Bytes field, where nil and empty both render as "0x".
func writeHexBytes(w jsonw.JSONWriter, name string, b hexutil.Bytes) { field(w, name).WriteHex(b) }

func writeU256(w jsonw.JSONWriter, name string, v *hexutil.U256) {
	if v == nil {
		field(w, name).WriteNil()
		return
	}
	field(w, name).WriteQuotedText(v)
}

func writeUint64(w jsonw.JSONWriter, name string, v *hexutil.Uint64) {
	field(w, name).WriteQuotedText(v)
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
	if !hashesOK && b.Transactions != nil {
		if fullTxs, err = json.Marshal(b.Transactions); err != nil {
			return err
		}
	}
	if b.TransactionCount != nil {
		if txCount, err = json.Marshal(b.TransactionCount); err != nil {
			return err
		}
	}
	if b.Calls != nil {
		if calls, err = json.Marshal(b.Calls); err != nil {
			return err
		}
	}

	w.WriteObjectStart()
	b.RPCHeader.WriteFieldsTo(w)

	writeUint64(w, "size", &b.Size)

	// omitempty on an `any` drops only a nil interface, so an empty list still shows.
	switch {
	case hashesOK:
		writeHashArray(w, "transactions", hashes)
	case fullTxs != nil:
		field(w, "transactions").WriteRawBytes(fullTxs)
	}

	writeHashArray(w, "uncles", b.Uncles)

	if b.Withdrawals != nil {
		writeWithdrawals(w, "withdrawals", b.Withdrawals)
	}
	if txCount != nil {
		field(w, "transactionCount").WriteRawBytes(txCount)
	}
	if b.TotalDifficulty != nil {
		writeU256(w, "totalDifficulty", b.TotalDifficulty)
	}
	if calls != nil {
		field(w, "calls").WriteRawBytes(calls)
	}
	w.WriteObjectEnd()
	return nil
}

// writeHashArray writes a hash slice, distinguishing nil from empty the way the
// reflection encoder does: a nil slice is null, an empty one is [].
func writeHashArray(w jsonw.JSONWriter, name string, hashes []common.Hash) {
	if hashes == nil {
		field(w, name).WriteNil()
		return
	}
	field(w, name).WriteArrayStart()
	for i := range hashes {
		if i > 0 {
			w.WriteMore()
		}
		w.WriteHex(hashes[i][:])
	}
	w.WriteArrayEnd()
}

// writeWithdrawals writes the withdrawal list. Four fixed-width fields each, so it needs
// no reflection; encoding it here keeps execution/types free of a JSON dependency.
func writeWithdrawals(w jsonw.JSONWriter, name string, ws *types.Withdrawals) {
	if ws == nil {
		field(w, name).WriteNil()
		return
	}
	field(w, name).WriteArrayStart()
	for i, wd := range *ws {
		if i > 0 {
			w.WriteMore()
		}
		if wd == nil {
			w.WriteNil()
			continue
		}
		w.WriteObjectStart()
		w.WriteObjectField("index").WriteQuotedText(&wd.Index)
		w.WriteMore()
		w.WriteObjectField("validatorIndex").WriteQuotedText(&wd.Validator)
		w.WriteMore()
		w.WriteObjectField("address").WriteHex(wd.Address[:])
		w.WriteMore()
		w.WriteObjectField("amount").WriteQuotedText(&wd.Amount)
		w.WriteObjectEnd()
	}
	w.WriteArrayEnd()
}
