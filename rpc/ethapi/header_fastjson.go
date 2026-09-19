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
	writeHashPtr(w, "hash", h.Hash)
	writeHash32(w, "parentHash", &h.ParentHash)
	writeBytesPtr(w, "nonce", nonceBytes(h.Nonce))
	writeHash32(w, "mixHash", &h.MixHash)
	writeHash32(w, "sha3Uncles", &h.Sha3Uncles)
	writeBytesPtr(w, "logsBloom", bloomBytes(h.LogsBloom))
	writeHash32(w, "stateRoot", &h.StateRoot)
	writeBytesPtr(w, "miner", addrBytes(h.Miner))
	writeU256(w, "difficulty", h.Difficulty)
	writeHexBytes(w, "extraData", h.ExtraData)
	writeUint64(w, "gasLimit", &h.GasLimit)
	writeUint64(w, "gasUsed", &h.GasUsed)
	writeUint64(w, "timestamp", &h.Timestamp)
	writeHash32(w, "transactionsRoot", &h.TransactionsRoot)
	writeHash32(w, "receiptsRoot", &h.ReceiptsRoot)

	// omitempty: a nil pointer is left out entirely.
	if h.BaseFeePerGas != nil {
		writeU256(w, "baseFeePerGas", h.BaseFeePerGas)
	}
	if h.WithdrawalsRoot != nil {
		writeHashPtr(w, "withdrawalsRoot", h.WithdrawalsRoot)
	}
	if h.BlobGasUsed != nil {
		writeUint64(w, "blobGasUsed", h.BlobGasUsed)
	}
	if h.ExcessBlobGas != nil {
		writeUint64(w, "excessBlobGas", h.ExcessBlobGas)
	}
	if h.ParentBeaconBlockRoot != nil {
		writeHashPtr(w, "parentBeaconBlockRoot", h.ParentBeaconBlockRoot)
	}
	if h.RequestsHash != nil {
		writeHashPtr(w, "requestsHash", h.RequestsHash)
	}
	if h.BlockAccessListHash != nil {
		writeHashPtr(w, "blockAccessListHash", h.BlockAccessListHash)
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

// field writes the comma a following field needs, then the field name. The first field of
// an object uses WriteObjectField directly.
func field(w jsonw.JSONWriter, name string) jsonw.JSONWriter {
	w.WriteMore()
	return w.WriteObjectField(name)
}

func writeHash32(w jsonw.JSONWriter, name string, h *common.Hash) { field(w, name).WriteHex(h[:]) }

func writeHashPtr(w jsonw.JSONWriter, name string, h *common.Hash) {
	if h == nil {
		field(w, name).WriteNil()
		return
	}
	field(w, name).WriteHex(h[:])
}

// bytesPtr writes nil as JSON null, which is what a nil pointer field marshals to.
func writeBytesPtr(w jsonw.JSONWriter, name string, b []byte) {
	if b == nil {
		field(w, name).WriteNil()
		return
	}
	field(w, name).WriteHex(b)
}

// hexBytes writes a non-pointer hexutil.Bytes, where nil and empty both render as "0x".
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
	var fullTxs, withdrawals, txCount, calls []byte
	hashes, hashesOK := b.Transactions.([]common.Hash)
	for _, pre := range []struct {
		v   any
		out *[]byte
	}{
		{orNil(b.Transactions, hashesOK), &fullTxs},
		{orNil(b.Withdrawals, b.Withdrawals == nil), &withdrawals},
		{orNil(b.TransactionCount, b.TransactionCount == nil), &txCount},
		{orNil(b.Calls, b.Calls == nil), &calls},
	} {
		if pre.v == nil {
			continue
		}
		enc, err := json.Marshal(pre.v)
		if err != nil {
			return err
		}
		*pre.out = enc
	}

	w.WriteObjectStart()
	b.RPCHeader.WriteFieldsTo(w)

	writeUint64(w, "size", &b.Size)

	// omitempty on an `any` drops only a nil interface, so an empty list still shows.
	switch {
	case hashesOK:
		field(w, "transactions").WriteArrayStart()
		for i := range hashes {
			if i > 0 {
				w.WriteMore()
			}
			w.WriteHex(hashes[i][:])
		}
		w.WriteArrayEnd()
	case fullTxs != nil:
		field(w, "transactions").WriteRawBytes(fullTxs)
	}

	if b.Uncles == nil {
		field(w, "uncles").WriteNil()
	} else {
		field(w, "uncles").WriteArrayStart()
		for i := range b.Uncles {
			if i > 0 {
				w.WriteMore()
			}
			w.WriteHex(b.Uncles[i][:])
		}
		w.WriteArrayEnd()
	}

	if withdrawals != nil {
		field(w, "withdrawals").WriteRawBytes(withdrawals)
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

// orNil returns v unless skip, so a field the fast path handles itself is not encoded twice.
func orNil(v any, skip bool) any {
	if skip {
		return nil
	}
	return v
}
