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
	h.WriteFieldsTo(w, false)
	w.WriteObjectEnd()
	return nil
}

// WriteFieldsTo writes the header's fields without the enclosing object, in the order
// the struct declares them so the bytes match reflection exactly. When precededByField
// is set a comma is written first, for an embedder that already wrote fields of its own.
func (h *RPCHeader) WriteFieldsTo(w jsonw.JSONWriter, precededByField bool) {
	f := fieldWriter{w: w, wrote: precededByField}

	f.u256("number", h.Number)
	f.hashPtr("hash", h.Hash)
	f.hash32("parentHash", h.ParentHash)
	f.bytesPtr("nonce", nonceBytes(h.Nonce))
	f.hash32("mixHash", h.MixHash)
	f.hash32("sha3Uncles", h.Sha3Uncles)
	f.bytesPtr("logsBloom", bloomBytes(h.LogsBloom))
	f.hash32("stateRoot", h.StateRoot)
	f.bytesPtr("miner", addrBytes(h.Miner))
	f.u256("difficulty", h.Difficulty)
	f.hexBytes("extraData", h.ExtraData)
	f.uint64("gasLimit", h.GasLimit)
	f.uint64("gasUsed", h.GasUsed)
	f.uint64("timestamp", h.Timestamp)
	f.hash32("transactionsRoot", h.TransactionsRoot)
	f.hash32("receiptsRoot", h.ReceiptsRoot)

	// omitempty: a nil pointer is left out entirely.
	if h.BaseFeePerGas != nil {
		f.u256("baseFeePerGas", h.BaseFeePerGas)
	}
	if h.WithdrawalsRoot != nil {
		f.hashPtr("withdrawalsRoot", h.WithdrawalsRoot)
	}
	if h.BlobGasUsed != nil {
		f.uint64("blobGasUsed", *h.BlobGasUsed)
	}
	if h.ExcessBlobGas != nil {
		f.uint64("excessBlobGas", *h.ExcessBlobGas)
	}
	if h.ParentBeaconBlockRoot != nil {
		f.hashPtr("parentBeaconBlockRoot", h.ParentBeaconBlockRoot)
	}
	if h.RequestsHash != nil {
		f.hashPtr("requestsHash", h.RequestsHash)
	}
	if h.BlockAccessListHash != nil {
		f.hashPtr("blockAccessListHash", h.BlockAccessListHash)
	}
	if h.SlotNumber != nil {
		f.uint64("slotNumber", *h.SlotNumber)
	}
	if h.AuraSeal != nil {
		f.hexBytes("auraSeal", *h.AuraSeal)
	}
	if h.AuraStep != nil {
		f.uint64("auraStep", *h.AuraStep)
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

// fieldWriter puts the comma between fields, so each field site states only its name
// and value.
type fieldWriter struct {
	w     jsonw.JSONWriter
	wrote bool
}

func (f *fieldWriter) field(name string) jsonw.JSONWriter {
	if f.wrote {
		f.w.WriteMore()
	}
	f.wrote = true
	return f.w.WriteObjectField(name)
}

func (f *fieldWriter) hash32(name string, h common.Hash) { f.field(name).WriteHex(h[:]) }

func (f *fieldWriter) hashPtr(name string, h *common.Hash) {
	if h == nil {
		f.field(name).WriteNil()
		return
	}
	f.field(name).WriteHex(h[:])
}

// bytesPtr writes nil as JSON null, which is what a nil pointer field marshals to.
func (f *fieldWriter) bytesPtr(name string, b []byte) {
	if b == nil {
		f.field(name).WriteNil()
		return
	}
	f.field(name).WriteHex(b)
}

// hexBytes writes a non-pointer hexutil.Bytes, where nil and empty both render as "0x".
func (f *fieldWriter) hexBytes(name string, b hexutil.Bytes) { f.field(name).WriteHex(b) }

func (f *fieldWriter) u256(name string, v *hexutil.U256) {
	if v == nil {
		f.field(name).WriteNil()
		return
	}
	f.field(name).WriteQuotedText(v)
}

func (f *fieldWriter) uint64(name string, v hexutil.Uint64) { f.field(name).WriteQuotedText(&v) }
