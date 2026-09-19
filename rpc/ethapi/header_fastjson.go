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

// MarshalFastJSONTo writes the whole block. It must exist: RPCBlock embeds RPCHeader, so
// without it the promoted header method would satisfy the fast-JSON interface and a block
// would serialise as a bare header, losing its transactions.
func (b *RPCBlock) MarshalFastJSONTo(w jsonw.JSONWriter) error {
	if b == nil {
		w.WriteNil()
		return nil
	}
	w.WriteObjectStart()
	b.RPCHeader.WriteFieldsTo(w, false)
	f := fieldWriter{w: w, wrote: true}

	f.uint64("size", b.Size)

	// omitempty on an `any` drops only a nil interface, so an empty list still shows.
	if b.Transactions != nil {
		switch txs := b.Transactions.(type) {
		case []common.Hash:
			f.field("transactions").WriteArrayStart()
			for i := range txs {
				if i > 0 {
					w.WriteMore()
				}
				w.WriteHex(txs[i][:])
			}
			w.WriteArrayEnd()
		default:
			if err := f.reflected("transactions", b.Transactions); err != nil {
				return err
			}
		}
	}

	if b.Uncles == nil {
		f.field("uncles").WriteNil()
	} else {
		f.field("uncles").WriteArrayStart()
		for i := range b.Uncles {
			if i > 0 {
				w.WriteMore()
			}
			w.WriteHex(b.Uncles[i][:])
		}
		w.WriteArrayEnd()
	}

	if b.Withdrawals != nil {
		if err := f.reflected("withdrawals", b.Withdrawals); err != nil {
			return err
		}
	}
	if b.TransactionCount != nil {
		if err := f.reflected("transactionCount", b.TransactionCount); err != nil {
			return err
		}
	}
	if b.TotalDifficulty != nil {
		f.u256("totalDifficulty", b.TotalDifficulty)
	}
	if b.Calls != nil {
		if err := f.reflected("calls", b.Calls); err != nil {
			return err
		}
	}
	w.WriteObjectEnd()
	return nil
}

// reflected encodes a value the fast path has no shape for, and hands the bytes over
// verbatim. Used for the `any` fields, whose concrete type varies by caller.
func (f *fieldWriter) reflected(name string, v any) error {
	enc, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f.field(name)
	if raw, ok := f.w.(interface{ WriteRawBytes([]byte) }); ok {
		raw.WriteRawBytes(enc)
		return nil
	}
	f.w.WriteString(string(enc))
	return nil
}
