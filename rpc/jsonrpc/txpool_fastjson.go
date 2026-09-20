// Copyright 2025 The Erigon Authors
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

package jsonrpc

import (
	"bytes"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream/jsonw"
)

// TxPoolContent is txpool_content's answer: sub-pool, then sender, then nonce. A map result
// reaches the reflection encoder whatever its values implement, so the transactions inside
// only get their own marshaller once the map has one.
type TxPoolContent map[string]map[string]map[string]*ethapi.RPCTransaction

// TxPoolContentFrom is txpool_contentFrom's answer: sub-pool, then nonce.
type TxPoolContentFrom map[string]map[string]*ethapi.RPCTransaction

func (c TxPoolContent) MarshalFastJSONTo(w jsonw.JSONWriter) error {
	writeSortedMap(w, c, func(w jsonw.JSONWriter, senders map[string]map[string]*ethapi.RPCTransaction) {
		writeSortedMap(w, senders, writeNonceMap)
	})
	return nil
}

func (c TxPoolContentFrom) MarshalFastJSONTo(w jsonw.JSONWriter) error {
	writeSortedMap(w, c, writeNonceMap)
	return nil
}

// A nil transaction writes itself as null, so the map's values go straight to the marshaller.
func writeNonceMap(w jsonw.JSONWriter, byNonce map[string]*ethapi.RPCTransaction) {
	writeSortedMap(w, byNonce, func(w jsonw.JSONWriter, txn *ethapi.RPCTransaction) {
		_ = txn.MarshalFastJSONTo(w)
	})
}

// writeSortedMap writes a map as a JSON object with its keys in the order encoding/json
// emits them, which is what keeps the answer byte-identical to the reflected one.
func writeSortedMap[V any](w jsonw.JSONWriter, m map[string]V, value func(jsonw.JSONWriter, V)) {
	if m == nil {
		w.WriteNil()
		return
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	w.WriteObjectStart()
	for i, k := range keys {
		if i > 0 {
			w.WriteMore()
		}
		w.WriteObjectField(k)
		value(w, m[k])
	}
	w.WriteObjectEnd()
}

// StorageValues is eth_getStorageValues' answer: the slots asked for, per account.
type StorageValues map[common.Address][]hexutil.Bytes

func (v StorageValues) MarshalFastJSONTo(w jsonw.JSONWriter) error {
	if v == nil {
		w.WriteNil()
		return nil
	}
	addrs := make([]common.Address, 0, len(v))
	for a := range v {
		addrs = append(addrs, a)
	}
	// encoding/json orders these by the key's marshalled text, which for an address is its
	// lowercase hex, so raw byte order is the same order.
	slices.SortFunc(addrs, func(a, b common.Address) int { return bytes.Compare(a[:], b[:]) })

	w.WriteObjectStart()
	for i := range addrs {
		if i > 0 {
			w.WriteMore()
		}
		// Not Hex(): that is the EIP-55 checksum form, where the key is lowercase. jsonw.Array
		// writes the field's separator too, which an object's first field must not have.
		w.WriteObjectField(hexutil.Encode(addrs[i][:]))
		slots := v[addrs[i]]
		if slots == nil {
			w.WriteNil()
			continue
		}
		w.WriteArrayStart()
		for j := range slots {
			if j > 0 {
				w.WriteMore()
			}
			w.WriteHex(slots[j])
		}
		w.WriteArrayEnd()
	}
	w.WriteObjectEnd()
	return nil
}
