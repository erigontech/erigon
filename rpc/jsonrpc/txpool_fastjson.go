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
	"maps"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// TxPoolContent is txpool_content's answer: sub-pool, then sender, then nonce. A map result
// reaches the reflection encoder whatever its values implement, so the transactions inside
// only get their marshaller once the map has one.
type TxPoolContent map[string]map[string]map[string]*ethapi.RPCTransaction

// TxPoolContentFrom is txpool_contentFrom's answer: sub-pool, then nonce.
type TxPoolContentFrom map[string]map[string]*ethapi.RPCTransaction

func (c TxPoolContent) MarshalFastJSONTo(w *jsonstream.StackStream) error {
	return writeSortedMap(w, c, func(w *jsonstream.StackStream, senders map[string]map[string]*ethapi.RPCTransaction) error {
		return writeSortedMap(w, senders, writeNonceMap)
	})
}

func (c TxPoolContentFrom) MarshalFastJSONTo(w *jsonstream.StackStream) error {
	return writeSortedMap(w, c, writeNonceMap)
}

// A nil transaction writes itself as null, so the map's values go straight to the marshaller.
func writeNonceMap(w *jsonstream.StackStream, byNonce map[string]*ethapi.RPCTransaction) error {
	return writeSortedMap(w, byNonce, func(w *jsonstream.StackStream, txn *ethapi.RPCTransaction) error {
		return txn.MarshalFastJSONTo(w)
	})
}

// writeSortedMap writes a map as a JSON object with its keys in the order encoding/json
// emits them, which is what keeps the answer byte-identical to the reflected one.
func writeSortedMap[V any](w *jsonstream.StackStream, m map[string]V, value func(*jsonstream.StackStream, V) error) error {
	if m == nil {
		w.WriteNil()
		return nil
	}
	keys := slices.Sorted(maps.Keys(m))

	w.WriteObjectStart()
	for _, k := range keys {
		w.WriteObjectField(k)
		if err := value(w, m[k]); err != nil {
			return err
		}
	}
	w.WriteObjectEnd()
	return nil
}

// StorageValues is eth_getStorageValues' answer: the slots asked for, per account.
type StorageValues map[common.Address][]hexutil.Bytes

func (v StorageValues) MarshalFastJSONTo(w *jsonstream.StackStream) error {
	if v == nil {
		w.WriteNil()
		return nil
	}
	// encoding/json orders these by the key's marshalled text, which for an address is its
	// lowercase hex, so raw byte order is the same order.
	addrs := slices.SortedFunc(maps.Keys(v), func(a, b common.Address) int { return bytes.Compare(a[:], b[:]) })

	w.WriteObjectStart()
	for i := range addrs {
		// Not Hex(): that is the EIP-55 checksum form, where the key is lowercase.
		w.WriteObjectField(hexutil.Encode(addrs[i][:]))
		slots := v[addrs[i]]
		if slots == nil {
			w.WriteNil()
			continue
		}
		jsonstream.WriteHexBytes(w, slots)
	}
	w.WriteObjectEnd()
	return nil
}
