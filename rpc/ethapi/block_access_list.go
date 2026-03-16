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
)

// RPC response types for eth_getBlockAccessList (EIP-7928).

type RPCStorageChange struct {
	Index hexutil.Uint64 `json:"index"`
	Value common.Hash    `json:"value"`
}

type RPCSlotChanges struct {
	Key     common.Hash         `json:"key"`
	Changes []*RPCStorageChange `json:"changes"`
}

type RPCBalanceChange struct {
	Index hexutil.Uint64 `json:"index"`
	Value *hexutil.Big   `json:"value"`
}

type RPCNonceChange struct {
	Index hexutil.Uint64 `json:"index"`
	Value hexutil.Uint64 `json:"value"`
}

type RPCCodeChange struct {
	Index hexutil.Uint64 `json:"index"`
	Code  hexutil.Bytes  `json:"code"`
}

// RPCAccountAccess is the JSON-RPC response element for eth_getBlockAccessList (EIP-7928).
type RPCAccountAccess struct {
	Address        common.Address      `json:"address"`
	StorageChanges []*RPCSlotChanges   `json:"storageChanges"`
	StorageReads   []common.Hash       `json:"storageReads"`
	BalanceChanges []*RPCBalanceChange `json:"balanceChanges"`
	NonceChanges   []*RPCNonceChange   `json:"nonceChanges"`
	CodeChanges    []*RPCCodeChange    `json:"codeChanges"`
}

// MarshalBlockAccessList converts a types.BlockAccessList into the JSON-RPC response format.
func MarshalBlockAccessList(bal types.BlockAccessList) []*RPCAccountAccess {
	result := make([]*RPCAccountAccess, len(bal))
	for i, ac := range bal {
		entry := &RPCAccountAccess{
			Address:        ac.Address.Value(),
			StorageChanges: make([]*RPCSlotChanges, len(ac.StorageChanges)),
			StorageReads:   make([]common.Hash, len(ac.StorageReads)),
			BalanceChanges: make([]*RPCBalanceChange, len(ac.BalanceChanges)),
			NonceChanges:   make([]*RPCNonceChange, len(ac.NonceChanges)),
			CodeChanges:    make([]*RPCCodeChange, len(ac.CodeChanges)),
		}
		for j, sc := range ac.StorageChanges {
			slot := &RPCSlotChanges{
				Key:     sc.Slot.Value(),
				Changes: make([]*RPCStorageChange, len(sc.Changes)),
			}
			for k, ch := range sc.Changes {
				slot.Changes[k] = &RPCStorageChange{
					Index: hexutil.Uint64(ch.Index),
					Value: ch.Value.Bytes32(),
				}
			}
			entry.StorageChanges[j] = slot
		}
		for j, sr := range ac.StorageReads {
			entry.StorageReads[j] = sr.Value()
		}
		for j, bc := range ac.BalanceChanges {
			v := bc.Value.ToBig()
			entry.BalanceChanges[j] = &RPCBalanceChange{
				Index: hexutil.Uint64(bc.Index),
				Value: (*hexutil.Big)(v),
			}
		}
		for j, nc := range ac.NonceChanges {
			entry.NonceChanges[j] = &RPCNonceChange{
				Index: hexutil.Uint64(nc.Index),
				Value: hexutil.Uint64(nc.Value),
			}
		}
		for j, cc := range ac.CodeChanges {
			entry.CodeChanges[j] = &RPCCodeChange{
				Index: hexutil.Uint64(cc.Index),
				Code:  cc.Bytecode,
			}
		}
		result[i] = entry
	}
	return result
}
