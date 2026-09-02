// Copyright 2024 The Erigon Authors
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

package builder

import (
	"bytes"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

// Parameters for PoS block building
// See also https://github.com/ethereum/execution-apis/blob/main/src/engine/amsterdam.md#payloadattributesv4
type Parameters struct {
	PayloadId             uint64
	ParentHash            common.Hash
	Timestamp             uint64
	PrevRandao            common.Hash
	SuggestedFeeRecipient common.Address
	Withdrawals           []*types.Withdrawal // added in Shapella (EIP-4895)
	ParentBeaconBlockRoot *common.Hash        // added in Dencun (EIP-4788)
	SlotNumber            *uint64             // added in Amsterdam (EIP-7843)
	TargetGasLimit        *uint64             // added in Gloas (EIP-7732)
	// CustomTxnProvider overrides the block's transaction source when non-nil.
	// nil → use the injected TxnProvider (normal mempool path)
	CustomTxnProvider txnprovider.TxnProvider
	// ExtraData overrides the builder's configured extra data when non-nil.
	ExtraData []byte
}

// Copy returns parameters that share nothing mutable with the receiver, except CustomTxnProvider:
// a provider is a live object and stays shared by reference. Reference-typed fields added to
// Parameters have to be handled here; TestParametersCopyCoversEveryField fails if one is not.
func (p *Parameters) Copy() *Parameters {
	if p == nil {
		return nil
	}
	copied := *p
	copied.ExtraData = bytes.Clone(p.ExtraData)
	if p.Withdrawals != nil {
		copied.Withdrawals = make([]*types.Withdrawal, len(p.Withdrawals))
		for i, withdrawal := range p.Withdrawals {
			if withdrawal != nil {
				w := *withdrawal
				copied.Withdrawals[i] = &w
			}
		}
	}
	if p.ParentBeaconBlockRoot != nil {
		root := *p.ParentBeaconBlockRoot
		copied.ParentBeaconBlockRoot = &root
	}
	if p.SlotNumber != nil {
		slot := *p.SlotNumber
		copied.SlotNumber = &slot
	}
	if p.TargetGasLimit != nil {
		limit := *p.TargetGasLimit
		copied.TargetGasLimit = &limit
	}
	return &copied
}
