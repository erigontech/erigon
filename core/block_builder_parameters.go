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

package core

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/types"
)

// Parameters for PoS block building
// See also https://github.com/ethereum/execution-apis/blob/main/src/engine/cancun.md#payloadattributesv3
type BlockBuilderParameters struct {
	PayloadId             uint64
	ParentHash            common.Hash
	Timestamp             uint64
	PrevRandao            common.Hash
	SuggestedFeeRecipient common.Address
	Withdrawals           []*types.Withdrawal // added in Shapella (EIP-4895)
	ParentBeaconBlockRoot *common.Hash        // added in Dencun (EIP-4788)
}
