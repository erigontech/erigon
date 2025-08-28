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

package misc

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/types"
)

func StoreBlockHashesEip2935(header *types.Header, state *state.IntraBlockState, config *chain.Config, headerReader consensus.ChainHeaderReader) error {
	codeSize, err := state.GetCodeSize(params.HistoryStorageAddress)
	if err != nil {
		return err
	}
	if codeSize == 0 {
		// log.Debug("[EIP-2935] No code deployed to HistoryStorageAddress before call to store EIP-2935 history")
		return nil
	}
	headerNum := header.Number.Uint64()
	if headerNum == 0 { // Activation of fork at Genesis
		return nil
	}
	return storeHash(headerNum-1, header.ParentHash, state)
}

func storeHash(num uint64, hash common.Hash, state *state.IntraBlockState) error {
	slotNum := num % params.BlockHashHistoryServeWindow
	storageSlot := common.BytesToHash(uint256.NewInt(slotNum).Bytes())
	parentHashInt := uint256.NewInt(0).SetBytes32(hash.Bytes())
	return state.SetState(params.HistoryStorageAddress, storageSlot, *parentHashInt)
}
