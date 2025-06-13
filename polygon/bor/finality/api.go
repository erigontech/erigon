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

package finality

import (
	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/polygon/bor/finality/whitelist"
)

func GetFinalizedBlockNumber(tx kv.Tx) uint64 {
	currentBlockNum := rawdb.ReadCurrentHeader(tx)

	service := whitelist.GetWhitelistingService()

	doExist, number, hash := service.GetWhitelistedMilestone()
	if doExist && number <= currentBlockNum.Number.Uint64() {

		blockHeader := rawdb.ReadHeaderByNumber(tx, number)

		if blockHeader == nil {
			return 0
		}

		if blockHeader.Hash() == hash {
			return number
		}
	}

	doExist, number, hash = service.GetWhitelistedCheckpoint()
	if doExist && number <= currentBlockNum.Number.Uint64() {

		blockHeader := rawdb.ReadHeaderByNumber(tx, number)

		if blockHeader == nil {
			return 0
		}

		if blockHeader.Hash() == hash {
			return number
		}
	}

	return 0
}

// CurrentFinalizedBlock retrieves the current finalized block of the canonical
// chain. The block is retrieved from the blockchain's internal cache.
func CurrentFinalizedBlock(tx kv.Tx, number uint64) *types.Block {
	// Assuming block is in DB then canoninical hash must be in DB as well.
	hash, err := rawdb.ReadCanonicalHash(tx, number)
	if err != nil || hash == (common.Hash{}) {
		return nil
	}

	return rawdb.ReadBlock(tx, hash, number)
}
