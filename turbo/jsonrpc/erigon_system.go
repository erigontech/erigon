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

package jsonrpc

import (
	"context"
	"errors"

	"github.com/erigontech/erigon/erigon-lib/common/hexutil"

	"github.com/erigontech/erigon/erigon-lib/common"

	"github.com/erigontech/erigon/core/forkid"
	borfinality "github.com/erigontech/erigon/polygon/bor/finality"
	"github.com/erigontech/erigon/polygon/bor/finality/whitelist"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
)

// Forks is a data type to record a list of forks passed by this node
type Forks struct {
	GenesisHash common.Hash `json:"genesis"`
	HeightForks []uint64    `json:"heightForks"`
	TimeForks   []uint64    `json:"timeForks"`
}

// Forks implements erigon_forks. Returns the genesis block hash and a sorted list of all forks block numbers
func (api *ErigonImpl) Forks(ctx context.Context) (Forks, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return Forks{}, err
	}
	defer tx.Rollback()

	chainConfig, genesis, err := api.chainConfigWithGenesis(ctx, tx)
	if err != nil {
		return Forks{}, err
	}
	heightForks, timeForks := forkid.GatherForks(chainConfig, genesis.Time())

	return Forks{genesis.Hash(), heightForks, timeForks}, nil
}

// Post the merge eth_blockNumber will return latest forkChoiceHead block number
// erigon_blockNumber will return latest executed block number or any block number requested
func (api *ErigonImpl) BlockNumber(ctx context.Context, rpcBlockNumPtr *rpc.BlockNumber) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var rpcBlockNum rpc.BlockNumber
	if rpcBlockNumPtr == nil {
		rpcBlockNum = rpc.LatestExecutedBlockNumber
	} else {
		rpcBlockNum = *rpcBlockNumPtr
	}

	var blockNum uint64
	switch rpcBlockNum {
	case rpc.LatestBlockNumber:
		blockNum, err = rpchelper.GetLatestBlockNumber(tx)
		if err != nil {
			return 0, err
		}
	case rpc.EarliestBlockNumber:
		blockNum = 0
	case rpc.SafeBlockNumber:
		blockNum, err = rpchelper.GetSafeBlockNumber(tx)
		if err != nil {
			return 0, err
		}
	case rpc.FinalizedBlockNumber:
		if whitelist.GetWhitelistingService() != nil {
			num := borfinality.GetFinalizedBlockNumber(tx)
			if num == 0 {
				return 0, errors.New("no finalized block")
			}

			blockNum = borfinality.CurrentFinalizedBlock(tx, num).NumberU64()
			return hexutil.Uint64(blockNum), nil
		}

		blockNum, err = rpchelper.GetFinalizedBlockNumber(tx)
		if err != nil {
			return 0, err
		}
	default:
		blockNum, err = rpchelper.GetLatestExecutedBlockNumber(tx)
		if err != nil {
			return 0, err
		}
	}

	return hexutil.Uint64(blockNum), nil
}
