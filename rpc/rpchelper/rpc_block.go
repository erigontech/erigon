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

package rpchelper

import (
	"fmt"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/rpc"
)

const UnknownBlockCode = -39001

func unknownBlockErr(requested string, latestBlock uint64) *rpc.CustomError {
	return &rpc.CustomError{
		Code:    UnknownBlockCode,
		Message: fmt.Sprintf("block %q not available (head block: %d)", requested, latestBlock),
	}
}

func GetLatestBlockNumber(tx kv.Tx) (uint64, error) {
	forkchoiceHeadHash, ok, err := rawdb.ReadForkchoiceHead(tx)
	if err != nil {
		return 0, err
	}
	if ok {
		forkchoiceHeadNum, ok, err := rawdb.ReadHeaderNumber(tx, forkchoiceHeadHash)
		if err != nil {
			return 0, err
		}
		if ok {
			return forkchoiceHeadNum, nil
		}
	}

	blockNum, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return 0, fmt.Errorf("getting latest block number: %w", err)
	}

	return blockNum, nil
}

func GetFinalizedBlockNumber(tx kv.Tx) (uint64, error) {
	forkchoiceFinalizedHash, ok, err := rawdb.ReadForkchoiceFinalized(tx)
	if err != nil {
		return 0, err
	}
	if ok {
		forkchoiceFinalizedNum, ok, err := rawdb.ReadHeaderNumber(tx, forkchoiceFinalizedHash)
		if err != nil {
			return 0, err
		}
		if ok {
			return forkchoiceFinalizedNum, nil
		}
	}

	latest, err := GetLatestBlockNumber(tx)
	if err != nil {
		return 0, err
	}
	return 0, unknownBlockErr("finalized", latest)
}

func GetSafeBlockNumber(tx kv.Tx) (uint64, error) {
	forkchoiceSafeHash, ok, err := rawdb.ReadForkchoiceSafe(tx)
	if err != nil {
		return 0, err
	}
	if ok {
		forkchoiceSafeNum, ok, err := rawdb.ReadHeaderNumber(tx, forkchoiceSafeHash)
		if err != nil {
			return 0, err
		}
		if ok {
			return forkchoiceSafeNum, nil
		}
	}

	latest, err := GetLatestBlockNumber(tx)
	if err != nil {
		return 0, err
	}
	return 0, unknownBlockErr("safe", latest)
}

func GetLatestExecutedBlockNumber(tx kv.Tx) (uint64, error) {
	blockNum, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return 0, err
	}
	return blockNum, err
}
