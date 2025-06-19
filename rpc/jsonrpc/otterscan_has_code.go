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
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func (api *OtterscanAPIImpl) HasCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (bool, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return false, fmt.Errorf("hasCode cannot open tx: %w", err)
	}
	defer tx.Rollback()

	blockNumber, _, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return false, err
	}
	//chainConfig, err := api.chainConfig(ctx, tx)
	//if err != nil {
	//	return false, err
	//}

	reader, err := rpchelper.CreateHistoryStateReader(tx, blockNumber, 0, api._txNumReader)
	if err != nil {
		return false, err
	}
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return false, err
	}
	return !acc.IsEmptyCodeHash(), nil
}
