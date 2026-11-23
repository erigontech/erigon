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

package jsonrpc

import (
	"context"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// latestHead is how "latest" resolves in one committed DB view.
type latestHead struct {
	viewID    uint64
	number    uint64
	executed  uint64
	canonical bool
}

// latestHeadAt resolves "latest" in tx. A DB view does not change until the
// next commit, so the reads are shared by every tx opened on the same view.
// A tx pinned to an overlay is never cached: its view ID only names the
// committed part.
func (api *BaseAPI) latestHeadAt(ctx context.Context, tx kv.Tx) (latestHead, error) {
	overlay, _ := membatchwithdb.ViewOverlay(tx)
	viewID := tx.ViewID()
	cacheable := overlay == nil && viewID != 0
	if cacheable {
		if h := api._latestHead.Load(); h != nil && h.viewID == viewID {
			return *h, nil
		}
	}
	executed, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return latestHead{}, err
	}
	number, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return latestHead{}, err
	}
	_, canonical, err := api._blockReader.CanonicalHash(ctx, tx, number)
	if err != nil {
		return latestHead{}, err
	}
	h := latestHead{viewID: viewID, number: number, executed: executed, canonical: canonical}
	if cacheable {
		api._latestHead.Store(&h)
	}
	return h, nil
}
