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

package commands

import (
	"context"
	"errors"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

func openPBTBlockReader(ctx context.Context, dirs datadir.Dirs, db kv.RoDB, logger log.Logger) (*freezeblocks.BlockReader, *blocksnapshots.View, func(), error) {
	var chainName string
	err := db.View(ctx, func(tx kv.Tx) error {
		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		chainConfig, err := rawdb.ReadChainConfig(tx, genesisHash)
		if err != nil {
			return err
		}
		if chainConfig == nil {
			return errors.New("pbt block reader: chain config is missing")
		}
		chainName = chainConfig.ChainName
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}
	cfg := ethconfig.NewSnapCfg(false, true, true, chainName)
	snapshots := blocksnapshots.NewRoSnapshots(cfg, dirs.Snap, logger)
	if err := snapshots.OpenFolder(); err != nil {
		snapshots.Close()
		return nil, nil, nil, err
	}
	reader := freezeblocks.NewBlockReader(snapshots)
	view := snapshots.View()
	return reader, view, func() { view.Close(); snapshots.Close() }, nil
}

func pbtHeaderByNumber(tx kv.Getter, reader *freezeblocks.BlockReader, view *blocksnapshots.View, blockNum uint64) (*types.Header, error) {
	header, err := reader.HeaderFromView(view, blockNum)
	if err != nil {
		return nil, err
	}
	if header != nil {
		return header, nil
	}
	return rawdb.ReadHeaderByNumber(tx, blockNum), nil
}

func pbtHeaderByHash(tx kv.Getter, reader *freezeblocks.BlockReader, view *blocksnapshots.View, hash common.Hash) (*types.Header, error) {
	header, err := rawdb.ReadHeaderByHash(tx, hash)
	if err != nil {
		return nil, err
	}
	if header != nil {
		return header, nil
	}
	for _, segment := range view.Headers() {
		for blockNum := segment.From(); blockNum < segment.To(); blockNum++ {
			header, err := reader.HeaderFromView(view, blockNum)
			if err != nil {
				return nil, err
			}
			if header != nil && header.Hash() == hash {
				return header, nil
			}
		}
	}
	return nil, nil
}

func pbtMaxTxNum(ctx context.Context, tx kv.Tx, view *blocksnapshots.View, blockNum uint64) (uint64, bool, error) {
	maxTxNum, found, err := rawdbv3.TxNums.MaxExact(ctx, tx, blockNum)
	if err != nil || found {
		return maxTxNum, found, err
	}
	segment, ok := view.Segment(snaptype2.Bodies, blockNum)
	if !ok {
		return 0, false, nil
	}
	body, _, err := freezeblocks.BodyForTxnFromSnapshot(blockNum, segment, nil)
	if err != nil {
		return 0, false, err
	}
	if body == nil || body.TxCount == 0 {
		return 0, false, nil
	}
	return body.BaseTxnID.U64() + uint64(body.TxCount) - 1, true, nil
}
