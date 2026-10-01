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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
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

type pbtBlockFilesTx struct {
	kv.Tx
	view *blocksnapshots.View
}

func (tx pbtBlockFilesTx) BlockFilesRoTx() *blocksnapshots.View { return tx.view }

type pbtTemporalBlockFilesTx struct {
	kv.TemporalTx
	view *blocksnapshots.View
}

func (tx pbtTemporalBlockFilesTx) BlockFilesRoTx() *blocksnapshots.View { return tx.view }
