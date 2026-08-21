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

package antiquary

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

type recordingDownloaderClient struct {
	dbservices.NoopSeederClient
	deleted [][]string
}

func (r *recordingDownloaderClient) Delete(_ context.Context, paths []string) error {
	r.deleted = append(r.deleted, paths)
	return nil
}

func (recordingDownloaderClient) Download(context.Context, *downloaderproto.DownloadRequest) error {
	return nil
}

// antiquate must reach RemoveOverlaps and hand it a real onDelete once a downloader is
// present: RemoveOverlaps calls onDelete even with nothing to remove, so a single Delete
// call proves the wiring — nothing else on this path calls Delete.
func TestAntiquateWiresDownloaderIntoRemoveOverlaps(t *testing.T) {
	const to = uint64(snaptype.CaplinMergeLimit)
	const finalized = to + safetyMargin

	dirs := datadir.New(t.TempDir())
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)

	require.NoError(t, db.Update(context.Background(), func(tx kv.RwTx) error {
		var prevRoot common.Hash
		// One block every 500 slots keeps skippedInARow below its 1000 limit.
		for slot := uint64(0); slot < to; slot += 500 {
			var root common.Hash
			binary.BigEndian.PutUint64(root[:], slot+1)
			if err := beacon_indicies.MarkRootCanonical(context.Background(), tx, slot, root); err != nil {
				return err
			}
			if err := beacon_indicies.WriteParentBlockRoot(context.Background(), tx, root, prevRoot); err != nil {
				return err
			}
			if err := tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(slot, root), root[:]); err != nil {
				return err
			}
			prevRoot = root
		}
		return beacon_indicies.WriteHighestFinalized(tx, finalized)
	}))

	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	downloader := &recordingDownloaderClient{}
	a := &Antiquary{
		ctx:        context.Background(),
		mainDB:     db,
		dirs:       dirs,
		downloader: downloader,
		logger:     log.New(),
		sn:         sn,
		snapgen:    true,
	}

	require.NoError(t, a.antiquate())

	require.Len(t, downloader.deleted, 1, "RemoveOverlaps must call through to Delete exactly once")
}
