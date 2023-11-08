package antiquary

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
)

// Antiquary is where the snapshots go, aka old history, it is what keep track of the oldest records.
type Antiquary struct {
	mainDB     kv.RwDB // this is the main DB
	dirs       datadir.Dirs
	downloader proto_downloader.DownloaderClient
	logger     log.Logger
	sn         *freezeblocks.CaplinSnapshots
	reader     freezeblocks.BeaconSnapshotReader
	ctx        context.Context
	beaconDB   persistence.BlockSource
}

func NewAntiquary(ctx context.Context, cfg *clparams.BeaconChainConfig, dirs datadir.Dirs, downloader proto_downloader.DownloaderClient, mainDB kv.RwDB, snapshotsDB kv.RwDB, sn *freezeblocks.CaplinSnapshots, reader freezeblocks.BeaconSnapshotReader, beaconDB persistence.BlockSource, logger log.Logger) (*Antiquary, error) {
	return &Antiquary{
		mainDB:     mainDB,
		dirs:       dirs,
		downloader: downloader,
		logger:     logger,
		sn:         sn,
		reader:     reader,
		ctx:        ctx,
	}, nil
}

// Antiquate is the function that starts transactions seeding and shit, very cool but very shit too as a name.
func (a *Antiquary) Loop() error {
	statsReply, err := a.downloader.Stats(a.ctx, &proto_downloader.StatsRequest{})
	if err != nil {
		return err
	}
	reCheckTicker := time.NewTicker(3 * time.Second)
	// Fist part of the antiquate is to download caplin snapshots
	for !statsReply.Completed {
		select {
		case <-reCheckTicker.C:
			statsReply, err = a.downloader.Stats(a.ctx, &proto_downloader.StatsRequest{})
			if err != nil {
				return err
			}
		case <-a.ctx.Done():
		}
	}
	// If it is ready, reopen the folder
	if err := a.sn.ReopenFolder(); err != nil {
		return err
	}
	if err := a.sn.BuildMissingIndices(a.ctx, a.logger, log.LvlDebug); err != nil {
		return err
	}
	// Here we need to start mdbx transaction and lock the thread
	log.Info("[Antiquary]: Stopping Caplin to process historical indicies")
	tx, err := a.mainDB.BeginRw(a.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// read the last beacon snapshots
	from, err := beacon_indicies.ReadLastBeaconSnapshot(tx)
	if err != nil {
		return err
	}
	lastProcessed := from
	logInterval := time.NewTicker(30 * time.Second)
	defer logInterval.Stop()
	// Now write the snapshots as indicies
	for i := from; i < a.reader.FrozenSlots(); i++ {
		// read the snapshot
		header, elBlockNumber, elBlockHash, err := a.reader.ReadHeader(i)
		if err != nil {
			return err
		}
		blockRoot, err := header.Header.HashSSZ()
		if err != nil {
			return err
		}
		lastProcessed = header.Header.Slot
		if err := beacon_indicies.WriteBeaconBlockHeaderAndIndicies(a.ctx, tx, header, false); err != nil {
			return err
		}
		if err := beacon_indicies.WriteExecutionBlockNumber(tx, blockRoot, elBlockNumber); err != nil {
			return err
		}
		if err := beacon_indicies.WriteExecutionBlockHash(tx, blockRoot, elBlockHash); err != nil {
			return err
		}
		select {
		case <-logInterval.C:
			log.Info("[Antiquary]: Processed snapshots", "progress", i, "target", a.reader.FrozenSlots())
		case <-a.ctx.Done():
		}
	}
	// write the indicies
	if err := beacon_indicies.WriteLastBeaconSnapshot(tx, lastProcessed); err != nil {
		return err
	}
	log.Info("[Antiquary]: Restarting Caplin")
	return tx.Commit()
}

// Antiquate will antiquate a specific block range (aka. retire snapshots), this should be ran in the background.
func (a *Antiquary) Antiquate(from, to uint64) error {
	log.Info("[Antiquary]: Antiquating", "from", from, "to", to)
	if err := freezeblocks.DumpBeaconBlocks(a.ctx, a.mainDB, a.beaconDB, 0, to, snaptype.Erigon2RecentMergeLimit, a.dirs.Tmp, a.dirs.Snap, 8, log.LvlDebug, a.logger); err != nil {
		return err
	}

	roTx, err := a.mainDB.BeginRo(a.ctx)
	if err != nil {
		return err
	}
	defer roTx.Rollback()
	if err := a.beaconDB.PurgeRange(a.ctx, roTx, from, to-from); err != nil {
		return err
	}
	roTx.Rollback()

	tx, err := a.mainDB.BeginRw(a.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := beacon_indicies.WriteLastBeaconSnapshot(tx, to); err != nil {
		return err
	}
	return tx.Commit()
}
