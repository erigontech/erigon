package antiquary

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
)

const safetyMargin = 10_000 // We retire snapshots 10k blocks after the finalized head

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
	backfilled *atomic.Bool
}

func NewAntiquary(ctx context.Context, cfg *clparams.BeaconChainConfig, dirs datadir.Dirs, downloader proto_downloader.DownloaderClient, mainDB kv.RwDB, sn *freezeblocks.CaplinSnapshots, reader freezeblocks.BeaconSnapshotReader, beaconDB persistence.BlockSource, logger log.Logger) *Antiquary {
	var backfilled *atomic.Bool
	backfilled.Store(false)
	return &Antiquary{
		mainDB:     mainDB,
		dirs:       dirs,
		downloader: downloader,
		logger:     logger,
		sn:         sn,
		reader:     reader,
		ctx:        ctx,
		backfilled: backfilled,
	}
}

// Antiquate is the function that starts transactions seeding and shit, very cool but very shit too as a name.
func (a *Antiquary) Loop() error {
	if a.downloader == nil {
		return nil // Just skip if we don't have a downloader
	}
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
	logInterval := time.NewTicker(30 * time.Second)
	defer logInterval.Stop()
	// Now write the snapshots as indicies
	for i := from; i < a.reader.FrozenSlots(); i++ {
		// read the snapshot
		header, elBlockNumber, elBlockHash, err := a.reader.ReadHeader(i)
		if err != nil {
			return err
		}
		if header == nil {
			continue
		}
		blockRoot, err := header.Header.HashSSZ()
		if err != nil {
			return err
		}
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
		default:
		}
	}
	// write the indicies
	if err := beacon_indicies.WriteLastBeaconSnapshot(tx, a.reader.FrozenSlots()-1); err != nil {
		return err
	}
	log.Info("[Antiquary]: Restarting Caplin")
	if err := tx.Commit(); err != nil {
		return err
	}
	// Check for snapshots retirement every 10 seconds
	retirementTicker := time.NewTicker(10 * time.Second)
	defer retirementTicker.Stop()
	for {
		select {
		case <-retirementTicker.C:
			if !a.backfilled.Load() {
				continue
			}
			// initialize roTx
			roTx, err := a.mainDB.BeginRo(a.ctx)
			if err != nil {
				return err
			}
			// read the last beacon snapshots
			from, err := beacon_indicies.ReadLastBeaconSnapshot(roTx)
			if err != nil {
				roTx.Rollback()
				return err
			}
			from += 1
			// read the finalized head
			to, err := beacon_indicies.ReadHighestFinalized(roTx)
			if err != nil {
				tx.Rollback()
				return err
			}
			roTx.Rollback()
			to = utils.Min64(to, to-safetyMargin) // We don't want to retire snapshots that are too close to the finalized head
			to = (to / snaptype.Erigon2RecentMergeLimit) * snaptype.Erigon2RecentMergeLimit
			if err := a.antiquate(from, to); err != nil {
				return err
			}
		case <-a.ctx.Done():
		}
	}
}

// Antiquate will antiquate a specific block range (aka. retire snapshots), this should be ran in the background.
func (a *Antiquary) antiquate(from, to uint64) error {
	if a.downloader == nil {
		return nil // Just skip if we don't have a downloader
	}
	log.Info("[Antiquary]: Antiquating", "from", from, "to", to)
	if err := freezeblocks.DumpBeaconBlocks(a.ctx, a.mainDB, a.beaconDB, 0, to, snaptype.Erigon2RecentMergeLimit, a.dirs.Tmp, a.dirs.Snap, 8, log.LvlDebug, a.logger); err != nil {
		return err
	}

	roTx, err := a.mainDB.BeginRo(a.ctx)
	if err != nil {
		return err
	}
	defer roTx.Rollback()
	if err := a.beaconDB.PurgeRange(a.ctx, roTx, from, to-from-1); err != nil {
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

func (a *Antiquary) NotifyBackfilled() {
	// we set up the range for [lowestRawSlot, finalized]
	a.backfilled.Store(true) // this is the lowest slot not in snapshots
}
