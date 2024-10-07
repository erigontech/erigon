package antiquary

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/persistence/blob_storage"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
)

const safetyMargin = 10_000 // We retire snapshots 10k blocks after the finalized head

var IsTest = true

// Antiquary is where the snapshots go, aka old history, it is what keep track of the oldest records.
type Antiquary struct {
	mainDB                kv.RwDB                  // this is the main DB
	blobStorage           blob_storage.BlobStorage // this is the blob storage
	dirs                  datadir.Dirs
	downloader            proto_downloader.DownloaderClient
	logger                log.Logger
	sn                    *freezeblocks.CaplinSnapshots
	snReader              freezeblocks.BeaconSnapshotReader
	ctx                   context.Context
	backfilled            *atomic.Bool
	blobBackfilled        *atomic.Bool
	cfg                   *clparams.BeaconChainConfig
	states, blocks, blobs bool
	validatorsTable       *state_accessors.StaticValidatorTable
	genesisState          *state.CachingBeaconState
	// set to nil
	currentState *state.CachingBeaconState
	balances32   []byte
}

func NewAntiquary(ctx context.Context, blobStorage blob_storage.BlobStorage, genesisState *state.CachingBeaconState, validatorsTable *state_accessors.StaticValidatorTable, cfg *clparams.BeaconChainConfig, dirs datadir.Dirs, downloader proto_downloader.DownloaderClient, mainDB kv.RwDB, sn *freezeblocks.CaplinSnapshots, reader freezeblocks.BeaconSnapshotReader, logger log.Logger, states, blocks, blobs bool) *Antiquary {
	backfilled := &atomic.Bool{}
	blobBackfilled := &atomic.Bool{}
	backfilled.Store(false)
	blobBackfilled.Store(false)
	return &Antiquary{
		mainDB:          mainDB,
		blobStorage:     blobStorage,
		dirs:            dirs,
		downloader:      downloader,
		logger:          logger,
		sn:              sn,
		ctx:             ctx,
		backfilled:      backfilled,
		blobBackfilled:  blobBackfilled,
		cfg:             cfg,
		states:          states,
		snReader:        reader,
		validatorsTable: validatorsTable,
		genesisState:    genesisState,
		blocks:          blocks,
		blobs:           blobs,
	}
}

// Antiquate is the function that starts transactions seeding and shit, very cool but very shit too as a name.
func (a *Antiquary) Loop() error {
	if a.downloader == nil || !a.blocks {
		return nil // Just skip if we don't have a downloader
	}
	// Skip if we dont support backfilling for the current network
	if !clparams.SupportBackfilling(a.cfg.DepositNetworkID) {
		return nil
	}
	statsReply, err := a.downloader.Stats(a.ctx, &proto_downloader.StatsRequest{})
	if err != nil {
		return err
	}
	reCheckTicker := time.NewTicker(3 * time.Second)
	defer reCheckTicker.Stop()
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
	if err := a.sn.BuildMissingIndices(a.ctx, a.logger); err != nil {
		return err
	}
	// Here we need to start mdbx transaction and lock the thread
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
	if err := a.sn.ReopenFolder(); err != nil {
		return err
	}
	defer logInterval.Stop()
	log.Info("[Antiquary]: Stopping Caplin to process historical indicies", "from", from, "to", a.sn.BlocksAvailable())

	// Now write the snapshots as indicies
	for i := from; i < a.sn.BlocksAvailable(); i++ {
		// read the snapshot
		header, elBlockNumber, elBlockHash, err := a.sn.ReadHeader(i)
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
		if err := beacon_indicies.MarkRootCanonical(a.ctx, tx, header.Header.Slot, blockRoot); err != nil {
			return err
		}
		if err := beacon_indicies.WriteHeaderSlot(tx, blockRoot, header.Header.Slot); err != nil {
			return err
		}
		if err := beacon_indicies.WriteStateRoot(tx, blockRoot, header.Header.Root); err != nil {
			return err
		}
		if err := beacon_indicies.WriteParentBlockRoot(a.ctx, tx, blockRoot, header.Header.ParentRoot); err != nil {
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
			log.Info("[Antiquary]: Processed snapshots", "progress", i, "target", a.sn.BlocksAvailable())
		case <-a.ctx.Done():
		default:
		}
	}

	frozenSlots := a.sn.BlocksAvailable()
	if frozenSlots != 0 {
		if err := beacon_indicies.PruneBlocks(a.ctx, tx, frozenSlots); err != nil {
			return err
		}
	}

	if a.states {
		go a.loopStates(a.ctx)
	}
	if !IsTest {
		return nil
	}

	if a.blobs {
		go a.loopBlobs(a.ctx)
	}

	// write the indicies
	if err := beacon_indicies.WriteLastBeaconSnapshot(tx, frozenSlots); err != nil {
		return err
	}
	log.Info("[Antiquary]: Restarting Caplin")
	if err := tx.Commit(); err != nil {
		return err
	}
	// Check for snapshots retirement every 3 minutes
	retirementTicker := time.NewTicker(3 * time.Minute)
	defer retirementTicker.Stop()
	for {
		select {
		case <-retirementTicker.C:
			if !a.backfilled.Load() {
				continue
			}
			var (
				from uint64
				to   uint64
			)
			if err := a.mainDB.View(a.ctx, func(roTx kv.Tx) error {
				// read the last beacon snapshots
				from, err = beacon_indicies.ReadLastBeaconSnapshot(roTx)
				if err != nil {
					return err
				}
				from += 1
				// read the finalized head
				to, err = beacon_indicies.ReadHighestFinalized(roTx)
				if err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
			// Sanity checks just to be safe.
			if from >= to {
				continue
			}
			to = utils.Min64(to, to-safetyMargin) // We don't want to retire snapshots that are too close to the finalized head
			to = (to / snaptype.Erigon2MergeLimit) * snaptype.Erigon2MergeLimit
			if to-from < snaptype.Erigon2MergeLimit {
				continue
			}
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
	if err := freezeblocks.DumpBeaconBlocks(a.ctx, a.mainDB, from, to, a.sn.Salt, a.dirs, 1, log.LvlDebug, a.logger); err != nil {
		return err
	}
	tx, err := a.mainDB.BeginRw(a.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := beacon_indicies.PruneBlocks(a.ctx, tx, to); err != nil {
		return err
	}
	if err := beacon_indicies.WriteLastBeaconSnapshot(tx, to-1); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	if err := a.sn.ReopenFolder(); err != nil {
		return err
	}

	paths := a.sn.SegFilePaths(from, to)
	downloadItems := make([]*proto_downloader.AddItem, len(paths))
	for i, path := range paths {
		downloadItems[i] = &proto_downloader.AddItem{
			Path: path,
		}
	}
	// Notify bittorent to seed the new snapshots
	if _, err := a.downloader.Add(a.ctx, &proto_downloader.AddRequest{Items: downloadItems}); err != nil {
		log.Warn("[Antiquary]: Failed to add items to bittorent", "err", err)
	}

	return tx.Commit()
}

func (a *Antiquary) NotifyBackfilled() {
	// we set up the range for [lowestRawSlot, finalized]
	a.backfilled.Store(true) // this is the lowest slot not in snapshots
}

func (a *Antiquary) NotifyBlobBackfilled() {
	a.blobBackfilled.Store(true)
}

func (a *Antiquary) loopBlobs(ctx context.Context) {
	if a.cfg.DenebForkEpoch == math.MaxUint64 {
		return
	}
	blobAntiquationTicker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-blobAntiquationTicker.C:
			if !a.blobBackfilled.Load() {
				continue
			}
			if err := a.antiquateBlobs(); err != nil {
				log.Error("[Antiquary]: Failed to antiquate blobs", "err", err)
			}
		}
	}
}

func (a *Antiquary) antiquateBlobs() error {
	roTx, err := a.mainDB.BeginRo(a.ctx)
	if err != nil {
		return err
	}
	defer roTx.Rollback()
	// perform blob antiquation if it is time to.
	currentBlobsProgress := a.sn.FrozenBlobs()
	// We should NEVER get ahead of the block snapshots.
	if currentBlobsProgress >= a.sn.BlocksAvailable() {
		return nil
	}
	minimunBlobsProgress := ((a.cfg.DenebForkEpoch * a.cfg.SlotsPerEpoch) / snaptype.Erigon2MergeLimit) * snaptype.Erigon2MergeLimit
	currentBlobsProgress = utils.Max64(currentBlobsProgress, minimunBlobsProgress)
	// read the finalized head
	to, err := beacon_indicies.ReadHighestFinalized(roTx)
	if err != nil {
		return err
	}
	if to <= currentBlobsProgress || to-currentBlobsProgress < snaptype.Erigon2MergeLimit {
		return nil
	}
	roTx.Rollback()
	a.logger.Info("[Antiquary]: Antiquating blobs", "from", currentBlobsProgress, "to", to)
	// now, we need to retire the blobs
	if err := freezeblocks.DumpBlobsSidecar(a.ctx, a.blobStorage, a.mainDB, currentBlobsProgress, to, a.sn.Salt, a.dirs, 1, log.LvlDebug, a.logger); err != nil {
		return err
	}
	to = (to / snaptype.Erigon2MergeLimit) * snaptype.Erigon2MergeLimit
	a.logger.Info("[Antiquary]: Finished Antiquating blobs", "from", currentBlobsProgress, "to", to)
	if err := a.sn.ReopenFolder(); err != nil {
		return err
	}

	paths := a.sn.SegFilePaths(currentBlobsProgress, to)
	downloadItems := make([]*proto_downloader.AddItem, len(paths))
	for i, path := range paths {
		downloadItems[i] = &proto_downloader.AddItem{
			Path: path,
		}
	}
	// Notify bittorent to seed the new snapshots
	if _, err := a.downloader.Add(a.ctx, &proto_downloader.AddRequest{Items: downloadItems}); err != nil {
		log.Warn("[Antiquary]: Failed to add items to bittorent", "err", err)
	}

	roTx, err = a.mainDB.BeginRo(a.ctx)
	if err != nil {
		return err
	}
	defer roTx.Rollback()
	// now prune blobs from the database
	for i := currentBlobsProgress; i < to; i++ {
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(roTx, i)
		if err != nil {
			return err
		}
		a.blobStorage.RemoveBlobSidecars(a.ctx, i, blockRoot)
	}
	return nil
}
