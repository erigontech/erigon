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
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/afero"
)

const safetyMargin = 10_000 // We retire snapshots 10k blocks after the finalized head

// Antiquary is where the snapshots go, aka old history, it is what keep track of the oldest records.
type Antiquary struct {
	mainDB          kv.RwDB // this is the main DB
	dirs            datadir.Dirs
	downloader      proto_downloader.DownloaderClient
	logger          log.Logger
	sn              *freezeblocks.CaplinSnapshots
	snReader        freezeblocks.BeaconSnapshotReader
	ctx             context.Context
	beaconDB        persistence.BlockSource
	backfilled      *atomic.Bool
	cfg             *clparams.BeaconChainConfig
	states, blocks  bool
	fs              afero.Fs
	validatorsTable *state_accessors.StaticValidatorTable
	genesisState    *state.CachingBeaconState
	// set to nil
	currentState *state.CachingBeaconState
	balances32   []byte
}

func NewAntiquary(ctx context.Context, genesisState *state.CachingBeaconState, validatorsTable *state_accessors.StaticValidatorTable, cfg *clparams.BeaconChainConfig, dirs datadir.Dirs, downloader proto_downloader.DownloaderClient, mainDB kv.RwDB, sn *freezeblocks.CaplinSnapshots, reader freezeblocks.BeaconSnapshotReader, beaconDB persistence.BlockSource, logger log.Logger, states, blocks bool, fs afero.Fs) *Antiquary {
	backfilled := &atomic.Bool{}
	backfilled.Store(false)
	return &Antiquary{
		mainDB:          mainDB,
		dirs:            dirs,
		downloader:      downloader,
		logger:          logger,
		sn:              sn,
		beaconDB:        beaconDB,
		ctx:             ctx,
		backfilled:      backfilled,
		cfg:             cfg,
		states:          states,
		snReader:        reader,
		fs:              fs,
		validatorsTable: validatorsTable,
		genesisState:    genesisState,
		blocks:          blocks,
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
	if err := a.sn.BuildMissingIndices(a.ctx, a.logger, log.LvlDebug); err != nil {
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
		if err := a.beaconDB.PurgeRange(a.ctx, tx, 0, frozenSlots); err != nil {
			return err
		}
	}

	if a.states {
		go a.loopStates(a.ctx)
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
			if err := a.antiquate(a.sn.Version(), from, to); err != nil {
				return err
			}
		case <-a.ctx.Done():
		}
	}
}

// Antiquate will antiquate a specific block range (aka. retire snapshots), this should be ran in the background.
func (a *Antiquary) antiquate(version uint8, from, to uint64) error {
	if a.downloader == nil {
		return nil // Just skip if we don't have a downloader
	}
	log.Info("[Antiquary]: Antiquating", "from", from, "to", to)
	if err := freezeblocks.DumpBeaconBlocks(a.ctx, a.mainDB, a.beaconDB, version, from, to, snaptype.Erigon2MergeLimit, a.dirs.Tmp, a.dirs.Snap, 1, log.LvlDebug, a.logger); err != nil {
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

	tx, err := a.mainDB.BeginRw(a.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	a.validatorsTable.SetSlot(to)
	if err := beacon_indicies.WriteLastBeaconSnapshot(tx, to-1); err != nil {
		return err
	}
	return tx.Commit()
}

func (a *Antiquary) NotifyBackfilled() {
	// we set up the range for [lowestRawSlot, finalized]
	a.backfilled.Store(true) // this is the lowest slot not in snapshots
}
