package main

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	persistence2 "github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cmd/caplin/caplin1"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/persistence/db_config"
	"github.com/ledgerwatch/erigon/cl/persistence/format/snapshot_format"
	"github.com/ledgerwatch/erigon/cl/phase1/core"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/phase1/stages"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2"
	"github.com/ledgerwatch/erigon/cl/transition/machine"
	"github.com/ledgerwatch/erigon/cl/utils"

	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/afero"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var CLI struct {
	Migrate Migrate `cmd:"" help:"migrate from one state to another"`

	Blocks Blocks `cmd:"" help:"download blocks from reqresp network"`
	Epochs Epochs `cmd:"" help:"download epochs from reqresp network"`

	Chain          Chain          `cmd:"" help:"download the entire chain from reqresp network"`
	DumpSnapshots  DumpSnapshots  `cmd:"" help:"generate caplin snapshots"`
	CheckSnapshots CheckSnapshots `cmd:"" help:"check snapshot folder against content of chain data"`
}

type chainCfg struct {
	Chain string `help:"chain" default:"mainnet"`
}

func (c *chainCfg) configs() (beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, err error) {
	genesisConfig, _, beaconConfig, _, err = clparams.GetConfigsByNetworkName(c.Chain)
	return
}

type outputFolder struct {
	Datadir string `help:"datadir" default:"~/.local/share/erigon" type:"existingdir"`
}

type withSentinel struct {
	Sentinel string `help:"sentinel url" default:"localhost:7777"`
}

func (w *withSentinel) connectSentinel() (sentinel.SentinelClient, error) {
	// YOLO message size
	gconn, err := grpc.Dial(w.Sentinel, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt)))
	if err != nil {
		return nil, err
	}
	return sentinel.NewSentinelClient(gconn), nil
}

func openFs(fsName string, path string) (afero.Fs, error) {
	return afero.NewBasePathFs(afero.NewBasePathFs(afero.NewOsFs(), fsName), path), nil
}

type Blocks struct {
	chainCfg
	outputFolder
	withSentinel

	FromBlock int `arg:"" name:"from" default:"0"`
	ToBlock   int `arg:"" name:"to" default:"-1"`
}

func (b *Blocks) Run(ctx *Context) error {
	s, err := b.withSentinel.connectSentinel()
	if err != nil {
		return err
	}
	beaconConfig, genesisConfig, err := b.configs()
	if err != nil {
		return err
	}

	beacon := rpc.NewBeaconRpcP2P(ctx, s, beaconConfig, genesisConfig)
	err = beacon.SetStatus(
		genesisConfig.GenesisValidatorRoot,
		beaconConfig.GenesisEpoch,
		genesisConfig.GenesisValidatorRoot,
		beaconConfig.GenesisSlot)
	if err != nil {
		return err
	}

	if b.ToBlock < 0 {
		b.ToBlock = int(utils.GetCurrentSlot(genesisConfig.GenesisTime, beaconConfig.SecondsPerSlot))
	}

	resp, _, err := beacon.SendBeaconBlocksByRangeReq(ctx, uint64(b.FromBlock), uint64(b.ToBlock))
	if err != nil {
		return fmt.Errorf("error get beacon blocks: %w", err)
	}
	aferoFS, err := openFs(b.Datadir, "caplin/beacon")
	if err != nil {
		return err
	}

	db := mdbx.MustOpen("caplin/db")
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	beaconDB := persistence2.NewBeaconChainDatabaseFilesystem(persistence2.NewAferoRawBlockSaver(aferoFS, beaconConfig), nil, beaconConfig)
	for _, vv := range resp {
		err := beaconDB.WriteBlock(ctx, tx, vv, true)
		if err != nil {
			return err
		}
	}
	return nil
}

type Epochs struct {
	chainCfg
	outputFolder
	withSentinel

	Concurrency int `help:"number of epochs to ask concurrently for" name:"concurrency" short:"c" default:"4"`

	FromEpoch int `arg:"" name:"from" default:"0"`
	ToEpoch   int `arg:"" name:"to" default:"-1"`
}

func (b *Epochs) Run(cctx *Context) error {
	ctx := cctx.Context
	s, err := b.withSentinel.connectSentinel()
	if err != nil {
		return err
	}
	beaconConfig, genesisConfig, err := b.configs()
	if err != nil {
		return err
	}

	aferoFS, err := openFs(b.Datadir, "caplin/beacon")
	if err != nil {
		return err
	}

	beaconDB := persistence.NewBeaconChainDatabaseFilesystem(persistence.NewAferoRawBlockSaver(aferoFS, beaconConfig), nil, beaconConfig)

	beacon := rpc.NewBeaconRpcP2P(ctx, s, beaconConfig, genesisConfig)
	rpcSource := persistence2.NewBeaconRpcSource(beacon)

	err = beacon.SetStatus(
		genesisConfig.GenesisValidatorRoot,
		beaconConfig.GenesisEpoch,
		genesisConfig.GenesisValidatorRoot,
		beaconConfig.GenesisSlot)
	if err != nil {
		return err
	}

	if b.ToEpoch < 0 {
		b.ToEpoch = int(utils.GetCurrentEpoch(genesisConfig.GenesisTime, beaconConfig.SecondsPerSlot, beaconConfig.SlotsPerEpoch))
	}

	ctx, cn := context.WithCancel(ctx)
	defer cn()
	egg, ctx := errgroup.WithContext(ctx)

	totalEpochs := (b.ToEpoch - b.FromEpoch + 1)
	pw := progress.NewWriter()
	pw.SetTrackerLength(50)
	pw.SetMessageWidth(24)
	pw.SetStyle(progress.StyleDefault)
	pw.SetUpdateFrequency(time.Millisecond * 100)
	pw.SetTrackerPosition(progress.PositionRight)
	pw.Style().Visibility.Percentage = true
	pw.Style().Visibility.Speed = true
	pw.Style().Visibility.Value = true
	pw.Style().Visibility.ETA = true
	pw.Style().Visibility.ETAOverall = false
	pw.Style().Visibility.Tracker = true
	pw.Style().Visibility.TrackerOverall = false
	pw.Style().Visibility.SpeedOverall = true
	pw.Style().Options.Separator = ""

	go pw.Render()

	total := int64(uint64(totalEpochs) * beaconConfig.SlotsPerEpoch)
	tk := &progress.Tracker{
		Message: fmt.Sprintf("downloading %d blocks", total),
		Total:   total,
		Units:   progress.UnitsDefault,
	}
	pw.AppendTracker(tk)
	tk.UpdateTotal(total)

	egg.SetLimit(b.Concurrency)

	db := mdbx.MustOpen("caplin/db")
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	defer cn()
	for i := b.FromEpoch; i <= b.ToEpoch; i = i + 1 {
		ii := i
		egg.Go(func() error {
			var blocks *peers.PeeredObject[[]*cltypes.SignedBeaconBlock]
			for {
				blocks, err = rpcSource.GetRange(ctx, tx, uint64(ii)*beaconConfig.SlotsPerEpoch, beaconConfig.SlotsPerEpoch)
				if err != nil {
					log.Error("dl error", "err", err, "epoch", ii)
				} else {
					break
				}
			}
			for _, v := range blocks.Data {
				tk.Increment(1)
				_, _ = beaconDB, v
				err := beaconDB.WriteBlock(ctx, tx, v, true)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	err = egg.Wait()
	if err != nil {
		return err
	}
	tk.MarkAsDone()
	return tx.Commit()
}

type Migrate struct {
	outputFolder
	chainCfg

	State  string   `arg:"" help:"state to start from (file or later url to checkpoint)"`
	Blocks []string `arg:"" name:"blocks" help:"blocks to migrate, in order" type:"path"`
}

func resolveState(source string, chain chainCfg) (abstract.BeaconState, error) {
	beaconConfig, _, err := chain.configs()
	if err != nil {
		return nil, err
	}
	s := state.New(beaconConfig)
	switch {
	default:
		var stateByte []byte
		if _, stateByte, err = clparams.ParseGenesisSSZToGenesisConfig(
			source,
			beaconConfig.GetCurrentStateVersion(0)); err != nil {
			return nil, err
		}
		if s.DecodeSSZ(stateByte, int(beaconConfig.GetCurrentStateVersion(0))); err != nil {
			return nil, err
		}
		return s, nil
	case strings.HasPrefix(strings.ToLower(source), "http://"), strings.HasPrefix(strings.ToLower(source), "https://"):
	}
	return nil, fmt.Errorf("unknown state format: '%s'", source)
}

func (m *Migrate) getBlock(ctx *Context, block string) (*cltypes.SignedBeaconBlock, error) {
	afs := afero.NewOsFs()

	bts, err := afero.ReadFile(afs, block)
	if err != nil {
		return nil, err
	}
	b, _, err := m.chainCfg.configs()
	if err != nil {
		return nil, err
	}
	blk := cltypes.NewSignedBeaconBlock(b)
	err = blk.DecodeSSZ(bts, 0)
	if err != nil {
		return nil, err
	}

	return blk, nil
}

func (m *Migrate) Run(ctx *Context) error {
	state, err := resolveState(m.State, m.chainCfg)
	if err != nil {
		return err
	}
	// get the machine
	cl := &eth2.Impl{}

	// TODO: two queues for download and transition
	for _, v := range m.Blocks {
		blk, err := m.getBlock(ctx, v)
		if err != nil {
			return err
		}
		err = machine.TransitionState(cl, state, blk)
		if err != nil {
			return err
		}
	}
	return nil
}

type Chain struct {
	chainCfg
	withSentinel
	outputFolder
}

func (c *Chain) Run(ctx *Context) error {
	s, err := c.withSentinel.connectSentinel()
	if err != nil {
		return err
	}

	genesisConfig, _, beaconConfig, networkType, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	log.Info("Started chain download", "chain", c.Chain)

	dirs := datadir.New(c.Datadir)

	rawDB := persistence.AferoRawBeaconBlockChainFromOsPath(beaconConfig, dirs.CaplinHistory)
	beaconDB, db, err := caplin1.OpenCaplinDatabase(ctx, db_config.DatabaseConfiguration{PruneDepth: math.MaxUint64}, beaconConfig, rawDB, dirs.CaplinIndexing, nil, false)
	if err != nil {
		return err
	}
	defer db.Close()

	beacon := rpc.NewBeaconRpcP2P(ctx, s, beaconConfig, genesisConfig)

	bs, err := core.RetrieveBeaconState(ctx, beaconConfig, genesisConfig, clparams.GetCheckpointSyncEndpoint(networkType))
	if err != nil {
		return err
	}

	bRoot, err := bs.BlockRoot()
	if err != nil {
		return err
	}

	if err := db.Update(ctx, func(tx kv.RwTx) error {
		return beacon_indicies.WriteHighestFinalized(tx, bs.Slot())
	}); err != nil {
		return err
	}

	err = beacon.SetStatus(
		genesisConfig.GenesisValidatorRoot,
		beaconConfig.GenesisEpoch,
		genesisConfig.GenesisValidatorRoot,
		beaconConfig.GenesisSlot)
	if err != nil {
		return err
	}
	downloader := network.NewBackwardBeaconDownloader(ctx, beacon)
	cfg := stages.StageHistoryReconstruction(downloader, beaconDB, db, nil, genesisConfig, beaconConfig, db_config.DatabaseConfiguration{
		PruneDepth: math.MaxUint64,
	}, bRoot, bs.Slot(), "/tmp", log.Root())
	return stages.SpawnStageHistoryDownload(cfg, ctx, log.Root())
}

type DumpSnapshots struct {
	chainCfg
	outputFolder
}

func (c *DumpSnapshots) Run(ctx *Context) error {
	_, _, beaconConfig, _, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))
	log.Info("Started chain download", "chain", c.Chain)

	dirs := datadir.New(c.Datadir)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	rawDB := persistence.AferoRawBeaconBlockChainFromOsPath(beaconConfig, dirs.CaplinHistory)
	beaconDB, db, err := caplin1.OpenCaplinDatabase(ctx, db_config.DatabaseConfiguration{PruneDepth: math.MaxUint64}, beaconConfig, rawDB, dirs.CaplinIndexing, nil, false)
	if err != nil {
		return err
	}
	var to uint64
	db.View(ctx, func(tx kv.Tx) (err error) {
		to, err = beacon_indicies.ReadHighestFinalized(tx)
		return
	})

	return freezeblocks.DumpBeaconBlocks(ctx, db, beaconDB, 0, to, snaptype.Erigon2SegmentSize, dirs.Tmp, dirs.Snap, 8, log.LvlInfo, log.Root())
}

type CheckSnapshots struct {
	chainCfg
	outputFolder

	Slot uint64 `name:"slot" help:"slot to check"`
}

func (c *CheckSnapshots) Run(ctx *Context) error {
	_, _, beaconConfig, _, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))
	log.Info("Started the checking process", "chain", c.Chain)

	dirs := datadir.New(c.Datadir)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	rawDB := persistence.AferoRawBeaconBlockChainFromOsPath(beaconConfig, dirs.CaplinHistory)
	beaconDB, db, err := caplin1.OpenCaplinDatabase(ctx, db_config.DatabaseConfiguration{PruneDepth: math.MaxUint64}, beaconConfig, rawDB, dirs.CaplinIndexing, nil, false)
	if err != nil {
		return err
	}
	var to uint64
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	to, err = beacon_indicies.ReadHighestFinalized(tx)
	if err != nil {
		return err
	}

	to = (to / snaptype.Erigon2SegmentSize) * snaptype.Erigon2SegmentSize

	csn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{}, dirs.Snap, log.Root())
	if err := csn.ReopenFolder(); err != nil {
		return err
	}

	br := &snapshot_format.MockBlockReader{}
	snReader := freezeblocks.NewBeaconSnapshotReader(csn, br, beaconConfig)
	for i := c.Slot; i < to; i++ {
		// Read the original canonical slot
		data, err := beaconDB.GetBlock(ctx, tx, i)
		if err != nil {
			return err
		}
		if data == nil {
			continue
		}
		blk := data.Data
		if blk == nil {
			continue
		}
		// first thing if the block is bellatrix update the mock block reader
		if blk.Version() >= clparams.BellatrixVersion {
			br.Block = blk.Block.Body.ExecutionPayload
		}
		blk2, err := snReader.ReadBlock(i)
		if err != nil {
			log.Error("Error detected in decoding snapshots", "err", err, "slot", i)
			return nil
		}
		if blk2 == nil {
			log.Error("Block not found in snapshot", "slot", i)
			return nil
		}

		hash1, _ := blk.Block.HashSSZ()
		hash2, _ := blk2.Block.HashSSZ()
		if hash1 != hash2 {
			log.Error("Mismatching blocks", "slot", i, "gotSlot", blk2.Block.Slot, "datadir", libcommon.Hash(hash1), "snapshot", libcommon.Hash(hash2))
			return nil
		}
		log.Info("Successfully checked", "slot", i)
	}
	return nil
}
