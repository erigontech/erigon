package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2"
	"github.com/ledgerwatch/erigon/cl/transition/machine"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/afero"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var CLI struct {
	Migrate Migrate `cmd:"" help:"migrate from one state to another"`

	Blocks Blocks `cmd:"" help:"download blocks from reqresp network"`
	Epochs Epochs `cmd:"" help:"download epochs from reqresp network"`
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
	gconn, err := grpc.Dial(w.Sentinel, grpc.WithInsecure())
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

	sqlDB, err := sql.Open("sqlite", "caplin/db")
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	tx, err := sqlDB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	beaconDB := persistence.NewBeaconChainDatabaseFilesystem(persistence.NewAferoRawBlockSaver(aferoFS, beaconConfig), nil, beaconConfig)
	for _, vv := range resp {
		err := beaconDB.WriteBlock(tx, ctx, vv, true)
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
	sqlDB, err := sql.Open("sqlite", "caplin/db")
	if err != nil {
		return err
	}
	defer sqlDB.Close()
	beaconDB := persistence.NewBeaconChainDatabaseFilesystem(persistence.NewAferoRawBlockSaver(aferoFS, beaconConfig), nil, beaconConfig)

	beacon := rpc.NewBeaconRpcP2P(ctx, s, beaconConfig, genesisConfig)
	rpcSource := persistence.NewBeaconRpcSource(beacon)

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

	tx, err := sqlDB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	defer cn()
	for i := b.FromEpoch; i <= b.ToEpoch; i = i + 1 {
		ii := i
		egg.Go(func() error {
			var blocks []*peers.PeeredObject[*cltypes.SignedBeaconBlock]
			for {
				blocks, err = rpcSource.GetRange(tx, ctx, uint64(ii)*beaconConfig.SlotsPerEpoch, beaconConfig.SlotsPerEpoch)
				if err != nil {
					log.Error("dl error", "err", err, "epoch", ii)
				} else {
					break
				}
			}
			for _, v := range blocks {
				tk.Increment(1)
				_, _ = beaconDB, v
				err := beaconDB.WriteBlock(tx, ctx, v.Data, true)
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
