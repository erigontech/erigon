package main

import (
	"fmt"
	"math"
	"os/exec"
	"time"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/caplin/caplin1"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"golang.org/x/net/context"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/persistence/db_config"
	"github.com/ledgerwatch/erigon/cl/utils"

	"github.com/ledgerwatch/log/v3"
)

var CLI struct {
	BucketCaplinAutomation BucketCaplinAutomation `cmd:"" help:"migrate from one state to another"`
}

type chainCfg struct {
	Chain string `help:"chain" default:"mainnet"`
}

// func (c *chainCfg) configs() (beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, err error) {
// 	genesisConfig, _, beaconConfig, _, err = clparams.GetConfigsByNetworkName(c.Chain)
// 	return
// }

type withDatadir struct {
	Datadir string `help:"datadir" default:"~/.local/share/erigon" type:"existingdir"`
}

// func (w *withPPROF) withProfile() {
// 	if w.Pprof {
// 		debug.StartPProf("localhost:6060", metrics.Setup("localhost:6060", log.Root()))
// 	}
// }

// func (w *withSentinel) connectSentinel() (sentinel.SentinelClient, error) {
// 	// YOLO message size
// 	gconn, err := grpc.Dial(w.Sentinel, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt)))
// 	if err != nil {
// 		return nil, err
// 	}
// 	return sentinel.NewSentinelClient(gconn), nil
// }

// func openFs(fsName string, path string) (afero.Fs, error) {
// 	return afero.NewBasePathFs(afero.NewBasePathFs(afero.NewOsFs(), fsName), path), nil
// }

type BucketCaplinAutomation struct {
	withDatadir
	chainCfg

	UploadPeriod time.Duration `help:"upload period" default:"1440h"`
	Bucket       string        `help:"r2 address" default:"http://localhost:8080"`
}

func (c *BucketCaplinAutomation) Run(ctx *Context) error {
	_, _, beaconConfig, _, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))
	log.Info("Started the automation tool for automatic snapshot sanity check and R2 uploading (caplin only)", "chain", c.Chain)
	dirs := datadir.New(c.Datadir)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	tickerTriggerer := time.NewTicker(c.UploadPeriod)
	defer tickerTriggerer.Stop()
	// do the checking at first run
	if err := checkSnapshots(ctx, beaconConfig, dirs); err != nil {
		return err
	}
	log.Info("Uploading snapshots to R2 bucket")
	// next upload to R2
	command := "rclone"
	args := []string{"sync", dirs.Snap, c.Bucket, "--include", "*beaconblocks*"}
	if err := exec.Command(command, args...).Run(); err != nil {
		return fmt.Errorf("rclone failed, make sure rclone is installed and is properly configured: %s", err)
	}
	log.Info("Finished snapshots to R2 bucket")
	for {
		select {
		case <-tickerTriggerer.C:
			log.Info("Checking snapshots")
			if err := checkSnapshots(ctx, beaconConfig, dirs); err != nil {
				return err
			}
			log.Info("Finishing snapshots")
			// next upload to R2
			command := "rclone"
			args := []string{"sync", dirs.Snap, c.Bucket, "--include", "*beaconblocks*"}
			log.Info("Uploading snapshots to R2 bucket")
			if err := exec.Command(command, args...).Run(); err != nil {
				return fmt.Errorf("rclone failed, make sure rclone is installed and is properly configured: %s", err)
			}
			log.Info("Finished snapshots to R2 bucket")
		case <-ctx.Done():
			return nil
		}
	}
}

func checkSnapshots(ctx context.Context, beaconConfig *clparams.BeaconChainConfig, dirs datadir.Dirs) error {
	rawDB, _ := persistence.AferoRawBeaconBlockChainFromOsPath(beaconConfig, dirs.CaplinHistory)
	_, db, err := caplin1.OpenCaplinDatabase(ctx, db_config.DatabaseConfiguration{PruneDepth: math.MaxUint64}, beaconConfig, rawDB, dirs.CaplinIndexing, nil, false)
	if err != nil {
		return err
	}
	defer db.Close()
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

	to = (to / snaptype.Erigon2RecentMergeLimit) * snaptype.Erigon2RecentMergeLimit

	csn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{}, dirs.Snap, log.Root())
	if err := csn.ReopenFolder(); err != nil {
		return err
	}

	genesisHeader, _, _, err := csn.ReadHeader(0)
	if err != nil {
		return err
	}
	previousBlockRoot, err := genesisHeader.Header.HashSSZ()
	if err != nil {
		return err
	}
	previousBlockSlot := genesisHeader.Header.Slot
	for i := uint64(1); i < to; i++ {
		if utils.Min64(0, i-320) > previousBlockSlot {
			return fmt.Errorf("snapshot %d has invalid slot", i)
		}
		// Checking of snapshots is a chain contiguity problem
		currentHeader, _, _, err := csn.ReadHeader(i)
		if err != nil {
			return err
		}
		if currentHeader == nil {
			continue
		}
		if currentHeader.Header.ParentRoot != previousBlockRoot {
			return fmt.Errorf("snapshot %d has invalid parent root", i)
		}
		previousBlockRoot, err = currentHeader.Header.HashSSZ()
		if err != nil {
			return err
		}
		previousBlockSlot = currentHeader.Header.Slot
		if i%20000 == 0 {
			log.Info("Successfully checked", "slot", i)
		}
	}
	return nil
}
