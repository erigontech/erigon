// Copyright 2024 The Erigon Authors
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

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/afero"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/estimate"
	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format/getters"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/checkpoint_sync"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/cl/phase1/stages"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/turbo/debug"
)

var CLI struct {
	Chain                     Chain                     `cmd:"" help:"download the entire chain from reqresp network"`
	DumpSnapshots             DumpSnapshots             `cmd:"" help:"generate caplin snapshots"`
	CheckSnapshots            CheckSnapshots            `cmd:"" help:"check snapshot folder against content of chain data"`
	LoopSnapshots             LoopSnapshots             `cmd:"" help:"loop over snapshots"`
	RetrieveHistoricalState   RetrieveHistoricalState   `cmd:"" help:"retrieve historical state from db"`
	ChainEndpoint             ChainEndpoint             `cmd:"" help:"chain endpoint"`
	ArchiveSanitizer          ArchiveSanitizer          `cmd:"" help:"archive sanitizer"`
	BenchmarkNode             BenchmarkNode             `cmd:"" help:"benchmark node"`
	BlobArchiveStoreCheck     BlobArchiveStoreCheck     `cmd:"" help:"blob archive store check"`
	DumpBlobsSnapshots        DumpBlobsSnapshots        `cmd:"" help:"dump blobs snapshots"`
	CheckBlobsSnapshots       CheckBlobsSnapshots       `cmd:"" help:"check blobs snapshots"`
	CheckBlobsSnapshotsCount  CheckBlobsSnapshotsCount  `cmd:"" help:"check blobs snapshots count"`
	DumpBlobsSnapshotsToStore DumpBlobsSnapshotsToStore `cmd:"" help:"dump blobs snapshots to store"`
	DumpStateSnapshots        DumpStateSnapshots        `cmd:"" help:"dump state snapshots"`
	MakeDepositArgs           MakeDepositArgs           `cmd:"" help:"make deposit args"`
}

type chainCfg struct {
	Chain string `help:"chain" default:"mainnet"`
}

func (c *chainCfg) configs() (beaconConfig *clparams.BeaconChainConfig, err error) {
	_, beaconConfig, _, err = clparams.GetConfigsByNetworkName(c.Chain)
	return
}

type outputFolder struct {
	Datadir string `help:"datadir" default:"~/.local/share/erigon" type:"existingdir"`
}

type withSentinel struct {
	Sentinel string `help:"sentinel url" default:"localhost:7777"`
}

type withPPROF struct {
	Pprof bool `help:"enable pprof" default:"false"`
}

func (w *withPPROF) withProfile() {
	if w.Pprof {
		debug.StartPProf("localhost:6060", metrics.Setup("localhost:6060", log.Root()))
	}
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

	_, beaconConfig, networkType, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	log.Info("Started chain download", "chain", c.Chain)

	dirs := datadir.New(c.Datadir)

	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = c.Chain
	csn := freezeblocks.NewCaplinSnapshots(freezingCfg, beaconConfig, dirs, log.Root())
	bs, err := checkpoint_sync.NewRemoteCheckpointSync(beaconConfig, networkType).GetLatestBeaconState(ctx)
	if err != nil {
		return err
	}

	ethClock := eth_clock.NewEthereumClock(bs.GenesisTime(), bs.GenesisValidatorsRoot(), beaconConfig)
	db, blobStorage, err := caplin1.OpenCaplinDatabase(ctx, beaconConfig, ethClock, dirs.CaplinIndexing, dirs.CaplinBlobs, nil, false, 0)
	if err != nil {
		return err
	}
	defer db.Close()

	beacon := rpc.NewBeaconRpcP2P(ctx, s, beaconConfig, ethClock, nil)

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
		ethClock.GenesisValidatorsRoot(),
		beaconConfig.GenesisEpoch,
		ethClock.GenesisValidatorsRoot(),
		beaconConfig.GenesisSlot)
	if err != nil {
		return err
	}

	downloader := network.NewBackwardBeaconDownloader(ctx, beacon, nil, nil, db)
	cfg := stages.StageHistoryReconstruction(downloader, antiquary.NewAntiquary(ctx, nil, nil, nil, nil, dirs, nil, nil, nil, nil, nil, nil, nil, false, false, false, false, nil), csn, db, nil, beaconConfig, clparams.CaplinConfig{}, true, bRoot, bs.Slot(), "/tmp", 300*time.Millisecond, nil, nil, blobStorage, log.Root(), nil)
	return stages.SpawnStageHistoryDownload(cfg, ctx, log.Root())
}

type ChainEndpoint struct {
	Endpoint string `help:"endpoint" default:""`
	chainCfg
	outputFolder
}

func retrieveAndSanitizeBlockFromRemoteEndpoint(ctx context.Context, beaconConfig *clparams.BeaconChainConfig, uri string, expectedBlockRoot *common.Hash) (*cltypes.SignedBeaconBlock, error) {
	log.Debug("[Checkpoint Sync] Requesting beacon block", "uri", uri)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/octet-stream")
	if err != nil {
		return nil, fmt.Errorf("checkpoint sync request failed %s", err)
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = r.Body.Close()
	}()
	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("checkpoint sync failed, bad status code %d", r.StatusCode)
	}
	marshaled, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("checkpoint sync read failed %s", err)
	}
	if len(marshaled) < 108 {
		return nil, errors.New("read failed, too short")
	}
	currentSlot := binary.LittleEndian.Uint64(marshaled[100:108])
	v := beaconConfig.GetCurrentStateVersion(currentSlot / beaconConfig.SlotsPerEpoch)

	block := cltypes.NewSignedBeaconBlock(beaconConfig, v)
	err = block.DecodeSSZ(marshaled, int(v))
	if err != nil {
		return nil, fmt.Errorf("checkpoint sync decode failed %s", err)
	}
	if expectedBlockRoot != nil {
		has, err := block.Block.HashSSZ()
		if err != nil {
			return nil, fmt.Errorf("checkpoint sync decode failed %s", err)
		}
		if has != *expectedBlockRoot {
			return nil, fmt.Errorf("checkpoint sync decode failed, unexpected block root %s", has)
		}
	}
	return block, nil
}

func (c *ChainEndpoint) Run(ctx *Context) error {
	_, beaconConfig, ntype, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	// Get latest state
	checkPointSyncer := checkpoint_sync.NewRemoteCheckpointSync(beaconConfig, ntype)
	bs, err := checkPointSyncer.GetLatestBeaconState(ctx)
	if err != nil {
		return err
	}
	ethClock := eth_clock.NewEthereumClock(bs.GenesisTime(), bs.GenesisValidatorsRoot(), beaconConfig)

	dirs := datadir.New(c.Datadir)
	db, _, err := caplin1.OpenCaplinDatabase(ctx, beaconConfig, ethClock, dirs.CaplinIndexing, dirs.CaplinBlobs, nil, false, 0)
	if err != nil {
		return err
	}
	defer db.Close()

	baseUri, err := url.JoinPath(c.Endpoint, "eth/v2/beacon/blocks")
	if err != nil {
		return err
	}
	log.Info("Hooked", "uri", baseUri)
	// Let's fetch the head first
	currentBlock, err := retrieveAndSanitizeBlockFromRemoteEndpoint(ctx, beaconConfig, baseUri+"/head", nil)
	if err != nil {
		return fmt.Errorf("failed to retrieve head: %w, uri: %s", err, baseUri+"/head")
	}
	currentRoot, err := currentBlock.Block.HashSSZ()
	if err != nil {
		return err
	}
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	log.Info("Starting with", "root", common.Hash(currentRoot), "slot", currentBlock.Block.Slot)
	currentRoot = currentBlock.Block.ParentRoot
	if err := beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, currentBlock, true); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	previousLogBlock := currentBlock.Block.Slot

	logInterval := time.NewTicker(30 * time.Second)
	defer logInterval.Stop()

	loopStep := func() (bool, error) {
		tx, err := db.BeginRw(ctx)
		if err != nil {
			return false, err
		}
		defer tx.Rollback()

		stringifiedRoot := common.Bytes2Hex(currentRoot[:])
		// Let's fetch the head first
		currentBlock, err := retrieveAndSanitizeBlockFromRemoteEndpoint(ctx, beaconConfig, fmt.Sprintf("%s/0x%s", baseUri, stringifiedRoot), (*common.Hash)(&currentRoot))
		if err != nil {
			return false, fmt.Errorf("failed to retrieve block: %w, uri: %s", err, fmt.Sprintf("%s/0x%s", baseUri, stringifiedRoot))
		}
		currentRoot, err = currentBlock.Block.HashSSZ()
		if err != nil {
			return false, err
		}
		if err := beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, currentBlock, true); err != nil {
			return false, err
		}
		currentRoot = currentBlock.Block.ParentRoot
		currentSlot := currentBlock.Block.Slot
		// it will stop if we end finding a gap or if we reach the maxIterations
		for {
			// check if the expected root is in db
			slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, currentRoot)
			if err != nil {
				return false, err
			}
			if slot == nil || *slot == 0 {
				break
			}
			if err := beacon_indicies.MarkRootCanonical(ctx, tx, *slot, currentRoot); err != nil {
				return false, err
			}
			currentRoot, err = beacon_indicies.ReadParentBlockRoot(ctx, tx, currentRoot)
			if err != nil {
				return false, err
			}
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		select {
		case <-logInterval.C:
			// up to 2 decimal places
			rate := float64(previousLogBlock-currentSlot) / 30
			log.Info("Successfully processed", "slot", currentSlot, "blk/sec", fmt.Sprintf("%.2f", rate))
			previousLogBlock = currentBlock.Block.Slot
		case <-ctx.Done():
		default:
		}
		return currentSlot != 0, nil
	}
	var keepGoing bool
	for keepGoing, err = loopStep(); keepGoing && err == nil; keepGoing, err = loopStep() {
		if !keepGoing {
			break
		}
	}

	return err
}

type DumpSnapshots struct {
	chainCfg
	outputFolder

	To uint64 `name:"to" help:"slot to dump"`
}

func (c *DumpSnapshots) Run(ctx *Context) error {
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))
	log.Info("Started chain download", "chain", c.Chain)

	dirs := datadir.New(c.Datadir)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	db, _, err := caplin1.OpenCaplinDatabase(ctx, beaconConfig, nil, dirs.CaplinIndexing, dirs.CaplinBlobs, nil, false, 0)
	if err != nil {
		return err
	}
	var to uint64
	db.View(ctx, func(tx kv.Tx) (err error) {
		if c.To == 0 {
			to, err = beacon_indicies.ReadHighestFinalized(tx)
			return
		}
		to = c.To
		return
	})

	salt, err := snaptype.GetIndexSalt(dirs.Snap, log.Root())

	if err != nil {
		return err
	}

	return freezeblocks.DumpBeaconBlocks(ctx, db, 0, to, salt, dirs, estimate.CompressSnapshot.Workers(), log.LvlInfo, log.Root())
}

type CheckSnapshots struct {
	chainCfg
	outputFolder
	withPPROF
}

func (c *CheckSnapshots) Run(ctx *Context) error {
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	c.withProfile()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))
	log.Info("Started the checking process", "chain", c.Chain, "datadir", c.Datadir)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	dirs := datadir.New(c.Datadir)

	db, _, err := caplin1.OpenCaplinDatabase(ctx, beaconConfig, nil, dirs.CaplinIndexing, dirs.CaplinBlobs, nil, false, 0)
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

	to = (to / snaptype.CaplinMergeLimit) * snaptype.CaplinMergeLimit

	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = c.Chain
	csn := freezeblocks.NewCaplinSnapshots(freezingCfg, beaconConfig, dirs, log.Root())
	if err := csn.OpenFolder(); err != nil {
		return err
	}

	genesisHeader, _, _, err := csn.ReadHeader(0)
	if err != nil {
		return err
	}

	if genesisHeader == nil {
		log.Warn("beaconIndices up to", "block", to, "caplinSnapIndexMax", csn.IndicesMax())
		return errors.New("genesis header is nil")
	}
	previousBlockRoot, err := genesisHeader.Header.HashSSZ()
	if err != nil {
		return err
	}
	previousBlockSlot := genesisHeader.Header.Slot
	for i := uint64(1); i < to; i++ {
		if min(0, i-320) > previousBlockSlot {
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
		if i%2000 == 0 {
			log.Info("Successfully checked", "slot", i)
		}
	}
	return nil
}

type LoopSnapshots struct {
	chainCfg
	outputFolder
	withPPROF

	Slot uint64 `name:"slot" help:"slot to check"`
}

func (c *LoopSnapshots) Run(ctx *Context) error {
	c.withProfile()

	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))
	log.Info("Started the checking process", "chain", c.Chain)

	dirs := datadir.New(c.Datadir)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	db, _, err := caplin1.OpenCaplinDatabase(ctx, beaconConfig, nil, dirs.CaplinIndexing, dirs.CaplinBlobs, nil, false, 0)
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

	to = (to / snaptype.CaplinMergeLimit) * snaptype.CaplinMergeLimit

	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = c.Chain
	csn := freezeblocks.NewCaplinSnapshots(freezingCfg, beaconConfig, dirs, log.Root())
	if err := csn.OpenFolder(); err != nil {
		return err
	}

	br := &snapshot_format.MockBlockReader{}
	snReader := freezeblocks.NewBeaconSnapshotReader(csn, br, beaconConfig)
	start := time.Now()
	for i := c.Slot; i < to; i++ {
		snReader.ReadBlockBySlot(ctx, tx, i)
	}
	log.Info("Successfully checked", "slot", c.Slot, "time", time.Since(start))
	return nil
}

type RetrieveHistoricalState struct {
	chainCfg
	outputFolder
	withPPROF
	CompareFile string `help:"compare file" default:""`
	CompareSlot uint64 `help:"compare slot" default:"0"`
	Out         string `help:"output file" default:""`
}

func (r *RetrieveHistoricalState) Run(ctx *Context) error {
	vt := state_accessors.NewStaticValidatorTable()
	_, beaconConfig, t, err := clparams.GetConfigsByNetworkName(r.Chain)
	if err != nil {
		return err
	}
	dirs := datadir.New(r.Datadir)
	db, _, err := caplin1.OpenCaplinDatabase(ctx, beaconConfig, nil, dirs.CaplinIndexing, dirs.CaplinBlobs, nil, false, 0)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = r.Chain
	allSnapshots := freezeblocks.NewRoSnapshots(freezingCfg, dirs.Snap, log.Root())
	if err := allSnapshots.OpenFolder(); err != nil {
		return err
	}
	if err := state_accessors.ReadValidatorsTable(tx, vt); err != nil {
		return err
	}

	blockReader := freezeblocks.NewBlockReader(allSnapshots, nil)
	eth1Getter := getters.NewExecutionSnapshotReader(ctx, blockReader, db)
	eth1Getter.SetBeaconChainConfig(beaconConfig)
	csn := freezeblocks.NewCaplinSnapshots(freezingCfg, beaconConfig, dirs, log.Root())
	if err := csn.OpenFolder(); err != nil {
		return err
	}
	snr := freezeblocks.NewBeaconSnapshotReader(csn, eth1Getter, beaconConfig)
	gSpot, err := initial_state.GetGenesisState(t)
	if err != nil {
		return err
	}

	snTypes := snapshotsync.MakeCaplinStateSnapshotsTypes(db)
	stateSn := snapshotsync.NewCaplinStateSnapshots(freezingCfg, beaconConfig, dirs, snTypes, log.Root())
	if err := stateSn.OpenFolder(); err != nil {
		return err
	}
	if _, err := antiquary.FillStaticValidatorsTableIfNeeded(ctx, log.Root(), stateSn, vt); err != nil {
		return err
	}

	bs, err := checkpoint_sync.NewRemoteCheckpointSync(beaconConfig, t).GetLatestBeaconState(ctx)
	if err != nil {
		return err
	}
	sn := synced_data.NewSyncedDataManager(beaconConfig, true)
	sn.OnHeadState(bs)

	r.withPPROF.withProfile()
	hr := historical_states_reader.NewHistoricalStatesReader(beaconConfig, snr, vt, gSpot, stateSn, sn)
	start := time.Now()
	haveState, err := hr.ReadHistoricalState(ctx, tx, r.CompareSlot)
	if err != nil {
		return err
	}
	endTime := time.Since(start)
	hRoot, err := haveState.HashSSZ()
	if err != nil {
		return err
	}
	log.Info("Got state", "slot", haveState.Slot(), "root", common.Hash(hRoot), "elapsed", endTime)

	if err := haveState.InitBeaconState(); err != nil {
		return err
	}

	v := haveState.Version()
	// encode and decode the state
	enc, err := haveState.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	haveState = state.New(beaconConfig)
	if err := haveState.DecodeSSZ(enc, int(v)); err != nil {
		return err
	}
	if r.Out != "" {
		// create file
		if err := os.WriteFile(r.Out, enc, 0644); err != nil {
			return err
		}
	}

	hRoot, err = haveState.HashSSZ()
	if err != nil {
		return err
	}
	if r.CompareFile == "" {
		return nil
	}
	// Read the content of CompareFile in a []byte  without afero
	rawBytes, err := os.ReadFile(r.CompareFile)
	if err != nil {
		return err
	}
	// Decode the []byte into a state
	wantState := state.New(beaconConfig)
	if err := wantState.DecodeSSZ(rawBytes, int(haveState.Version())); err != nil {
		return err
	}
	wRoot, err := wantState.HashSSZ()
	if err != nil {
		return err
	}
	if hRoot != wRoot {
		haveState.PrintLeaves()
		wantState.PrintLeaves()
		for i := 0; i < haveState.ValidatorLength(); i++ {
			haveState.ValidatorSet().Get(i)
			// Compare each field
			if haveState.ValidatorSet().Get(i).PublicKey() != wantState.ValidatorSet().Get(i).PublicKey() {
				log.Error("PublicKey mismatch", "index", i, "have", haveState.ValidatorSet().Get(i).PublicKey(), "want", wantState.ValidatorSet().Get(i).PublicKey())
			}
			if haveState.ValidatorSet().Get(i).WithdrawalCredentials() != wantState.ValidatorSet().Get(i).WithdrawalCredentials() {
				log.Error("WithdrawalCredentials mismatch", "index", i, "have", haveState.ValidatorSet().Get(i).WithdrawalCredentials(), "want", wantState.ValidatorSet().Get(i).WithdrawalCredentials())
			}
			if haveState.ValidatorSet().Get(i).EffectiveBalance() != wantState.ValidatorSet().Get(i).EffectiveBalance() {
				log.Error("EffectiveBalance mismatch", "index", i, "have", haveState.ValidatorSet().Get(i).EffectiveBalance(), "want", wantState.ValidatorSet().Get(i).EffectiveBalance())
			}
			if haveState.ValidatorSet().Get(i).Slashed() != wantState.ValidatorSet().Get(i).Slashed() {
				log.Error("Slashed mismatch", "index", i, "have", haveState.ValidatorSet().Get(i).Slashed(), "want", wantState.ValidatorSet().Get(i).Slashed())
			}
			if haveState.ValidatorSet().Get(i).ActivationEligibilityEpoch() != wantState.ValidatorSet().Get(i).ActivationEligibilityEpoch() {
				log.Error("ActivationEligibilityEpoch mismatch", "index", i, "have", haveState.ValidatorSet().Get(i).ActivationEligibilityEpoch(), "want", wantState.ValidatorSet().Get(i).ActivationEligibilityEpoch())
			}
			if haveState.ValidatorSet().Get(i).ActivationEpoch() != wantState.ValidatorSet().Get(i).ActivationEpoch() {
				log.Error("ActivationEpoch mismatch", "index", i, "have", haveState.ValidatorSet().Get(i).ActivationEpoch(), "want", wantState.ValidatorSet().Get(i).ActivationEpoch())
			}
			if haveState.ValidatorSet().Get(i).ExitEpoch() != wantState.ValidatorSet().Get(i).ExitEpoch() {
				log.Error("ExitEpoch mismatch", "index", i, "have", haveState.ValidatorSet().Get(i).ExitEpoch(), "want", wantState.ValidatorSet().Get(i).ExitEpoch())
			}
			if haveState.ValidatorSet().Get(i).WithdrawableEpoch() != wantState.ValidatorSet().Get(i).WithdrawableEpoch() {
				log.Error("WithdrawableEpoch mismatch", "index", i, "have", haveState.ValidatorSet().Get(i).WithdrawableEpoch(), "want", wantState.ValidatorSet().Get(i).WithdrawableEpoch())
			}
		}
		return fmt.Errorf("state mismatch: got %s, want %s", common.Hash(hRoot), common.Hash(wRoot))
	}
	return nil
}

type ArchiveSanitizer struct {
	chainCfg
	outputFolder
	BeaconApiURL string `help:"beacon api url" default:"http://localhost:5555"`
	IntervalSlot uint64 `help:"interval slot" default:"19"` // odd number so that we can test many potential cases.
	StartSlot    uint64 `help:"start slot" default:"0"`
	FaultOut     string `help:"fault out" default:""`
}

func getHead(beaconApiURL string) (uint64, error) {
	headResponse := map[string]interface{}{}
	req, err := http.NewRequest("GET", beaconApiURL+"/eth/v2/debug/beacon/heads", nil)
	if err != nil {
		return 0, err
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&headResponse); err != nil {
		return 0, err
	}
	data := headResponse["data"].([]interface{})
	if len(data) == 0 {
		return 0, errors.New("no head found")
	}
	head := data[0].(map[string]interface{})
	slotStr, ok := head["slot"].(string)
	if !ok {
		return 0, errors.New("no slot found")
	}
	slot, err := strconv.ParseUint(slotStr, 10, 64)
	if err != nil {
		return 0, err
	}
	return slot, nil
}

func getStateRootAtSlot(beaconApiURL string, slot uint64) (common.Hash, error) {
	response := map[string]interface{}{}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/eth/v1/beacon/states/%d/root", beaconApiURL, slot), nil)
	if err != nil {
		return common.Hash{}, err
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return common.Hash{}, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return common.Hash{}, nil
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return common.Hash{}, err
	}
	data := response["data"].(map[string]interface{})
	if len(data) == 0 {
		return common.Hash{}, errors.New("no head found")
	}
	rootStr := data["root"].(string)

	return common.HexToHash(rootStr), nil
}

func getBeaconState(ctx context.Context, beaconConfig *clparams.BeaconChainConfig, uri string, slot uint64) (*state.CachingBeaconState, error) {
	log.Info("[Checkpoint Sync] Requesting beacon state", "uri", uri)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/octet-stream")
	if err != nil {
		return nil, fmt.Errorf("checkpoint sync request failed %s", err)
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = r.Body.Close()
	}()
	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("checkpoint sync failed, bad status code %d", r.StatusCode)
	}
	marshaled, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("checkpoint sync read failed %s", err)
	}

	epoch := slot / beaconConfig.SlotsPerEpoch

	beaconState := state.New(beaconConfig)
	err = beaconState.DecodeSSZ(marshaled, int(beaconConfig.GetCurrentStateVersion(epoch)))
	if err != nil {
		return nil, fmt.Errorf("checkpoint sync decode failed %s", err)
	}
	return beaconState, nil
}

func (a *ArchiveSanitizer) Run(ctx *Context) error {
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(a.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))

	// retrieve the head slot first through /eth/v2/debug/beacon/heads
	headSlot, err := getHead(a.BeaconApiURL)
	if err != nil {
		return err
	}
	for i := a.StartSlot; i < headSlot; i += a.IntervalSlot {
		// retrieve the state root at slot i and skip if not found (can happen)
		stateRoot, err := getStateRootAtSlot(a.BeaconApiURL, i)
		if err != nil {
			return err
		}
		if stateRoot == (common.Hash{}) {
			continue
		}
		state, err := getBeaconState(ctx, beaconConfig, fmt.Sprintf("%s/eth/v2/debug/beacon/states/%d", a.BeaconApiURL, i), i)
		if err != nil {
			return err
		}
		stateRoot2, err := state.HashSSZ()
		if err != nil {
			return err
		}
		if stateRoot != stateRoot2 {
			if a.FaultOut != "" {
				enc, err := state.EncodeSSZ(nil)
				if err != nil {
					return err
				}
				if err := os.WriteFile(a.FaultOut, enc, 0644); err != nil {
					return err
				}
			}
			return fmt.Errorf("state mismatch at slot %d: got %x, want %x", i, stateRoot2, stateRoot)
		}
		log.Info("State at slot", "slot", i, "root", stateRoot)
	}
	return nil
}

type BenchmarkNode struct {
	chainCfg
	BaseURL  string `help:"base url" default:"http://localhost:5555"`
	Endpoint string `help:"endpoint" default:"/eth/v1/beacon/states/{slot}/validators"`
	OutCSV   string `help:"output csv" default:""`
	Accept   string `help:"accept" default:"application/json"`
	Head     bool   `help:"head" default:"false"`
	Method   string `help:"method" default:"GET"`
	Body     string `help:"body" default:"{}"`
}

func (b *BenchmarkNode) Run(ctx *Context) error {
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(b.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))

	// retrieve the head slot first through /eth/v2/debug/beacon/heads
	headSlot, err := getHead(b.BaseURL)
	if err != nil {
		return err
	}
	startSlot := 0
	interval := 20_000
	if b.Head {
		startSlot = int(headSlot) - 20
		interval = 1
	}
	// make a csv file
	f, err := os.Create(b.OutCSV)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString("slot,elapsed\n")
	if err != nil {
		return err
	}

	for i := uint64(startSlot); i < headSlot; i += uint64(interval) {
		uri := b.BaseURL + b.Endpoint
		uri = strings.Replace(uri, "{slot}", strconv.FormatUint(i, 10), 1)
		uri = strings.Replace(uri, "{epoch}", strconv.FormatUint(i/beaconConfig.SlotsPerEpoch, 10), 1)
		elapsed, err := timeRequest(uri, b.Accept, b.Method, b.Body)
		if err != nil {
			log.Warn("Failed to benchmark", "error", err, "uri", uri)
			continue
		}
		_, err = f.WriteString(fmt.Sprintf("%d,%d\n", i, elapsed.Milliseconds()))
		if err != nil {
			return err
		}
		log.Info("Benchmarked", "slot", i, "elapsed", elapsed, "uri", uri)
	}
	return nil
}

func timeRequest(uri, accept, method, body string) (time.Duration, error) {
	req, err := http.NewRequest(method, uri, nil)
	if err != nil {
		return 0, err
	}
	if method == "POST" {
		req.Body = io.NopCloser(strings.NewReader(body))
	}
	req.Header.Set("Accept", accept)
	start := time.Now()
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("bad status code %d", r.StatusCode)
	}
	_, err = io.ReadAll(r.Body) // we wait for the body to be read
	if err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

type BlobArchiveStoreCheck struct {
	chainCfg
	outputFolder
	FromSlot uint64 `help:"from slot" default:"0"`
}

func (b *BlobArchiveStoreCheck) Run(ctx *Context) error {

	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(b.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	log.Info("Started archive node checking", "chain", b.Chain)

	dirs := datadir.New(b.Datadir)

	db, blobStorage, err := caplin1.OpenCaplinDatabase(ctx, beaconConfig, nil, dirs.CaplinIndexing, dirs.CaplinBlobs, nil, false, 0)
	if err != nil {
		return err
	}
	defer db.Close()

	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = b.Chain
	csn := freezeblocks.NewCaplinSnapshots(freezingCfg, beaconConfig, dirs, log.Root())
	if err := csn.OpenFolder(); err != nil {
		return err
	}
	snr := freezeblocks.NewBeaconSnapshotReader(csn, nil, beaconConfig)

	targetSlot := beaconConfig.DenebForkEpoch * beaconConfig.SlotsPerEpoch
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i := b.FromSlot; i >= targetSlot; i-- {
		blk, err := snr.ReadBlindedBlockBySlot(ctx, tx, i)
		if err != nil {
			return err
		}
		if blk == nil {
			continue
		}
		if blk.Version() < clparams.DenebVersion {
			continue
		}
		if blk.Block.Slot%10_000 == 0 {
			log.Info("Checking slot", "slot", blk.Block.Slot)
		}
		blockRoot, err := blk.Block.HashSSZ()
		if err != nil {
			return err
		}

		haveBlobs, err := blobStorage.KzgCommitmentsCount(ctx, blockRoot)
		if err != nil {
			return err
		}
		if haveBlobs != uint32(blk.Block.Body.BlobKzgCommitments.Len()) {
			if err := blobStorage.RemoveBlobSidecars(ctx, i, blockRoot); err != nil {
				return err
			}
			log.Warn("Slot", "slot", i, "have", haveBlobs, "want", blk.Block.Body.BlobKzgCommitments.Len())
		}
	}
	log.Info("Blob archive store check passed")
	return nil
}

type DumpBlobsSnapshots struct {
	chainCfg
	outputFolder

	To uint64 `name:"to" help:"slot to dump"`
}

func (c *DumpBlobsSnapshots) Run(ctx *Context) error {
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))
	log.Info("Started chain download", "chain", c.Chain)

	dirs := datadir.New(c.Datadir)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	db, blobStorage, err := caplin1.OpenCaplinDatabase(ctx, beaconConfig, nil, dirs.CaplinIndexing, dirs.CaplinBlobs, nil, false, 0)
	if err != nil {
		return err
	}
	var to uint64
	db.View(ctx, func(tx kv.Tx) (err error) {
		if c.To == 0 {
			to, err = beacon_indicies.ReadHighestFinalized(tx)
			return
		}
		to = c.To
		return
	})
	from := ((beaconConfig.DenebForkEpoch * beaconConfig.SlotsPerEpoch) / snaptype.CaplinMergeLimit) * snaptype.CaplinMergeLimit

	salt, err := snaptype.GetIndexSalt(dirs.Snap, log.Root())

	if err != nil {
		return err
	}

	return freezeblocks.DumpBlobsSidecar(ctx, blobStorage, db, from, to, salt, dirs, estimate.CompressSnapshot.Workers(), nil, log.LvlInfo, log.Root())
}

type CheckBlobsSnapshots struct {
	chainCfg
	outputFolder
	withPPROF
}

func (c *CheckBlobsSnapshots) Run(ctx *Context) error {
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	c.withProfile()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))
	log.Info("Started the checking process", "chain", c.Chain)
	dirs := datadir.New(c.Datadir)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	db, blobStorage, err := caplin1.OpenCaplinDatabase(ctx, beaconConfig, nil, dirs.CaplinIndexing, dirs.CaplinBlobs, nil, false, 0)
	if err != nil {
		return err
	}
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = c.Chain
	csn := freezeblocks.NewCaplinSnapshots(freezingCfg, beaconConfig, dirs, log.Root())
	if err := csn.OpenFolder(); err != nil {
		return err
	}
	to := csn.FrozenBlobs()

	for i := beaconConfig.SlotsPerEpoch*beaconConfig.DenebForkEpoch + 1; i < to; i++ {
		sds, err := csn.ReadBlobSidecars(i)
		if err != nil {
			return err
		}
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, i)
		if err != nil {
			return err
		}

		haveSds, _, err := blobStorage.ReadBlobSidecars(ctx, i, blockRoot)
		if err != nil {
			return err
		}
		for i, sd := range sds {
			if sd.Blob != haveSds[i].Blob {
				return fmt.Errorf("slot %d: blob %d mismatch", i, i)
			}
		}
		if i%2000 == 0 {
			log.Info("Successfully checked", "slot", i)
		}
	}
	return nil
}

type CheckBlobsSnapshotsCount struct {
	chainCfg
	outputFolder
	withPPROF
	From           uint64 `name:"from" help:"from slot" default:"0"`
	CheckNeedRegen bool   `name:"check-need-regen" help:"check if blobs need regen"`
}

func (c *CheckBlobsSnapshotsCount) Run(ctx *Context) error {
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	c.withProfile()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))
	log.Info("Started the checking process", "chain", c.Chain)
	dirs := datadir.New(c.Datadir)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	db, _, err := caplin1.OpenCaplinDatabase(ctx, beaconConfig, nil, dirs.CaplinIndexing, dirs.CaplinBlobs, nil, false, 0)
	if err != nil {
		return err
	}
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = c.Chain
	csn := freezeblocks.NewCaplinSnapshots(freezingCfg, beaconConfig, dirs, log.Root())
	if err := csn.OpenFolder(); err != nil {
		return err
	}
	to := csn.FrozenBlobs()
	snr := freezeblocks.NewBeaconSnapshotReader(csn, nil, beaconConfig)
	start := max(beaconConfig.SlotsPerEpoch*beaconConfig.DenebForkEpoch+1, c.From)
	slotsToRegen := map[uint64]struct{}{}
	for i := start; i < to; i++ {
		sds, err := csn.ReadBlobSidecars(i)
		if err != nil {
			return err
		}

		bBlock, err := snr.ReadBlindedBlockBySlot(ctx, tx, i)
		if err != nil {
			return err
		}
		if bBlock == nil {
			continue
		}
		if len(sds) != bBlock.Block.Body.BlobKzgCommitments.Len() {
			if !c.CheckNeedRegen {
				return fmt.Errorf("slot %d: blob count mismatch, have %d, want %d", i, len(sds), bBlock.Block.Body.BlobKzgCommitments.Len())
			}
			log.Warn("Slot", "slot", i, "have", len(sds), "want", bBlock.Block.Body.BlobKzgCommitments.Len())
			slotsToRegen[i] = struct{}{}
		}
		if i%2000 == 0 {
			log.Info("Successfully checked", "slot", i)
		}
	}
	if c.CheckNeedRegen {
		for slot := range slotsToRegen {
			log.Info("The following slot need to be regenerated", "slot", slot)
		}
	}
	return nil
}

type DumpBlobsSnapshotsToStore struct {
	chainCfg
	outputFolder
	withPPROF
}

func (c *DumpBlobsSnapshotsToStore) Run(ctx *Context) error {
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	c.withProfile()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))
	log.Info("Started the dumping process", "chain", c.Chain)
	dirs := datadir.New(c.Datadir)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	db, blobStore, err := caplin1.OpenCaplinDatabase(ctx, beaconConfig, nil, dirs.CaplinIndexing, dirs.CaplinBlobs, nil, false, 0)
	if err != nil {
		return err
	}
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = c.Chain
	csn := freezeblocks.NewCaplinSnapshots(freezingCfg, beaconConfig, dirs, log.Root())
	if err := csn.OpenFolder(); err != nil {
		return err
	}
	to := csn.FrozenBlobs()
	start := max(beaconConfig.SlotsPerEpoch * beaconConfig.DenebForkEpoch)
	for i := start; i < to; i++ {
		sds, err := csn.ReadBlobSidecars(i)
		if err != nil {
			return err
		}
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, i)
		if err != nil {
			return err
		}
		if blockRoot == (common.Hash{}) {
			continue
		}
		if err := blobStore.WriteBlobSidecars(ctx, blockRoot, sds); err != nil {
			return err
		}
		if i%2000 == 0 {
			log.Info("Successfully dumped", "slot", i)
		}
	}

	return nil
}

type DumpStateSnapshots struct {
	chainCfg
	outputFolder
	To       uint64 `name:"to" help:"slot to dump"`
	StepSize uint64 `name:"step-size" help:"step size" default:"10000"`
}

func (c *DumpStateSnapshots) Run(ctx *Context) error {
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))
	log.Info("Started chain download", "chain", c.Chain)

	dirs := datadir.New(c.Datadir)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	db, _, err := caplin1.OpenCaplinDatabase(ctx, beaconConfig, nil, dirs.CaplinIndexing, dirs.CaplinBlobs, nil, false, 0)
	if err != nil {
		return err
	}
	var to uint64
	db.View(ctx, func(tx kv.Tx) (err error) {
		if c.To == 0 {
			to, err = state_accessors.GetStateProcessingProgress(tx)
			return
		}
		to = c.To
		return
	})

	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = c.Chain

	salt, err := snaptype.GetIndexSalt(dirs.Snap, log.Root())

	if err != nil {
		return err
	}
	snTypes := snapshotsync.MakeCaplinStateSnapshotsTypes(db)
	stateSn := snapshotsync.NewCaplinStateSnapshots(freezingCfg, beaconConfig, dirs, snTypes, log.Root())
	if err := stateSn.OpenFolder(); err != nil {
		return err
	}
	r, _ := stateSn.Get(kv.BlockRoot, 999424)
	fmt.Printf("%x\n", r)

	if err := stateSn.DumpCaplinState(ctx, stateSn.BlocksAvailable(), to, c.StepSize, salt, dirs, runtime.NumCPU(), log.LvlInfo, log.Root()); err != nil {
		return err
	}
	if err := stateSn.OpenFolder(); err != nil {
		return err
	}
	r, _ = stateSn.Get(kv.BlockRoot, 999424)
	fmt.Printf("%x\n", r)

	return nil
}

type MakeDepositArgs struct {
	PrivateKey         string `name:"private-key" help:"private key to use for signing deposit" default:""`
	WithdrawalAddress  string `name:"withdrawal-address" help:"withdrawal address to use for deposit" default:""`
	AmountEth          uint64 `name:"amount-eth" help:"amount of ETH to deposit" default:"32"`                                     // in ETH
	DomainDeposit      string `name:"domain-deposit" help:"domain for deposit signature" default:"0x03000000"`                     // 0x03000000 for mainnet
	GenesisForkVersion string `name:"genesis-fork-version" help:"genesis fork version for deposit signature" default:"0x00000000"` // 0x00000000 for mainnet
}

func (m *MakeDepositArgs) Run(ctx *Context) error {

	var privateKeyBls *bls.PrivateKey
	if m.PrivateKey == "" {
		var err error
		privateKeyBls, err = bls.GenerateKey()
		if err != nil {
			return fmt.Errorf("failed to generate private key: %w", err)
		}
	} else {
		var err error
		privateKeyBls, err = bls.NewPrivateKeyFromBytes(common.Hex2Bytes(m.PrivateKey))
		if err != nil {
			return fmt.Errorf("failed to create private key from bytes: %w", err)
		}
	}
	withdrawalAddress := common.HexToAddress(m.WithdrawalAddress)
	if withdrawalAddress == (common.Address{}) {
		return fmt.Errorf("invalid withdrawal address: %s", m.WithdrawalAddress)
	}

	publicKey := privateKeyBls.PublicKey()
	if publicKey == nil {
		return errors.New("failed to get public key from private key")
	}
	// get the public key in compressed bytes format
	publicKey48 := common.Bytes48(bls.CompressPublicKey(publicKey))
	// amount in gwei
	amountGwei := m.AmountEth * 1_000_000_000

	var credentials common.Hash
	credentials[0] = 0x2
	copy(credentials[1:], make([]byte, 11))
	copy(credentials[12:], withdrawalAddress[:])

	var genesisForkVersion clparams.ConfigForkVersion
	var genesisForkVersion4 common.Bytes4

	if err := genesisForkVersion4.UnmarshalText([]byte(m.GenesisForkVersion)); err != nil {
		return fmt.Errorf("failed to parse genesis fork version: %w", err)
	}
	genesisForkVersion = clparams.ConfigForkVersion(utils.Bytes4ToUint32(genesisForkVersion4))

	deposit := &cltypes.DepositData{
		PubKey:                publicKey48,
		WithdrawalCredentials: credentials,
		Amount:                amountGwei,
		// Signature:             nil, // will be set later
	}

	// trim 0x prefix if present
	if len(m.DomainDeposit) > 2 && m.DomainDeposit[:2] == "0x" {
		m.DomainDeposit = m.DomainDeposit[2:]
	}

	domainDeposit := common.Hex2Bytes(m.DomainDeposit)

	domain, err := fork.ComputeDomain(
		domainDeposit,
		utils.Uint32ToBytes4(uint32(genesisForkVersion)),
		[32]byte{},
	)

	if err != nil {
		return fmt.Errorf("failed to compute domain: %w", err)
	}

	depositMessageRootForSigning, err := deposit.MessageHash()
	if err != nil {
		return err
	}

	messageToSign := utils.Sha256(depositMessageRootForSigning[:], domain)

	signature := privateKeyBls.Sign(messageToSign[:])
	signatureBytes := signature.Bytes()

	var signature96 common.Bytes96
	if len(signatureBytes) != 96 {
		return fmt.Errorf("signature length is not 96 bytes, got %d bytes", len(signatureBytes))
	}
	copy(signature96[:], signatureBytes)
	deposit.Signature = signature96

	depositTreeRoot, err := deposit.HashSSZ()
	if err != nil {
		return fmt.Errorf("failed to compute deposit tree root: %w", err)
	}

	privateKey := privateKeyBls.Bytes()

	// Print all the details in json format
	depositDetails := map[string]interface{}{
		"deposit":           deposit,
		"deposit_tree_root": common.Hash(depositTreeRoot),
		"private_key":       "0x" + common.Bytes2Hex(privateKey),
	}

	depositJSON, err := json.MarshalIndent(depositDetails, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal deposit details to JSON: %w", err)
	}
	fmt.Println(string(depositJSON))
	return nil
}
