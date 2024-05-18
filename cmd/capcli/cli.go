package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/turbo/debug"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon-lib/metrics"

	"github.com/ledgerwatch/erigon/cl/antiquary"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/clparams/initial_state"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
	"github.com/ledgerwatch/erigon/cmd/caplin/caplin1"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/persistence/format/snapshot_format"
	"github.com/ledgerwatch/erigon/cl/persistence/format/snapshot_format/getters"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/persistence/state/historical_states_reader"
	"github.com/ledgerwatch/erigon/cl/phase1/core"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/phase1/stages"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cl/utils"

	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/afero"
	"google.golang.org/grpc"

	sentinel "github.com/ledgerwatch/erigon-lib/gointerfaces/sentinelproto"
)

var CLI struct {
	Chain                   Chain                   `cmd:"" help:"download the entire chain from reqresp network"`
	DumpSnapshots           DumpSnapshots           `cmd:"" help:"generate caplin snapshots"`
	CheckSnapshots          CheckSnapshots          `cmd:"" help:"check snapshot folder against content of chain data"`
	LoopSnapshots           LoopSnapshots           `cmd:"" help:"loop over snapshots"`
	RetrieveHistoricalState RetrieveHistoricalState `cmd:"" help:"retrieve historical state from db"`
	ChainEndpoint           ChainEndpoint           `cmd:"" help:"chain endpoint"`
	ArchiveSanitizer        ArchiveSanitizer        `cmd:"" help:"archive sanitizer"`
	BenchmarkNode           BenchmarkNode           `cmd:"" help:"benchmark node"`
	BlobArchiveStoreCheck   BlobArchiveStoreCheck   `cmd:"" help:"blob archive store check"`
	DumpBlobsSnapshots      DumpBlobsSnapshots      `cmd:"" help:"dump blobs snapshots"`
	CheckBlobsSnapshots     CheckBlobsSnapshots     `cmd:"" help:"check blobs snapshots"`
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

	csn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{}, beaconConfig, dirs, log.Root())
	bs, err := core.RetrieveBeaconState(ctx, beaconConfig, clparams.GetCheckpointSyncEndpoint(networkType))
	if err != nil {
		return err
	}
	ethClock := eth_clock.NewEthereumClock(bs.GenesisTime(), bs.GenesisValidatorsRoot(), beaconConfig)
	db, blobStorage, err := caplin1.OpenCaplinDatabase(ctx, beaconConfig, ethClock, dirs.CaplinIndexing, dirs.CaplinBlobs, nil, false, 0)
	if err != nil {
		return err
	}
	defer db.Close()

	beacon := rpc.NewBeaconRpcP2P(ctx, s, beaconConfig, ethClock)

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

	downloader := network.NewBackwardBeaconDownloader(ctx, beacon, nil, db)
	cfg := stages.StageHistoryReconstruction(downloader, antiquary.NewAntiquary(ctx, nil, nil, nil, nil, dirs, nil, nil, nil, nil, nil, false, false, false, nil), csn, db, nil, beaconConfig, true, false, true, bRoot, bs.Slot(), "/tmp", 300*time.Millisecond, nil, nil, blobStorage, log.Root())
	return stages.SpawnStageHistoryDownload(cfg, ctx, log.Root())
}

type ChainEndpoint struct {
	Endpoint string `help:"endpoint" default:""`
	chainCfg
	outputFolder
}

func (c *ChainEndpoint) Run(ctx *Context) error {
	_, beaconConfig, ntype, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	bs, err := core.RetrieveBeaconState(ctx, beaconConfig, clparams.GetCheckpointSyncEndpoint(ntype))
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
	currentBlock, err := core.RetrieveBlock(ctx, beaconConfig, fmt.Sprintf("%s/head", baseUri), nil)
	if err != nil {
		return err
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

	log.Info("Starting with", "root", libcommon.Hash(currentRoot), "slot", currentBlock.Block.Slot)
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
		currentBlock, err := core.RetrieveBlock(ctx, beaconConfig, fmt.Sprintf("%s/0x%s", baseUri, stringifiedRoot), (*libcommon.Hash)(&currentRoot))
		if err != nil {
			return false, err
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

	salt, err := snaptype.GetIndexSalt(dirs.Snap)

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

	to = (to / snaptype.Erigon2MergeLimit) * snaptype.Erigon2MergeLimit

	csn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{}, beaconConfig, dirs, log.Root())
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

	to = (to / snaptype.Erigon2MergeLimit) * snaptype.Erigon2MergeLimit

	csn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{}, beaconConfig, dirs, log.Root())
	if err := csn.ReopenFolder(); err != nil {
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

	allSnapshots := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{}, dirs.Snap, 0, log.Root())
	if err := allSnapshots.ReopenFolder(); err != nil {
		return err
	}
	if err := state_accessors.ReadValidatorsTable(tx, vt); err != nil {
		return err
	}

	var bor *freezeblocks.BorRoSnapshots
	blockReader := freezeblocks.NewBlockReader(allSnapshots, bor)
	eth1Getter := getters.NewExecutionSnapshotReader(ctx, beaconConfig, blockReader, db)
	csn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{}, beaconConfig, dirs, log.Root())
	if err := csn.ReopenFolder(); err != nil {
		return err
	}
	snr := freezeblocks.NewBeaconSnapshotReader(csn, eth1Getter, beaconConfig)
	gSpot, err := initial_state.GetGenesisState(t)
	if err != nil {
		return err
	}

	hr := historical_states_reader.NewHistoricalStatesReader(beaconConfig, snr, vt, gSpot)
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
	log.Info("Got state", "slot", haveState.Slot(), "root", libcommon.Hash(hRoot), "elapsed", endTime)

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
		// for i := 0; i < haveState.PreviousEpochParticipation().Length(); i++ {
		// 	if haveState.PreviousEpochParticipation().Get(i) != wantState.PreviousEpochParticipation().Get(i) {
		// 		log.Info("Participation mismatch", "index", i, "have", haveState.PreviousEpochParticipation().Get(i), "want", wantState.PreviousEpochParticipation().Get(i))
		// 	}
		// }
		return fmt.Errorf("state mismatch: got %s, want %s", libcommon.Hash(hRoot), libcommon.Hash(wRoot))
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
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/eth/v2/debug/beacon/heads", beaconApiURL), nil)
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
		return 0, fmt.Errorf("no head found")
	}
	head := data[0].(map[string]interface{})
	slotStr, ok := head["slot"].(string)
	if !ok {
		return 0, fmt.Errorf("no slot found")
	}
	slot, err := strconv.ParseUint(slotStr, 10, 64)
	if err != nil {
		return 0, err
	}
	return slot, nil
}

func getStateRootAtSlot(beaconApiURL string, slot uint64) (libcommon.Hash, error) {
	response := map[string]interface{}{}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/eth/v1/beacon/states/%d/root", beaconApiURL, slot), nil)
	if err != nil {
		return libcommon.Hash{}, err
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return libcommon.Hash{}, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return libcommon.Hash{}, nil
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return libcommon.Hash{}, err
	}
	data := response["data"].(map[string]interface{})
	if len(data) == 0 {
		return libcommon.Hash{}, fmt.Errorf("no head found")
	}
	rootStr := data["root"].(string)

	return libcommon.HexToHash(rootStr), nil
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
		if stateRoot == (libcommon.Hash{}) {
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
		uri = strings.Replace(uri, "{slot}", fmt.Sprintf("%d", i), 1)
		uri = strings.Replace(uri, "{epoch}", fmt.Sprintf("%d", i/beaconConfig.SlotsPerEpoch), 1)
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

	csn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{}, beaconConfig, dirs, log.Root())
	if err := csn.ReopenFolder(); err != nil {
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
	from := ((beaconConfig.DenebForkEpoch * beaconConfig.SlotsPerEpoch) / snaptype.Erigon2MergeLimit) * snaptype.Erigon2MergeLimit

	salt, err := snaptype.GetIndexSalt(dirs.Snap)

	if err != nil {
		return err
	}

	return freezeblocks.DumpBlobsSidecar(ctx, blobStorage, db, from, to, salt, dirs, estimate.CompressSnapshot.Workers(), log.LvlInfo, log.Root())
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

	csn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{}, beaconConfig, dirs, log.Root())
	if err := csn.ReopenFolder(); err != nil {
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
