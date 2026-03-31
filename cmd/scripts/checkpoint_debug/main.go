package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

func main() {
	// Usage:
	//   checkpoint_debug --datadir /path/to/datadir <slot>   # read from local DB
	//   checkpoint_debug [slot|url]                          # fetch from network
	beaconConfig := clparams.BeaconConfigs[clparams.NetworkType(10200)]

	if len(os.Args) >= 3 && os.Args[1] == "--datadir" {
		datadir := os.Args[2]
		if len(os.Args) >= 4 && len(os.Args[3]) > 2 && os.Args[3][:2] == "0x" {
			// Lookup by block root
			lookupByRoot(datadir, os.Args[3], &beaconConfig)
			return
		}
		var slot uint64
		if len(os.Args) >= 4 {
			var err error
			slot, err = strconv.ParseUint(os.Args[3], 10, 64)
			fatal(err, "parsing slot")
		}
		readFromDatadir(datadir, slot, &beaconConfig)
		return
	}

	// Network fetch mode (original behavior)
	baseURL := "https://checkpoint.chiadochain.net"
	url := baseURL + "/eth/v2/debug/beacon/states/finalized"

	if len(os.Args) > 1 {
		arg := os.Args[1]
		if arg[0] >= '0' && arg[0] <= '9' {
			url = baseURL + "/eth/v2/debug/beacon/states/" + arg
		} else {
			url = arg
		}
	}

	fmt.Println("=== Checkpoint Debug Tool (network) ===")
	fmt.Println("URL:", url)
	fmt.Println()
	fetchAndDecode(url, &beaconConfig)
}

func readFromDatadir(datadir string, slot uint64, beaconConfig *clparams.BeaconChainConfig) {
	fmt.Println("=== Checkpoint Debug Tool (datadir) ===")
	fmt.Printf("Datadir: %s\n", datadir)

	dbPath := filepath.Join(datadir, "caplin", "indexing")
	fmt.Printf("DB path: %s\n\n", dbPath)

	db := mdbx.New(dbcfg.CaplinDB, log.New()).Path(dbPath).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
			return kv.ChaindataTablesCfg
		}).Readonly(true).MustOpen()
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRo(ctx)
	fatal(err, "begin read tx")
	defer tx.Rollback()

	// Print highest finalized
	highestFinalized, err := beacon_indicies.ReadHighestFinalized(tx)
	fatal(err, "reading highest finalized")
	fmt.Printf("Highest finalized slot: %d\n", highestFinalized)
	fmt.Printf("Highest finalized epoch: %d\n\n", highestFinalized/beaconConfig.SlotsPerEpoch)

	if slot == 0 {
		slot = highestFinalized
		fmt.Printf("Using highest finalized slot: %d\n\n", slot)
	}

	fmt.Printf("=== Slot %d ===\n", slot)
	epoch := slot / beaconConfig.SlotsPerEpoch
	version := beaconConfig.GetCurrentStateVersion(epoch)
	fmt.Printf("Epoch:   %d\n", epoch)
	fmt.Printf("Version: %d (%s)\n\n", version, versionName(version))

	// Read canonical block root for this slot
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
	fatal(err, "reading canonical block root")
	fmt.Printf("Canonical block root: 0x%s\n", hex.EncodeToString(canonicalRoot[:]))

	if canonicalRoot == (common.Hash{}) {
		fmt.Println("(no canonical block root found for this slot)")
		return
	}

	// Read state root
	stateRoot, err := beacon_indicies.ReadStateRootByBlockRoot(ctx, tx, canonicalRoot)
	fatal(err, "reading state root")
	fmt.Printf("State root:          0x%s\n", hex.EncodeToString(stateRoot[:]))

	// Read parent root
	parentRoot, err := beacon_indicies.ReadParentBlockRoot(ctx, tx, canonicalRoot)
	fatal(err, "reading parent root")
	fmt.Printf("Parent root:         0x%s\n", hex.EncodeToString(parentRoot[:]))

	// Read execution block number
	execBlockNum, err := beacon_indicies.ReadExecutionBlockNumber(tx, canonicalRoot)
	fatal(err, "reading execution block number")
	if execBlockNum != nil {
		fmt.Printf("Exec block number:   %d\n", *execBlockNum)
	}

	// Read execution block hash
	execBlockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, canonicalRoot)
	fatal(err, "reading execution block hash")
	if execBlockHash != (common.Hash{}) {
		fmt.Printf("Exec block hash:     0x%s\n", hex.EncodeToString(execBlockHash[:]))
	}

	// Also look up the slot by block root (cross-check)
	slotByRoot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, canonicalRoot)
	fatal(err, "reading slot by block root")
	if slotByRoot != nil {
		fmt.Printf("Slot (by root):      %d\n", *slotByRoot)
	}

	fmt.Println()

	// Try a few surrounding slots
	fmt.Println("=== Nearby Slots ===")
	start := slot - 3
	if slot < 3 {
		start = 0
	}
	for s := start; s <= slot+3; s++ {
		root, err := beacon_indicies.ReadCanonicalBlockRoot(tx, s)
		if err != nil {
			continue
		}
		marker := "  "
		if s == slot {
			marker = "> "
		}
		if root == (common.Hash{}) {
			fmt.Printf("%sslot %d: (empty)\n", marker, s)
		} else {
			fmt.Printf("%sslot %d: 0x%s\n", marker, s, hex.EncodeToString(root[:]))
		}
	}
}

func lookupByRoot(datadir string, rootHex string, beaconConfig *clparams.BeaconChainConfig) {
	fmt.Println("=== Checkpoint Debug Tool (lookup by root) ===")
	fmt.Printf("Datadir: %s\n", datadir)
	fmt.Printf("Root:    %s\n\n", rootHex)

	dbPath := filepath.Join(datadir, "caplin", "indexing")
	db := mdbx.New(dbcfg.CaplinDB, log.New()).Path(dbPath).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
			return kv.ChaindataTablesCfg
		}).Readonly(true).MustOpen()
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRo(ctx)
	fatal(err, "begin read tx")
	defer tx.Rollback()

	rootBytes, err := hex.DecodeString(rootHex[2:])
	fatal(err, "decoding root hex")
	var root common.Hash
	copy(root[:], rootBytes)

	// Lookup slot by block root
	slotByRoot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
	fatal(err, "reading slot by block root")
	if slotByRoot != nil {
		fmt.Printf("Found! Slot: %d\n", *slotByRoot)
	} else {
		fmt.Println("Block root NOT FOUND in BlockRootToSlot table")
	}

	// Lookup state root
	stateRoot, err := beacon_indicies.ReadStateRootByBlockRoot(ctx, tx, root)
	fatal(err, "reading state root")
	if stateRoot != (common.Hash{}) {
		fmt.Printf("State root:  0x%s\n", hex.EncodeToString(stateRoot[:]))
	} else {
		fmt.Println("State root NOT FOUND")
	}

	// Lookup parent root
	parentRoot, err := beacon_indicies.ReadParentBlockRoot(ctx, tx, root)
	fatal(err, "reading parent root")
	if parentRoot != (common.Hash{}) {
		fmt.Printf("Parent root: 0x%s\n", hex.EncodeToString(parentRoot[:]))
	} else {
		fmt.Println("Parent root NOT FOUND")
	}
}

func fetchAndDecode(url string, beaconConfig *clparams.BeaconChainConfig) {
	fmt.Println("Fetching beacon state...")
	client := &http.Client{Timeout: 120 * time.Second}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	fatal(err, "creating request")
	req.Header.Set("Accept", "application/octet-stream")

	resp, err := client.Do(req)
	fatal(err, "fetching state")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "bad status code: %d\n", resp.StatusCode)
		os.Exit(1)
	}

	marshaled, err := io.ReadAll(resp.Body)
	fatal(err, "reading response")
	fmt.Printf("State bytes: %d\n\n", len(marshaled))

	slot, err := utils.ExtractSlotFromSerializedBeaconState(marshaled)
	fatal(err, "extracting slot")

	epoch := slot / beaconConfig.SlotsPerEpoch
	version := beaconConfig.GetCurrentStateVersion(epoch)
	fmt.Printf("Slot:    %d\n", slot)
	fmt.Printf("Epoch:   %d\n", epoch)
	fmt.Printf("Version: %d (%s)\n\n", version, versionName(version))

	beaconState := state.New(beaconConfig)
	err = beaconState.DecodeSSZ(marshaled, int(version))
	fatal(err, "decoding SSZ")

	hdr := beaconState.LatestBlockHeader()
	fmt.Println("=== Latest Block Header (from state) ===")
	fmt.Printf("Slot:          %d\n", hdr.Slot)
	fmt.Printf("ProposerIndex: %d\n", hdr.ProposerIndex)
	fmt.Printf("ParentRoot:    0x%s\n", hex.EncodeToString(hdr.ParentRoot[:]))
	fmt.Printf("BodyRoot:      0x%s\n", hex.EncodeToString(hdr.BodyRoot[:]))
	fmt.Println()

	stateRoot, err := beaconState.HashSSZ()
	fatal(err, "hashing state SSZ")

	blockRoot, err := (&cltypes.BeaconBlockHeader{
		Slot:          hdr.Slot,
		ProposerIndex: hdr.ProposerIndex,
		BodyRoot:      hdr.BodyRoot,
		ParentRoot:    hdr.ParentRoot,
		Root:          stateRoot,
	}).HashSSZ()
	fatal(err, "computing block root")

	fmt.Println("=== Computed Hashes ===")
	fmt.Printf("State Root:  0x%s\n", hex.EncodeToString(stateRoot[:]))
	fmt.Printf("Block Root:  0x%s\n", hex.EncodeToString(blockRoot[:]))
	fmt.Printf("Parent Root: 0x%s\n", hex.EncodeToString(hdr.ParentRoot[:]))
}

func versionName(v clparams.StateVersion) string {
	switch v {
	case clparams.Phase0Version:
		return "Phase0"
	case clparams.AltairVersion:
		return "Altair"
	case clparams.BellatrixVersion:
		return "Bellatrix"
	case clparams.CapellaVersion:
		return "Capella"
	case clparams.DenebVersion:
		return "Deneb"
	case clparams.ElectraVersion:
		return "Electra"
	case clparams.FuluVersion:
		return "Fulu"
	default:
		return "Unknown"
	}
}

func fatal(err error, context string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL [%s]: %v\n", context, err)
		os.Exit(1)
	}
}
