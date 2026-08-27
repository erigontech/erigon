// Copyright 2026 The Erigon Authors
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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/node/ethconfig"
)

// BlobFetchToStore fills gaps in the blob store from beacon API endpoints, for slots whose
// sidecars no peer serves any more. It is a one-shot offline repair: run it with the node
// stopped, then dump the affected range with a release binary.
type BlobFetchToStore struct {
	chainCfg
	outputFolder

	SlotsFile string `name:"slots-file" help:"file with one slot number per line; extra columns are ignored" type:"existingfile" required:""`
	Endpoints string `name:"endpoints" help:"comma separated beacon API base URLs, tried in order" required:""`
	Commit    bool   `name:"commit" help:"actually write to the store; without it the run only reports what it would do" default:"false"`
	Overwrite bool   `name:"overwrite" help:"also touch slots that already hold sidecars (their count row is replaced, so a partial write orphans files)" default:"false"`
	SkipRoot  bool   `name:"skip-remote-root-check" help:"do not cross-check the local canonical root against an endpoint" default:"false"`
	Timeout   uint64 `name:"timeout" help:"per-request timeout in seconds" default:"30"`
	PauseMs   uint64 `name:"pause-ms" help:"pause between slots, to stay polite to public endpoints" default:"200"`
}

type blobFetchTally struct {
	filled     int
	alreadyOk  int
	noBlobs    int
	unserved   int
	rootDiff   int
	incomplete int
	rejected   int
	wouldFill  int
}

func (t *blobFetchTally) failures() int {
	return t.unserved + t.rootDiff + t.incomplete + t.rejected
}

func (c *BlobFetchToStore) Run(ctx *Context) error {
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(c.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	endpoints := splitEndpoints(c.Endpoints)
	if len(endpoints) == 0 {
		return errors.New("no endpoints given")
	}
	slots, err := readSlotsFile(c.SlotsFile)
	if err != nil {
		return err
	}
	if len(slots) == 0 {
		return fmt.Errorf("%s contained no slots", c.SlotsFile)
	}

	dirs := datadir.New(c.Datadir)
	db, blobStorage, err := caplin1.OpenCaplinDatabase(ctx, beaconConfig, nil, dirs.CaplinIndexing, dirs.CaplinBlobs, nil, false, 0)
	if err != nil {
		return err
	}
	defer db.Close()

	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = c.Chain
	csn := freezeblocks.NewCaplinSnapshots(freezingCfg, beaconConfig, dirs, log.Root())
	if err := csn.OpenFolder(); err != nil {
		return err
	}
	snr := freezeblocks.NewBeaconSnapshotReader(csn, nil, beaconConfig)

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	src := &beaconAPISource{
		endpoints: endpoints,
		client:    &http.Client{Timeout: time.Duration(c.Timeout) * time.Second},
	}
	frozen := csn.FrozenBlobs()
	log.Info("Filling blob store gaps", "slots", len(slots), "endpoints", len(endpoints),
		"commit", c.Commit, "frozenBlobs", frozen)

	var tally blobFetchTally
	for _, slot := range slots {
		// Below the frozen frontier the sidecars already live in segments and the store is
		// expected to be empty, so writing there would be pointless and confusing.
		if slot < frozen {
			return fmt.Errorf("slot %d is below the frozen blob frontier %d", slot, frozen)
		}
		if err := c.fillSlot(ctx, tx, snr, blobStorage, src, slot, &tally); err != nil {
			return err
		}
		if c.PauseMs > 0 {
			time.Sleep(time.Duration(c.PauseMs) * time.Millisecond)
		}
	}

	log.Info("Blob store gap fill finished",
		"filled", tally.filled, "wouldFill", tally.wouldFill, "alreadyComplete", tally.alreadyOk,
		"noBlobs", tally.noBlobs, "noEndpointHadThem", tally.unserved,
		"rootMismatch", tally.rootDiff, "incompleteAnswer", tally.incomplete,
		"rejected", tally.rejected, "commit", c.Commit)

	// A run that could not fill everything must not look like a success: the caller decides
	// what to do, but only if it is told.
	if n := tally.failures(); n > 0 {
		return fmt.Errorf("%d of %d slots were not filled", n, len(slots))
	}
	return nil
}

func (c *BlobFetchToStore) fillSlot(ctx context.Context, tx kv.Tx, snr freezeblocks.BeaconSnapshotReader,
	store blob_storage.BlobStorage, src *beaconAPISource, slot uint64, tally *blobFetchTally) error {
	// The canonical root from the index, never block.HashSSZ(): a block read out of a
	// beaconblocks segment carries no execution payload, so hashing it yields a root that
	// never existed on chain and every store lookup would miss.
	blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
	if err != nil {
		return err
	}
	if blockRoot == (common.Hash{}) {
		log.Warn("Slot has no canonical root", "slot", slot)
		tally.unserved++
		return nil
	}

	block, err := snr.ReadBeaconBlockBodyBySlot(ctx, tx, slot)
	if err != nil {
		return err
	}
	if block == nil {
		log.Warn("Slot has no block", "slot", slot, "blockRoot", blockRoot)
		tally.unserved++
		return nil
	}
	commitments := block.Block.Body.GetBlobKzgCommitments()
	want := 0
	if commitments != nil {
		want = commitments.Len()
	}
	if want == 0 {
		log.Info("Slot carries no blobs, nothing to fetch", "slot", slot)
		tally.noBlobs++
		return nil
	}

	stored, err := store.KzgCommitmentsCount(ctx, blockRoot)
	if err != nil {
		return err
	}
	if int(stored) == want {
		tally.alreadyOk++
		return nil
	}
	if stored > 0 && !c.Overwrite {
		// WriteBlobSidecars replaces the count row, so writing a partial set over a
		// non-empty one would orphan the files it does not cover.
		log.Warn("Slot already holds sidecars, skipping", "slot", slot, "stored", stored, "want", want)
		tally.rejected++
		return nil
	}

	if !c.SkipRoot {
		remoteRoot, ok, err := src.headerRoot(ctx, slot)
		if err != nil {
			return err
		}
		if !ok {
			log.Warn("No endpoint served a header for the slot", "slot", slot)
			tally.unserved++
			return nil
		}
		if remoteRoot != blockRoot {
			// Everything else verifies against our own block, so this is the only check
			// that can catch our own canonical index being wrong.
			log.Error("Canonical root disagrees with the endpoint, skipping",
				"slot", slot, "local", blockRoot, "remote", remoteRoot)
			tally.rootDiff++
			return nil
		}
	}

	sidecars, err := src.sidecars(ctx, blockRoot)
	if err != nil {
		return err
	}
	if len(sidecars) == 0 {
		log.Warn("No endpoint had sidecars for the slot", "slot", slot, "blockRoot", blockRoot)
		tally.unserved++
		return nil
	}
	// All or nothing: a partial set leaves the slot short and rewrites the count row to the
	// smaller number, which is worse than leaving it alone.
	if len(sidecars) != want {
		log.Warn("Endpoint answer is incomplete, skipping", "slot", slot, "got", len(sidecars), "want", want)
		tally.incomplete++
		return nil
	}
	for i, sidecar := range sidecars {
		if common.Bytes48(*commitments.Get(i)) != sidecar.KzgCommitment {
			log.Error("Sidecar commitment does not match the block, skipping",
				"slot", slot, "index", i)
			tally.rejected++
			return nil
		}
	}

	if !c.Commit {
		log.Info("Would fill slot", "slot", slot, "blockRoot", blockRoot, "sidecars", len(sidecars))
		tally.wouldFill++
		return nil
	}

	identifiers := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](want, 40)
	for i := range want {
		identifiers.Append(&cltypes.BlobIdentifier{BlockRoot: blockRoot, Index: uint64(i)})
	}
	_, inserted, err := blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx, store, identifiers, sidecars,
		func(header *cltypes.SignedBeaconBlockHeader) error {
			if header.Header.Slot != slot {
				return fmt.Errorf("sidecar header slot %d does not match the requested slot %d", header.Header.Slot, slot)
			}
			if header.Signature != block.Signature {
				return errors.New("sidecar header signature does not match the stored block")
			}
			return nil
		})
	if err != nil {
		log.Error("Sidecars rejected", "slot", slot, "err", err)
		tally.rejected++
		return nil
	}
	// A nil error does not mean everything landed: the insert stops at the first identifier
	// the answer does not match and reports how many it stored.
	if inserted != uint64(want) {
		log.Error("Store took fewer sidecars than the block needs", "slot", slot, "stored", inserted, "want", want)
		tally.rejected++
		return nil
	}

	readBack, found, err := store.ReadBlobSidecars(ctx, slot, blockRoot)
	if err != nil {
		return err
	}
	if !found || len(readBack) != want {
		log.Error("Read-back after insert does not match", "slot", slot, "found", found, "got", len(readBack), "want", want)
		tally.rejected++
		return nil
	}
	count, err := store.KzgCommitmentsCount(ctx, blockRoot)
	if err != nil {
		return err
	}
	if int(count) != want {
		log.Error("Count row after insert does not match", "slot", slot, "count", count, "want", want)
		tally.rejected++
		return nil
	}

	log.Info("Filled slot", "slot", slot, "blockRoot", blockRoot, "sidecars", want)
	tally.filled++
	return nil
}

// beaconAPISource reads blocks and sidecars from beacon API endpoints, in the order given.
type beaconAPISource struct {
	endpoints []string
	client    *http.Client
}

// headerRoot returns the block root an endpoint reports for a slot. An endpoint that cannot
// answer is skipped; ok is false only when none of them could.
func (s *beaconAPISource) headerRoot(ctx context.Context, slot uint64) (common.Hash, bool, error) {
	for _, endpoint := range s.endpoints {
		var body struct {
			Data struct {
				Root string `json:"root"`
			} `json:"data"`
		}
		ok, err := s.get(ctx, fmt.Sprintf("%s/eth/v1/beacon/headers/%d", endpoint, slot), &body)
		if err != nil {
			log.Warn("Endpoint unreachable or erroring", "endpoint", endpoint, "slot", slot, "err", err)
			continue
		}
		if !ok || body.Data.Root == "" {
			continue
		}
		return common.HexToHash(body.Data.Root), true, nil
	}
	return common.Hash{}, false, nil
}

// sidecars returns the first non-empty sidecar set any endpoint serves for blockRoot.
func (s *beaconAPISource) sidecars(ctx context.Context, blockRoot common.Hash) ([]*cltypes.BlobSidecar, error) {
	for _, endpoint := range s.endpoints {
		var body struct {
			Data []*cltypes.BlobSidecar `json:"data"`
		}
		ok, err := s.get(ctx, fmt.Sprintf("%s/eth/v1/beacon/blob_sidecars/0x%x", endpoint, blockRoot), &body)
		if err != nil {
			log.Warn("Endpoint unreachable or erroring", "endpoint", endpoint, "blockRoot", blockRoot, "err", err)
			continue
		}
		if !ok || len(body.Data) == 0 {
			continue
		}
		return body.Data, nil
	}
	return nil, nil
}

// get reports ok=false for a 404, and an error for anything else that is not a 200, so
// "this endpoint does not have it" is never confused with "this endpoint is broken".
func (s *beaconAPISource) get(ctx context.Context, url string, out any) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("bad status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return false, fmt.Errorf("decode: %w", err)
	}
	return true, nil
}

func splitEndpoints(in string) []string {
	var out []string
	for _, e := range strings.Split(in, ",") {
		if e = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(e), "/")); e != "" {
			out = append(out, e)
		}
	}
	return out
}

// readSlotsFile reads one slot per line, ignoring blank lines, '#' comments and any extra
// columns. A line that is not a slot is an error rather than a skip: a malformed list must
// not read as a short one.
func readSlotsFile(path string) ([]uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var slots []uint64
	seen := map[uint64]struct{}{}
	scanner := bufio.NewScanner(f)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		field := strings.Fields(text)[0]
		slot, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %q is not a slot", path, line, field)
		}
		if _, dup := seen[slot]; dup {
			continue
		}
		seen[slot] = struct{}{}
		slots = append(slots, slot)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return slots, nil
}
