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
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/afero"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/node/ethconfig"
)

// fillWorkers is the number of slots checked and filled at once. Each holds at most one block's
// sidecars, a few MB.
const fillWorkers = 8

// fillRequestTimeout bounds one endpoint request. A full response, 21 blobs of hex JSON, is about
// 6 MB. A variable so tests can shorten it.
var fillRequestTimeout = time.Minute

var fillRetryWait = time.Second

type BlobArchiveStoreFill struct {
	chainCfg
	outputFolder
	Endpoints []string
	FromSlot  uint64
	ToSlot    uint64
	DryRun    bool

	windows []endpointWindow
}

type endpointWindow struct {
	once sync.Once
	from atomic.Uint64
	err  error
}

var errNoSidecars = errors.New("the endpoint has no sidecars for the block")

// fillReport counts slots by outcome; only the slots that could not be filled are listed.
type fillReport struct {
	withBlobs, valid, filled atomic.Uint64

	mu               sync.Mutex
	failed, unserved []uint64
}

func (r *fillReport) add(slots *[]uint64, slot uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	*slots = append(*slots, slot)
}

func (b *BlobArchiveStoreFill) Run(ctx context.Context) error {
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(b.Chain)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	if b.FromSlot > b.ToSlot {
		return fmt.Errorf("--from-slot %d is after --to-slot %d", b.FromSlot, b.ToSlot)
	}
	if len(b.Endpoints) == 0 && !b.DryRun {
		return errors.New("--endpoint is required unless --dry-run is set")
	}
	for i, endpoint := range b.Endpoints {
		if !validEndpoint(endpoint) {
			return fmt.Errorf("--endpoint %d is not an http or https URL with a host", i+1)
		}
	}

	// Open, not New: New creates the datadir's directories, and a dry run writes nothing.
	dirs := datadir.Open(b.Datadir)
	unlock, err := dirs.TryFlock()
	if err != nil {
		return fmt.Errorf("stop the node that uses the datadir first: %w", err)
	}
	defer unlock()

	// Both databases are closed before the lock is released: closing syncs the blob store's rows.
	// The index is only read.
	db, err := openCaplinDB(ctx, dirs.CaplinIndexing, true)
	if err != nil {
		return fmt.Errorf("open the beacon index: %w", err)
	}
	defer db.Close()
	blobStore, closeBlobs, err := b.openBlobStore(ctx, dirs)
	if err != nil {
		return err
	}
	defer closeBlobs()
	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = b.Chain
	csn := freezeblocks.NewCaplinSnapshots(freezingCfg, beaconConfig, dirs, log.Root())
	if err := csn.OpenFolder(); err != nil {
		return err
	}
	defer csn.Close()
	return b.fill(ctx, db, freezeblocks.NewBeaconSnapshotReader(csn, nil, beaconConfig), blobStore, csn.FrozenBlobs())
}

func openCaplinDB(ctx context.Context, path string, readonly bool) (kv.RwDB, error) {
	return mdbx.New(dbcfg.CaplinDB, log.New()).Path(path).Readonly(readonly).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.ChaindataTablesCfg }).Open(ctx)
}

// openBlobStore opens the blob store for writing, or read-only for a dry run, where a missing
// store reads as empty.
func (b *BlobArchiveStoreFill) openBlobStore(ctx context.Context, dirs datadir.Dirs) (blob_storage.BlobStorage, func(), error) {
	path := filepath.Join(dirs.CaplinBlobs, "chaindata")
	if b.DryRun {
		// A directory without its data file is a store nothing was ever written to.
		if _, err := os.Stat(filepath.Join(path, "mdbx.dat")); errors.Is(err, os.ErrNotExist) {
			return emptyBlobStore{}, func() {}, nil
		}
	} else if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, nil, err
	}
	db, err := openCaplinDB(ctx, path, b.DryRun)
	if err != nil {
		return nil, nil, fmt.Errorf("open the blob store: %w", err)
	}
	return blob_storage.NewBlobStore(db, afero.NewBasePathFs(afero.NewOsFs(), dirs.CaplinBlobs)), db.Close, nil
}

// emptyBlobStore stands in for a blob store that does not exist yet; a dry run only reads it.
type emptyBlobStore struct{ blob_storage.BlobStorage }

func (emptyBlobStore) ReadBlobSidecars(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
	return nil, false, nil
}

func (b *BlobArchiveStoreFill) fill(ctx context.Context, db kv.RoDB, blocks freezeblocks.BeaconSnapshotReader, store blob_storage.BlobStorage, frozenBlobs uint64) error {
	// The API serves slots below the frontier from blob segments, so a store write there would
	// never be read.
	if b.FromSlot < frozenBlobs {
		return fmt.Errorf("slots below %d are served from blob segments, which this command does not repair; use --from-slot=%d or later", frozenBlobs, frozenBlobs)
	}
	if err := db.View(ctx, func(tx kv.Tx) error { return checkRangeIndexed(ctx, tx, b.FromSlot, b.ToSlot) }); err != nil {
		return err
	}
	recorder := &writeErrRecorder{BlobStorage: store}
	report := &fillReport{}
	b.windows = make([]endpointWindow, len(b.Endpoints))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(fillWorkers)
	go logFillProgress(gctx, b.FromSlot, b.ToSlot, report)
	for slot := b.FromSlot; gctx.Err() == nil; slot++ {
		g.Go(func() error { return b.fillSlot(gctx, db, blocks, recorder, slot, report) })
		if slot == b.ToSlot {
			break
		}
	}
	err := g.Wait()
	slices.Sort(report.failed)
	slices.Sort(report.unserved)
	log.Info("Blob store fill finished", "fromSlot", b.FromSlot, "toSlot", b.ToSlot, "slotsWithBlobs", report.withBlobs.Load(),
		"valid", report.valid.Load(), "missing", report.withBlobs.Load()-report.valid.Load(), "filled", report.filled.Load(), "failed", len(report.failed),
		"unserved", len(report.unserved), "dryRun", b.DryRun)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(report.failed) > 0 {
		return fmt.Errorf("%d slots could not be checked or filled, the first: %v", len(report.failed), report.failed[:min(len(report.failed), 20)])
	}
	if len(report.unserved) > 0 && !b.DryRun {
		return fmt.Errorf("%d slots with blobs are not served by any endpoint, the first: %v", len(report.unserved), report.unserved[:min(len(report.unserved), 20)])
	}
	return nil
}

// checkRangeIndexed fails when the local index cannot vouch for every slot of [from, to]. A slot
// with no canonical root counts as empty, so the run must not reach one that is only unindexed:
// past the local head, or in a stretch the backfill never wrote. Blocks are linked by their parent
// roots, from the first block of the range back below it and from the last one to the next block
// after the range, so an empty slot is one the chain itself skipped.
func checkRangeIndexed(ctx context.Context, tx kv.Tx, from, to uint64) error {
	head, _, err := beacon_indicies.ReadCanonicalHead(tx)
	if err != nil {
		return err
	}
	if to > head {
		return fmt.Errorf("--to-slot %d is past the local chain, which ends at slot %d", to, head)
	}
	var prevSlot uint64
	var prevRoot common.Hash
	linked := func(slot uint64, root common.Hash) error {
		parent, err := beacon_indicies.ReadParentBlockRoot(tx, root)
		if err != nil {
			return err
		}
		if prevRoot != (common.Hash{}) {
			if parent == prevRoot {
				return nil
			}
			if slot == prevSlot+1 {
				return fmt.Errorf("the local index is inconsistent: the block at slot %d does not follow the one at slot %d", slot, prevSlot)
			}
			return fmt.Errorf("slots %d to %d are not in the local index: block at slot %d does not follow the one at slot %d", prevSlot+1, slot-1, slot, prevSlot)
		}
		parentSlot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, parent)
		if err != nil {
			return err
		}
		if slot > from && (parentSlot == nil || *parentSlot >= from) {
			return fmt.Errorf("slots %d to %d are not in the local index: the parent of the block at slot %d is not indexed below them", from, slot-1, slot)
		}
		return nil
	}
	// The scan ends at the first block at or after to: a range that ends on a block needs nothing after it, and
	// empty slots at its end are empty only if the next block follows the last one in the range.
	for slot := from; slot <= head; slot++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		root, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil {
			return err
		}
		if root == (common.Hash{}) {
			continue
		}
		if err := linked(slot, root); err != nil {
			return err
		}
		if slot >= to {
			break
		}
		prevSlot, prevRoot = slot, root
	}
	return nil
}

// fillSlot leaves a slot alone when the store serves all of its sidecars correctly, and otherwise
// fetches them from the first endpoint that has them. Only a store that cannot write, or a local
// read that fails, stops the run; a slot no endpoint can fill is reported at the end.
func (b *BlobArchiveStoreFill) fillSlot(ctx context.Context, db kv.RoDB, blocks freezeblocks.BeaconSnapshotReader, store *writeErrRecorder, slot uint64, report *fillReport) error {
	root, block, err := readSlot(ctx, db, blocks, slot)
	if err != nil {
		return fmt.Errorf("read slot %d: %w", slot, err)
	}
	// An empty slot has neither; with only one of them the slot cannot be checked.
	if (root == common.Hash{}) != (block == nil) {
		log.Warn("Slot has a block without a canonical root, or a root without a block, cannot be checked", "slot", slot, "blockRoot", root)
		report.add(&report.failed, slot)
		return nil
	}
	blobs, unresolved, err := expectedBlobs(ctx, db, blocks, root, block)
	if err != nil {
		return fmt.Errorf("read the slots after %d: %w", slot, err)
	}
	if blobs == 0 {
		return nil
	}
	report.withBlobs.Add(1)
	if storedBlobsValid(ctx, store, slot, root, block) {
		report.valid.Add(1)
		return nil
	}
	if b.DryRun {
		served := !unresolved && len(b.Endpoints) == 0
		inWindow := false
		for i := range b.Endpoints {
			if err := b.findWindow(ctx, db, blocks, i, b.FromSlot); err != nil {
				return fmt.Errorf("find the window of endpoint %d: %w", i+1, err)
			}
			if slot >= b.windows[i].from.Load() {
				inWindow = true
				served = served || !unresolved
			}
		}
		log.Info("Slot needs its sidecars", "slot", slot, "blockRoot", root, "blobs", blobs, "served", served)
		if !served && (!unresolved || !inWindow) {
			report.add(&report.unserved, slot)
			return nil
		}
		return nil
	}
	var lastErr error
	unserved, attempted := true, false
	for i, endpoint := range b.Endpoints {
		if slot < b.windows[i].from.Load() {
			continue
		}
		attempted = true
		lastErr = fetchAndStoreBlobs(ctx, store, endpoint, root, block)
		if err := store.err(); err != nil {
			return fmt.Errorf("blob store write failed, stopping: %w", err)
		}
		if lastErr == nil && !storedBlobsValid(ctx, store, slot, root, block) {
			lastErr = errors.New("the stored sidecars do not read back")
		}
		if lastErr == nil {
			report.filled.Add(1)
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		if !notServed(lastErr) {
			unserved = false
		} else if !unresolved {
			if err := b.findWindow(ctx, db, blocks, i, slot+1); err != nil {
				return fmt.Errorf("find the window of endpoint %d: %w", i+1, err)
			}
		}
		log.Debug("Endpoint could not fill slot", "slot", slot, "endpoint", redactEndpoint(endpoint), "err", lastErr)
	}
	if unserved {
		if !unresolved || !attempted {
			log.Debug("No endpoint serves slot", "slot", slot, "blockRoot", root)
			report.add(&report.unserved, slot)
		}
		return nil
	}
	log.Warn("Could not fill slot", "slot", slot, "blockRoot", root, "err", lastErr)
	report.add(&report.failed, slot)
	return nil
}

func readSlot(ctx context.Context, db kv.RoDB, blocks freezeblocks.BeaconSnapshotReader, slot uint64) (root common.Hash, block *cltypes.SignedBeaconBlock, err error) {
	err = db.View(ctx, func(tx kv.Tx) error {
		if root, err = beacon_indicies.ReadCanonicalBlockRoot(tx, slot); err != nil {
			return err
		}
		block, err = blocks.ReadBeaconBlockBodyBySlot(ctx, tx, slot)
		return err
	})
	return root, block, err
}

func blobCount(block *cltypes.SignedBeaconBlock) int {
	if block == nil || block.Version() < clparams.DenebVersion {
		return 0
	}
	if commitments := block.Block.Body.GetBlobKzgCommitments(); commitments != nil {
		return commitments.Len()
	}
	return 0
}

func expectedBlobs(ctx context.Context, db kv.RoDB, blocks freezeblocks.BeaconSnapshotReader, root common.Hash, block *cltypes.SignedBeaconBlock) (n int, unresolved bool, err error) {
	err = db.View(ctx, func(tx kv.Tx) error {
		n, unresolved, err = expectedBlobsInTx(ctx, tx, blocks, root, block)
		return err
	})
	return n, unresolved, err
}

func expectedBlobsInTx(ctx context.Context, tx kv.Tx, blocks freezeblocks.BeaconSnapshotReader, root common.Hash, block *cltypes.SignedBeaconBlock) (int, bool, error) {
	n := blobCount(block)
	if n == 0 || block.Version() < clparams.GloasVersion {
		return n, false, nil
	}
	var childSlot uint64
	var childRoot common.Hash
	if err := beacon_indicies.RangeBlockRoots(tx, block.Block.Slot+1, math.MaxUint64, func(slot uint64, root common.Hash) bool {
		childSlot, childRoot = slot, root
		return false
	}); err != nil {
		return 0, false, err
	}
	if childRoot == (common.Hash{}) {
		return n, true, nil
	}
	child, err := blocks.ReadBeaconBlockBodyBySlot(ctx, tx, childSlot)
	if err != nil {
		return 0, false, err
	}
	if child == nil || child.Block.ParentRoot != root {
		return n, true, nil
	}
	if forkchoice.ParentPayloadStatusFromBids(block, child.Block) == cltypes.PayloadStatusFull {
		return n, false, nil
	}
	return 0, false, nil
}

func notServed(err error) bool {
	status, ok := errors.AsType[blobStatusError](err)
	return errors.Is(err, errNoSidecars) || ok && status == http.StatusBadRequest
}

func (b *BlobArchiveStoreFill) findWindow(ctx context.Context, db kv.RoDB, blocks freezeblocks.BeaconSnapshotReader, i int, lo uint64) error {
	w := &b.windows[i]
	w.once.Do(func() {
		scanned := map[uint64]uint64{}
		answers := map[common.Hash]bool{}
		n := sort.Search(int(b.ToSlot+1-lo), func(n int) bool {
			if w.err != nil {
				return true
			}
			root, block, unresolved, err := nextSlotWithBlobs(ctx, db, blocks, lo+uint64(n), b.ToSlot, scanned)
			if err != nil {
				w.err = err
				return true
			}
			if block == nil || unresolved {
				return true
			}
			if served, ok := answers[root]; ok {
				return served
			}
			sidecars, err := retrieveSidecars(ctx, b.Endpoints[i], root, block)
			if err == nil && len(sidecars) == 0 {
				err = errNoSidecars
			}
			answers[root] = !notServed(err)
			return answers[root]
		})
		if w.err != nil {
			return
		}
		_, block, _, err := nextSlotWithBlobs(ctx, db, blocks, lo+uint64(n), b.ToSlot, scanned)
		if err != nil {
			w.err = err
			return
		}
		if block == nil {
			w.from.Store(b.ToSlot + 1)
			log.Info("Endpoint serves no blobs in the requested range", "endpoint", redactEndpoint(b.Endpoints[i]), "fromSlot", lo, "toSlot", b.ToSlot)
			return
		}
		w.from.Store(block.Block.Slot)
		log.Info("First slot the endpoint serves", "endpoint", redactEndpoint(b.Endpoints[i]), "slot", w.from.Load())
	})
	return w.err
}

func nextSlotWithBlobs(ctx context.Context, db kv.RoDB, blocks freezeblocks.BeaconSnapshotReader, from, to uint64, scanned map[uint64]uint64) (common.Hash, *cltypes.SignedBeaconBlock, bool, error) {
	for slot := from; slot <= to; slot++ {
		for start, end := range scanned {
			if start <= slot && slot < end {
				slot = end
				break
			}
		}
		root, block, err := readSlot(ctx, db, blocks, slot)
		n := 0
		unresolved := false
		if err == nil {
			n, unresolved, err = expectedBlobs(ctx, db, blocks, root, block)
		}
		if err != nil || n > 0 {
			scanned[from] = slot
			return root, block, unresolved, err
		}
	}
	scanned[from] = to
	return common.Hash{}, nil, false, nil
}

func fetchAndStoreBlobs(ctx context.Context, store blob_storage.BlobStorage, endpoint string, root common.Hash, block *cltypes.SignedBeaconBlock) error {
	commitments := block.Block.Body.GetBlobKzgCommitments()
	sidecars, err := retrieveSidecars(ctx, endpoint, root, block)
	if err != nil {
		return err
	}
	if len(sidecars) == 0 {
		return errNoSidecars
	}
	slices.SortFunc(sidecars, func(a, b *cltypes.BlobSidecar) int { return cmp.Compare(a.Index, b.Index) })
	// Identifiers carry the canonical root: the block read from storage keeps no transactions,
	// so its own hash is not that root.
	ids := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](commitments.Len(), 40)
	for i := range commitments.Len() {
		ids.Append(&cltypes.BlobIdentifier{BlockRoot: root, Index: uint64(i)})
	}
	return storeRemoteBlobs(ctx, store, ids, sidecars, block)
}

// storedBlobsValid reports whether the store serves one valid sidecar per commitment: each carries
// its commitment, a header of this block with its signature, a valid KZG proof and, before Gloas, a
// valid inclusion proof. A
// read error counts as invalid, so the slot is rewritten; a store that cannot write then stops the
// run.
func storedBlobsValid(ctx context.Context, store blob_storage.BlobStorage, slot uint64, root common.Hash, block *cltypes.SignedBeaconBlock) bool {
	commitments := block.Block.Body.GetBlobKzgCommitments()
	sidecars, found, err := store.ReadBlobSidecars(ctx, slot, root)
	if err != nil || !found || len(sidecars) != commitments.Len() {
		return false
	}
	for i, sidecar := range sidecars {
		if sidecar.Index != uint64(i) || common.Bytes48(*commitments.Get(i)) != sidecar.KzgCommitment {
			return false
		}
	}
	return blob_storage.VerifyBlobSidecars(sidecars, block.Version(), func(header *cltypes.SignedBeaconBlockHeader) error {
		headerRoot, err := header.Header.HashSSZ()
		if err != nil || headerRoot != root || header.Signature != block.Signature {
			return errors.New("the sidecar header is not the block's")
		}
		return nil
	}) == nil
}

// writeErrRecorder keeps the first failed store write. The insert path reports it like a bad
// response, but a store that cannot write has to stop the run rather than fail every slot
// against every endpoint.
type writeErrRecorder struct {
	blob_storage.BlobStorage
	mu       sync.Mutex
	writeErr error
}

func (w *writeErrRecorder) WriteBlobSidecars(ctx context.Context, root common.Hash, sidecars []*cltypes.BlobSidecar) error {
	err := w.BlobStorage.WriteBlobSidecars(ctx, root, sidecars)
	if err != nil && ctx.Err() == nil {
		w.mu.Lock()
		if w.writeErr == nil {
			w.writeErr = err
		}
		w.mu.Unlock()
	}
	return err
}

func (w *writeErrRecorder) err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeErr
}

func logFillProgress(ctx context.Context, from, to uint64, report *fillReport) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report.mu.Lock()
			failed, unserved := len(report.failed), len(report.unserved)
			report.mu.Unlock()
			log.Info("Blob store fill progress", "fromSlot", from, "toSlot", to, "slotsWithBlobs", report.withBlobs.Load(),
				"valid", report.valid.Load(), "filled", report.filled.Load(), "failed", failed, "unserved", unserved)
		}
	}
}
