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
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/node/ethconfig"
)

// fillWorkers is the number of slots checked and filled at once. Each holds at most one block's
// sidecars, a few MB.
const fillWorkers = 8

type BlobArchiveStoreFill struct {
	chainCfg
	outputFolder
	Endpoints []string
	FromSlot  uint64
	ToSlot    uint64
	DryRun    bool
}

// fillReport counts slots by outcome; only the slots that could not be filled are listed.
type fillReport struct {
	withBlobs, valid, missing, filled atomic.Uint64

	mu     sync.Mutex
	failed []uint64
}

func (r *fillReport) fail(slot uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed = append(r.failed, slot)
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

	dirs := datadir.New(b.Datadir)
	unlock, err := dirs.TryFlock()
	if err != nil {
		return fmt.Errorf("stop the node that uses the datadir first: %w", err)
	}
	defer unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	db, blobStore, err := caplin1.OpenCaplinDatabase(ctx, dirs.CaplinIndexing, dirs.CaplinBlobs, false)
	if err != nil {
		return err
	}
	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = b.Chain
	csn := freezeblocks.NewCaplinSnapshots(freezingCfg, beaconConfig, dirs, log.Root())
	if err := csn.OpenFolder(); err != nil {
		return err
	}
	defer csn.Close()
	// The API serves slots below the frontier from blob segments, so a store write there would
	// never be read.
	if frozen := csn.FrozenBlobs(); b.FromSlot < frozen {
		return fmt.Errorf("slots below %d are served from blob segments, which this command does not repair; use --from-slot=%d or later", frozen, frozen)
	}
	return b.fill(ctx, db, freezeblocks.NewBeaconSnapshotReader(csn, nil, beaconConfig), blobStore)
}

func (b *BlobArchiveStoreFill) fill(ctx context.Context, db kv.RoDB, blocks freezeblocks.BeaconSnapshotReader, store blob_storage.BlobStorage) error {
	recorder := &writeErrRecorder{BlobStorage: store}
	report := &fillReport{}
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
	log.Info("Blob store fill finished", "fromSlot", b.FromSlot, "toSlot", b.ToSlot, "slotsWithBlobs", report.withBlobs.Load(),
		"valid", report.valid.Load(), "missing", report.missing.Load(), "filled", report.filled.Load(), "failed", len(report.failed),
		"dryRun", b.DryRun)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(report.failed) > 0 {
		return fmt.Errorf("%d slots could not be filled from any endpoint: %s", len(report.failed), formatSlots(report.failed))
	}
	return nil
}

// fillSlot leaves a slot alone when the store serves all of its sidecars correctly, and otherwise
// fetches them from the first endpoint that has them. Only a store that cannot write, or a local
// read that fails, stops the run; a slot no endpoint can fill is reported at the end.
func (b *BlobArchiveStoreFill) fillSlot(ctx context.Context, db kv.RoDB, blocks freezeblocks.BeaconSnapshotReader, store *writeErrRecorder, slot uint64, report *fillReport) error {
	var root common.Hash
	var block *cltypes.SignedBeaconBlock
	if err := db.View(ctx, func(tx kv.Tx) error {
		var err error
		if root, err = beacon_indicies.ReadCanonicalBlockRoot(tx, slot); err != nil || root == (common.Hash{}) {
			return err
		}
		block, err = blocks.ReadBeaconBlockBodyBySlot(ctx, tx, slot)
		return err
	}); err != nil {
		return fmt.Errorf("read slot %d: %w", slot, err)
	}
	if block == nil || block.Version() < clparams.DenebVersion {
		return nil
	}
	commitments := block.Block.Body.GetBlobKzgCommitments()
	if commitments == nil || commitments.Len() == 0 {
		return nil
	}
	report.withBlobs.Add(1)
	if storedBlobsValid(ctx, store, slot, root, commitments) {
		report.valid.Add(1)
		return nil
	}
	report.missing.Add(1)
	if b.DryRun {
		log.Info("Slot needs its sidecars", "slot", slot, "blockRoot", root, "blobs", commitments.Len())
		return nil
	}
	var lastErr error
	for _, endpoint := range b.Endpoints {
		lastErr = fetchAndStoreBlobs(ctx, store, endpoint, root, block)
		if err := store.err(); err != nil {
			return fmt.Errorf("blob store write failed, stopping: %w", err)
		}
		if lastErr == nil && !storedBlobsValid(ctx, store, slot, root, commitments) {
			lastErr = errors.New("the stored sidecars do not read back")
		}
		if lastErr == nil {
			report.filled.Add(1)
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		log.Debug("Endpoint could not fill slot", "slot", slot, "endpoint", endpoint, "err", lastErr)
	}
	log.Warn("Could not fill slot", "slot", slot, "blockRoot", root, "err", lastErr)
	report.fail(slot)
	return nil
}

func fetchAndStoreBlobs(ctx context.Context, store blob_storage.BlobStorage, endpoint string, root common.Hash, block *cltypes.SignedBeaconBlock) error {
	base, err := url.JoinPath(endpoint, "eth/v1/beacon/blob_sidecars")
	if err != nil {
		return err
	}
	sidecars, err := retrieveBlobsFromRemoteEndpoint(ctx, base, root)
	if err != nil {
		return err
	}
	if len(sidecars) == 0 {
		return errors.New("the endpoint has no sidecars for the block")
	}
	if slices.Contains(sidecars, nil) {
		return errors.New("blob response contains a nil sidecar")
	}
	slices.SortFunc(sidecars, func(a, b *cltypes.BlobSidecar) int { return cmp.Compare(a.Index, b.Index) })
	// Identifiers carry the canonical root: the block read from storage keeps no transactions,
	// so its own hash is not that root.
	commitments := block.Block.Body.GetBlobKzgCommitments()
	ids := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](commitments.Len(), 40)
	for i := range commitments.Len() {
		ids.Append(&cltypes.BlobIdentifier{BlockRoot: root, Index: uint64(i)})
	}
	return storeRemoteBlobs(ctx, store, ids, sidecars, block)
}

// storedBlobsValid reports whether the store serves one sidecar per commitment, each carrying its
// commitment and a valid KZG proof for its blob. A read error counts as invalid, so the slot is
// rewritten; a store that cannot write then stops the run.
func storedBlobsValid(ctx context.Context, store blob_storage.BlobStorage, slot uint64, root common.Hash, commitments *solid.ListSSZ[*cltypes.KZGCommitment]) bool {
	sidecars, found, err := store.ReadBlobSidecars(ctx, slot, root)
	if err != nil || !found || len(sidecars) != commitments.Len() {
		return false
	}
	blobs := make([]*goethkzg.Blob, len(sidecars))
	kzgCommitments := make([]goethkzg.KZGCommitment, len(sidecars))
	proofs := make([]goethkzg.KZGProof, len(sidecars))
	for i, sidecar := range sidecars {
		if sidecar.Index != uint64(i) || common.Bytes48(*commitments.Get(i)) != sidecar.KzgCommitment {
			return false
		}
		blobs[i] = (*goethkzg.Blob)(&sidecar.Blob)
		kzgCommitments[i] = goethkzg.KZGCommitment(sidecar.KzgCommitment)
		proofs[i] = goethkzg.KZGProof(sidecar.KzgProof)
	}
	return kzg.Ctx().VerifyBlobKZGProofBatch(blobs, kzgCommitments, proofs) == nil
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
			failed := len(report.failed)
			report.mu.Unlock()
			log.Info("Blob store fill progress", "fromSlot", from, "toSlot", to, "slotsWithBlobs", report.withBlobs.Load(),
				"valid", report.valid.Load(), "filled", report.filled.Load(), "failed", failed)
		}
	}
}

// formatSlots lists up to 20 slots, which is enough to start a re-run from.
func formatSlots(slots []uint64) string {
	shown := slots[:min(len(slots), 20)]
	parts := make([]string, 0, len(shown)+1)
	for _, slot := range shown {
		parts = append(parts, fmt.Sprint(slot))
	}
	if len(slots) > len(shown) {
		parts = append(parts, "...")
	}
	return strings.Join(parts, ", ")
}
