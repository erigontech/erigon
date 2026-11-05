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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/execution/chain/networkname"
)

const fillTestSlot = uint64(9_000_000) // past the mainnet Deneb fork

type fillFixture struct {
	t        *testing.T
	db       kv.RwDB
	blocks   *storeCheckReader
	store    blob_storage.BlobStorage
	sidecars map[common.Hash][]*cltypes.BlobSidecar
}

func newFillFixture(t *testing.T) *fillFixture {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB) // the index and the blob store use different tables
	return &fillFixture{
		t:        t,
		db:       db,
		blocks:   &storeCheckReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{}},
		store:    blob_storage.NewBlobStore(db, afero.NewMemMapFs()),
		sidecars: map[common.Hash][]*cltypes.BlobSidecar{},
	}
}

// addBlock makes slot hold a canonical Deneb block with valid sidecars for blobs blobs, which the
// endpoint serves. It returns the block root.
func (f *fillFixture) addBlock(slot uint64, blobs int) common.Hash {
	t := f.t
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = slot
	data := make([]goethkzg.Blob, blobs)
	commitments := make([]goethkzg.KZGCommitment, blobs)
	for i := range data {
		data[i][0], data[i][1] = byte(slot), byte(i+1)
		var err error
		commitments[i], err = kzg.Ctx().BlobToKZGCommitment(&data[i], 0)
		require.NoError(t, err)
		block.Block.Body.BlobKzgCommitments.Append((*cltypes.KZGCommitment)(&commitments[i]))
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	for i := range data {
		proof, err := kzg.Ctx().ComputeBlobKZGProof(&data[i], commitments[i], 0)
		require.NoError(t, err)
		branch, err := block.Block.Body.KzgCommitmentMerkleProof(i)
		require.NoError(t, err)
		inclusion := solid.NewHashVector(cltypes.CommitmentBranchSize)
		for j := range branch {
			inclusion.Set(j, common.Hash(branch[j]))
		}
		f.sidecars[root] = append(f.sidecars[root], cltypes.NewBlobSidecar(uint64(i), (*cltypes.Blob)(&data[i]),
			common.Bytes48(commitments[i]), common.Bytes48(proof), block.SignedBeaconBlockHeader(), inclusion))
	}
	require.NoError(t, f.db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(tx, slot, root)
	}))
	f.blocks.blocks[slot] = block
	return root
}

func (f *fillFixture) storeValid(root common.Hash) {
	require.NoError(f.t, f.store.WriteBlobSidecars(f.t.Context(), root, f.sidecars[root]))
}

func (f *fillFixture) storedValid(slot uint64, root common.Hash) bool {
	return storedBlobsValid(f.t.Context(), f.store, slot, root, f.blocks.blocks[slot].Block.Body.BlobKzgCommitments)
}

// endpoint serves the fixture's sidecars, or 404s when missing is set, and counts requests per root.
func (f *fillFixture) endpoint(missing bool) (string, *sync.Map) {
	requests := &sync.Map{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		counter, _ := requests.LoadOrStore(id, &atomic.Int32{})
		counter.(*atomic.Int32).Add(1)
		sidecars, ok := f.sidecars[common.HexToHash(id)]
		if missing || !ok || !strings.Contains(r.URL.Path, "/eth/v1/beacon/blob_sidecars/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewEncoder(w).Encode(map[string]any{"data": sidecars}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))
	f.t.Cleanup(srv.Close)
	return srv.URL, requests
}

func requestsFor(requests *sync.Map, root common.Hash) int32 {
	counter, ok := requests.Load(fmt.Sprintf("0x%x", root))
	if !ok {
		return 0
	}
	return counter.(*atomic.Int32).Load()
}

func (f *fillFixture) fill(cmd *BlobArchiveStoreFill, store blob_storage.BlobStorage) error {
	if store == nil {
		store = f.store
	}
	return cmd.fill(f.t.Context(), f.db, f.blocks, store)
}

func TestBlobArchiveStoreFillFetchesOnlyMissingSlots(t *testing.T) {
	f := newFillFixture(t)
	missing := f.addBlock(fillTestSlot, 2)
	stored := f.addBlock(fillTestSlot+1, 1)
	f.storeValid(stored)
	f.addBlock(fillTestSlot+2, 0)
	url, requests := f.endpoint(false)

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + 3}, nil))

	require.True(t, f.storedValid(fillTestSlot, missing))
	require.EqualValues(t, 1, requestsFor(requests, missing))
	require.Zero(t, requestsFor(requests, stored), "a slot the store serves correctly was fetched again")
}

func TestBlobArchiveStoreFillReplacesInvalidSidecars(t *testing.T) {
	f := newFillFixture(t)
	root := f.addBlock(fillTestSlot, 2)
	bad := *f.sidecars[root][1]
	bad.Blob[0] ^= 0xff // the proof no longer matches
	require.NoError(t, f.store.WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{f.sidecars[root][0], &bad}))
	require.False(t, f.storedValid(fillTestSlot, root))
	url, _ := f.endpoint(false)

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil))
	require.True(t, f.storedValid(fillTestSlot, root))
}

func TestBlobArchiveStoreFillDryRunFetchesAndWritesNothing(t *testing.T) {
	f := newFillFixture(t)
	root := f.addBlock(fillTestSlot, 1)
	url, requests := f.endpoint(false)

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot, DryRun: true}, nil))
	require.Zero(t, requestsFor(requests, root))
	require.False(t, f.storedValid(fillTestSlot, root))
}

func TestBlobArchiveStoreFillFallsBackToTheNextEndpoint(t *testing.T) {
	f := newFillFixture(t)
	root := f.addBlock(fillTestSlot, 1)
	empty, emptyRequests := f.endpoint(true)
	full, _ := f.endpoint(false)

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{empty, full}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil))
	require.EqualValues(t, 1, requestsFor(emptyRequests, root))
	require.True(t, f.storedValid(fillTestSlot, root))
}

// A slot no endpoint can fill must not stop the others, and the run must not end as a success.
func TestBlobArchiveStoreFillReportsSlotsNoEndpointHas(t *testing.T) {
	f := newFillFixture(t)
	unserved := f.addBlock(fillTestSlot, 1)
	served := f.addBlock(fillTestSlot+1, 1)
	url, _ := f.endpoint(false)
	delete(f.sidecars, unserved)

	err := f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + 1}, nil)
	require.ErrorContains(t, err, fmt.Sprint(fillTestSlot))
	require.True(t, f.storedValid(fillTestSlot+1, served))
}

type failingWrites struct{ blob_storage.BlobStorage }

func (failingWrites) WriteBlobSidecars(context.Context, common.Hash, []*cltypes.BlobSidecar) error {
	return errors.New("no space left on device")
}

// A store that cannot write fails every slot alike, so the run has to stop instead of trying the
// next endpoint.
func TestBlobArchiveStoreFillStopsWhenTheStoreCannotWrite(t *testing.T) {
	f := newFillFixture(t)
	root := f.addBlock(fillTestSlot, 1)
	first, _ := f.endpoint(false)
	second, secondRequests := f.endpoint(false)

	err := f.fill(&BlobArchiveStoreFill{Endpoints: []string{first, second}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, failingWrites{f.store})
	require.ErrorContains(t, err, "no space left on device")
	require.Zero(t, requestsFor(secondRequests, root))
}

func TestBlobArchiveStoreFillRefusesALockedDatadir(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	unlock, err := dirs.TryFlock()
	require.NoError(t, err)
	defer unlock()

	cmd := &BlobArchiveStoreFill{
		chainCfg:     chainCfg{Chain: networkname.Mainnet},
		outputFolder: outputFolder{Datadir: dirs.DataDir},
		FromSlot:     fillTestSlot,
		ToSlot:       fillTestSlot,
		DryRun:       true,
	}
	require.ErrorIs(t, cmd.Run(t.Context()), datadir.ErrDataDirLocked)
}
