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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	tip      common.Hash // the last block added; the next one builds on it
	frozen   uint64      // the frozen blob frontier
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
	return f.addBlockWithTransactions(slot, blobs, false)
}

// addBlockWithTransactions gives the block payload transactions when txs is set. The reader then
// serves the block as storage does, without them, so its own hash is not the canonical root.
func (f *fillFixture) addBlockWithTransactions(slot uint64, blobs int, txs bool) common.Hash {
	t := f.t
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = slot
	block.Block.ParentRoot = f.tip
	if txs {
		block.Block.Body.ExecutionPayload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{0x02, 0x01}})
	}
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
		if err := beacon_indicies.WriteHeaderSlot(tx, root, slot); err != nil {
			return err
		}
		if err := beacon_indicies.WriteParentBlockRoot(tx, root, f.tip); err != nil {
			return err
		}
		return beacon_indicies.MarkRootCanonical(tx, slot, root)
	}))
	f.tip = root
	if txs {
		block.Block.Body.ExecutionPayload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
		storedRoot, err := block.Block.HashSSZ()
		require.NoError(t, err)
		require.NotEqual(t, root, common.Hash(storedRoot))
	}
	f.blocks.blocks[slot] = block
	return root
}

func (f *fillFixture) storeValid(root common.Hash) {
	require.NoError(f.t, f.store.WriteBlobSidecars(f.t.Context(), root, f.sidecars[root]))
}

func (f *fillFixture) storedValid(slot uint64, root common.Hash) bool {
	return storedBlobsValid(f.t.Context(), f.store, slot, root, f.blocks.blocks[slot])
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
	return cmd.fill(f.t.Context(), f.db, f.blocks, store, f.frozen)
}

func TestBlobArchiveStoreFillFetchesOnlyMissingSlots(t *testing.T) {
	f := newFillFixture(t)
	missing := f.addBlock(fillTestSlot, 2)
	stored := f.addBlock(fillTestSlot+1, 1)
	f.storeValid(stored)
	f.addBlock(fillTestSlot+2, 0)
	url, requests := f.endpoint(false)

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + 2}, nil))

	require.True(t, f.storedValid(fillTestSlot, missing))
	require.EqualValues(t, 1, requestsFor(requests, missing))
	require.Zero(t, requestsFor(requests, stored), "a slot the store serves correctly was fetched again")
}

func TestBlobArchiveStoreFillReplacesInvalidSidecars(t *testing.T) {
	for name, corrupt := range map[string]func(*cltypes.BlobSidecar){
		"blob":            func(s *cltypes.BlobSidecar) { s.Blob[0] ^= 0xff },
		"header":          func(s *cltypes.BlobSidecar) { s.SignedBlockHeader.Header.ProposerIndex++ },
		"signature":       func(s *cltypes.BlobSidecar) { s.SignedBlockHeader.Signature[0] ^= 0xff },
		"inclusion proof": func(s *cltypes.BlobSidecar) { s.CommitmentInclusionProof.Set(0, common.Hash{0xff}) },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFillFixture(t)
			root := f.addBlock(fillTestSlot, 2)
			bad := cloneSidecar(t, f.sidecars[root][1])
			corrupt(bad)
			require.NoError(t, f.store.WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{f.sidecars[root][0], bad}))
			require.False(t, f.storedValid(fillTestSlot, root))
			url, _ := f.endpoint(false)

			require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil))
			require.True(t, f.storedValid(fillTestSlot, root))
		})
	}
}

// A block the reader serves without a canonical root cannot be checked, so the run must not
// report success over it.
func TestBlobArchiveStoreFillReportsASlotWithoutCanonicalRoot(t *testing.T) {
	f := newFillFixture(t)
	f.addBlock(fillTestSlot-1, 0)
	f.blocks.blocks[fillTestSlot] = cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	f.addBlock(fillTestSlot+1, 0)
	url, _ := f.endpoint(false)

	err := f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + 1}, nil)
	require.ErrorContains(t, err, fmt.Sprint(fillTestSlot))
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

// Past the local head every slot reads as empty, so a range there would pass unchecked.
func TestBlobArchiveStoreFillRejectsARangePastTheLocalHead(t *testing.T) {
	f := newFillFixture(t)
	f.addBlock(fillTestSlot, 1)
	url, requests := f.endpoint(false)

	err := f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + 1}, nil)
	require.ErrorContains(t, err, "past the local chain")
	n := 0
	requests.Range(func(any, any) bool { n++; return true })
	require.Zero(t, n)
}

// Storage keeps no transactions, so the stored block hashes to another root; the fill must fetch
// and verify under the canonical one.
func TestBlobArchiveStoreFillUsesTheCanonicalRootOfAStoredBlock(t *testing.T) {
	f := newFillFixture(t)
	root := f.addBlockWithTransactions(fillTestSlot, 2, true)
	url, requests := f.endpoint(false)

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil))
	require.EqualValues(t, 1, requestsFor(requests, root))
	require.True(t, f.storedValid(fillTestSlot, root))
}

// A dry run must not write: it creates no directory and opens an existing blob store read-only.
func TestBlobArchiveStoreFillDryRunIsReadOnly(t *testing.T) {
	for name, store := range map[string]string{"no blob store": "", "empty blob directory": "dir", "read-only blob store": "file"} {
		t.Run(name, func(t *testing.T) {
			dirs := datadir.Open(t.TempDir())
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
			block.Block.Slot = fillTestSlot
			block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
			db, err := openCaplinDB(t.Context(), dirs.CaplinIndexing, false)
			require.NoError(t, err)
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				return beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true)
			}))
			db.Close()
			blobData := filepath.Join(dirs.CaplinBlobs, "chaindata", "mdbx.dat")
			if store != "" {
				require.NoError(t, os.MkdirAll(filepath.Dir(blobData), 0o700))
			}
			if store == "file" {
				blobDB, err := openCaplinDB(t.Context(), filepath.Dir(blobData), false)
				require.NoError(t, err)
				blobDB.Close()
				require.NoError(t, os.Chmod(blobData, 0o400)) // a read-write open fails
			}

			cmd := &BlobArchiveStoreFill{
				chainCfg:     chainCfg{Chain: networkname.Mainnet},
				outputFolder: outputFolder{Datadir: dirs.DataDir},
				FromSlot:     fillTestSlot,
				ToSlot:       fillTestSlot,
				DryRun:       true,
			}
			require.NoError(t, cmd.Run(t.Context()))
			require.NoDirExists(t, dirs.Chaindata, "the dry run created datadir directories")
			if store != "file" {
				require.NoFileExists(t, blobData)
			}
		})
	}
}

// The insert stores nothing for a response whose headers name another block and returns no error,
// so only the read-back after the fetch can tell that the slot is still invalid.
func TestBlobArchiveStoreFillReportsASlotTheResponseDidNotRepair(t *testing.T) {
	f := newFillFixture(t)
	root := f.addBlock(fillTestSlot, 1)
	bad := cloneSidecar(t, f.sidecars[root][0])
	bad.SignedBlockHeader.Header.ProposerIndex++
	require.NoError(t, f.store.WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{bad}))
	f.sidecars[root] = []*cltypes.BlobSidecar{bad} // the endpoint serves the same wrong header
	url, _ := f.endpoint(false)

	err := f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil)
	require.ErrorContains(t, err, fmt.Sprint(fillTestSlot))
}

// An endpoint that never answers, or answers with a malformed or oversized body, fails like any
// other, so the next endpoint fills the slot.
func TestBlobArchiveStoreFillMovesPastABadEndpoint(t *testing.T) {
	defer func(old time.Duration) { fillRequestTimeout = old }(fillRequestTimeout)
	fillRequestTimeout = 200 * time.Millisecond
	for name, respond := range map[string]func(good string) http.HandlerFunc{
		"stalled": func(string) http.HandlerFunc {
			return func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }
		},
		"null blob": func(string) http.HandlerFunc {
			return func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"data":[{"blob":null}]}`)) }
		},
		"too many": func(string) http.HandlerFunc {
			return func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"data":[{},{},{}]}`)) }
		},
		// A correct response behind more padding than the size cap allows: only the cap rejects it.
		"oversized": func(good string) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, good+r.URL.Path, nil)
				if err != nil {
					return
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return
				}
				defer resp.Body.Close()
				_, _ = w.Write(bytes.Repeat([]byte(" "), 2*maxBlobSidecarJSONSize))
				_, _ = io.Copy(w, resp.Body)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFillFixture(t)
			root := f.addBlock(fillTestSlot, 1)
			good, requests := f.endpoint(false)
			bad := httptest.NewServer(respond(good))
			t.Cleanup(bad.Close)

			require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{bad.URL, good}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil))
			require.True(t, f.storedValid(fillTestSlot, root))
			want := 1
			if name == "oversized" {
				want = 2 // the padded copy it proxied, then the fill after the cap rejected that copy
			}
			require.EqualValues(t, want, requestsFor(requests, root), "the slot was filled from the bad endpoint")
		})
	}
}

// Providers that authenticate with a query-string key must get the block id in the path.
func TestBlobArchiveStoreFillKeepsTheEndpointQuery(t *testing.T) {
	f := newFillFixture(t)
	root := f.addBlock(fillTestSlot, 1)
	good, requests := f.endpoint(false)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("apikey") != "k" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		http.Redirect(w, r, good+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(srv.Close)

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{srv.URL + "?apikey=k"}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil))
	require.EqualValues(t, 1, requestsFor(requests, root))
}

func TestBlobArchiveStoreFillRefusesSlotsBelowTheFrozenFrontier(t *testing.T) {
	f := newFillFixture(t)
	f.addBlock(fillTestSlot, 1)
	f.frozen = fillTestSlot + 1
	url, requests := f.endpoint(false)

	err := f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil)
	require.ErrorContains(t, err, "served from blob segments")
	n := 0
	requests.Range(func(any, any) bool { n++; return true })
	require.Zero(t, n)
}

// A stretch the index does not cover reads as empty slots; the run must refuse it rather than
// report it as checked. The gap is inside the range, before its first block, or after its last.
// A range that ends on a block has no trailing empty slots, so a gap after it does not matter.
func TestBlobArchiveStoreFillRefusesAnUnindexedStretch(t *testing.T) {
	for name, tc := range map[string]struct {
		from, to uint64
		refused  bool
	}{
		"inside":              {fillTestSlot, fillTestSlot + 3, true},
		"leading":             {fillTestSlot + 1, fillTestSlot + 3, true},
		"trailing":            {fillTestSlot, fillTestSlot + 2, true},
		"ends before the gap": {fillTestSlot, fillTestSlot, false},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFillFixture(t)
			f.addBlock(fillTestSlot, 1)
			f.tip = common.HexToHash("0x0a") // slots +1 and +2 hold a block the index never got
			f.addBlock(fillTestSlot+3, 1)
			url, _ := f.endpoint(false)

			err := f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: tc.from, ToSlot: tc.to}, nil)
			if tc.refused {
				require.ErrorContains(t, err, "not in the local index")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Adjacent blocks that do not link mean an inconsistent index, not a missing stretch.
func TestBlobArchiveStoreFillRefusesAdjacentBlocksThatDoNotLink(t *testing.T) {
	f := newFillFixture(t)
	f.addBlock(fillTestSlot, 1)
	f.tip = common.HexToHash("0x0a")
	f.addBlock(fillTestSlot+1, 1)
	url, _ := f.endpoint(false)

	err := f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + 1}, nil)
	require.ErrorContains(t, err, fmt.Sprintf("the block at slot %d does not follow the one at slot %d", fillTestSlot+1, fillTestSlot))
	require.NotContains(t, err.Error(), "not in the local index")
}

// The index check reads every slot of the range, so it has to stop when the run is cancelled.
func TestCheckRangeIndexedStopsWhenCancelled(t *testing.T) {
	f := newFillFixture(t)
	f.addBlock(fillTestSlot, 0)
	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, f.db.View(t.Context(), func(tx kv.Tx) error {
		cancel()
		require.ErrorIs(t, checkRangeIndexed(ctx, tx, fillTestSlot, fillTestSlot), context.Canceled)
		return nil
	}))
}

func cloneSidecar(t *testing.T, sidecar *cltypes.BlobSidecar) *cltypes.BlobSidecar {
	t.Helper()
	encoded, err := sidecar.EncodeSSZ(nil)
	require.NoError(t, err)
	clone := &cltypes.BlobSidecar{}
	require.NoError(t, clone.DecodeSSZ(encoded, int(clparams.DenebVersion)))
	return clone
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
