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
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
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
	payload  common.Hash
	frozen   uint64 // the frozen blob frontier
	version  clparams.StateVersion
}

func newFillFixture(t *testing.T) *fillFixture {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB) // the index and the blob store use different tables
	return &fillFixture{
		t:        t,
		db:       db,
		blocks:   &storeCheckReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{}},
		store:    blob_storage.NewBlobStore(db, afero.NewMemMapFs()),
		sidecars: map[common.Hash][]*cltypes.BlobSidecar{},
		version:  clparams.DenebVersion,
	}
}

// addBlock makes slot hold a canonical block of the fixture's version with valid sidecars for blobs blobs, which the
// endpoint serves. It returns the block root.
func (f *fillFixture) addBlock(slot uint64, blobs int) common.Hash {
	return f.addBlockWithTransactions(slot, blobs, false)
}

// addBlockWithTransactions gives the block payload transactions when txs is set. The reader then
// serves the block as storage does, without them, so its own hash is not the canonical root.
func (f *fillFixture) addBlockWithTransactions(slot uint64, blobs int, txs bool) common.Hash {
	t := f.t
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, f.version)
	block.Block.Slot = slot
	block.Block.ParentRoot = f.tip
	if f.version >= clparams.GloasVersion {
		bid := block.Block.Body.SignedExecutionPayloadBid.Message
		bid.ParentBlockHash, bid.BlockHash = f.payload, common.Hash{byte(slot >> 8), byte(slot)}
		f.payload = bid.BlockHash
	}
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
		block.Block.Body.GetBlobKzgCommitments().Append((*cltypes.KZGCommitment)(&commitments[i]))
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	for i := range data {
		proof, err := kzg.Ctx().ComputeBlobKZGProof(&data[i], commitments[i], 0)
		require.NoError(t, err)
		inclusion := solid.NewHashVector(cltypes.CommitmentBranchSize)
		if f.version < clparams.GloasVersion {
			branch, err := block.Block.Body.KzgCommitmentMerkleProof(i)
			require.NoError(t, err)
			for j := range branch {
				inclusion.Set(j, common.Hash(branch[j]))
			}
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

// endpoint serves the fixture's sidecars, only their blobs for Gloas, or 404s when missing is set, and counts requests per root.
func (f *fillFixture) endpoint(missing bool) (string, *sync.Map) {
	requests := &sync.Map{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		counter, _ := requests.LoadOrStore(id, &atomic.Int32{})
		counter.(*atomic.Int32).Add(1)
		sidecars, ok := f.sidecars[common.HexToHash(id)]
		var data any = sidecars
		switch {
		case missing || !ok:
			if !missing && f.version >= clparams.GloasVersion && strings.Contains(r.URL.Path, "/eth/v1/beacon/blobs/") {
				data = []*cltypes.Blob{}
				break
			}
			w.WriteHeader(http.StatusNotFound)
			return
		case strings.Contains(r.URL.Path, "/eth/v1/beacon/blobs/"):
			blobs := make([]*cltypes.Blob, len(sidecars))
			for i, sidecar := range sidecars {
				blobs[i] = &sidecar.Blob
			}
			data = blobs
		case !strings.Contains(r.URL.Path, "/eth/v1/beacon/blob_sidecars/"):
			w.WriteHeader(http.StatusNotFound)
			return
		case f.version >= clparams.GloasVersion:
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := json.NewEncoder(w).Encode(map[string]any{"data": data}); err != nil {
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

func TestBlobArchiveStoreFillBuildsGloasSidecarsFromBlobs(t *testing.T) {
	f := newFillFixture(t)
	f.version = clparams.GloasVersion
	root := f.addBlock(fillTestSlot, 2)
	url, _ := f.endpoint(false)

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil))

	require.True(t, f.storedValid(fillTestSlot, root))
}

func TestBlobArchiveStoreFillSkipsAGloasBlockWhosePayloadIsNotOnChain(t *testing.T) {
	for _, gap := range []uint64{1, 200} {
		t.Run(fmt.Sprintf("next block %d slots later", gap), func(t *testing.T) {
			f, before, after := newSkippedPayloadFixture(t, gap)
			url, _ := f.endpoint(false)

			require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + 1 + gap}, nil))

			require.True(t, f.storedValid(fillTestSlot, before))
			require.True(t, f.storedValid(fillTestSlot+1+gap, after))
		})
	}
}

func TestBlobArchiveStoreFillDryRunProbesPastASkippedGloasPayload(t *testing.T) {
	f, _, _ := newSkippedPayloadFixture(t, 1)
	url, _ := f.endpoint(false)
	cmd := &BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + 2, DryRun: true}

	require.NoError(t, f.fill(cmd, nil))

	require.Equal(t, uint64(fillTestSlot), cmd.windows[0].from.Load())
}

func TestBlobArchiveStoreFillDryRunServesAnUnresolvedGloasHead(t *testing.T) {
	for name, notServed := range notServedAnswers {
		t.Run(name, func(t *testing.T) {
			f := newFillFixture(t)
			f.version = clparams.GloasVersion
			f.addBlock(fillTestSlot, 1)
			head := f.addBlock(fillTestSlot+1, 1)
			good, _ := f.endpoint(false)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if common.HexToHash(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]) == head {
					notServed(w, r)
					return
				}
				http.Redirect(w, r, good+r.URL.Path, http.StatusTemporaryRedirect)
			}))
			t.Cleanup(srv.Close)
			cmd := &BlobArchiveStoreFill{Endpoints: []string{srv.URL}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + 1, DryRun: true}
			var logs bytes.Buffer
			oldHandler := log.Root().GetHandler()
			log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.SyncHandler(log.StreamHandler(&logs, log.LogfmtFormat()))))
			t.Cleanup(func() { log.Root().SetHandler(oldHandler) })

			require.NoError(t, f.fill(cmd, nil))

			require.Equal(t, uint64(fillTestSlot), cmd.windows[0].from.Load())
			require.Contains(t, logs.String(), fmt.Sprintf("msg=\"Slot needs its sidecars\" slot=%d", fillTestSlot+1))
			require.Equal(t, 1, strings.Count(logs.String(), "served=false"))
		})
	}
}

func TestBlobArchiveStoreFillReportsAnUnresolvedSlotBelowAnEndpointWindow(t *testing.T) {
	f := newFillFixture(t)
	f.version = clparams.GloasVersion
	f.addBlock(fillTestSlot, 1)
	cmd := &BlobArchiveStoreFill{Endpoints: []string{"http://beacon.example"}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}
	cmd.windows = make([]endpointWindow, 1)
	cmd.windows[0].from.Store(fillTestSlot + 1)
	cmd.windows[0].once.Do(func() {})
	report := &fillReport{}

	require.NoError(t, cmd.fillSlot(t.Context(), f.db, f.blocks, &writeErrRecorder{BlobStorage: f.store}, fillTestSlot, report))
	require.Empty(t, report.unserved)
}

func TestExpectedBlobsInTxTreatsAMissingGloasChildBodyAsUnresolved(t *testing.T) {
	f := newFillFixture(t)
	f.version = clparams.GloasVersion
	root := f.addBlock(fillTestSlot, 1)
	childRoot := common.HexToHash("0xcafe")
	require.NoError(t, f.db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(tx, fillTestSlot+1, childRoot)
	}))

	var got int
	var unresolved bool
	require.NoError(t, f.db.View(t.Context(), func(tx kv.Tx) error {
		var err error
		got, unresolved, err = expectedBlobsInTx(t.Context(), tx, f.blocks, root, f.blocks.blocks[fillTestSlot])
		return err
	}))
	require.Equal(t, 1, got)
	require.True(t, unresolved)
}

func TestBlobArchiveStoreFillKeepsAGloasBlockWhoseSuccessorIsNotItsChild(t *testing.T) {
	f := newFillFixture(t)
	f.version = clparams.GloasVersion
	root := f.addBlock(fillTestSlot, 1)
	f.tip, f.payload = common.Hash{0xee}, common.Hash{0xdd}
	f.addBlock(fillTestSlot+2, 1)
	url, _ := f.endpoint(false)

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil))

	require.True(t, f.storedValid(fillTestSlot, root))
}

func TestBlobArchiveStoreFillDryRunServesAGloasBlockWithANonChildSuccessor(t *testing.T) {
	f := newFillFixture(t)
	f.version = clparams.GloasVersion
	root := f.addBlock(fillTestSlot, 1)
	f.tip, f.payload = common.Hash{0xee}, common.Hash{0xdd}
	f.addBlock(fillTestSlot+2, 1)
	delete(f.sidecars, root)
	url, _ := f.endpoint(false)
	cmd := &BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot, DryRun: true}

	require.NoError(t, f.fill(cmd, nil))

	require.Equal(t, uint64(fillTestSlot), cmd.windows[0].from.Load())
}

func TestBlobArchiveStoreFillFillsBeforeAnUnresolvedGloasHead(t *testing.T) {
	f := newFillFixture(t)
	f.version = clparams.GloasVersion
	root := f.addBlock(fillTestSlot, 1)
	head := f.addBlock(fillTestSlot+1, 1)
	delete(f.sidecars, head)
	url, _ := f.endpoint(false)
	cmd := &BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + 1}
	var logs bytes.Buffer
	oldHandler := log.Root().GetHandler()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.SyncHandler(log.StreamHandler(&logs, log.LogfmtFormat()))))
	t.Cleanup(func() { log.Root().SetHandler(oldHandler) })

	require.NoError(t, f.fill(cmd, nil))
	require.True(t, f.storedValid(fillTestSlot, root))
	require.Zero(t, cmd.windows[0].from.Load())
	output := logs.String()
	require.Contains(t, output, "missing=2")
	require.Contains(t, output, "filled=1")
	require.Contains(t, output, "failed=0")
	require.Contains(t, output, "unserved=0")
	require.Contains(t, output, "unresolved=1")
	require.Contains(t, output, fmt.Sprintf("lvl=warn msg=\"Slot has unresolved payload status\" slot=%d", fillTestSlot+1))
}

func TestBlobArchiveStoreFillTriesTheNextEndpointForAnUnresolvedGloasHead(t *testing.T) {
	f := newFillFixture(t)
	f.version = clparams.GloasVersion
	head := f.addBlock(fillTestSlot, 1)
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	t.Cleanup(empty.Close)
	full, _ := f.endpoint(false)
	cmd := &BlobArchiveStoreFill{Endpoints: []string{empty.URL, full}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}

	require.NoError(t, f.fill(cmd, nil))
	require.True(t, f.storedValid(fillTestSlot, head))
	require.Zero(t, cmd.windows[0].from.Load())
}

func TestBlobArchiveStoreFillKeepsAGloasBlockWhoseChildIsNotIndexed(t *testing.T) {
	f := newFillFixture(t)
	f.version = clparams.GloasVersion
	root := f.addBlock(fillTestSlot, 1)
	child := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	child.Block.Slot, child.Block.ParentRoot = fillTestSlot+1, root
	child.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash = common.Hash{0xdd}
	f.blocks.blocks[fillTestSlot+1] = child
	f.addBlock(fillTestSlot+2, 0)
	url, _ := f.endpoint(false)

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil))

	require.True(t, f.storedValid(fillTestSlot, root))
}

func newSkippedPayloadFixture(t *testing.T, gap uint64) (*fillFixture, common.Hash, common.Hash) {
	f := newFillFixture(t)
	f.version = clparams.GloasVersion
	before := f.addBlock(fillTestSlot, 1)
	onChain := f.payload
	skipped := f.addBlock(fillTestSlot+1, 2)
	delete(f.sidecars, skipped)
	f.payload = onChain
	after := f.addBlock(fillTestSlot+1+gap, 1)
	return f, before, after
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

// Fulu blocks carry no per-blob KZG proof, so an endpoint that rebuilds sidecars from columns can
// serve the empty proof, the compressed point at infinity.
func TestBlobArchiveStoreFillRecomputesAnEmptyKZGProof(t *testing.T) {
	f := newFillFixture(t)
	f.version = clparams.FuluVersion
	root := f.addBlock(fillTestSlot, 2)
	for _, sidecar := range f.sidecars[root] {
		sidecar.KzgProof = common.Bytes48{0xc0}
	}
	url, _ := f.endpoint(false)

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil))

	require.True(t, f.storedValid(fillTestSlot, root))
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

func TestBlobArchiveStoreFillDryRunWritesNothing(t *testing.T) {
	f := newFillFixture(t)
	root := f.addBlock(fillTestSlot, 1)
	url, _ := f.endpoint(false)

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot, DryRun: true}, nil))
	require.False(t, f.storedValid(fillTestSlot, root))
}

func TestBlobArchiveStoreFillDryRunAsksNothingOfAStoredRange(t *testing.T) {
	f := newFillFixture(t)
	root := f.addBlock(fillTestSlot, 1)
	f.storeValid(root)
	url, requests := f.endpoint(false)

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot, DryRun: true}, nil))
	require.Zero(t, requestsFor(requests, root))
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
	defer func(timeout, wait time.Duration) { fillRequestTimeout, fillRetryWait = timeout, wait }(fillRequestTimeout, fillRetryWait)
	fillRequestTimeout, fillRetryWait = 200*time.Millisecond, time.Millisecond
	for name, respond := range map[string]func(good string) http.HandlerFunc{
		"stalled": func(string) http.HandlerFunc {
			return func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }
		},
		"unavailable": func(string) http.HandlerFunc {
			return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }
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

func failOnce(t *testing.T, good string, fail http.HandlerFunc) (string, *atomic.Int64) {
	var calls atomic.Int32
	var failedAt, retryGap atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			failedAt.Store(time.Now().UnixNano())
			fail(w, r)
			return
		case 2:
			retryGap.Store(time.Now().UnixNano() - failedAt.Load())
		}
		http.Redirect(w, r, good+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &retryGap
}

func TestBlobArchiveStoreFillRetriesATransientFailure(t *testing.T) {
	defer func(timeout, wait time.Duration) { fillRequestTimeout, fillRetryWait = timeout, wait }(fillRequestTimeout, fillRetryWait)
	fillRequestTimeout, fillRetryWait = 200*time.Millisecond, time.Millisecond
	for name, fail := range map[string]http.HandlerFunc{
		"503":     func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) },
		"429":     func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTooManyRequests) },
		"stalled": func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() },
		"stalled body": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"data":[`))
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFillFixture(t)
			root := f.addBlock(fillTestSlot, 1)
			good, _ := f.endpoint(false)

			url, _ := failOnce(t, good, fail)

			require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil))
			require.True(t, f.storedValid(fillTestSlot, root))
		})
	}
}

func TestBlobArchiveStoreFillWaitsForRetryAfter(t *testing.T) {
	defer func(wait time.Duration) { fillRetryWait = wait }(fillRetryWait)
	fillRetryWait = time.Millisecond
	f := newFillFixture(t)
	root := f.addBlock(fillTestSlot, 1)
	good, _ := f.endpoint(false)
	url, retryGap := failOnce(t, good, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	})

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil))
	require.True(t, f.storedValid(fillTestSlot, root))
	require.GreaterOrEqual(t, time.Duration(retryGap.Load()), time.Second)
}

func TestBlobArchiveStoreFillDoesNotRetryABadRequest(t *testing.T) {
	defer func(wait time.Duration) { fillRetryWait = wait }(fillRetryWait)
	fillRetryWait = time.Millisecond
	f := newFillFixture(t)
	root := f.addBlock(fillTestSlot, 1)
	good, _ := f.endpoint(false)
	var calls atomic.Int32
	pruned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(pruned.Close)

	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{pruned.URL, good}, FromSlot: fillTestSlot, ToSlot: fillTestSlot}, nil))
	require.True(t, f.storedValid(fillTestSlot, root))
	require.EqualValues(t, 1, calls.Load())
}

const windowTestSlots, windowTestServedFrom = 32, 24

var notServedAnswers = map[string]http.HandlerFunc{
	"400":   func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadRequest) },
	"404":   func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) },
	"empty": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"data":[]}`)) },
}

func (f *fillFixture) prunedChain(notServed http.HandlerFunc) (url string, roots []common.Hash, prunedRequests, allRequests *atomic.Int32) {
	pruned := map[common.Hash]bool{}
	for i := range uint64(windowTestSlots) {
		roots = append(roots, f.addBlock(fillTestSlot+i, 1))
		pruned[roots[i]] = i < windowTestServedFrom
	}
	good, _ := f.endpoint(false)
	prunedRequests, allRequests = &atomic.Int32{}, &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allRequests.Add(1)
		if pruned[common.HexToHash(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:])] {
			prunedRequests.Add(1)
			notServed(w, r)
			return
		}
		http.Redirect(w, r, good+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	f.t.Cleanup(srv.Close)
	return srv.URL, roots, prunedRequests, allRequests
}

func TestBlobArchiveStoreFillSkipsSlotsBelowTheFirstSlotAnEndpointServes(t *testing.T) {
	for name, notServed := range notServedAnswers {
		t.Run(name, func(t *testing.T) {
			f := newFillFixture(t)
			url, roots, prunedRequests, _ := f.prunedChain(notServed)

			err := f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + windowTestSlots - 1}, nil)
			require.LessOrEqual(t, prunedRequests.Load(), int32(fillWorkers+5), "every pruned slot was requested")
			require.ErrorContains(t, err, fmt.Sprintf("%d slots with blobs are not served by any endpoint", windowTestServedFrom))
			for i := windowTestServedFrom; i < windowTestSlots; i++ {
				require.True(t, f.storedValid(fillTestSlot+uint64(i), roots[i]))
			}
		})
	}
}

func TestBlobArchiveStoreFillDryRunFindsTheFirstSlotAnEndpointServes(t *testing.T) {
	for name, notServed := range notServedAnswers {
		t.Run(name, func(t *testing.T) {
			f := newFillFixture(t)
			url, roots, _, allRequests := f.prunedChain(notServed)

			cmd := &BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + windowTestSlots - 1, DryRun: true}
			require.NoError(t, f.fill(cmd, nil))
			require.EqualValues(t, fillTestSlot+windowTestServedFrom, cmd.windows[0].from.Load())
			require.LessOrEqual(t, allRequests.Load(), int32(6), "more requests than a binary search over the range")
			for i, root := range roots {
				require.False(t, f.storedValid(fillTestSlot+uint64(i), root))
			}
		})
	}
}

func TestBlobArchiveStoreFillDryRunLogsSlotsBelowTheFirstSlotAnEndpointServes(t *testing.T) {
	f := newFillFixture(t)
	url, _, _, _ := f.prunedChain(notServedAnswers["400"])
	cmd := &BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + windowTestSlots - 1, DryRun: true}
	var logs bytes.Buffer
	oldHandler := log.Root().GetHandler()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.SyncHandler(log.StreamHandler(&logs, log.LogfmtFormat()))))
	t.Cleanup(func() { log.Root().SetHandler(oldHandler) })

	require.NoError(t, f.fill(cmd, nil))
	output := logs.String()
	for i := range windowTestServedFrom {
		require.Contains(t, output, fmt.Sprintf("msg=\"Slot needs its sidecars\" slot=%d", fillTestSlot+uint64(i)))
	}
	require.Equal(t, windowTestServedFrom, strings.Count(output, "served=false"))
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

type fillReadCounter struct {
	freezeblocks.BeaconSnapshotReader
	reads atomic.Uint64
}

func (r *fillReadCounter) ReadBeaconBlockBodyBySlot(ctx context.Context, tx kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	r.reads.Add(1)
	return r.BeaconSnapshotReader.ReadBeaconBlockBodyBySlot(ctx, tx, slot)
}

type failingFillReader struct {
	freezeblocks.BeaconSnapshotReader
	calls  atomic.Uint64
	failAt uint64
	err    error
}

func (r *failingFillReader) ReadBeaconBlockBodyBySlot(ctx context.Context, tx kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	if r.calls.Add(1) == r.failAt {
		return nil, r.err
	}
	return r.BeaconSnapshotReader.ReadBeaconBlockBodyBySlot(ctx, tx, slot)
}

func TestBlobArchiveStoreFillStopsWhenWindowReadFails(t *testing.T) {
	for _, dryRun := range []bool{true, false} {
		t.Run(fmt.Sprint(dryRun), func(t *testing.T) {
			f := newFillFixture(t)
			f.addBlock(fillTestSlot, 1)
			toSlot := fillTestSlot
			if !dryRun {
				f.addBlock(fillTestSlot+1, 1)
				toSlot++
			}
			url, _ := f.endpoint(true)
			readErr := errors.New("window read failed")
			reader := &failingFillReader{BeaconSnapshotReader: f.blocks, failAt: 2, err: readErr}
			cmd := &BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: toSlot, DryRun: dryRun, windows: make([]endpointWindow, 1)}

			err := cmd.fillSlot(t.Context(), f.db, reader, &writeErrRecorder{BlobStorage: f.store}, fillTestSlot, &fillReport{})
			require.ErrorIs(t, err, readErr)
			require.ErrorContains(t, err, "find the window of endpoint 1")
		})
	}
}

func TestBlobArchiveStoreFillSparseWindowReads(t *testing.T) {
	const slots = uint64(65_536)
	f := newFillFixture(t)
	f.addBlock(fillTestSlot-1, 0)
	f.addBlock(fillTestSlot+slots-1, 1)
	url, _ := f.endpoint(false)
	reader := &fillReadCounter{BeaconSnapshotReader: f.blocks}
	cmd := &BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + slots - 1, DryRun: true}
	require.NoError(t, cmd.fill(t.Context(), f.db, reader, f.store, 0))
	t.Logf("sparse slots=%d reads=%d", slots, reader.reads.Load())
	require.LessOrEqual(t, reader.reads.Load(), 2*slots+64, "the window search scanned a blob-free interval more than once")
}

func TestBlobArchiveStoreFillSparseWindowBoundary(t *testing.T) {
	for _, served := range []bool{true, false} {
		t.Run(fmt.Sprint(served), func(t *testing.T) {
			f := newFillFixture(t)
			pruned := f.addBlock(fillTestSlot, 1)
			f.addBlock(fillTestSlot+24, 1)
			f.addBlock(fillTestSlot+32, 0)
			good, _ := f.endpoint(false)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				root := common.HexToHash(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:])
				if !served || root == pruned {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				http.Redirect(w, r, good+r.URL.Path, http.StatusTemporaryRedirect)
			}))
			t.Cleanup(srv.Close)
			cmd := &BlobArchiveStoreFill{Endpoints: []string{srv.URL}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + 31, DryRun: true}
			require.NoError(t, f.fill(cmd, nil))
			want := cmd.ToSlot + 1
			if served {
				want = fillTestSlot + 24
			}
			require.Equal(t, want, cmd.windows[0].from.Load())
		})
	}
}

func TestBlobArchiveStoreFillDenseWindowReads(t *testing.T) {
	f := newFillFixture(t)
	url, _, _, requests := f.prunedChain(notServedAnswers["400"])
	reader := &fillReadCounter{BeaconSnapshotReader: f.blocks}
	cmd := &BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + windowTestSlots - 1, DryRun: true}
	require.NoError(t, cmd.fill(t.Context(), f.db, reader, f.store, 0))
	t.Logf("dense slots=%d reads=%d requests=%d", windowTestSlots, reader.reads.Load(), requests.Load())
	require.EqualValues(t, fillTestSlot+windowTestServedFrom, cmd.windows[0].from.Load())
	require.LessOrEqual(t, reader.reads.Load(), uint64(windowTestSlots+64))
}

func TestBlobArchiveStoreCheckPassesAfterFillingASkippedGloasPayload(t *testing.T) {
	f, _, _ := newSkippedPayloadFixture(t, 1)
	url, _ := f.endpoint(false)
	require.NoError(t, f.fill(&BlobArchiveStoreFill{Endpoints: []string{url}, FromSlot: fillTestSlot, ToSlot: fillTestSlot + 2}, nil))

	tx, err := f.db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	mismatched, _, err := checkBlobStore(t.Context(), tx, f.blocks, f.store, fillTestSlot+2, fillTestSlot, false)
	require.NoError(t, err)
	require.Zero(t, mismatched)
}

func TestBlobArchiveStoreCheckKeepsTheBlobsOfASkippedGloasPayload(t *testing.T) {
	f := newFillFixture(t)
	f.version = clparams.GloasVersion
	before := f.addBlock(fillTestSlot, 1)
	onChain := f.payload
	skipped := f.addBlock(fillTestSlot+1, 2)
	f.payload = onChain
	after := f.addBlock(fillTestSlot+2, 1)
	for _, root := range []common.Hash{before, skipped, after} {
		f.storeValid(root)
	}

	tx, err := f.db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	mismatched, _, err := checkBlobStore(t.Context(), tx, f.blocks, f.store, fillTestSlot+2, fillTestSlot, true)
	require.NoError(t, err)
	require.Zero(t, mismatched)
	require.True(t, f.storedValid(fillTestSlot+1, skipped))
}

func TestBlobArchiveStoreCheckFlagsPartialBlobsOfASkippedGloasPayload(t *testing.T) {
	f := newFillFixture(t)
	f.version = clparams.GloasVersion
	before := f.addBlock(fillTestSlot, 1)
	onChain := f.payload
	skipped := f.addBlock(fillTestSlot+1, 2)
	f.payload = onChain
	after := f.addBlock(fillTestSlot+2, 1)
	f.storeValid(before)
	f.storeValid(after)
	require.NoError(t, f.store.WriteBlobSidecars(t.Context(), skipped, f.sidecars[skipped][:1]))

	tx, err := f.db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	mismatched, unresolved, err := checkBlobStore(t.Context(), tx, f.blocks, f.store, fillTestSlot+2, fillTestSlot, true)
	require.NoError(t, err)
	require.Equal(t, uint64(1), mismatched)
	require.Zero(t, unresolved)
	count, err := f.store.KzgCommitmentsCount(t.Context(), skipped)
	require.NoError(t, err)
	require.Zero(t, count)
}
