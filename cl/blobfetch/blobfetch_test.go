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

package blobfetch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"sync/atomic"
	"testing"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

// prooflessBlock returns a Deneb block with blobs blobs and its sidecars carrying proof, a placeholder some endpoints
// serve instead of a real proof, so the fetch has to compute every proof.
func prooflessBlock(t *testing.T, blobs int, proof common.Bytes48) (*cltypes.SignedBeaconBlock, common.Hash, []*cltypes.BlobSidecar) {
	t.Helper()
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = 100
	data := make([]goethkzg.Blob, blobs)
	commitments := make([]goethkzg.KZGCommitment, blobs)
	for i := range data {
		data[i][1] = byte(i + 1)
		var err error
		commitments[i], err = kzg.Ctx().BlobToKZGCommitment(&data[i], 0)
		require.NoError(t, err)
		block.Block.Body.GetBlobKzgCommitments().Append((*cltypes.KZGCommitment)(&commitments[i]))
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	sidecars := make([]*cltypes.BlobSidecar, blobs)
	for i := range data {
		branch, err := block.Block.Body.KzgCommitmentMerkleProof(i)
		require.NoError(t, err)
		inclusion := solid.NewHashVector(cltypes.CommitmentBranchSize)
		for j := range branch {
			inclusion.Set(j, common.Hash(branch[j]))
		}
		sidecars[i] = cltypes.NewBlobSidecar(uint64(i), (*cltypes.Blob)(&data[i]), common.Bytes48(commitments[i]),
			proof, block.SignedBeaconBlockHeader(), inclusion)
	}
	return block, root, sidecars
}

func serveSidecars(t *testing.T, sidecars []*cltypes.BlobSidecar) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := json.NewEncoder(w).Encode(map[string]any{"data": sidecars}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestFetchAndStoreComputesMissingProofsWithinACPUBudget(t *testing.T) {
	block, root, sidecars := prooflessBlock(t, 4, common.Bytes48(goethkzg.PointAtInfinity))
	store := blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	budget := semaphore.NewWeighted(2)

	require.NoError(t, FetchAndStore(t.Context(), store, serveSidecars(t, sidecars), root, block, nil, budget))

	stored, found, err := store.ReadBlobSidecars(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, stored, 4)
	require.NoError(t, blob_storage.VerifyBlobSidecars(stored, block.Version(), func(*cltypes.SignedBeaconBlockHeader) error { return nil }))
	require.True(t, budget.TryAcquire(2), "every budget slot is returned")
}

// Endpoints that rebuild sidecars from columns fill the proof with a placeholder: Lighthouse the point at infinity,
// Teku 48 zero bytes, which is not a valid point. Any proof that is not a G1 point is computed.
func TestFetchAndStoreComputesPlaceholderProofs(t *testing.T) {
	for name, proof := range map[string]common.Bytes48{
		"zero bytes":  {},
		"not a point": {0xff, 0xff, 0xff},
	} {
		t.Run(name, func(t *testing.T) {
			block, root, sidecars := prooflessBlock(t, 2, proof)
			store := blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())

			require.NoError(t, FetchAndStore(t.Context(), store, serveSidecars(t, sidecars), root, block, nil, semaphore.NewWeighted(1)))

			stored, found, err := store.ReadBlobSidecars(t.Context(), block.Block.Slot, root)
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, stored, 2)
		})
	}
}

func TestFetchAndStoreComputesAProofOnlyWithAFreeBudgetSlot(t *testing.T) {
	block, root, sidecars := prooflessBlock(t, 1, common.Bytes48(goethkzg.PointAtInfinity))
	store := blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	budget := semaphore.NewWeighted(1)
	require.True(t, budget.TryAcquire(1))

	done := make(chan error, 1)
	go func() {
		done <- FetchAndStore(t.Context(), store, serveSidecars(t, sidecars), root, block, nil, budget)
	}()
	select {
	case <-done:
		t.Fatal("the proof was computed while the budget was taken")
	case <-time.After(300 * time.Millisecond):
	}
	budget.Release(1)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the proof was not computed after a budget slot was released")
	}
}

func TestRetryBackoffCapsRetryAfter(t *testing.T) {
	for header, want := range map[string]time.Duration{
		"0":           0,
		"30":          30 * time.Second,
		"31":          30 * time.Second,
		"9223372037":  30 * time.Second,
		"18446744074": 30 * time.Second,
		"+9223372037": 30 * time.Second,
	} {
		resp := &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": {header}}}
		require.Equal(t, want, retryBackoff(time.Second, 30*time.Second, 0, resp), "Retry-After: %s", header)
	}
}

func TestFailedFetchKeepsDefaultTransportIdleConnections(t *testing.T) {
	defer func(wait time.Duration) { RetryWait = wait }(RetryWait)
	RetryWait = time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	get := func() (reused bool) {
		ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
		})
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return reused
	}
	get()

	_, err := getBeaconData(t.Context(), srv.URL, "blobs", 1)
	require.Error(t, err)
	require.True(t, get(), "a failed fetch closed the idle connections of http.DefaultTransport")
}

func TestGetBeaconDataRejectsTrailingData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[]} {"data":[]}`))
	}))
	t.Cleanup(srv.Close)

	_, err := getBeaconData(t.Context(), srv.URL, "blobs", 1)
	require.ErrorContains(t, err, "decode")
}

// blockFixture is a block with a transaction and a withdrawal, so the body root of the block as stored (blinded) differs
// from that of the same block promoted with empty execution data, which is what callers read.
type blockFixture struct {
	root     common.Hash
	promoted *cltypes.SignedBeaconBlock
	blinded  *cltypes.SignedBlindedBeaconBlock
	// sidecars are the ones the block has on the network, with real proofs.
	sidecars []*cltypes.BlobSidecar
}

func newBlockFixture(t *testing.T, version clparams.StateVersion, blobs int) blockFixture {
	t.Helper()
	cfg := &clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(cfg, version)
	block.Signature = common.Bytes96{1}
	block.Block.Slot = 100
	payload := block.Block.Body.ExecutionPayload
	payload.BlockHash = common.Hash{2}
	payload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{1, 2, 3}, {4}})
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	payload.Withdrawals.Append(&cltypes.Withdrawal{Index: 7, Amount: 9})
	data := make([]goethkzg.Blob, blobs)
	commitments := make([]goethkzg.KZGCommitment, blobs)
	proofs := make([]goethkzg.KZGProof, blobs)
	for i := range data {
		data[i][1] = byte(i + 1)
		var err error
		commitments[i], err = kzg.Ctx().BlobToKZGCommitment(&data[i], 0)
		require.NoError(t, err)
		proofs[i], err = kzg.Ctx().ComputeBlobKZGProof(&data[i], commitments[i], 0)
		require.NoError(t, err)
		block.Block.Body.BlobKzgCommitments.Append((*cltypes.KZGCommitment)(&commitments[i]))
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	header := block.SignedBeaconBlockHeader()
	sidecars := make([]*cltypes.BlobSidecar, blobs)
	for i := range data {
		branch, err := block.Block.Body.KzgCommitmentMerkleProof(i)
		require.NoError(t, err)
		inclusion := solid.NewHashVector(cltypes.CommitmentBranchSize)
		for j := range branch {
			inclusion.Set(j, common.Hash(branch[j]))
		}
		sidecars[i] = cltypes.NewBlobSidecar(uint64(i), (*cltypes.Blob)(&data[i]), common.Bytes48(commitments[i]),
			common.Bytes48(proofs[i]), header, inclusion)
	}
	blinded, err := block.Blinded()
	require.NoError(t, err)
	promoted := blinded.Full(&solid.TransactionsSSZ{}, solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44))
	require.NotEqual(t, header.Header.BodyRoot, promoted.SignedBeaconBlockHeader().Header.BodyRoot)
	return blockFixture{root: root, promoted: promoted, blinded: blinded, sidecars: sidecars}
}

func (f blockFixture) blobs() []*cltypes.Blob {
	blobs := make([]*cltypes.Blob, len(f.sidecars))
	for i, sidecar := range f.sidecars {
		blobs[i] = &sidecar.Blob
	}
	return blobs
}

func (f blockFixture) blindedLoader() (load func(context.Context) (*cltypes.SignedBlindedBeaconBlock, error), calls *int) {
	calls = new(int)
	return func(context.Context) (*cltypes.SignedBlindedBeaconBlock, error) {
		*calls++
		return f.blinded, nil
	}, calls
}

func respondData(data any) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if err := json.NewEncoder(w).Encode(map[string]any{"data": data}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func respondStatus(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }
}

// serveBeacon serves blob_sidecars and blobs with the given handlers. A nil handler fails the test when its endpoint is
// requested.
func serveBeacon(t *testing.T, sidecars, blobs http.HandlerFunc) string {
	t.Helper()
	unexpected := func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request to %s", r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	}
	mux := http.NewServeMux()
	for path, handler := range map[string]http.HandlerFunc{"/eth/v1/beacon/blob_sidecars/": sidecars, "/eth/v1/beacon/blobs/": blobs} {
		if handler == nil {
			handler = unexpected
		}
		mux.Handle(path, handler)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

func requireStoredSidecars(t *testing.T, store blob_storage.BlobStorage, f blockFixture) {
	t.Helper()
	stored, found, err := store.ReadBlobSidecars(t.Context(), f.promoted.Block.Slot, f.root)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, stored, len(f.sidecars))
	for i := range stored {
		got, err := stored[i].EncodeSSZ(nil)
		require.NoError(t, err)
		want, err := f.sidecars[i].EncodeSSZ(nil)
		require.NoError(t, err)
		require.Equal(t, want, got, "sidecar %d", i)
	}
}

func TestFetchAndStoreFallsBackToBlobsWhenSidecarsAreNotServed(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.ElectraVersion, clparams.FuluVersion} {
		t.Run(version.String(), func(t *testing.T) {
			fixture := newBlockFixture(t, version, 3)
			store := blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
			endpoint := serveBeacon(t, respondStatus(http.StatusNotFound), respondData(fixture.blobs()))
			blinded, loads := fixture.blindedLoader()

			require.NoError(t, FetchAndStore(t.Context(), store, endpoint, fixture.root, fixture.promoted, blinded, semaphore.NewWeighted(2)))

			require.Equal(t, 1, *loads)
			requireStoredSidecars(t, store, fixture)
		})
	}
}

func TestFetchAndStoreFallsBackOnEveryNotServedAnswer(t *testing.T) {
	for name, notServed := range map[string]http.HandlerFunc{
		"bad request":    respondStatus(http.StatusBadRequest),
		"gone":           respondStatus(http.StatusGone),
		"empty response": respondData([]any{}),
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newBlockFixture(t, clparams.ElectraVersion, 1)
			store := blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
			blinded, _ := fixture.blindedLoader()

			require.NoError(t, FetchAndStore(t.Context(), store, serveBeacon(t, notServed, respondData(fixture.blobs())),
				fixture.root, fixture.promoted, blinded, nil))

			requireStoredSidecars(t, store, fixture)
		})
	}
}

func TestFetchAndStoreAsksBlobsOnlyWhenSidecarsAreNotServed(t *testing.T) {
	fixture := newBlockFixture(t, clparams.FuluVersion, 2)
	store := blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	blinded, loads := fixture.blindedLoader()
	// With the only slot taken, computing a proof would block until the timeout: served proofs are not recomputed.
	budget := semaphore.NewWeighted(1)
	require.True(t, budget.TryAcquire(1))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, FetchAndStore(ctx, store, serveBeacon(t, respondData(fixture.sidecars), nil), fixture.root, fixture.promoted, blinded, budget))

	require.Zero(t, *loads)
	requireStoredSidecars(t, store, fixture)
}

func TestFetchAndStoreDoesNotFallBackOnAPartialAnswer(t *testing.T) {
	fixture := newBlockFixture(t, clparams.ElectraVersion, 2)
	store := blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	blinded, loads := fixture.blindedLoader()

	err := FetchAndStore(t.Context(), store, serveBeacon(t, respondData(fixture.sidecars[:1]), nil), fixture.root, fixture.promoted, blinded, nil)

	require.Error(t, err)
	require.False(t, NotServed(err))
	require.Zero(t, *loads)
}

func TestFetchAndStoreDoesNotFallBackOnAFailure(t *testing.T) {
	defer func(wait time.Duration) { RetryWait = wait }(RetryWait)
	RetryWait = time.Millisecond
	fixture := newBlockFixture(t, clparams.ElectraVersion, 1)
	store := blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	blinded, loads := fixture.blindedLoader()

	err := FetchAndStore(t.Context(), store, serveBeacon(t, respondStatus(http.StatusInternalServerError), nil),
		fixture.root, fixture.promoted, blinded, nil)

	require.ErrorIs(t, err, StatusError(http.StatusInternalServerError))
	require.False(t, NotServed(err))
	require.Zero(t, *loads)
}

func TestFetchAndStoreReportsNotServedWhenNeitherEndpointServes(t *testing.T) {
	fixture := newBlockFixture(t, clparams.ElectraVersion, 1)
	store := blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	blinded, loads := fixture.blindedLoader()

	err := FetchAndStore(t.Context(), store, serveBeacon(t, respondStatus(http.StatusNotFound), respondStatus(http.StatusNotFound)),
		fixture.root, fixture.promoted, blinded, nil)

	require.ErrorIs(t, err, ErrNoSidecars)
	require.True(t, NotServed(err))
	require.Zero(t, *loads)
}

func TestFetchAndStoreReportsAGoneBlobsAnswerAsAFailure(t *testing.T) {
	fixture := newBlockFixture(t, clparams.ElectraVersion, 1)
	store := blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	blinded, _ := fixture.blindedLoader()
	var blobsRequests atomic.Int32
	gone := respondStatus(http.StatusGone)
	endpoint := serveBeacon(t, gone, func(w http.ResponseWriter, r *http.Request) {
		blobsRequests.Add(1)
		gone(w, r)
	})

	err := FetchAndStore(t.Context(), store, endpoint, fixture.root, fixture.promoted, blinded, nil)

	require.EqualValues(t, 1, blobsRequests.Load())
	require.ErrorIs(t, err, StatusError(http.StatusGone))
	require.False(t, NotServed(err))
}

func TestFetchAndStoreAsksOnlyBlobsFromGloas(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = 100
	var blob goethkzg.Blob
	blob[1] = 1
	commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 0)
	require.NoError(t, err)
	block.Block.Body.GetBlobKzgCommitments().Append((*cltypes.KZGCommitment)(&commitment))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	store := blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	endpoint := serveBeacon(t, nil, respondData([]*cltypes.Blob{(*cltypes.Blob)(&blob)}))

	require.NoError(t, FetchAndStore(t.Context(), store, endpoint, root, block, func(context.Context) (*cltypes.SignedBlindedBeaconBlock, error) {
		t.Error("a Gloas block has no blinded form")
		return nil, nil
	}, nil))

	stored, found, err := store.ReadBlobSidecars(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, stored, 1)
	require.Equal(t, common.Bytes48(commitment), stored[0].KzgCommitment)
}
