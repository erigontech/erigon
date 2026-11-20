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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
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

	require.NoError(t, FetchAndStore(t.Context(), store, serveSidecars(t, sidecars), root, block, budget))

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

			require.NoError(t, FetchAndStore(t.Context(), store, serveSidecars(t, sidecars), root, block, semaphore.NewWeighted(1)))

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
	go func() { done <- FetchAndStore(t.Context(), store, serveSidecars(t, sidecars), root, block, budget) }()
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
	} {
		resp := &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": {header}}}
		require.Equal(t, want, retryBackoff(time.Second, 30*time.Second, 0, resp), "Retry-After: %s", header)
	}
}

func TestFailedFetchKeepsDefaultTransportIdleConnections(t *testing.T) {
	defer func(wait time.Duration) { RetryWait = wait }(RetryWait)
	RetryWait = time.Millisecond
	shared := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(shared.Close)
	get := func() (reused bool) {
		ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
		})
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, shared.URL, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return reused
	}
	get()
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(failing.Close)

	_, err := getBeaconData(t.Context(), failing.URL, "blobs", 1)
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
