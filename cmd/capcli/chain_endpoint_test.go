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
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

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
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/execution/chain/networkname"
)

// denebHeadBlock builds a head block at the Deneb fork, carrying commitments KZG commitments. A
// head with commitments has to have its blob sidecars fetched before it can be committed.
func denebHeadBlock(t *testing.T, beaconConfig *clparams.BeaconChainConfig, commitments int) (common.Hash, []byte) {
	t.Helper()

	slot := beaconConfig.DenebForkEpoch * beaconConfig.SlotsPerEpoch
	require.Equal(t, clparams.DenebVersion, beaconConfig.GetCurrentStateVersion(slot/beaconConfig.SlotsPerEpoch))

	block := cltypes.NewSignedBeaconBlock(beaconConfig, clparams.DenebVersion)
	block.Block.Slot = slot
	block.Block.ParentRoot = common.HexToHash("0xbeef")
	for range commitments {
		block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	}

	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)

	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	return root, encoded
}

// headServer serves the given head block and 404s everything else, counting blob requests.
func headServer(t *testing.T, encoded []byte, blobRequests *int) string {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/eth/v2/beacon/blocks/head"):
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(encoded)
		case strings.Contains(r.URL.Path, "/blob_sidecars/"):
			*blobRequests++
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func headSlotInDB(t *testing.T, datadirPath string, root common.Hash) *uint64 {
	t.Helper()

	dirs := datadir.New(datadirPath)
	// Only the indexing database: Run closes it on return, while the blob database it opens
	// stays held until the context is done.
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	defer db.Close()

	var slot *uint64
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		var err error
		slot, err = beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		return err
	}))
	return slot
}

// The bootstrap head takes a different path from every other block: ChainEndpoint.Run fetches it,
// writes it and commits before the loop starts. With blobs requested, a blob-bearing head must not
// be committed unless its sidecars are stored, or the download reports success over a block whose
// blobs nothing will look for again.
func TestChainEndpointRunDoesNotCommitABlobBearingHeadWithoutItsSidecars(t *testing.T) {
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(networkname.Mainnet)
	require.NoError(t, err)

	root, encoded := denebHeadBlock(t, beaconConfig, 1)

	var blobRequests int
	datadirPath := t.TempDir()
	endpoint := &ChainEndpoint{
		Endpoint:     headServer(t, encoded, &blobRequests),
		Blobs:        true,
		chainCfg:     chainCfg{Chain: networkname.Mainnet},
		outputFolder: outputFolder{Datadir: datadirPath},
	}

	require.Error(t, endpoint.Run(t.Context()), "a head whose sidecars are unavailable must fail the download")
	require.Positive(t, blobRequests, "the head's sidecars were never requested")
	require.Nil(t, headSlotInDB(t, datadirPath, root), "the head was committed without its blob sidecars")
}

// The head still has to be committed when it carries no commitments, otherwise routing it through
// the blob path would stall every download that has no blobs to fetch.
func TestChainEndpointRunCommitsAHeadWithNoCommitments(t *testing.T) {
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(networkname.Mainnet)
	require.NoError(t, err)

	root, encoded := denebHeadBlock(t, beaconConfig, 0)

	var blobRequests int
	datadirPath := t.TempDir()
	endpoint := &ChainEndpoint{
		Endpoint:     headServer(t, encoded, &blobRequests),
		Blobs:        true,
		chainCfg:     chainCfg{Chain: networkname.Mainnet},
		outputFolder: outputFolder{Datadir: datadirPath},
	}

	// Run walks back from the head and the parent is not served, so it stops with an error; what
	// matters is that the head itself was handed off to the loop.
	require.Error(t, endpoint.Run(t.Context()))
	require.Zero(t, blobRequests, "sidecars were requested for a head with no commitments")

	slot := headSlotInDB(t, datadirPath, root)
	require.NotNil(t, slot, "a head with no blobs must still be committed")
	require.Equal(t, beaconConfig.DenebForkEpoch*beaconConfig.SlotsPerEpoch, *slot)
}

// The walk skips a block already in the local database when its blobs are stored, and fetches it
// from the endpoint only when they are missing. The walk reads the local block from storage, which
// keeps no transactions, so the block's own hash is not the root its sidecars are stored under.
// Using that hash, every local blob-bearing block with transactions looked incomplete and was
// downloaded again, with its blobs, from the endpoint.
func TestChainEndpointRunSkipsALocalBlockWhoseBlobsAreStored(t *testing.T) {
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(networkname.Mainnet)
	require.NoError(t, err)
	slot := beaconConfig.DenebForkEpoch * beaconConfig.SlotsPerEpoch

	newBlock := func(slot uint64, parent common.Hash, commitments int) *cltypes.SignedBeaconBlock {
		block := cltypes.NewSignedBeaconBlock(beaconConfig, clparams.DenebVersion)
		block.Block.Slot = slot
		block.Block.ParentRoot = parent
		for range commitments {
			block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
		}
		return block
	}
	rootOf := func(block *cltypes.SignedBeaconBlock) common.Hash {
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		return root
	}
	local := newBlock(slot, common.HexToHash("0xbeef"), 1)
	local.Block.Body.ExecutionPayload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{0x02, 0x01}, {0x02, 0x02}})
	localRoot := rootOf(local)
	parent := newBlock(slot+1, localRoot, 0)
	head := newBlock(slot+2, rootOf(parent), 0)

	datadirPath := t.TempDir()
	dirs := datadir.New(datadirPath)
	indexDB, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	require.NoError(t, indexDB.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteBeaconBlockAndIndicies(tx, local, true)
	}))
	indexDB.Close()
	blobDB := mdbx.New(dbcfg.CaplinDB, log.New()).Path(filepath.Join(dirs.CaplinBlobs, "chaindata")).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.ChaindataTablesCfg }).MustOpen()
	sidecar := cltypes.NewBlobSidecar(0, &cltypes.Blob{}, common.Bytes48{}, common.Bytes48{}, local.SignedBeaconBlockHeader(), solid.NewHashVector(cltypes.CommitmentBranchSize))
	err = blob_storage.NewBlobStore(blobDB, afero.NewBasePathFs(afero.NewOsFs(), dirs.CaplinBlobs)).
		WriteBlobSidecars(t.Context(), localRoot, []*cltypes.BlobSidecar{sidecar})
	blobDB.Close()
	require.NoError(t, err)

	served := map[string][]byte{"head": encodeBlock(t, head), fmt.Sprintf("0x%x", rootOf(parent)): encodeBlock(t, parent)}
	var localRequests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		if id == fmt.Sprintf("0x%x", localRoot) {
			localRequests.Add(1)
		}
		if encoded, ok := served[id]; ok && strings.Contains(r.URL.Path, "/eth/v2/beacon/blocks/") {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(encoded)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	endpoint := &ChainEndpoint{
		Endpoint:     srv.URL,
		Blobs:        true,
		chainCfg:     chainCfg{Chain: networkname.Mainnet},
		outputFolder: outputFolder{Datadir: datadirPath},
	}
	// The walk passes the local block and ends at its parent, which nobody serves.
	require.ErrorContains(t, endpoint.Run(t.Context()), fmt.Sprintf("0x%x", local.Block.ParentRoot))
	require.Zero(t, localRequests.Load(), "the local block, whose blobs are stored, was requested from the endpoint")
}

func encodeBlock(t *testing.T, block *cltypes.SignedBeaconBlock) []byte {
	t.Helper()
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	return encoded
}
