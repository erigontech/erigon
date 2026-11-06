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

// denebBlock builds a Deneb block at slot on parent, carrying commitments KZG commitments.
func denebBlock(t *testing.T, beaconConfig *clparams.BeaconChainConfig, slot uint64, parent common.Hash, commitments int) *cltypes.SignedBeaconBlock {
	t.Helper()
	require.Equal(t, clparams.DenebVersion, beaconConfig.GetCurrentStateVersion(slot/beaconConfig.SlotsPerEpoch))
	block := cltypes.NewSignedBeaconBlock(beaconConfig, clparams.DenebVersion)
	block.Block.Slot = slot
	block.Block.ParentRoot = parent
	for range commitments {
		block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	}
	return block
}

func blockRootOf(t *testing.T, block *cltypes.SignedBeaconBlock) common.Hash {
	t.Helper()
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	return root
}

func encodeBlock(t *testing.T, block *cltypes.SignedBeaconBlock) []byte {
	t.Helper()
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	return encoded
}

// denebHeadBlock builds a head block at the Deneb fork, carrying commitments KZG commitments. A
// head with commitments has to have its blob sidecars fetched before it can be committed.
func denebHeadBlock(t *testing.T, beaconConfig *clparams.BeaconChainConfig, commitments int) (common.Hash, []byte) {
	t.Helper()
	block := denebBlock(t, beaconConfig, beaconConfig.DenebForkEpoch*beaconConfig.SlotsPerEpoch, common.HexToHash("0xbeef"), commitments)
	return blockRootOf(t, block), encodeBlock(t, block)
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

// The walk goes past a local block whose blobs are all stored, and fetches the block from the
// endpoint when a blob is missing, also when pruning kept its count row. The local block carries
// execution transactions, which the stored body does not, so its blobs have to be looked up under
// the walked root.
func TestChainEndpointRunFetchesALocalBlockOnlyWhenItsBlobsAreMissing(t *testing.T) {
	for name, tc := range map[string]struct {
		store, prune bool
		fetched      bool
	}{
		"stored":  {store: true},
		"missing": {fetched: true},
		"pruned":  {store: true, prune: true, fetched: true},
	} {
		t.Run(name, func(t *testing.T) {
			_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(networkname.Mainnet)
			require.NoError(t, err)
			slot := beaconConfig.DenebForkEpoch * beaconConfig.SlotsPerEpoch
			local := denebBlock(t, beaconConfig, slot, common.HexToHash("0xbeef"), 1)
			local.Block.Body.ExecutionPayload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{0x02, 0x01}, {0x02, 0x02}})
			localRoot := blockRootOf(t, local)
			parent := denebBlock(t, beaconConfig, slot+1, localRoot, 0)
			head := denebBlock(t, beaconConfig, slot+2, blockRootOf(t, parent), 0)

			datadirPath := t.TempDir()
			dirs := datadir.New(datadirPath)
			indexDB, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
			require.NoError(t, err)
			require.NoError(t, indexDB.Update(t.Context(), func(tx kv.RwTx) error {
				return beacon_indicies.WriteBeaconBlockAndIndicies(tx, local, true)
			}))
			indexDB.Close()
			if tc.store {
				blobDB := mdbx.New(dbcfg.CaplinDB, log.New()).Path(filepath.Join(dirs.CaplinBlobs, "chaindata")).
					WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.ChaindataTablesCfg }).MustOpen()
				store := blob_storage.NewBlobStore(blobDB, afero.NewBasePathFs(afero.NewOsFs(), dirs.CaplinBlobs))
				sidecar := cltypes.NewBlobSidecar(0, &cltypes.Blob{}, common.Bytes48{}, common.Bytes48{}, local.SignedBeaconBlockHeader(), solid.NewHashVector(cltypes.CommitmentBranchSize))
				err = store.WriteBlobSidecars(t.Context(), localRoot, []*cltypes.BlobSidecar{sidecar})
				if err == nil && tc.prune {
					err = store.PruneBelow(slot * 2)
				}
				blobDB.Close()
				require.NoError(t, err)
			}

			served := map[string][]byte{"head": encodeBlock(t, head), fmt.Sprintf("0x%x", blockRootOf(t, parent)): encodeBlock(t, parent)}
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
			// Nobody serves the local block or its parent, so the walk ends at the first one it asks for.
			err = endpoint.Run(t.Context())
			if tc.fetched {
				require.ErrorContains(t, err, fmt.Sprintf("0x%x", localRoot))
				require.EqualValues(t, 1, localRequests.Load())
			} else {
				require.ErrorContains(t, err, fmt.Sprintf("0x%x", local.Block.ParentRoot))
				require.Zero(t, localRequests.Load())
			}
		})
	}
}
