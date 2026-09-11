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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
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

	require.Error(t, endpoint.Run(&Context{Context: t.Context()}), "a head whose sidecars are unavailable must fail the download")
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
	require.Error(t, endpoint.Run(&Context{Context: t.Context()}))
	require.Zero(t, blobRequests, "sidecars were requested for a head with no commitments")

	slot := headSlotInDB(t, datadirPath, root)
	require.NotNil(t, slot, "a head with no blobs must still be committed")
	require.Equal(t, beaconConfig.DenebForkEpoch*beaconConfig.SlotsPerEpoch, *slot)
}
