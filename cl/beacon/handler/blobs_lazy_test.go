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

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das"
	das_mock "github.com/erigontech/erigon/cl/das/mock_services"
	das_state_mock "github.com/erigontech/erigon/cl/das/state/mock_services"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

type lazyBlobsFixture struct {
	blobsTestFixture
	peerDas *das_mock.MockPeerDas
	root    common.Hash
}

// setupLazyBlobsTest is a node in lazy blob mode: its blob storage is empty, and PeerDas builds blobs from columns.
// With gloas the block is a Gloas block.
func setupLazyBlobsTest(t *testing.T, gloas bool) lazyBlobsFixture {
	t.Helper()
	return lazyBlobsOf(t, setupBlobsTest(t), gloas)
}

func lazyBlobsOf(t *testing.T, f blobsTestFixture, gloas bool) lazyBlobsFixture {
	t.Helper()
	cfg := f.handler.beaconChainCfg
	cfg.FuluForkEpoch = 1
	if gloas {
		cfg.GloasForkEpoch = 1
	}
	require.GreaterOrEqual(t, f.slot/cfg.SlotsPerEpoch, uint64(1))

	peerDas := newLazyPeerDas(t)
	f.handler.peerDas = peerDas

	var root common.Hash
	require.NoError(t, f.handler.indiciesDB.View(t.Context(), func(tx kv.Tx) (err error) {
		root, err = beacon_indicies.ReadCanonicalBlockRoot(tx, f.slot)
		return err
	}))
	return lazyBlobsFixture{blobsTestFixture: f, peerDas: peerDas, root: root}
}

func newLazyPeerDas(t *testing.T) *das_mock.MockPeerDas {
	t.Helper()
	ctrl := gomock.NewController(t)
	stateReader := das_state_mock.NewMockPeerDasStateReader(ctrl)
	stateReader.EXPECT().IsSupernode().Return(true).AnyTimes()
	peerDas := das_mock.NewMockPeerDas(ctrl)
	peerDas.EXPECT().IsArchivedMode().Return(false).AnyTimes()
	peerDas.EXPECT().StateReader().Return(stateReader).AnyTimes()
	return peerDas
}

func columnBlobs(first byte, n int) []*cltypes.Blob {
	blobs := make([]*cltypes.Blob, n)
	for i := range blobs {
		blobs[i] = &cltypes.Blob{first + byte(i)}
	}
	return blobs
}

func columnSidecars(indices ...uint64) []*cltypes.BlobSidecar {
	sidecars := make([]*cltypes.BlobSidecar, len(indices))
	for i, index := range indices {
		sidecars[i] = &cltypes.BlobSidecar{Index: index, Blob: cltypes.Blob{byte(index + 1)}}
	}
	return sidecars
}

type lazyResponse struct {
	status  int
	data    []json.RawMessage
	message string
}

func requestLazyBlobEndpoint(t *testing.T, f lazyBlobsFixture, endpoint, query string) lazyResponse {
	t.Helper()
	resp := requestBlobEndpoint(t, f.blobsTestFixture, endpoint, query)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	out := lazyResponse{status: resp.StatusCode}
	var envelope struct {
		Data    []json.RawMessage `json:"data"`
		Message string            `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body, &envelope), string(body))
	out.data, out.message = envelope.Data, envelope.Message
	return out
}

func sidecarIndices(t *testing.T, data []json.RawMessage) []string {
	t.Helper()
	indices := make([]string, len(data))
	for i := range data {
		var sidecar struct {
			Index string `json:"index"`
		}
		require.NoError(t, json.Unmarshal(data[i], &sidecar))
		indices[i] = sidecar.Index
	}
	return indices
}

func TestBlobEndpointsDoNotBuildBlobsFromColumnsWhenStorageHasThem(t *testing.T) {
	f := setupLazyBlobsTest(t, false)
	f.handler.caplinSnapshots = frozenBlobSnapshotReader{
		frozenBlobsExclusive: f.slot + 1,
		sidecars:             []*cltypes.BlobSidecar{{Index: 0, Blob: cltypes.Blob{1}}, {Index: 1, Blob: cltypes.Blob{2}}, {Index: 2, Blob: cltypes.Blob{3}}},
	}

	for _, endpoint := range []string{"blob_sidecars", "blobs"} {
		t.Run(endpoint, func(t *testing.T) {
			out := requestLazyBlobEndpoint(t, f, endpoint, "")

			require.Equal(t, http.StatusOK, out.status)
			require.Len(t, out.data, 3)
		})
	}
}

func TestBlobSidecarsAreBuiltFromColumnsWhenStorageIsEmpty(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		asked []uint64
		want  []uint64
	}{
		{name: "every index of the block", asked: nil, want: []uint64{0, 1, 2}},
		{name: "the selected indices in ascending order", query: "indices=2,0", asked: []uint64{0, 2}, want: []uint64{0, 2}},
		{name: "the selected indices below the blob count", query: "indices=7,1,3", asked: []uint64{1, 3, 7}, want: []uint64{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := setupLazyBlobsTest(t, false)
			var asked []uint64
			var withSidecars bool
			f.peerDas.EXPECT().BlobsFromColumns(gomock.Any(), f.slot, f.root, gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ uint64, _ common.Hash, indices []uint64, sidecars bool) ([]*cltypes.Blob, []*cltypes.BlobSidecar, error) {
					asked, withSidecars = indices, sidecars
					return nil, columnSidecars(tc.want...), nil
				})

			out := requestLazyBlobEndpoint(t, f, "blob_sidecars", tc.query)

			require.Equal(t, http.StatusOK, out.status)
			require.True(t, withSidecars)
			require.Equal(t, tc.asked, asked)
			want := make([]string, len(tc.want))
			for i, index := range tc.want {
				want[i] = strconv.FormatUint(index, 10)
			}
			require.Equal(t, want, sidecarIndices(t, out.data))
		})
	}
}

func TestBlobsAreBuiltFromColumnsWhenStorageIsEmpty(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query func(f lazyBlobsFixture) string
		want  []uint64
		blobs []*cltypes.Blob
	}{
		{name: "every blob", query: func(lazyBlobsFixture) string { return "" }, want: []uint64{0, 1, 2}, blobs: columnBlobs(1, 3)},
		{name: "the blobs of the versioned hashes in commitment order", query: func(f lazyBlobsFixture) string {
			return "versioned_hashes=" + f.versionedHashes[2].Hex() + "," + f.versionedHashes[0].Hex()
		}, want: []uint64{0, 2}, blobs: []*cltypes.Blob{{1}, {3}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := setupLazyBlobsTest(t, false)
			f.peerDas.EXPECT().BlobsFromColumns(gomock.Any(), f.slot, f.root, tc.want, false).Return(tc.blobs, nil, nil)

			out := requestLazyBlobEndpoint(t, f, "blobs", tc.query(f))

			require.Equal(t, http.StatusOK, out.status)
			require.Len(t, out.data, len(tc.blobs))
			for i := range tc.blobs {
				var hex string
				require.NoError(t, json.Unmarshal(out.data[i], &hex))
				require.True(t, strings.HasPrefix(hex, fmt.Sprintf("0x%02x", tc.blobs[i][0])), "blob %d: %s", i, hex[:6])
			}
		})
	}
}

func TestBlobEndpointsReportBlobsThatCannotBeBuiltFromColumns(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		status  int
		message string
	}{
		{name: "the node keeps no columns", err: das.ErrColumnsNotKept, status: http.StatusOK},
		{name: "a column is unavailable", err: das.ErrBlobsUnavailable, status: http.StatusNotFound, message: "blob data not available, retry later"},
		{name: "reconstruction is busy", err: das.ErrReconstructionBusy, status: http.StatusInternalServerError, message: "blob reconstruction busy, retry later"},
		{name: "another error", err: errors.New("storage failed"), status: http.StatusInternalServerError, message: "storage failed"},
	} {
		for _, endpoint := range []string{"blob_sidecars", "blobs"} {
			t.Run(tc.name+"/"+endpoint, func(t *testing.T) {
				f := setupLazyBlobsTest(t, false)
				f.peerDas.EXPECT().BlobsFromColumns(gomock.Any(), f.slot, f.root, gomock.Any(), gomock.Any()).Return(nil, nil, tc.err)

				out := requestLazyBlobEndpoint(t, f, endpoint, "")

				require.Equal(t, tc.status, out.status)
				require.Empty(t, out.data)
				require.Contains(t, out.message, tc.message)
			})
		}
	}
}

func TestBlobsAnswerEmptyWithoutBuildingBlobsWhenNoBlobIsSelected(t *testing.T) {
	threeBlobs := []cltypes.KZGCommitment{{69}, {1}, {2}}
	for _, tc := range []struct {
		name        string
		commitments []cltypes.KZGCommitment
		query       string
	}{
		{name: "a block without blobs"},
		{name: "a versioned hash that no commitment has", commitments: threeBlobs, query: "versioned_hashes=" + common.Hash{0x42}.Hex()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := lazyBlobsOf(t, setupBlobsTestWithCommitments(t, tc.commitments), false)
			f.peerDas.EXPECT().BlobsFromColumns(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			out := requestLazyBlobEndpoint(t, f, "blobs", tc.query)

			require.Equal(t, http.StatusOK, out.status)
			require.Empty(t, out.data)
		})
	}
}

func TestBlobSidecarsAnswerEmptyWhenNoBlobIsBuiltFromColumns(t *testing.T) {
	f := setupLazyBlobsTest(t, false)
	f.peerDas.EXPECT().BlobsFromColumns(gomock.Any(), f.slot, f.root, nil, true).Return(nil, nil, nil)

	out := requestLazyBlobEndpoint(t, f, "blob_sidecars", "")

	require.Equal(t, http.StatusOK, out.status)
	require.NotNil(t, out.data)
	require.Empty(t, out.data)
}

func TestBlobEndpointsOfAGloasBlockBuildBlobsOnly(t *testing.T) {
	f := setupLazyBlobsTest(t, true)
	f.peerDas.EXPECT().BlobsFromColumns(gomock.Any(), f.slot, f.root, []uint64{0, 1, 2}, false).Return(columnBlobs(1, 3), nil, nil)

	blobs := requestLazyBlobEndpoint(t, f, "blobs", "")
	require.Equal(t, http.StatusOK, blobs.status)
	require.Len(t, blobs.data, 3)

	sidecars := requestLazyBlobEndpoint(t, f, "blob_sidecars", "")
	require.Equal(t, http.StatusOK, sidecars.status)
	require.Empty(t, sidecars.data)
}

func TestBlobEndpointsCloseTheTransactionBeforeBuildingBlobsFromColumns(t *testing.T) {
	for _, endpoint := range []string{"blob_sidecars", "blobs"} {
		t.Run(endpoint, func(t *testing.T) {
			f := setupLazyBlobsTest(t, false)
			db := &readTrackingDB{RwDB: f.handler.indiciesDB}
			f.handler.indiciesDB = db
			openReads := int32(-1)
			f.peerDas.EXPECT().BlobsFromColumns(gomock.Any(), f.slot, f.root, gomock.Any(), gomock.Any()).
				DoAndReturn(func(context.Context, uint64, common.Hash, []uint64, bool) ([]*cltypes.Blob, []*cltypes.BlobSidecar, error) {
					openReads = db.open.Load()
					return columnBlobs(1, 3), columnSidecars(0, 1, 2), nil
				})

			out := requestLazyBlobEndpoint(t, f, endpoint, "")

			require.Equal(t, http.StatusOK, out.status)
			require.Zero(t, openReads, "open read transactions while the blobs are built")
		})
	}
}
