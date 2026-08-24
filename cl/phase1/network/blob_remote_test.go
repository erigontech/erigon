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

package network

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// sidecarServer serves n sidecars for any root, recording the paths it was asked for.
func sidecarServer(t *testing.T, n int, status int) (*httptest.Server, *[]string) {
	t.Helper()
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.Path)
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		data := make([]*cltypes.BlobSidecar, 0, n)
		for i := range n {
			s := &cltypes.BlobSidecar{}
			s.Index = uint64(i)
			data = append(data, s)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Data []*cltypes.BlobSidecar `json:"data"`
		}{Data: data})
	}))
	t.Cleanup(srv.Close)
	return srv, &seen
}

func TestRemoteBlobSourceFetchesFromTheFirstEndpointThatHasThem(t *testing.T) {
	srv, seen := sidecarServer(t, 2, http.StatusOK)

	src := newRemoteBlobSource([]string{srv.URL}, log.New())
	got, err := src.fetch(context.Background(), common.Hash{0xab})

	require.NoError(t, err)
	require.Len(t, got, 2)
	require.True(t, strings.HasPrefix((*seen)[0], "/eth/v1/beacon/blob_sidecars/0x"),
		"unexpected path %q", (*seen)[0])
}

// A source that does not have the block must not end the search: coverage is stitched
// across endpoints, which is the whole reason the config is a list.
func TestRemoteBlobSourceFallsThroughOnNotFound(t *testing.T) {
	missing, _ := sidecarServer(t, 0, http.StatusNotFound)
	holder, holderSeen := sidecarServer(t, 1, http.StatusOK)

	src := newRemoteBlobSource([]string{missing.URL, holder.URL}, log.New())
	got, err := src.fetch(context.Background(), common.Hash{0xcd})

	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, *holderSeen, 1, "second endpoint was not consulted")
}

// An endpoint that is down or erroring is a transport problem, not an answer, so the
// search continues rather than reporting the block unavailable.
func TestRemoteBlobSourceFallsThroughOnServerError(t *testing.T) {
	broken, _ := sidecarServer(t, 0, http.StatusInternalServerError)
	holder, _ := sidecarServer(t, 1, http.StatusOK)

	src := newRemoteBlobSource([]string{broken.URL, holder.URL}, log.New())
	got, err := src.fetch(context.Background(), common.Hash{0xef})

	require.NoError(t, err)
	require.Len(t, got, 1)
}

// An empty data array is a real answer meaning "this block has no blobs", but it is
// indistinguishable from "I do not have them", so it must not stop the search either.
func TestRemoteBlobSourceTreatsAnEmptySetAsNoAnswer(t *testing.T) {
	empty, _ := sidecarServer(t, 0, http.StatusOK)
	holder, holderSeen := sidecarServer(t, 1, http.StatusOK)

	src := newRemoteBlobSource([]string{empty.URL, holder.URL}, log.New())
	got, err := src.fetch(context.Background(), common.Hash{0x11})

	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, *holderSeen, 1)
}

func TestRemoteBlobSourceReturnsNothingWhenNoEndpointHasThem(t *testing.T) {
	a, _ := sidecarServer(t, 0, http.StatusNotFound)
	b, _ := sidecarServer(t, 0, http.StatusNotFound)

	src := newRemoteBlobSource([]string{a.URL, b.URL}, log.New())
	got, err := src.fetch(context.Background(), common.Hash{0x22})

	require.NoError(t, err)
	require.Empty(t, got)
}

// No endpoints configured is the default, and must stay inert rather than erroring.
func TestRemoteBlobSourceIsInertWithoutEndpoints(t *testing.T) {
	src := newRemoteBlobSource(nil, log.New())
	require.False(t, src.enabled())

	got, err := src.fetch(context.Background(), common.Hash{0x33})
	require.NoError(t, err)
	require.Empty(t, got)
}
