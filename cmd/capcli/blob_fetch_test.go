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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func writeSlots(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "slots.txt")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// A list the tool cannot parse must stop the run. Skipping the line instead would make a
// truncated list look like a short one, and the operator would think those slots were fine.
func TestReadSlotsFileRefusesAMalformedLine(t *testing.T) {
	_, err := readSlotsFile(writeSlots(t, "29405015\nnot-a-slot\n29405055\n"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "not-a-slot")
	require.Contains(t, err.Error(), ":2:", "the error must name the line")
}

func TestReadSlotsFileAcceptsTheProbeOutputFormat(t *testing.T) {
	// The gap lists we generate carry a second column (the wanted sidecar count), plus
	// comments and blank lines when hand-edited.
	slots, err := readSlotsFile(writeSlots(t, "# gating slots\n29405015 1\n\n29405055 2\n29405015 1\n"))
	require.NoError(t, err)
	require.Equal(t, []uint64{29405015, 29405055}, slots, "duplicates must collapse, order must hold")
}

func TestSplitEndpoints(t *testing.T) {
	require.Equal(t,
		[]string{"http://a:5555", "https://b"},
		splitEndpoints(" http://a:5555/ , https://b , "))
	require.Empty(t, splitEndpoints(" , "))
}

// "this endpoint does not have it" and "this endpoint is broken" must not look the same: the
// first is a fact about the data, the second is the operator's to fix. The callers turn both
// into skip-and-try-the-next, so the distinction has to be pinned where it is made.
func TestBeaconAPIGetDistinguishesAbsentFromBroken(t *testing.T) {
	var status int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		if status == http.StatusOK {
			_, _ = w.Write([]byte(`{"data":{"root":"0x1234"}}`))
		}
	}))
	defer srv.Close()
	src := &beaconAPISource{endpoints: []string{srv.URL}, client: srv.Client()}
	var body struct {
		Data struct {
			Root string `json:"root"`
		} `json:"data"`
	}

	status = http.StatusNotFound
	ok, err := src.get(t.Context(), srv.URL, &body)
	require.NoError(t, err, "a 404 is an answer: the endpoint does not hold it")
	require.False(t, ok)

	status = http.StatusTooManyRequests
	ok, err = src.get(t.Context(), srv.URL, &body)
	require.Error(t, err, "a rate limit or server error is a fault, not an answer")
	require.False(t, ok)

	status = http.StatusOK
	ok, err = src.get(t.Context(), srv.URL, &body)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "0x1234", body.Data.Root)
}

func TestBeaconAPISourceReadsTheHeaderRoot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/eth/v1/beacon/headers/29405015", r.URL.Path)
		_, _ = w.Write([]byte(`{"data":{"root":"0x1234"}}`))
	}))
	defer srv.Close()
	src := &beaconAPISource{endpoints: []string{srv.URL}, client: srv.Client()}

	root, ok, err := src.headerRoot(t.Context(), 29405015)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, common.HexToHash("0x1234"), root)
}

// Coverage is complementary across endpoints, so an empty or failing first endpoint must not
// end the search.
func TestBeaconAPISourceFallsThroughToTheNextEndpoint(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer first.Close()
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer broken.Close()

	var hits int
	last := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		require.Contains(t, r.URL.Path, "/eth/v1/beacon/blob_sidecars/0x")
		w.WriteHeader(http.StatusNotFound)
	}))
	defer last.Close()

	src := &beaconAPISource{
		endpoints: []string{first.URL, broken.URL, last.URL},
		client:    first.Client(),
	}
	sidecars, err := src.sidecars(t.Context(), common.HexToHash("0xabc"))
	require.NoError(t, err)
	require.Empty(t, sidecars)
	require.Equal(t, 1, hits, "the empty and the erroring endpoint must both be passed over")
}

func TestBlobFetchTallyCountsOnlyRealFailures(t *testing.T) {
	// A slot with no blobs, or one already complete, is not a failure: counting it as one
	// would make a healthy range look broken and mask the slots that do need attention.
	tally := blobFetchTally{filled: 3, alreadyOk: 2, noBlobs: 5, wouldFill: 1}
	require.Zero(t, tally.failures())

	tally.unserved = 1
	tally.rootDiff = 1
	tally.incomplete = 1
	tally.rejected = 1
	require.Equal(t, 4, tally.failures())
}
