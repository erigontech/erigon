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
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/kong"
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
	sidecars, err := src.sidecars(t.Context(), 14300002, common.HexToHash("0xabc"))
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

// Pacing is driven by this counter: the loop sleeps only when a slot actually contacted an
// endpoint. Without it the pause applies to every slot, and a range that is 97% blobless takes
// days instead of minutes.
func TestBeaconAPISourceCountsOnlyOutboundRequests(t *testing.T) {
	var served int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"root":"0x1234"}}`))
	}))
	defer srv.Close()

	src := &beaconAPISource{endpoints: []string{srv.URL}, client: srv.Client()}
	require.Zero(t, src.requests, "a source that has not been used must report no requests")

	_, ok, err := src.headerRoot(t.Context(), 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, served, src.requests, "every outbound request must be counted")

	before := src.requests
	_, _, err = src.headerRoot(t.Context(), 2)
	require.NoError(t, err)
	require.Greater(t, src.requests, before, "a second fetch must advance the counter")
}

// --from is the only way to repair a range: without it the dump starts at the Deneb fork and
// rewrites every segment from a store that no longer holds the already-frozen blobs.
func TestDumpBlobsSnapshotsFromMustBeOnASegmentBoundary(t *testing.T) {
	var cli struct {
		DumpBlobsSnapshots DumpBlobsSnapshots `cmd:""`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)

	_, err = parser.Parse([]string{"dump-blobs-snapshots", "--datadir", t.TempDir(), "--from", "28880000", "--to", "30060000"})
	require.NoError(t, err, "the command must accept --from")
	require.Equal(t, uint64(28880000), cli.DumpBlobsSnapshots.From)
	require.Equal(t, uint64(30060000), cli.DumpBlobsSnapshots.To)

	var defaults struct {
		DumpBlobsSnapshots DumpBlobsSnapshots `cmd:""`
	}
	dp, err := kong.New(&defaults)
	require.NoError(t, err)
	_, err = dp.Parse([]string{"dump-blobs-snapshots", "--datadir", t.TempDir()})
	require.NoError(t, err)
	require.Zero(t, defaults.DumpBlobsSnapshots.From, "omitting --from must keep the fork-boundary default")
}

// A public endpoint answers a long run with 429 or a 5xx sooner or later. Treating that as
// fatal aborts the whole run, so it must be retried the same way archive mode already does.
func TestBeaconAPIGetRetriesThrottlingAndServerErrors(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch calls {
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
		case 2:
			w.WriteHeader(http.StatusBadGateway)
		default:
			w.Write([]byte(`{"data":{"root":"0x01"}}`))
		}
	}))
	defer srv.Close()

	src := &beaconAPISource{endpoints: []string{srv.URL}, client: srv.Client(), maxAttempts: 5}
	var out struct {
		Data struct {
			Root string `json:"root"`
		} `json:"data"`
	}
	ok, err := src.get(t.Context(), srv.URL, &out)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 3, calls, "both retryable statuses must be retried")
	require.Equal(t, "0x01", out.Data.Root)
}

// A 404 is an answer, not a failure, and retrying it would multiply the cost of every absent
// slot across a 200k-slot run.
func TestBeaconAPIGetDoesNotRetryA404(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	src := &beaconAPISource{endpoints: []string{srv.URL}, client: srv.Client(), maxAttempts: 5}
	ok, err := src.get(t.Context(), srv.URL, &struct{}{})
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 1, calls, "a 404 must not be retried")
}

// Exhausting the attempts must surface the status, never a silent "absent" that would let the
// run record the slot as unobtainable when the endpoint was merely throttling.
func TestBeaconAPIGetGivesUpWithTheStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	src := &beaconAPISource{endpoints: []string{srv.URL}, client: srv.Client(), maxAttempts: 2}
	ok, err := src.get(t.Context(), srv.URL, &struct{}{})
	require.Error(t, err)
	require.False(t, ok)
	require.ErrorContains(t, err, "503")
}

// A zero value must still make one attempt: the struct is built directly in several places.
func TestBeaconAPIGetMakesOneAttemptWhenUnconfigured(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	src := &beaconAPISource{endpoints: []string{srv.URL}, client: srv.Client()}
	ok, err := src.get(t.Context(), srv.URL, &struct{}{})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, calls)
}

// The point of the file is to hand the leftover to another source, so it has to be readable
// back by the same parser that reads the input slots file.
func TestWriteRemainingSlotsRoundTripsThroughTheSlotsFileReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remaining.txt")
	require.NoError(t, writeRemainingSlots(path, []uint64{14300007, 14300001, 14300004}))

	slots, err := readSlotsFile(path)
	require.NoError(t, err)
	require.Equal(t, []uint64{14300001, 14300004, 14300007}, slots, "written sorted and re-readable")
}

// Writing an empty file would make a clean run look like it left work behind.
func TestWriteRemainingSlotsWritesNothingWhenAllFilled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remaining.txt")
	require.NoError(t, writeRemainingSlots(path, nil))
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "no file when there is nothing left to do")
}

// A slot no endpoint could serve is exactly what another source has to pick up.
func TestTallyRecordsUnservedSlotsAsRemaining(t *testing.T) {
	var tally blobFetchTally
	tally.record(14300001)
	tally.record(14300002)
	require.Equal(t, []uint64{14300001, 14300002}, tally.remaining)
}

// Recording is driven by the failure count rather than by each of the sixteen tally sites, so
// a newly added failure kind cannot silently omit its slot from the remaining file.
func TestTallyRecordsASlotOnlyWhenItFailed(t *testing.T) {
	var tally blobFetchTally

	before := tally.failures()
	tally.filled++
	tally.recordIfFailed(14300001, before)
	require.Empty(t, tally.remaining, "a filled slot is not remaining")

	before = tally.failures()
	tally.unserved++
	tally.recordIfFailed(14300002, before)
	require.Equal(t, []uint64{14300002}, tally.remaining)

	before = tally.failures()
	tally.rejected++
	tally.recordIfFailed(14300003, before)
	require.Equal(t, []uint64{14300002, 14300003}, tally.remaining, "every failure kind counts")

	before = tally.failures()
	tally.missed++
	tally.recordIfFailed(14300004, before)
	require.Equal(t, []uint64{14300002, 14300003}, tally.remaining, "a missed slot is not a failure")
}

// A 403 or 400 will not fix itself. Retrying it would burn every attempt on each of 200k slots
// and bury the real cause (a bad key, a wrong path) under backoff noise.
func TestBeaconAPIGetDoesNotRetryANonRetryable4xx(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	src := &beaconAPISource{endpoints: []string{srv.URL}, client: srv.Client(), maxAttempts: 5}
	ok, err := src.get(t.Context(), srv.URL, &struct{}{})
	require.Error(t, err)
	require.False(t, ok)
	require.ErrorContains(t, err, "403")
	require.Equal(t, 1, calls, "a 403 is final, not a throttle")
}

// A dropped connection mid-run is the common way a long job dies; it has to be retried like a
// 5xx, and every attempt has to be counted or the pacing logic stops seeing remote traffic.
func TestBeaconAPIGetRetriesATransportErrorAndCountsEveryAttempt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing is listening now, so every attempt fails at the transport

	src := &beaconAPISource{endpoints: []string{url}, client: &http.Client{}, maxAttempts: 3}
	ok, err := src.get(t.Context(), url, &struct{}{})
	require.Error(t, err)
	require.False(t, ok)
	require.Equal(t, 3, src.requests, "each attempt must count, including the failed ones")
}

// Interrupting a multi-hour run must stop it, not leave it sleeping out its backoff.
func TestBeaconAPIGetStopsOnContextCancellationDuringBackoff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(t.Context())
	src := &beaconAPISource{endpoints: []string{srv.URL}, client: srv.Client(), maxAttempts: 100}
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	started := time.Now()
	_, err := src.get(ctx, srv.URL, &struct{}{})
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(started), 5*time.Second, "must not sleep out 100 attempts")
}

// Without a path there is nowhere to write, and that must not be an error: the flag is optional.
func TestWriteRemainingSlotsIsANoOpWithoutAPath(t *testing.T) {
	require.NoError(t, writeRemainingSlots("", []uint64{14300001}))
}

// The same slot can be recorded twice (once by its own failure, once by the abort sweep), and
// a duplicated line would make the next run re-fetch it.
func TestWriteRemainingSlotsDeduplicates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remaining.txt")
	require.NoError(t, writeRemainingSlots(path, []uint64{14300002, 14300001, 14300002}))

	// Asserted on the raw file: readSlotsFile deduplicates on its own, so a round trip through
	// it would pass whether or not the writer deduplicates.
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "14300001\n14300002\n", string(raw))
}

// A bare status code cannot be diagnosed: endpoints explain a 400 in the body, and without it
// the only way to find out is to reproduce the request by hand.
func TestBeaconAPIGetIncludesTheResponseBodyInTheError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"code":400,"message":"Invalid block ID: expected hex or slot"}`))
	}))
	defer srv.Close()

	src := &beaconAPISource{endpoints: []string{srv.URL}, client: srv.Client(), maxAttempts: 3}
	_, err := src.get(t.Context(), srv.URL, &struct{}{})
	require.Error(t, err)
	require.ErrorContains(t, err, "400")
	require.ErrorContains(t, err, "Invalid block ID", "the endpoint's explanation must survive")
}

// Some providers cannot answer blob_sidecars by block root at all: they try to reconstruct the
// blobs from data columns they do not hold and return 400, while the same slot answers fine.
func TestBeaconAPISourceCanAskForSidecarsBySlot(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer srv.Close()

	src := &beaconAPISource{endpoints: []string{srv.URL}, client: srv.Client(), bySlot: true}
	_, err := src.sidecars(t.Context(), 14300002, common.HexToHash("0xabc"))
	require.NoError(t, err)
	require.Equal(t, "/eth/v1/beacon/blob_sidecars/14300002", gotPath)
}

// The default stays by root: it pins the request to the exact block we hold, with no reliance
// on the endpoint agreeing with us about what is canonical at that slot.
func TestBeaconAPISourceAsksByRootByDefault(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer srv.Close()

	src := &beaconAPISource{endpoints: []string{srv.URL}, client: srv.Client()}
	_, err := src.sidecars(t.Context(), 14300002, common.HexToHash("0xabc"))
	require.NoError(t, err)
	require.Contains(t, gotPath, "0x0000000000000000000000000000000000000000000000000000000000000abc")
}

// A linear 500ms step gives up 5s into a throttle that lasts minutes. Growth has to outlast the
// rate limit, and the endpoint's own Retry-After outranks any guess we make.
func TestBackoffDelayGrowsAndHonoursRetryAfter(t *testing.T) {
	require.Zero(t, backoffDelay(1, 0), "the first attempt must not wait")

	var prev time.Duration
	for attempt := 2; attempt <= 8; attempt++ {
		d := backoffDelay(attempt, 0)
		require.Greater(t, d, prev, "attempt %d must wait longer than the previous", attempt)
		require.LessOrEqual(t, d, maxBackoff, "must stay capped")
		prev = d
	}

	require.Equal(t, 30*time.Second, backoffDelay(2, 30*time.Second),
		"Retry-After must win over the computed delay")
	require.Equal(t, maxBackoff, backoffDelay(2, time.Hour),
		"an absurd Retry-After must still be capped")
}

// A 429 without Retry-After is normal; the caller must not stall on a zero parse.
func TestParseRetryAfterSeconds(t *testing.T) {
	require.Equal(t, 5*time.Second, parseRetryAfter("5"))
	require.Zero(t, parseRetryAfter(""))
	require.Zero(t, parseRetryAfter("not-a-number"))
}

// Without jitter every retry of a throttled run lands in lockstep, so the endpoint sees a burst
// at each step rather than a spread. Providers ask for jitter for exactly this reason.
func TestBackoffDelayIsJittered(t *testing.T) {
	seen := map[time.Duration]int{}
	for i := 0; i < 50; i++ {
		d := backoffDelay(5, 0)
		require.GreaterOrEqual(t, d, 8*time.Second/2, "jitter must not collapse the wait")
		require.LessOrEqual(t, d, 8*time.Second, "jitter must not exceed the computed step")
		seen[d]++
	}
	require.Greater(t, len(seen), 5, "repeated calls must not all return the same delay")
}

// Retry-After is an instruction, not a suggestion: jitter must not shorten it.
func TestRetryAfterIsNotJittered(t *testing.T) {
	for i := 0; i < 10; i++ {
		require.Equal(t, 30*time.Second, backoffDelay(3, 30*time.Second))
	}
}
