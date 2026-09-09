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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// The archive is keyed by versioned hash, which is derived from the commitment the block
// already carries. Getting this wrong silently looks up someone else's blob, so it is pinned
// against a commitment/hash pair taken from a real Gnosis block.
func TestVersionedHashForKnownCommitment(t *testing.T) {
	var c common.Bytes48
	copy(c[:], common.FromHex("0xa4b83dd9c27738975e06f85210a51ba3eb1118f1370fd628637685b25a1842df8660e269cb13fb7ce13e0465174ada0c"))
	require.Equal(t,
		"0x012090adc6ba2ad21b1e612606fd195a2c4ae15caab5ffe888c82c0da9f08f79",
		versionedHashFor(c).Hex())

	var c2 common.Bytes48
	copy(c2[:], common.FromHex("0x875f279c511fc2b3132dfc201f3d9d88db387baff31665869e007e6a7c39738595659c7126714c156f4e13f9640500c8"))
	require.Equal(t,
		"0x015e16f65a0dcef9110249605c3bc39ac81f525091597a83c7d06edde4a7d582",
		versionedHashFor(c2).Hex())
}

// The first byte is the blob-commitment version. A hash that did not carry it would still
// look plausible but would never match an archive key.
func TestVersionedHashCarriesTheVersionByte(t *testing.T) {
	var c common.Bytes48
	copy(c[:], common.FromHex("0xa4b83dd9c27738975e06f85210a51ba3eb1118f1370fd628637685b25a1842df8660e269cb13fb7ce13e0465174ada0c"))
	require.Equal(t, byte(0x01), versionedHashFor(c)[0])
}

// An archive payload is untrusted until it reproduces the commitment the chain recorded.
// Accepting a mismatch would write a blob that is not the one the block committed to.
func TestVerifyPayloadRejectsAMismatchedBlob(t *testing.T) {
	var want common.Bytes48
	copy(want[:], common.FromHex("0xa4b83dd9c27738975e06f85210a51ba3eb1118f1370fd628637685b25a1842df8660e269cb13fb7ce13e0465174ada0c"))

	payload := make([]byte, blobLenBytes)
	payload[0] = 0x01 // a valid field element, but not this blob

	_, _, err := verifyPayloadAgainstCommitment(payload, want)
	require.Error(t, err)
	require.ErrorContains(t, err, "commitment mismatch")
}

func TestVerifyPayloadRejectsAWrongLengthBlob(t *testing.T) {
	var want common.Bytes48
	_, _, err := verifyPayloadAgainstCommitment(make([]byte, 100), want)
	require.Error(t, err)
	require.ErrorContains(t, err, "unexpected blob length")
}

// A public archive throttles. A 429 must be retried rather than counted as "the archive does
// not have this blob", which would silently leave a permanent gap in the repair.
func TestGetRetryBacksOffOnThrottleThenSucceeds(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	s := newArchiveSource(nil, srv.URL, 5, 0)
	body, found, err := s.getRetry(t.Context(), srv.URL, "text/plain")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "ok", string(body))
	require.Equal(t, 3, calls, "must have retried through both throttles")
}

// A 404 is a real answer: the archive does not hold it. Retrying it wastes the whole budget.
func TestGetRetryDoesNotRetryANotFound(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	s := newArchiveSource(nil, srv.URL, 5, 0)
	_, found, err := s.getRetry(t.Context(), srv.URL, "text/plain")
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, 1, calls, "a 404 must not be retried")
}

// Exhausting retries must surface an error, never a silent "not found".
func TestGetRetryGivesUpWithAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	s := newArchiveSource(nil, srv.URL, 2, 0)
	_, found, err := s.getRetry(t.Context(), srv.URL, "text/plain")
	require.Error(t, err)
	require.False(t, found)
	require.ErrorContains(t, err, "503")
}

// A slot with no proposed block is normal on any chain with missed slots. Counting it as an
// unfilled gap would inflate the failure count, make an otherwise clean run exit non-zero,
// and bury genuine archive misses among hundreds of false ones.
func TestMissedSlotsAreNotFailures(t *testing.T) {
	var tally blobFetchTally
	tally.missed = 300
	require.Zero(t, tally.failures(), "missed slots must not count as failures")

	tally.unserved = 1
	require.Equal(t, 1, tally.failures(), "a genuine miss must still count")
}

// The request counter is what lets the caller pace only the slots that reach a remote. If it
// stopped advancing, every slot would be treated as local and a public archive would be hit
// with no delay at all.
func TestArchiveSourceCountsItsRequests(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	s := newArchiveSource(nil, srv.URL, 3, 0)
	require.Zero(t, s.requests)
	_, _, err := s.getRetry(t.Context(), srv.URL, "text/plain")
	require.NoError(t, err)
	require.Equal(t, 1, s.requests)
	_, _, err = s.getRetry(t.Context(), srv.URL, "text/plain")
	require.NoError(t, err)
	require.Equal(t, 2, s.requests, "each attempt must be counted")
}
