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

package flow

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// captureVerifier records every entry it's asked to verify so tests
// can inspect ordering / Anchors passthrough; configurable to fail
// for specific names.
type captureVerifier struct {
	mu        sync.Mutex
	seen      []*snapshot.FileEntry
	failNames map[string]string
}

func (c *captureVerifier) VerifyProofRoot(e *snapshot.FileEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	clone := e.Clone()
	c.seen = append(c.seen, clone)
	if reason, fail := c.failNames[e.Name]; fail {
		return AlwaysFailVerifier{Reason: reason}.VerifyProofRoot(e)
	}
	return nil
}

func (c *captureVerifier) seenNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.seen))
	for i, e := range c.seen {
		out[i] = e.Name
	}
	return out
}

// TestProofRootVerifier_PassPromotesTrust pins the success path: a
// download whose manifest carried an anchor passes re-verification and
// the orchestrator emits TrustPromoted as normal.
func TestProofRootVerifier_PassPromotesTrust(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())

	verifier := &captureVerifier{}
	require.NoError(t, o.SetProofRootVerifier(verifier))

	var promoted atomic.Int32
	bus.Subscribe(func(TrustPromoted) { promoted.Add(1) })

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	entry := peerEntry("v2.0-commitment.0-256.kv", 0, 256)
	entry.Anchors = snapshot.Anchors{Root: [32]byte{0xab}, AtBlock: 100, AtTxNum: 10}

	bus.Publish(PeerManifestReceived{
		PeerID: "peer-1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			testDomain: {entry},
		},
	})
	bus.Publish(DownloadComplete{FileName: entry.Name, Size: entry.Size})

	waitUntil(t, func() bool {
		return len(storage.snapshotRecorded()) == 1
	}, 2*time.Second, "RecordFile call")

	require.Equal(t, []string{entry.Name}, verifier.seenNames(),
		"verifier must be called exactly once with the freshly-downloaded entry")
	require.Equal(t, snapshot.TrustVerified, storage.snapshotRecorded()[0].Trust)

	waitUntil(t, func() bool { return promoted.Load() == 1 }, 2*time.Second, "TrustPromoted fires on pass")
}

// TestProofRootVerifier_FailDemotesTrust pins the failure path: when
// the verifier rejects, the file is RecordFile'd at TrustNone and the
// orchestrator suppresses TrustPromoted.
func TestProofRootVerifier_FailDemotesTrust(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())

	verifier := &captureVerifier{failNames: map[string]string{
		"v2.0-commitment.0-256.kv": "trie root mismatch",
	}}
	require.NoError(t, o.SetProofRootVerifier(verifier))

	var promoted atomic.Int32
	bus.Subscribe(func(TrustPromoted) { promoted.Add(1) })

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	entry := peerEntry("v2.0-commitment.0-256.kv", 0, 256)
	entry.Anchors = snapshot.Anchors{Root: [32]byte{0xab}, AtBlock: 100, AtTxNum: 10}

	bus.Publish(PeerManifestReceived{
		PeerID: "peer-1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			testDomain: {entry},
		},
	})
	bus.Publish(DownloadComplete{FileName: entry.Name, Size: entry.Size})

	waitUntil(t, func() bool {
		return len(storage.snapshotRecorded()) == 1
	}, 2*time.Second, "RecordFile call")

	got := storage.snapshotRecorded()[0]
	require.Equal(t, snapshot.TrustNone, got.Trust,
		"verification failure demotes the file's trust to None — never falsely Verified")
	require.True(t, got.Local, "the bytes are on disk; only trust is downgraded")

	// Allow time for any (incorrectly fired) TrustPromoted to land.
	time.Sleep(50 * time.Millisecond)
	require.Zero(t, promoted.Load(), "TrustPromoted must NOT fire on a verification failure")
}

// TestProofRootVerifier_SkippedWhenAnchorsZero confirms the verifier
// is NOT consulted for entries with no manifest anchor — the
// fail-injection would otherwise cascade through every non-commitment
// file in production.
func TestProofRootVerifier_SkippedWhenAnchorsZero(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())

	verifier := &captureVerifier{failNames: map[string]string{
		"v1.0-accounts.0-256.kv": "should never be called",
	}}
	require.NoError(t, o.SetProofRootVerifier(verifier))

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	entry := peerEntry("v1.0-accounts.0-256.kv", 0, 256)
	// Deliberately leave Anchors at the zero value.

	bus.Publish(PeerManifestReceived{
		PeerID: "peer-1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			testDomain: {entry},
		},
	})
	bus.Publish(DownloadComplete{FileName: entry.Name, Size: entry.Size})

	waitUntil(t, func() bool {
		return len(storage.snapshotRecorded()) == 1
	}, 2*time.Second, "RecordFile call")

	require.Empty(t, verifier.seenNames(),
		"verifier must NOT be called for entries with a zero-Anchors manifest record")
	require.Equal(t, snapshot.TrustVerified, storage.snapshotRecorded()[0].Trust,
		"no-anchor files keep the pre-verifier behaviour — promoted to Verified")
}

// TestProofRootVerifierStub_NoopAcceptsAll documents that the
// Phase-1 stub passes every call — the orchestrator hook is wired
// without a real verifier yet, replaceable when the segment-proof
// scheme is selected.
func TestProofRootVerifierStub_NoopAcceptsAll(t *testing.T) {
	v := HashProofVerifierStub{}
	e := &snapshot.FileEntry{
		Name:    "v2.0-commitment.0-256.kv",
		Anchors: snapshot.Anchors{Root: [32]byte{0xab}, AtBlock: 100, AtTxNum: 10},
	}
	require.NoError(t, v.VerifyProofRoot(e))

	// Sanity: the stub is the zero value, no construction needed.
	var zero HashProofVerifierStub
	require.True(t, reflect.DeepEqual(v, zero))
}
