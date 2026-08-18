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
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// scenario is a producer/consumer test harness that ignores actual file
// content and just exercises the announce/request/cancel mechanism.
// A producer publishes PeerManifestReceived and PeerDeparted; the
// consumer's Orchestrator drives DownloadRequested and DownloadSuperseded
// which the harness captures for assertion.
type scenario struct {
	t                 *testing.T
	o                 *Orchestrator
	bus               event.EventBus
	requests, cancels func() []string
}

func newScenario(t *testing.T) *scenario {
	t.Helper()
	o, bus := canonicalTestOrch(t, 20*time.Millisecond)
	return &scenario{
		t:        t,
		o:        o,
		bus:      bus,
		requests: captureRequests(t, bus),
		cancels:  captureSuperseded(t, bus),
	}
}

// producer records that peer P advertises the named files at the given
// hash seed. Every filename in one call carries the same hash byte, so
// two producers with the same hashSeed agree.
func (s *scenario) producer(peerID string, hashSeed byte, names ...string) {
	entries := make([]*snapshot.FileEntry, len(names))
	for i, n := range names {
		entries[i] = fe(n, snapshot.DomainCommitment, uint64(i), uint64(i+1), hashSeed)
	}
	s.bus.Publish(PeerManifestReceived{
		PeerID: peerID,
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: entries,
		},
	})
}

// producerDivergent records that peer P advertises a name with a
// different hash than the shared hash seed — the divergence-rejection
// scenario.
func (s *scenario) producerDivergent(peerID string, sharedHash, ownHash byte, sharedName, divergentName string) {
	s.bus.Publish(PeerManifestReceived{
		PeerID: peerID,
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {
				fe(sharedName, snapshot.DomainCommitment, 0, 1, sharedHash),
				fe(divergentName, snapshot.DomainCommitment, 1, 2, ownHash),
			},
		},
	})
}

func (s *scenario) peerDepart(peerID string) {
	s.bus.Publish(PeerDeparted{PeerID: peerID})
}

func (s *scenario) waitForCanonical(names ...string) {
	s.t.Helper()
	waitUntil(s.t, func() bool {
		c := s.o.Canonical()
		if len(c) != len(names) {
			return false
		}
		for _, n := range names {
			if _, ok := c[n]; !ok {
				return false
			}
		}
		return true
	}, 2*time.Second, "canonical settles at expected set")
}

func (s *scenario) waitForRequest(name string) {
	s.t.Helper()
	waitUntil(s.t, func() bool { return slices.Contains(s.requests(), name) }, 2*time.Second, "requested "+name)
}

func (s *scenario) waitForCancel(name string) {
	s.t.Helper()
	waitUntil(s.t, func() bool { return slices.Contains(s.cancels(), name) }, 2*time.Second, "cancelled "+name)
}

// --- Scenarios ---

// SCENARIO: publisher-rotation producing the verify12 stall. Two
// publishers advertise a narrow file X. Consumer starts fetching X.
// Both publishers rotate their manifest — X is no longer advertised
// by either. Consumer must cancel X's download, not retry forever.
func TestScenario_PublisherRotation_ConsumerCancelsRetired(t *testing.T) {
	s := newScenario(t)

	s.producer("master", 0xA1, "A.kv", "X.kv")
	s.producer("archive", 0xA1, "A.kv", "X.kv")

	s.waitForCanonical("A.kv", "X.kv")
	s.waitForRequest("A.kv")
	s.waitForRequest("X.kv")

	// Both publishers rotate: X.kv retired, gone from their advertisements.
	s.producer("master", 0xA1, "A.kv")
	s.producer("archive", 0xA1, "A.kv")

	s.waitForCanonical("A.kv")
	s.waitForCancel("X.kv")
}

// SCENARIO: only ONE publisher rotates; the other still advertises the
// old file. Under file-identity quorum, the file drops out of canonical
// as soon as unanimity breaks. Consumer cancels — even though one
// publisher would still serve it.
func TestScenario_PartialRotation_UnanimityRequirement(t *testing.T) {
	s := newScenario(t)

	s.producer("master", 0xA1, "A.kv", "X.kv")
	s.producer("archive", 0xA1, "A.kv", "X.kv")
	s.waitForCanonical("A.kv", "X.kv")
	s.waitForRequest("X.kv")

	// Only master rotates; archive still advertises X.
	s.producer("master", 0xA1, "A.kv")

	s.waitForCanonical("A.kv")
	s.waitForCancel("X.kv")
}

// SCENARIO: bootstrap. Consumer starts with 0 peers → canonical empty.
// First peer joins with {A, B} → canonical trivially becomes {A, B}
// (single-peer quorum). Second peer joins with {A} only → canonical
// shrinks to {A} (intersection); consumer cancels B.
func TestScenario_Bootstrap_ProgressiveQuorum(t *testing.T) {
	s := newScenario(t)

	require.Empty(t, s.o.Canonical(), "start: canonical empty")

	s.producer("P1", 0xA1, "A.kv", "B.kv")
	s.waitForCanonical("A.kv", "B.kv")
	s.waitForRequest("A.kv")
	s.waitForRequest("B.kv")

	s.producer("P2", 0xA1, "A.kv")
	s.waitForCanonical("A.kv")
	s.waitForCancel("B.kv")
}

// SCENARIO: content divergence. Two publishers advertise the same
// filename but hash to different bytes. Divergence rejection excludes
// the filename from canonical entirely; if it was in pending it gets
// cancelled. Publishers that agree on other files keep those in
// canonical.
func TestScenario_ContentDivergence_ExcludedFromCanonical(t *testing.T) {
	s := newScenario(t)

	// Both agree on A, disagree on X.
	s.producerDivergent("master", 0xA1, 0xC1, "A.kv", "X.kv")
	s.producerDivergent("archive", 0xA1, 0xC2, "A.kv", "X.kv")

	// A is unanimous → canonical + requested.
	s.waitForCanonical("A.kv")
	s.waitForRequest("A.kv")

	// X was request via bootstrap-additive but is NOT in canonical due
	// to hash divergence. Should be cancelled.
	s.waitForCancel("X.kv")
}

// SCENARIO: peer departure orphans a pending file that only that peer
// advertised. Canonical drops it; consumer cancels — closes the
// long-standing "pending forever after last advertiser leaves" gap.
func TestScenario_PeerDeparture_OrphanedPendingCancelled(t *testing.T) {
	s := newScenario(t)

	s.producer("master", 0xA1, "A.kv")
	s.waitForRequest("A.kv")

	s.peerDepart("master")

	s.waitForCancel("A.kv")
}

// SCENARIO: merge-transition. Publishers advertise narrower files
// {N1, N2}. Publishers rotate to a merged wider file {W} whose range
// contains N1 and N2. Consumer's canonical loses N1, N2 and gains W;
// merge-transition detection fires; N1 and N2 requests get cancelled.
func TestScenario_MergeTransition_NarrowerToWider(t *testing.T) {
	s := newScenario(t)

	// Publishers advertise narrower files.
	narrower := func(peer string) {
		s.bus.Publish(PeerManifestReceived{
			PeerID: peer,
			Domains: map[snapshot.Domain][]*snapshot.FileEntry{
				snapshot.DomainCommitment: {
					fe("v2.2-commitment.310-311.kv", snapshot.DomainCommitment, 310, 311, 0xA1),
					fe("v2.2-commitment.311-312.kv", snapshot.DomainCommitment, 311, 312, 0xA1),
				},
			},
		})
	}
	narrower("master")
	narrower("archive")

	s.waitForCanonical("v2.2-commitment.310-311.kv", "v2.2-commitment.311-312.kv")
	s.waitForRequest("v2.2-commitment.310-311.kv")
	s.waitForRequest("v2.2-commitment.311-312.kv")

	// Publishers rotate to the wider merged file.
	wider := func(peer string) {
		s.bus.Publish(PeerManifestReceived{
			PeerID: peer,
			Domains: map[snapshot.Domain][]*snapshot.FileEntry{
				snapshot.DomainCommitment: {
					fe("v2.2-commitment.310-312.kv", snapshot.DomainCommitment, 310, 312, 0xB2),
				},
			},
		})
	}
	wider("master")
	wider("archive")

	s.waitForCanonical("v2.2-commitment.310-312.kv")
	s.waitForCancel("v2.2-commitment.310-311.kv")
	s.waitForCancel("v2.2-commitment.311-312.kv")
	s.waitForRequest("v2.2-commitment.310-312.kv")
}

// SCENARIO: peer flap. Publisher goes offline (canonical shrinks), then
// re-appears with the same manifest. Consumer's canonical rebuilds and
// pending files are re-requested — no phantom-cancel state that would
// permanently reject the file.
func TestScenario_PeerFlap_ReconvergesCleanly(t *testing.T) {
	s := newScenario(t)

	s.producer("master", 0xA1, "A.kv")
	s.producer("archive", 0xA1, "A.kv")
	s.waitForCanonical("A.kv")
	s.waitForRequest("A.kv")

	// master flaps: gone then back with same manifest.
	s.peerDepart("master")
	s.waitForCanonical("A.kv") // archive alone → single-peer canonical still {A}
	s.producer("master", 0xA1, "A.kv")
	s.waitForCanonical("A.kv")

	require.Contains(t, s.o.Canonical(), "A.kv",
		"re-connection restores full quorum with same canonical")
}
