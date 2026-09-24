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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestStaleCoordinates pins the consume half of the atomic-set rule: holding a
// file by name is not the same as holding the generation a peer is currently
// advertising.
//
// A publisher that rebuilds a file in place keeps its name and changes its
// hash. Matching on name alone leaves the rest of the coordinate a generation
// behind, and an accessor built from a different primary indexes past the end
// of the one it is paired with.
func TestStaleCoordinates(t *testing.T) {
	hash := func(b byte) [20]byte {
		var h [20]byte
		h[0] = b
		return h
	}
	entry := func(name string, h byte) *snapshot.FileEntry {
		return &snapshot.FileEntry{Name: name, TorrentHash: hash(h)}
	}

	held := map[string][20]byte{
		"domain/v2.2-commitment.330-331.kv":  hash(0x11),
		"domain/v2.1-commitment.330-331.kvi": hash(0x22),
		"domain/v2.2-commitment.328-330.kv":  hash(0x33),
		"domain/v2.1-commitment.328-330.kvi": hash(0x44),
	}

	peer := []*snapshot.FileEntry{
		entry("domain/v2.2-commitment.330-331.kv", 0x11),
		// Rebuilt: same name, new hash.
		entry("domain/v2.1-commitment.330-331.kvi", 0x99),
		entry("domain/v2.2-commitment.328-330.kv", 0x33),
		entry("domain/v2.1-commitment.328-330.kvi", 0x44),
	}

	stale := staleCoordinates(peer, held)

	rebuilt, ok := snapshot.CoordinateOf("domain/v2.1-commitment.330-331.kvi")
	require.True(t, ok)
	require.Contains(t, stale, rebuilt,
		"a member advertised with a different hash makes its whole coordinate stale")

	intact, ok := snapshot.CoordinateOf("domain/v2.2-commitment.328-330.kv")
	require.True(t, ok)
	require.NotContains(t, stale, intact, "an unchanged coordinate stays put")
	require.Len(t, stale, 1)
}

// TestStaleCoordinates_UnheldAndUnhashedAreNotStale keeps the gate from
// re-requesting on absent information: a file we do not hold is an ordinary
// gap, and a peer entry with no hash says nothing about generations.
func TestStaleCoordinates_UnheldAndUnhashedAreNotStale(t *testing.T) {
	held := map[string][20]byte{"domain/v2.2-commitment.330-331.kv": {0x11}}
	peer := []*snapshot.FileEntry{
		{Name: "domain/v2.1-commitment.330-331.kvi", TorrentHash: [20]byte{0x99}}, // not held
		{Name: "domain/v2.2-commitment.330-331.kv"},                               // held, no hash advertised
	}
	require.Empty(t, staleCoordinates(peer, held))
}
