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

package snapshot

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCoordinateOf pins that a primary and the accessors built from it share
// one coordinate despite carrying independent version prefixes, which is what
// lets publish and consume treat them as a single unit.
func TestCoordinateOf(t *testing.T) {
	sameSet := func(t *testing.T, a, b string) {
		t.Helper()
		ca, ok := CoordinateOf(a)
		require.True(t, ok, "%s should yield a coordinate", a)
		cb, ok := CoordinateOf(b)
		require.True(t, ok, "%s should yield a coordinate", b)
		require.Equal(t, ca, cb, "%s and %s belong to one set", a, b)
	}

	sameSet(t, "domain/v2.2-commitment.330-331.kv", "domain/v2.1-commitment.330-331.kvi")
	sameSet(t, "idx/v3.0-logaddrs.330-331.ef", "accessor/v2.1-logaddrs.330-331.efi")
	sameSet(t, "history/v3.1-rcache.330-331.v", "accessor/v1.1-rcache.330-331.vi")
	sameSet(t, "v1.1-003670-003671-headers.seg", "v2.0-003670-003671-headers.idx")

	differs := func(t *testing.T, a, b string) {
		t.Helper()
		ca, _ := CoordinateOf(a)
		cb, _ := CoordinateOf(b)
		require.NotEqual(t, ca, cb)
	}
	differs(t, "domain/v2.2-commitment.330-331.kv", "domain/v2.2-commitment.328-330.kv")
	differs(t, "idx/v3.0-logaddrs.330-331.ef", "idx/v3.0-logtopics.330-331.ef")
	differs(t, "v1.1-003670-003671-headers.seg", "v1.1-003670-003671-bodies.seg")

	for _, name := range []string{"", "erigondb.toml", "salt-blocks.txt"} {
		_, ok := CoordinateOf(name)
		require.False(t, ok, "%q is not part of a primary/accessor set", name)
	}
}
