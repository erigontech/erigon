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

package snapcfg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPinnedGenesisRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// No file pinned yet.
	items, present, err := LoadPinnedGenesis(dir)
	require.NoError(t, err)
	require.False(t, present)
	require.Nil(t, items)

	want := PreverifiedItems{
		{Name: "v1.0-000000-000500-headers.seg", Hash: "aabb"},
		{Name: "v1.0-accounts.0-2048.kv", Hash: "ccdd"},
	}
	require.NoError(t, WritePinnedGenesis(dir, want))

	got, present, err := LoadPinnedGenesis(dir)
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, want, got)
}

func TestParsePreverifiedItems(t *testing.T) {
	items, err := ParsePreverifiedItems([]byte("\"b.seg\" = \"22\"\n\"a.seg\" = \"11\"\n"))
	require.NoError(t, err)
	require.Equal(t, PreverifiedItems{
		{Name: "a.seg", Hash: "11"},
		{Name: "b.seg", Hash: "22"},
	}, items)

	_, err = ParsePreverifiedItems([]byte("not = valid = toml"))
	require.Error(t, err)
}

func TestQuorumConfigFor(t *testing.T) {
	// Unknown chain → default policy.
	require.Equal(t, DefaultQuorumConfig, QuorumConfigFor("no-such-chain"))
	require.Equal(t, 0.5, DefaultQuorumConfig.F)
	require.Equal(t, 2, DefaultQuorumConfig.QFloor)

	EmbeddedQuorumConfig["unit-test-chain"] = QuorumConfig{F: 0.75, QFloor: 5}
	defer delete(EmbeddedQuorumConfig, "unit-test-chain")
	require.Equal(t, QuorumConfig{F: 0.75, QFloor: 5}, QuorumConfigFor("unit-test-chain"))
}
