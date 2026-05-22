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

package snapshotsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRevalidationPolicy(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want RevalidationPolicy
	}{
		{"", RevalidationRedownload},
		{"redownload", RevalidationRedownload},
		{"REDOWNLOAD", RevalidationRedownload},
		{" stop ", RevalidationStop}, // surrounding whitespace tolerated
		{"warn", RevalidationWarn},
	}
	for _, c := range cases {
		got, err := ParseRevalidationPolicy(c.in)
		require.NoError(t, err, c.in)
		require.Equal(t, c.want, got, c.in)
	}

	got, err := ParseRevalidationPolicy("delete")
	require.Error(t, err, "unknown policy is rejected")
	require.Equal(t, RevalidationRedownload, got, "rejected input falls back to the default")
}

func TestRevalidationPolicyString(t *testing.T) {
	t.Parallel()
	require.Equal(t, "redownload", RevalidationRedownload.String())
	require.Equal(t, "stop", RevalidationStop.String())
	require.Equal(t, "warn", RevalidationWarn.String())
}
