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

func TestParseAdoptionPolicy(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want AdoptionPolicy
	}{
		{"", AdoptionAuto},
		{"auto", AdoptionAuto},
		{"AUTO", AdoptionAuto},
		{" stage ", AdoptionStage}, // surrounding whitespace tolerated
		{"warn", AdoptionWarn},
	}
	for _, c := range cases {
		got, err := ParseAdoptionPolicy(c.in)
		require.NoError(t, err, c.in)
		require.Equal(t, c.want, got, c.in)
	}

	_, err := ParseAdoptionPolicy("cutover")
	require.Error(t, err, "unknown policy is rejected")
}

func TestAdoptionPolicyString(t *testing.T) {
	t.Parallel()
	require.Equal(t, "auto", AdoptionAuto.String())
	require.Equal(t, "stage", AdoptionStage.String())
	require.Equal(t, "warn", AdoptionWarn.String())
}
