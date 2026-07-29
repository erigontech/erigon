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

package snapshotauth

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForkTransitionCapability_BuildsCanonicalString(t *testing.T) {
	cap, err := ForkTransitionCapability("hoodi-fork-42")
	require.NoError(t, err)
	require.Equal(t, "fork:transition:hoodi-fork-42", cap)
}

func TestForkTransitionCapability_RejectsInvalidNames(t *testing.T) {
	_, err := ForkTransitionCapability("")
	require.ErrorContains(t, err, "empty fork chain name")

	_, err = ForkTransitionCapability("   ")
	require.ErrorContains(t, err, "empty fork chain name")

	_, err = ForkTransitionCapability("hoodi fork 42")
	require.ErrorContains(t, err, "whitespace")

	_, err = ForkTransitionCapability("hoodi\tfork")
	require.ErrorContains(t, err, "whitespace")
}

func TestParseForkTransitionCapability_RoundTrip(t *testing.T) {
	built, err := ForkTransitionCapability("hoodi-fork-42")
	require.NoError(t, err)

	name, ok := ParseForkTransitionCapability(built)
	require.True(t, ok)
	require.Equal(t, "hoodi-fork-42", name)
}

func TestParseForkTransitionCapability_RejectsNonMatching(t *testing.T) {
	// Not our prefix.
	_, ok := ParseForkTransitionCapability("snapshot:advertise")
	require.False(t, ok)
	_, ok = ParseForkTransitionCapability("fork:from:abc")
	require.False(t, ok)
	_, ok = ParseForkTransitionCapability("chain.v2:hash:abc")
	require.False(t, ok)

	// Our prefix but empty name.
	_, ok = ParseForkTransitionCapability(CapForkTransitionPrefix)
	require.False(t, ok, "empty fork chain name after prefix must reject")
}

// TestValidateCapability_AcceptsForkTransition pins that the new cap
// type flows through the programmatic-construction path used by
// New() — otherwise minting a UCAN with a fork-transition cap would
// fail at capability validation.
func TestValidateCapability_AcceptsForkTransition(t *testing.T) {
	require.NoError(t, validateCapability("fork:transition:hoodi-fork-42"))
	require.Error(t, validateCapability("fork:transition:")) // empty target rejects at parse
}
