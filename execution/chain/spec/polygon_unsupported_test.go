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

package chainspec_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// A re-added Bor chainspec registration would silently make these chains
// selectable again, so pin that they stay unresolvable.
func TestPolygonChainsUnsupported(t *testing.T) {
	for _, name := range []string{
		networkname.BorMainnet,
		networkname.Amoy,
		networkname.Mumbai,
		networkname.BorDevnet,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := chainspec.ChainSpecByName(name)
			require.Error(t, err)
			require.False(t, networkname.Supported(name))
		})
	}
}

func TestSupportedChainsStillResolve(t *testing.T) {
	for _, name := range []string{
		networkname.Mainnet,
		networkname.Sepolia,
		networkname.Hoodi,
		networkname.Gnosis,
		networkname.Chiado,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := chainspec.ChainSpecByName(name)
			require.NoError(t, err)
			require.True(t, networkname.Supported(name))
		})
	}
}
