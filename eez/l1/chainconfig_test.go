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

package l1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/eez/l1"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestChainConfig(t *testing.T) {
	require.Same(t, chainspec.Sepolia.Config, l1.ChainConfig(11155111))
	require.Same(t, chainspec.Chiado.Config, l1.ChainConfig(10200))
	devnet := l1.ChainConfig(3151908)
	require.Equal(t, uint64(3151908), devnet.ChainID.Uint64())
	require.True(t, devnet.IsOsaka(0))
}
