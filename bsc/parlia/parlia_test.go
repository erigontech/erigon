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

package parlia

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/bsc/parlia/seal"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/types"
)

func TestSealHashUsesChainID(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	header := &types.Header{Number: *uint256.NewInt(40000000), Extra: make([]byte, 32+65)}

	want, err := seal.Hash(header, spec.Config.ChainID.ToBig())
	require.NoError(t, err)
	require.Equal(t, want, New(spec.Config, log.New()).SealHash(header))
}
