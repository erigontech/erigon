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

package misc

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
)

func TestIsPoSHeader(t *testing.T) {
	t.Parallel()
	require.True(t, IsPoSHeader(&types.Header{Difficulty: *uint256.NewInt(0)}))
	require.False(t, IsPoSHeader(&types.Header{Difficulty: *uint256.NewInt(1)}))
}

func TestEthTransferLog(t *testing.T) {
	t.Parallel()
	from := common.HexToAddress("0x01")
	to := common.HexToAddress("0x02")
	amount := *uint256.NewInt(1234)

	l := EthTransferLog(from, to, amount)
	require.Equal(t, params.SystemAddress.Value(), l.Address)
	require.Equal(t, []common.Hash{EthTransferLogEvent, from.Hash(), to.Hash()}, l.Topics)
	want := amount.Bytes32()
	require.Equal(t, want[:], []byte(l.Data))
}

func TestEthBurnLog(t *testing.T) {
	t.Parallel()
	from := common.HexToAddress("0x01")
	amount := *uint256.NewInt(1234)

	l := EthBurnLog(from, amount)
	require.Equal(t, params.SystemAddress.Value(), l.Address)
	require.Equal(t, []common.Hash{EthBurnLogEvent, from.Hash()}, l.Topics)
	want := amount.Bytes32()
	require.Equal(t, want[:], []byte(l.Data))
}

func TestVerifyDAOHeaderExtraData(t *testing.T) {
	t.Parallel()

	noFork := copyConfig(chain.AllProtocolChanges)
	noFork.DAOForkBlock = nil
	require.NoError(t, VerifyDAOHeaderExtraData(noFork, &types.Header{Number: *uint256.NewInt(12)}))

	withFork := copyConfig(chain.AllProtocolChanges)
	withFork.DAOForkBlock = common.NewUint64(10)

	header := func(n uint64, extra []byte) *types.Header {
		return &types.Header{Number: *uint256.NewInt(n), Extra: extra}
	}

	// Outside the [fork, fork+10) range -> always ok.
	require.NoError(t, VerifyDAOHeaderExtraData(withFork, header(5, nil)))
	require.NoError(t, VerifyDAOHeaderExtraData(withFork, header(25, nil)))

	// In range with the wrong extra-data -> rejected.
	require.ErrorIs(t, VerifyDAOHeaderExtraData(withFork, header(12, []byte("nope"))), ErrBadProDAOExtra)

	// In range with the expected extra-data -> ok.
	require.NoError(t, VerifyDAOHeaderExtraData(withFork, header(12, DAOForkBlockExtra)))
}

func TestDAODrainList(t *testing.T) {
	t.Parallel()
	list := DAODrainList()
	require.NotEmpty(t, list)
	for _, addr := range list {
		require.False(t, addr.IsNil())
	}
}

func TestVerifyGaslimit(t *testing.T) {
	t.Parallel()
	const parent = uint64(10_000_000)
	limit := parent / params.GasLimitBoundDivisor

	require.NoError(t, VerifyGaslimit(parent, parent))     // no change
	require.Error(t, VerifyGaslimit(parent, parent+limit)) // delta at/over bound
	require.Error(t, VerifyGaslimit(5000, 4997))           // below MinBlockGasLimit
}

func TestCalcGasLimit_Bounds(t *testing.T) {
	t.Parallel()
	const parent = uint64(10_000_000)
	delta := parent/params.GasLimitBoundDivisor - 1

	require.Equal(t, parent+delta, CalcGasLimit(parent, parent+5_000_000)) // hone up, capped by delta
	require.Equal(t, parent-delta, CalcGasLimit(parent, parent-5_000_000)) // hone down, capped by delta
	require.Equal(t, parent, CalcGasLimit(parent, parent))                 // already on target
	require.Equal(t, parent-delta, CalcGasLimit(parent, 1))                // desired clamped to MinBlockGasLimit
}

func TestVerifyEip1559Header_Errors(t *testing.T) {
	t.Parallel()
	cfg := config()
	parent := &types.Header{
		Number:   *uint256.NewInt(10),
		GasLimit: 20_000_000,
		GasUsed:  10_000_000,
		BaseFee:  uint256.NewInt(params.InitialBaseFee),
	}

	// Missing baseFee.
	missing := &types.Header{Number: *uint256.NewInt(11), GasLimit: 20_000_000}
	require.ErrorContains(t, VerifyEip1559Header(cfg, parent, missing, true /*skipGasLimit*/), "missing baseFee")

	// Wrong baseFee.
	want := CalcBaseFee(cfg, parent)
	wrong := &types.Header{
		Number:   *uint256.NewInt(11),
		GasLimit: 20_000_000,
		BaseFee:  new(uint256.Int).AddUint64(want, 1),
	}
	require.ErrorContains(t, VerifyEip1559Header(cfg, parent, wrong, true /*skipGasLimit*/), "invalid baseFee")
}
