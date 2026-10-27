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
package rules

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var bystander = accounts.InternAddress(common.HexToAddress("0xb0b"))

func fund(t *testing.T, ibs *state.IntraBlockState, amount uint64, addrs ...accounts.Address) {
	t.Helper()
	for _, a := range addrs {
		require.NoError(t, ibs.AddBalance(a, *uint256.NewInt(amount), tracing.BalanceChangeUnspecified))
	}
}

func requireBalance(t *testing.T, ibs *state.IntraBlockState, want uint64, addrs ...accounts.Address) {
	t.Helper()
	for _, a := range addrs {
		b, err := ibs.GetBalance(a)
		require.NoError(t, err)
		require.Equal(t, want, b.Uint64(), "%x", a)
	}
}

// TestRollupXBlockStartBurn checks that a rollupX block zeroes the balances
// of EEZL2 and the system address after the EIP-2935 write, and a rollup0
// block does not.
func TestRollupXBlockStartBurn(t *testing.T) {
	eezl2 := accounts.InternAddress(protocol.RollupXEEZL2Address)
	cfg, ibs, header := block1(t, networks.Chiado)
	e, err := New(cfg)
	require.NoError(t, err)
	custom, _ := syscalls(cfg, e, header, ibs)
	fund(t, ibs, 5, eezl2, systemAddress, bystander)

	require.NoError(t, e.Initialize(cfg, nil, header, ibs, custom, log.New(), nil))
	requireBalance(t, ibs, 0, eezl2, systemAddress)
	requireBalance(t, ibs, 5, bystander)
	stored, err := ibs.GetState(params.HistoryStorageAddress, accounts.InternKey(common.Hash{}))
	require.NoError(t, err)
	require.Equal(t, header.ParentHash, common.Hash(stored.Bytes32()))
	// A zero balance leaves no trace.
	exists, err := ibs.Exist(bystander)
	require.NoError(t, err)
	require.True(t, exists)
	require.NoError(t, e.Initialize(cfg, nil, header, ibs, custom, log.New(), nil))

	cfg, ibs, header = devnetBlock1(t)
	e, err = New(cfg)
	require.NoError(t, err)
	custom, _ = syscalls(cfg, e, header, ibs)
	rollup0EEZL2 := accounts.InternAddress(protocol.EEZL2Address)
	fund(t, ibs, 5, rollup0EEZL2, systemAddress, eezl2)
	require.NoError(t, e.Initialize(cfg, nil, header, ibs, custom, log.New(), nil))
	requireBalance(t, ibs, 5, rollup0EEZL2, systemAddress, eezl2)
}

func TestRollupXVerifyHeader(t *testing.T) {
	cfg, _, header := block1(t, networks.Chiado)
	e, reader := newTestEngine(t, cfg)
	genesis, _, err := genesiswrite.GenesisToBlock(networks.Chiado.Genesis, datadir.New(t.TempDir()), log.New())
	require.NoError(t, err)
	reader.headers[genesis.Hash()] = genesis.Header()

	// prevRandao comes from L1, and the base fee falls by 1/50.
	require.NotEqual(t, common.Hash{}, header.MixDigest)
	require.Equal(t, uint64(980_000_000), header.BaseFee.Uint64())
	require.NoError(t, e.VerifyHeader(reader, header, true))

	for _, mutate := range []func(*types.Header){
		func(h *types.Header) { h.BaseFee = uint256.NewInt(875_000_000) },
		func(h *types.Header) { h.GasLimit = 36_000_000 },
		func(h *types.Header) { h.Time++ },
	} {
		h := types.CopyHeader(header)
		mutate(h)
		require.Error(t, e.VerifyHeader(reader, h, true))
	}
}

func TestRollupXNew(t *testing.T) {
	cfg := networks.Chiado.Config.Copy()
	cfg.BaseFeeChangeDenominator = nil
	_, err := New(cfg)
	require.ErrorIs(t, err, config.ErrInvalid)

	cfg = networks.Devnet.Config.Copy()
	cfg.BaseFeeChangeDenominator = new(config.RollupXBaseFeeChangeDenominator)
	_, err = New(cfg)
	require.ErrorIs(t, err, config.ErrInvalid)
}
