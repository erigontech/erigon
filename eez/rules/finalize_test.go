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
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// devnetBlock1 returns the eez-devnet genesis state and the header of block 1.
func devnetBlock1(t *testing.T) (*chain.Config, *state.IntraBlockState, *types.Header) {
	t.Helper()
	g := networks.Devnet.Genesis
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	for addr, acc := range g.Alloc {
		a := accounts.InternAddress(addr)
		require.NoError(t, ibs.AddBalance(a, *uint256.MustFromBig(acc.Balance), tracing.BalanceIncreaseGenesisBalance))
		require.NoError(t, ibs.SetNonce(a, acc.Nonce, tracing.NonceChangeGenesis))
		require.NoError(t, ibs.SetCode(a, acc.Code, tracing.CodeChangeGenesis))
		for k, v := range acc.Storage {
			require.NoError(t, ibs.SetState(a, accounts.InternKey(k), *new(uint256.Int).SetBytes(v[:])))
		}
	}

	genesisBlock, _, err := genesiswrite.GenesisToBlock(g, datadir.New(t.TempDir()), log.New())
	require.NoError(t, err)
	genesis := genesisBlock.Header()
	require.Equal(t, networks.Devnet.GenesisHash, genesis.Hash())

	var zero uint64
	header := &types.Header{
		ParentHash:            genesis.Hash(),
		UncleHash:             empty.UncleHash,
		Number:                *uint256.NewInt(1),
		Time:                  genesis.Time + 2,
		GasLimit:              genesis.GasLimit,
		BaseFee:               misc.CalcBaseFee(g.Config, genesis),
		WithdrawalsHash:       &empty.RootHash,
		BlobGasUsed:           &zero,
		ExcessBlobGas:         &zero,
		ParentBeaconBlockRoot: &common.Hash{},
		RequestsHash:          &empty.RequestsHash,
	}
	return g.Config, ibs, header
}

func syscalls(cfg *chain.Config, e *Engine, header *types.Header, ibs *state.IntraBlockState) (rules.SysCallCustom, rules.SystemCall) {
	custom := func(contract accounts.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
		return protocol.SysCallContract(contract, data, cfg, ibs, header, e, constCall, vm.Config{})
	}
	plain := func(contract accounts.Address, data []byte) ([]byte, error) {
		return custom(contract, data, ibs, header, false)
	}
	return custom, plain
}

// TestInitializeAndFinalize is T-EVM-3 on the eez-devnet genesis.
func TestInitializeAndFinalize(t *testing.T) {
	cfg, ibs, header := devnetBlock1(t)
	e, err := New(cfg)
	require.NoError(t, err)
	custom, syscall := syscalls(cfg, e, header, ibs)
	logger := log.New()

	require.NoError(t, e.Initialize(cfg, nil, header, ibs, custom, logger, nil))

	// EIP-2935 records the parent hash in slot (number-1) % 8191.
	stored, err := ibs.GetState(params.HistoryStorageAddress, accounts.InternKey(common.Hash{}))
	require.NoError(t, err)
	require.Equal(t, header.ParentHash, common.Hash(stored.Bytes32()))

	// EIP-4788 has no contract in the genesis and leaves no trace; neither
	// does the system caller.
	for _, addr := range []accounts.Address{params.BeaconRootsAddress, params.SystemAddress} {
		exists, err := ibs.Exist(addr)
		require.NoError(t, err)
		require.False(t, exists, "%x", addr)
	}

	// A deposit log from the mainnet deposit address yields no request.
	depositLog := &types.Log{
		Address: common.HexToAddress("0x00000000219ab540356cBB839Cbe05303d7705Fa"),
		Topics:  []common.Hash{common.HexToHash("0x649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c5")},
		Data:    []byte{1},
	}
	receipts := types.Receipts{{Status: types.ReceiptStatusSuccessful, Logs: types.Logs{depositLog}}}
	requests, err := e.Finalize(cfg, header, ibs, nil, receipts, []*types.Withdrawal{}, nil, syscall, false, logger)
	require.NoError(t, err)
	require.Empty(t, requests)

	assembled := types.CopyHeader(header)
	block, requests, err := e.FinalizeAndAssemble(cfg, assembled, ibs, nil, nil, receipts, []*types.Withdrawal{}, nil, syscall, nil, logger)
	require.NoError(t, err)
	require.Empty(t, requests)
	require.Equal(t, empty.RequestsHash, *block.Header().RequestsHash)
}

// TestConsensusLayerContractsIgnored deploys the EIP-4788, 7002 and 7251
// contracts with code that writes storage when called: an EEZ block calls
// none of them.
func TestConsensusLayerContractsIgnored(t *testing.T) {
	cfg, ibs, header := devnetBlock1(t)
	e, err := New(cfg)
	require.NoError(t, err)
	custom, syscall := syscalls(cfg, e, header, ibs)
	logger := log.New()
	contracts := []accounts.Address{params.BeaconRootsAddress, cfg.GetWithdrawalRequestContract(), cfg.GetConsolidationRequestContract()}
	for _, addr := range contracts {
		// PUSH1 1 PUSH1 0 SSTORE STOP
		require.NoError(t, ibs.SetCode(addr, []byte{0x60, 0x01, 0x60, 0x00, 0x55, 0x00}, tracing.CodeChangeUnspecified))
	}

	require.NoError(t, e.Initialize(cfg, nil, header, ibs, custom, logger, nil))
	requests, err := e.Finalize(cfg, header, ibs, nil, types.Receipts{}, []*types.Withdrawal{}, nil, syscall, false, logger)
	require.NoError(t, err)
	require.Empty(t, requests)
	for _, addr := range contracts {
		v, err := ibs.GetState(addr, accounts.InternKey(common.Hash{}))
		require.NoError(t, err)
		require.True(t, v.IsZero(), "%x was called", addr)
	}
}

func TestFinalizeRejectsWithdrawals(t *testing.T) {
	cfg, ibs, header := devnetBlock1(t)
	e, err := New(cfg)
	require.NoError(t, err)
	_, syscall := syscalls(cfg, e, header, ibs)
	withdrawals := []*types.Withdrawal{{Index: 0, Address: common.HexToAddress("0x01"), Amount: 1}}

	_, err = e.Finalize(cfg, header, ibs, nil, nil, withdrawals, nil, syscall, false, log.New())
	require.ErrorIs(t, err, rules.ErrUnexpectedWithdrawals)
	require.ErrorIs(t, err, rules.ErrInvalidBlock)
	_, _, err = e.FinalizeAndAssemble(cfg, types.CopyHeader(header), ibs, nil, nil, nil, withdrawals, nil, syscall, nil, log.New())
	require.ErrorIs(t, err, rules.ErrUnexpectedWithdrawals)
}
