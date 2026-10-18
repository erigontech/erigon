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
package systemtx_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/chain"
	execprotocol "github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

var (
	sysAddr   = accounts.InternAddress(protocol.SystemAddress)
	eezl2Addr = accounts.InternAddress(protocol.EEZL2Address)
	coinbase  = accounts.InternAddress(common.HexToAddress("0xc0ffee"))
)

// EEZL2 stand-ins.
var (
	codeStop   = []byte{0x00}
	codeRevert = []byte{0x60, 0x00, 0x60, 0x00, 0xfd} // PUSH1 0 PUSH1 0 REVERT
	codeHalt   = []byte{0xfe}                         // INVALID
	codeLoop   = []byte{0x5b, 0x60, 0x00, 0x56}       // JUMPDEST PUSH1 0 JUMP
)

func eezChainConfig() *chain.Config {
	cfg := chain.TestChainOsakaConfig.Copy()
	cfg.L2 = &config.L2Config{
		RollupID:      1,
		L1ChainID:     7331,
		Registry:      common.HexToAddress("0x01"),
		BlockInterval: 2,
		L1BlockTime:   12,
		BlockGasLimit: 30_000_000,
	}
	return cfg
}

type execEnv struct {
	cfg *chain.Config
	ibs *state.IntraBlockState
	evm *vm.EVM
}

func newExecEnv(t *testing.T, eezl2Code []byte, systemBalance uint64) *execEnv {
	t.Helper()
	cfg := eezChainConfig()
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	require.NoError(t, ibs.SetCode(eezl2Addr, eezl2Code, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.AddBalance(sysAddr, *uint256.NewInt(systemBalance), tracing.BalanceChangeUnspecified))
	blockCtx := evmtypes.BlockContext{
		CanTransfer: execprotocol.CanTransfer,
		Transfer:    misc.Transfer,
		Coinbase:    coinbase,
		GasLimit:    30_000_000,
		BlockNumber: 1,
		Time:        2,
		BaseFee:     *uint256.NewInt(7),
		PrevRanDao:  &common.Hash{},
	}
	return &execEnv{cfg: cfg, ibs: ibs, evm: vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, cfg, vm.Config{})}
}

func (e *execEnv) apply(t *testing.T, tx types.Transaction, blockGas uint64) (*evmtypes.ExecutionResult, error) {
	t.Helper()
	baseFee := e.evm.Context.BaseFee
	msg, err := tx.AsMessage(*types.MakeSigner(e.cfg, 1, 2), &baseFee, e.evm.ChainRules())
	require.NoError(t, err)
	e.evm.Reset(execprotocol.NewEVMTxContext(msg), e.ibs)
	return execprotocol.ApplyMessage(e.evm, msg, new(execprotocol.GasPool).AddGas(blockGas), true, false, nil)
}

func (e *execEnv) balance(t *testing.T, addr accounts.Address) uint64 {
	t.Helper()
	b, err := e.ibs.GetBalance(addr)
	require.NoError(t, err)
	return b.Uint64()
}

func (e *execEnv) nonce(t *testing.T, addr accounts.Address) uint64 {
	t.Helper()
	n, err := e.ibs.GetNonce(addr)
	require.NoError(t, err)
	return n
}

func systemTx(t *testing.T, cfg *chain.Config, nonce, value uint64) *systemtx.Transaction {
	return mustNew(t, protocol.SystemTx{ChainID: cfg.ChainID.Uint64(), Nonce: nonce, To: protocol.EEZL2Address, Value: *uint256.NewInt(value)})
}

// TestMint is the T-EVM-1 mint and rollback set.
func TestMint(t *testing.T) {
	for _, tc := range []struct {
		name          string
		code          []byte
		systemBalance uint64
		value         uint64
		failed        bool
		gasUsed       uint64
	}{
		{name: "transfer", code: codeStop, value: 5, gasUsed: 21_000},
		{name: "transfer keeps system funds", code: codeStop, systemBalance: 10, value: 5, gasUsed: 21_000},
		{name: "zero value", code: codeStop, systemBalance: 10, gasUsed: 21_000},
		{name: "revert", code: codeRevert, value: 5, failed: true, gasUsed: 21_006},
		{name: "revert keeps system funds", code: codeRevert, systemBalance: 10, value: 5, failed: true, gasUsed: 21_006},
		{name: "halt", code: codeHalt, value: 5, failed: true, gasUsed: protocol.SystemTxGasLimit},
		{name: "out of gas", code: codeLoop, systemBalance: 10, value: 5, failed: true, gasUsed: protocol.SystemTxGasLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newExecEnv(t, tc.code, tc.systemBalance)
			res, err := env.apply(t, systemTx(t, env.cfg, 0, tc.value), 30_000_000)
			require.NoError(t, err)
			require.Equal(t, tc.failed, res.Failed())
			require.Equal(t, tc.gasUsed, res.ReceiptGasUsed)
			require.Equal(t, uint64(1), env.nonce(t, sysAddr))

			transferred := tc.value
			if tc.failed {
				transferred = 0
			}
			require.Equal(t, tc.systemBalance, env.balance(t, sysAddr), "the mint leaves no trace on the system balance")
			require.Equal(t, transferred, env.balance(t, eezl2Addr))
			require.Zero(t, env.balance(t, coinbase))
		})
	}
}

func TestMintRejects(t *testing.T) {
	env := newExecEnv(t, codeStop, 0)
	_, err := env.apply(t, systemTx(t, env.cfg, 1, 5), 30_000_000)
	require.ErrorIs(t, err, execprotocol.ErrNonceTooHigh)
	require.Zero(t, env.balance(t, sysAddr))

	// The whole budget must fit the block before execution.
	env = newExecEnv(t, codeStop, 0)
	_, err = env.apply(t, systemTx(t, env.cfg, 0, 5), protocol.SystemTxGasLimit-1)
	require.ErrorIs(t, err, execprotocol.ErrGasLimitReached)
	require.Zero(t, env.balance(t, sysAddr))
	_, err = env.apply(t, systemTx(t, env.cfg, 0, 0), protocol.SystemTxGasLimit)
	require.NoError(t, err)
	require.Zero(t, env.balance(t, sysAddr))
	require.Zero(t, env.balance(t, eezl2Addr))

	env = newExecEnv(t, codeStop, 1)
	tx := mustNew(t, protocol.SystemTx{ChainID: env.cfg.ChainID.Uint64(), To: protocol.EEZL2Address, Value: *new(uint256.Int).SetAllOne()})
	_, err = env.apply(t, tx, 30_000_000)
	require.ErrorIs(t, err, protocol.ErrSystemTx)
	require.Equal(t, uint64(1), env.balance(t, sysAddr))
}
