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
	"crypto/ecdsa"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	collector     = common.HexToAddress("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266")
	collectorAddr = accounts.InternAddress(collector)
	toggleAddr    = accounts.InternAddress(common.HexToAddress("0x7066"))
	revertAddr    = accounts.InternAddress(common.HexToAddress("0x4e7e"))
	balanceAddr   = accounts.InternAddress(common.HexToAddress("0xba1a"))
	gasPriceAddr  = accounts.InternAddress(common.HexToAddress("0x6a5f"))
)

// codeToggle sets slot 0 to 1 when it is empty and clears it otherwise.
var codeToggle = []byte{
	0x60, 0x00, 0x54, 0x15, 0x60, 0x0d, 0x57, // PUSH1 0 SLOAD ISZERO PUSH1 13 JUMPI
	0x60, 0x00, 0x60, 0x00, 0x55, 0x00, // PUSH1 0 PUSH1 0 SSTORE STOP
	0x5b, 0x60, 0x01, 0x60, 0x00, 0x55, 0x00, // JUMPDEST PUSH1 1 PUSH1 0 SSTORE STOP
}

// codeCollectorBalance stores the collector's balance in slot 0.
var codeCollectorBalance = append(append([]byte{0x73}, collector[:]...), 0x31, 0x60, 0x00, 0x55, 0x00)

// codeGasPrice stores GASPRICE+1 in slot 0.
var codeGasPrice = []byte{0x3a, 0x60, 0x01, 0x01, 0x60, 0x00, 0x55}

type feeEnv struct {
	*execEnv
	key   *ecdsa.PrivateKey
	user  accounts.Address
	nonce uint64
}

func newFeeEnv(t *testing.T, withCollector bool) *feeEnv {
	t.Helper()
	// EEZL2 records GASPRICE, then calls a contract that records it again.
	eezl2 := append(append([]byte{}, codeGasPrice...), 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x73)
	gasPrice := gasPriceAddr.Value()
	eezl2 = append(append(eezl2, gasPrice[:]...), 0x5a, 0xf1, 0x50, 0x00)
	env := newExecEnv(t, eezl2, 0)
	if withCollector {
		c := collector
		env.cfg.FeeCollector = &c
	}
	for addr, code := range map[accounts.Address][]byte{toggleAddr: codeToggle, revertAddr: codeRevert, balanceAddr: codeCollectorBalance, gasPriceAddr: append(codeGasPrice, 0x00)} {
		require.NoError(t, env.ibs.SetCode(addr, code, tracing.CodeChangeUnspecified))
	}
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	user := accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))
	require.NoError(t, env.ibs.AddBalance(user, *uint256.NewInt(1e18), tracing.BalanceChangeUnspecified))
	return &feeEnv{execEnv: env, key: key, user: user}
}

// call sends a user transaction paying a tip of 2 over the base fee of 7.
func (e *feeEnv) call(t *testing.T, to accounts.Address) (gasUsed uint64, failed bool) {
	t.Helper()
	tx := types.MustSignNewTx(e.key, *types.LatestSignerForChainID(e.cfg.ChainID),
		types.NewEIP1559Transaction(*e.cfg.ChainID, e.nonce, to.Value(), uint256.NewInt(0), 100_000, uint256.NewInt(2), uint256.NewInt(100), nil))
	e.nonce++
	return e.run(t, tx)
}

func (e *feeEnv) run(t *testing.T, tx types.Transaction) (gasUsed uint64, failed bool) {
	t.Helper()
	res, err := e.apply(t, tx, 30_000_000)
	require.NoError(t, err)
	require.NoError(t, e.ibs.FinalizeTx(e.evm.ChainRules(), state.NewNoopWriter()))
	return res.ReceiptGasUsed, res.Failed()
}

func (e *feeEnv) slot0(t *testing.T, addr accounts.Address) uint64 {
	t.Helper()
	v, err := e.ibs.GetState(addr, accounts.InternKey(common.Hash{}))
	require.NoError(t, err)
	return v.Uint64()
}

// TestFeeCollector is T-EVM-2.
func TestFeeCollector(t *testing.T) {
	env := newFeeEnv(t, true)

	set, _ := env.call(t, toggleAddr)
	require.Equal(t, 7*set, env.balance(t, collectorAddr))
	require.Equal(t, 2*set, env.balance(t, coinbase))

	// Clearing the slot earns a refund; the collector is credited after it.
	before := env.balance(t, collectorAddr)
	clear, _ := env.call(t, toggleAddr)
	require.Less(t, clear, set)
	require.Equal(t, before+7*clear, env.balance(t, collectorAddr))

	// A reverted transaction pays.
	before = env.balance(t, collectorAddr)
	reverted, failed := env.call(t, revertAddr)
	require.True(t, failed)
	require.Equal(t, before+7*reverted, env.balance(t, collectorAddr))

	// The next transaction sees the credit.
	credited := env.balance(t, collectorAddr)
	env.call(t, balanceAddr)
	require.Equal(t, credited, env.slot0(t, balanceAddr))

	// A system transaction credits nothing and pays nothing.
	collected, tips := env.balance(t, collectorAddr), env.balance(t, coinbase)
	_, failed = env.run(t, systemTx(t, env.cfg, 0, 0))
	require.False(t, failed)
	require.Equal(t, collected, env.balance(t, collectorAddr))
	require.Equal(t, tips, env.balance(t, coinbase))
}

func TestBurnWithoutCollector(t *testing.T) {
	env := newFeeEnv(t, false)
	gasUsed, _ := env.call(t, toggleAddr)
	require.Zero(t, env.balance(t, collectorAddr))
	require.Equal(t, 2*gasUsed, env.balance(t, coinbase))
	require.Equal(t, uint64(1e18)-9*gasUsed, env.balance(t, env.user))
}

// TestSystemTxGasPrice is the GASPRICE part of T-EVM-1: zero in the outer
// frame and in nested ones.
func TestSystemTxGasPrice(t *testing.T) {
	env := newFeeEnv(t, true)
	_, failed := env.run(t, systemTx(t, env.cfg, 0, 0))
	require.False(t, failed)
	require.Equal(t, uint64(1), env.slot0(t, eezl2Addr))
	require.Equal(t, uint64(1), env.slot0(t, gasPriceAddr))
}
