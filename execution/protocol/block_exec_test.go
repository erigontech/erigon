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

package protocol

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// A system call runs in the block's environment: only the caller is SYSTEM_ADDRESS, COINBASE stays the fee recipient.
func TestSysCallContractSeesBlockCoinbase(t *testing.T) {
	t.Parallel()
	// COINBASE PUSH0 SSTORE STOP
	code := []byte{0x41, 0x5f, 0x55, 0x00}
	contract := accounts.InternAddress(common.HexToAddress("0x00000961Ef480Eb55e80D19ad83579A64c007002"))
	coinbase := common.HexToAddress("0x0000000000000000000000000000000000C0FFEE")
	header := &types.Header{
		Number:     *uint256.NewInt(25604144),
		Time:       1755000000,
		Difficulty: uint256.Int{},
		BaseFee:    uint256.NewInt(7),
		Coinbase:   coinbase,
	}
	ibs := state.New(state.NewNoopReader())
	require.NoError(t, ibs.CreateAccount(contract, false))
	require.NoError(t, ibs.SetCode(contract, code, tracing.CodeChangeUnspecified))

	_, err := SysCallContract(contract, nil, chainspec.Mainnet.Config, ibs, header, ethash.NewFaker(), false, vm.Config{})
	require.NoError(t, err)

	got, err := ibs.GetState(contract, accounts.ZeroKey)
	require.NoError(t, err)
	var want uint256.Int
	want.SetBytes(coinbase[:])
	require.Equal(t, want.Hex(), got.Hex())
}

// Stateless execution rejects an invalid tx and goes on, but a panic inside execution is a
// failure of the run, not a verdict on the tx.
func TestExecuteBlockEphemerallyStatelessFailsOnExecPanic(t *testing.T) {
	t.Parallel()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	cfg := chain.TestChainBerlinConfig
	txn, err := types.SignTx(types.NewTransaction(0, common.HexToAddress("0xc0ffee"), uint256.NewInt(1), 21_000, uint256.NewInt(1), nil), *types.LatestSignerForChainID(cfg.ChainID), key)
	require.NoError(t, err)
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 1_000_000}
	block := types.NewBlock(header, []types.Transaction{txn}, nil, nil, nil, nil)

	res, err := ExecuteBlockEphemerally(cfg, &vm.Config{StatelessExec: true}, nil, ethash.NewFaker(), block,
		&panicOnceReader{NoopReader: state.NewNoopReader()}, state.NewNoopWriter(), nil, nil, log.New())
	require.ErrorIs(t, err, ErrExecPanic)
	require.Nil(t, res)
}

// panicOnceReader panics on the first account read only, so a run that wrongly
// survives the panic reaches the assertions instead of panicking again in finalize.
type panicOnceReader struct {
	*state.NoopReader
	panicked bool
}

func (r *panicOnceReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if !r.panicked {
		r.panicked = true
		panic("injected exec panic")
	}
	return r.NoopReader.ReadAccountData(addr)
}
