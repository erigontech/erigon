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

package paymentlane

import (
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	_ "github.com/erigontech/erigon/bsc/chain"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// The PaymentLane storage layout: slot 0 is the ratio, slot 1 is the length of the listed
// contracts array, whose elements start at keccak(1).
const (
	ratioSlot               = 0
	paymentContractsLenSlot = 1
)

var contract = accounts.InternAddress(ContractAddress)

type laneState struct {
	ibs *state.IntraBlockState
	cfg *chain.Config
}

func newLaneState(t *testing.T) *laneState {
	t.Helper()
	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	alloc, err := types.DecodeGenesisAlloc(spec.Config.Parlia.BlockAlloc["1793241000"])
	require.NoError(t, err)
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	require.NoError(t, ibs.SetCode(contract, alloc[ContractAddress].Code, tracing.CodeChangeUnspecified))
	return &laneState{ibs: ibs, cfg: spec.Config}
}

func (s *laneState) set(t *testing.T, slot common.Hash, v *uint256.Int) {
	t.Helper()
	require.NoError(t, s.ibs.SetState(contract, accounts.InternKey(slot), *v))
}

func (s *laneState) setList(t *testing.T, length uint64, addrs []common.Address) {
	t.Helper()
	s.set(t, common.Hash{31: paymentContractsLenSlot}, uint256.NewInt(length))
	lenSlot := common.Hash{31: paymentContractsLenSlot}
	base := new(uint256.Int).SetBytes(crypto.Keccak256(lenSlot[:]))
	for i, a := range addrs {
		s.set(t, new(uint256.Int).AddUint64(base, uint64(i)).Bytes32(), new(uint256.Int).SetBytes(a[:]))
	}
}

func (s *laneState) call(t *testing.T) getter {
	t.Helper()
	blockContext := evmtypes.BlockContext{
		CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
		BlockNumber: 139533777,
		Time:        1793241001,
		GasLimit:    55_000_000,
	}
	evm := vm.NewEVM(blockContext, evmtypes.TxContext{}, s.ibs, s.cfg, vm.Config{})
	return func(input []byte) ([]byte, error) {
		ret, _, _, err := evm.StaticCall(accounts.ZeroAddress, contract, input, mdgas.MdGas{Execution: 30_000_000})
		return ret, err
	}
}

func TestLoadMetaReadsTheUnwrittenDefault(t *testing.T) {
	s := newLaneState(t)
	meta, err := LoadMeta(s.call(t))
	require.NoError(t, err)
	require.EqualValues(t, 2_750_000, meta.Quota(55_000_000))
	require.Empty(t, meta.Listed())
}

func TestLoadMetaPagesLongListsAndReadsTheGovernedRatio(t *testing.T) {
	s := newLaneState(t)
	s.set(t, common.Hash{31: ratioSlot}, uint256.NewInt(800))
	listed := make([]common.Address, 300)
	for i := range listed {
		listed[i] = common.BigToAddress(new(big.Int).SetUint64(uint64(i + 0x10000)))
	}
	s.setList(t, uint64(len(listed)), listed)

	meta, err := LoadMeta(s.call(t))
	require.NoError(t, err)
	require.EqualValues(t, 800*55_000_000/10_000, meta.Quota(55_000_000))
	require.Len(t, meta.Listed(), len(listed))
	for _, a := range listed {
		require.Contains(t, meta.Listed(), a)
	}
}

func TestLoadMetaRejectsListedSetAboveContractLimit(t *testing.T) {
	s := newLaneState(t)
	s.setList(t, maxListedContracts+1, nil)
	_, err := LoadMeta(s.call(t))
	require.ErrorIs(t, err, ErrCorruptConfig)
	require.Contains(t, err.Error(), "exceeds limit")
}

func TestLoadMetaRejectsDuplicates(t *testing.T) {
	s := newLaneState(t)
	dup := common.Address{0xaa}
	s.setList(t, 2, []common.Address{dup, dup})
	_, err := LoadMeta(s.call(t))
	require.ErrorIs(t, err, ErrCorruptConfig)
	require.Contains(t, err.Error(), "duplicate")
}

func TestLoadMetaRejectsARatioOutsideTheGuard(t *testing.T) {
	for _, bad := range []uint64{maxLaneRatio + 1, math.MaxUint64} {
		s := newLaneState(t)
		s.set(t, common.Hash{31: ratioSlot}, uint256.NewInt(bad))
		_, err := LoadMeta(s.call(t))
		require.ErrorIsf(t, err, ErrCorruptConfig, "ratio %d", bad)
	}
}

func TestLoadMetaRejectsAFailingGetter(t *testing.T) {
	_, err := LoadMeta(func([]byte) ([]byte, error) { return nil, vm.ErrExecutionReverted })
	require.ErrorIs(t, err, ErrCorruptConfig)
	require.ErrorIs(t, err, vm.ErrExecutionReverted)
}
