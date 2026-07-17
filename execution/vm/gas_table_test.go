// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package vm_test

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func TestMemoryGasCost(t *testing.T) {
	t.Parallel()
	tests := []struct {
		size     uint64
		cost     uint64
		overflow bool
	}{
		{0x1fffffffe0, 36028809887088637, false},
		{0x1fffffffe1, 0, true},
	}
	for i, tt := range tests {
		v, err := vm.MemoryGasCost(&vm.CallContext{}, tt.size)
		if (err == vm.ErrGasUintOverflow) != tt.overflow {
			t.Errorf("test %d: overflow mismatch: have %v, want %v", i, err == vm.ErrGasUintOverflow, tt.overflow)
		}
		if v != tt.cost {
			t.Errorf("test %d: gas cost mismatch: have %v, want %v", i, v, tt.cost)
		}
	}
}

var eip2200Tests = []struct {
	original byte
	gaspool  uint64
	input    string
	used     uint64
	refund   uint64
	failure  error
}{
	{0, math.MaxUint64, "0x60006000556000600055", 1612, 0, nil},                // 0 -> 0 -> 0
	{0, math.MaxUint64, "0x60006000556001600055", 20812, 0, nil},               // 0 -> 0 -> 1
	{0, math.MaxUint64, "0x60016000556000600055", 20812, 19200, nil},           // 0 -> 1 -> 0
	{0, math.MaxUint64, "0x60016000556002600055", 20812, 0, nil},               // 0 -> 1 -> 2
	{0, math.MaxUint64, "0x60016000556001600055", 20812, 0, nil},               // 0 -> 1 -> 1
	{1, math.MaxUint64, "0x60006000556000600055", 5812, 15000, nil},            // 1 -> 0 -> 0
	{1, math.MaxUint64, "0x60006000556001600055", 5812, 4200, nil},             // 1 -> 0 -> 1
	{1, math.MaxUint64, "0x60006000556002600055", 5812, 0, nil},                // 1 -> 0 -> 2
	{1, math.MaxUint64, "0x60026000556000600055", 5812, 15000, nil},            // 1 -> 2 -> 0
	{1, math.MaxUint64, "0x60026000556003600055", 5812, 0, nil},                // 1 -> 2 -> 3
	{1, math.MaxUint64, "0x60026000556001600055", 5812, 4200, nil},             // 1 -> 2 -> 1
	{1, math.MaxUint64, "0x60026000556002600055", 5812, 0, nil},                // 1 -> 2 -> 2
	{1, math.MaxUint64, "0x60016000556000600055", 5812, 15000, nil},            // 1 -> 1 -> 0
	{1, math.MaxUint64, "0x60016000556002600055", 5812, 0, nil},                // 1 -> 1 -> 2
	{1, math.MaxUint64, "0x60016000556001600055", 1612, 0, nil},                // 1 -> 1 -> 1
	{0, math.MaxUint64, "0x600160005560006000556001600055", 40818, 19200, nil}, // 0 -> 1 -> 0 -> 1
	{1, math.MaxUint64, "0x600060005560016000556000600055", 10818, 19200, nil}, // 1 -> 0 -> 1 -> 0
	{1, 2306, "0x6001600055", 2306, 0, vm.ErrOutOfGas},                         // 1 -> 1 (2300 sentry + 2xPUSH)
	{1, 2307, "0x6001600055", 806, 0, nil},                                     // 1 -> 1 (2301 sentry + 2xPUSH)
}

func testTemporalTxSD(t *testing.T) (kv.TemporalRwTx, *execctx.SharedDomains) {
	dirs := datadir.New(t.TempDir())

	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(context.Background()) //nolint:gocritic
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	return tx, sd
}

func TestEIP2200(t *testing.T) {
	for i, tt := range eip2200Tests {

		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			tx, sd := testTemporalTxSD(t)
			txNum, _, err := sd.SeekCommitment(t.Context(), tx)
			require.NoError(t, err)

			r, w := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)
			s := state.New(r)

			address := accounts.InternAddress(common.BytesToAddress([]byte("contract")))
			s.CreateAccount(address, true)
			s.SetCode(address, hexutil.MustDecode(tt.input), tracing.CodeChangeUnspecified)
			s.SetState(address, accounts.ZeroKey, *uint256.NewInt(uint64(tt.original)))

			vmctx := evmtypes.BlockContext{
				CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
				Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
					return nil
				},
			}
			_ = s.CommitBlock(vmctx.Rules(chain.TestChainBerlinConfig), w)
			vmenv := vm.NewEVM(vmctx, evmtypes.TxContext{}, s, chain.TestChainBerlinConfig, vm.Config{ExtraEips: []int{2200}})
			mdGas := mdgas.MdGas{
				Regular: tt.gaspool,
			}
			_, gas, _, err := vmenv.Call(accounts.ZeroAddress, address, nil, mdGas, uint256.Int{}, false /* bailout */)
			if !errors.Is(err, tt.failure) {
				t.Errorf("test %d: failure mismatch: have %v, want %v", i, err, tt.failure)
			}
			if used := tt.gaspool - gas.Regular; used != tt.used {
				t.Errorf("test %d: gas used mismatch: have %v, want %v", i, used, tt.used)
			}
			if refund := vmenv.IntraBlockState().GetRefund(); refund != tt.refund {
				t.Errorf("test %d: gas refund mismatch: have %v, want %v", i, refund, tt.refund)
			}
		})
	}
}

// push1 is the gas for a PUSH1 (GasFastestStep); each SSTORE in the fixtures below
// is preceded by two PUSH1 (value, slot).
const push1 = 3

var eip8038SStoreTests = []struct {
	name     string
	original byte
	input    string
	// gas used in each dimension and the accumulated refund, computed from the
	// EIP-8038 SSTORE matrix (cold slot 3000, STORAGE_WRITE 10000, per-slot state
	// gas 97920, clear refund 12480, restore refund = STORAGE_WRITE).
	usedRegular uint64
	usedState   uint64
	refund      uint64
}{
	{
		// 0 -> 1: create slot. cold access + STORAGE_WRITE, plus per-slot state gas.
		name:        "create slot",
		original:    0,
		input:       "0x6001600055",
		usedRegular: 2*push1 + params.ColdStorageAccessCostEIP8038 + params.StorageWriteCostEIP8038,
		usedState:   params.StateGasPerStorageSet,
	},
	{
		// 1 -> 0: delete slot. cold access + STORAGE_WRITE regular, 12480 clear refund.
		name:        "delete slot adds clear refund",
		original:    1,
		input:       "0x6000600055",
		usedRegular: 2*push1 + params.ColdStorageAccessCostEIP8038 + params.StorageWriteCostEIP8038,
		refund:      12480, // REFUND_STORAGE_CLEAR
	},
	{
		// 1 -> 2 -> 1: reset to original (existing) refunds the full STORAGE_WRITE.
		name:        "reset to original refunds storage write",
		original:    1,
		input:       "0x60026000556001600055",
		usedRegular: 4*push1 + params.ColdStorageAccessCostEIP8038 + params.StorageWriteCostEIP8038 + params.WarmStorageReadCostEIP2929,
		refund:      10000, // full STORAGE_WRITE
	},
	{
		// 1 -> 1: no-op still pays the cold slot access (slot warmed before the check).
		name:        "noop pays cold access",
		original:    1,
		input:       "0x6001600055",
		usedRegular: 2*push1 + params.ColdStorageAccessCostEIP8038,
	},
	{
		// 0 -> 1 -> 2: dirty update after a same-tx create; second write is warm, no refund.
		name:        "dirty update after create",
		original:    0,
		input:       "0x60016000556002600055",
		usedRegular: 4*push1 + params.ColdStorageAccessCostEIP8038 + params.StorageWriteCostEIP8038 + params.WarmStorageReadCostEIP2929,
		usedState:   params.StateGasPerStorageSet,
	},
}

// TestEIP8038SStore pins the EIP-8038 SSTORE gas matrix (repriced cold access and
// STORAGE_WRITE, the split regular/state dimensions on slot creation, and the 12480
// clear / full STORAGE_WRITE restore refunds) so a regression in any of those
// constants surfaces here. Behaviour is already validated end-to-end by the devnet
// spec shards; this locks it in cheaply at the unit level.
func TestEIP8038SStore(t *testing.T) {
	for _, tt := range eip8038SStoreTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tx, sd := testTemporalTxSD(t)
			txNum, _, err := sd.SeekCommitment(t.Context(), tx)
			require.NoError(t, err)
			r, w := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)
			s := state.New(r)
			address := accounts.InternAddress(common.BytesToAddress([]byte("contract")))
			s.CreateAccount(address, true)
			s.SetCode(address, hexutil.MustDecode(tt.input), tracing.CodeChangeUnspecified)
			s.SetState(address, accounts.ZeroKey, *uint256.NewInt(uint64(tt.original)))
			vmctx := evmtypes.BlockContext{
				CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
				Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
					return nil
				},
			}
			_ = s.CommitBlock(vmctx.Rules(chain.AllProtocolChanges), w)
			vmenv := vm.NewEVM(vmctx, evmtypes.TxContext{}, s, chain.AllProtocolChanges, vm.Config{})
			pool := mdgas.MdGas{Regular: 10_000_000, State: 10_000_000}
			_, gas, _, err := vmenv.Call(accounts.ZeroAddress, address, nil, pool, uint256.Int{}, false /* bailout */)
			require.NoError(t, err)
			require.Equal(t, tt.usedRegular, pool.Regular-gas.Regular, "regular gas used")
			require.Equal(t, tt.usedState, pool.State-gas.State, "state gas used")
			require.Equal(t, tt.refund, vmenv.IntraBlockState().GetRefund(), "refund")
		})
	}
}

// TestCallNewAccountSpillBefore63of64 pins the EIP-8037 charge order for a value
// CALL to a dead account: the NEW_ACCOUNT state charge (spilling into regular gas
// when the reservoir can't cover it) is applied before the 63/64 child allowance,
// per EELS amsterdam call(). With the reverse order a caller forwarding ~all gas
// would OOG the whole frame on the spill.
func TestCallNewAccountSpillBefore63of64(t *testing.T) {
	t.Parallel()
	// CALL(gas(), 0xdeadbeef, value=1, no args/ret); store success at mem[0]; return 32 bytes.
	callerCode := "0x60006000600060006001" +
		"7300000000000000000000000000000000deadbeef" +
		"5af1" +
		"600052" +
		"60206000f3"
	callee := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000deadbeef"))
	// Leftover gas is hand-computed against the Amsterdam jump table (EELS pin):
	// pre-CALL opcodes 20, warm base 100, cold access 2900, CALL_VALUE 10300,
	// tail 15; NEW_ACCOUNT 183600; empty callee returns callGas + 2300 stipend.
	for _, tt := range []struct {
		name            string
		pool            mdgas.MdGas
		leftoverRegular uint64
		leftoverState   uint64
	}{
		{
			// zero reservoir → full NEW_ACCOUNT spills to regular.
			// base = 500000-20-100-2900-10300-183600 = 303080; callGas = 303080 - 303080/64 = 298345;
			// leftover = 4735 + 298345 + 2300 - 15 = 305365.
			name:            "zero reservoir, full spill",
			pool:            mdgas.MdGas{Regular: 500_000, State: 0},
			leftoverRegular: 305_365,
			leftoverState:   0,
		},
		{
			// funded reservoir → no spill; base = 500000-20-100-13200 = 486680;
			// callGas = 486680 - 486680/64 = 479076; leftover = 7604 + 479076 + 2300 - 15 = 488965.
			name:            "funded reservoir, no spill",
			pool:            mdgas.MdGas{Regular: 500_000, State: 200_000},
			leftoverRegular: 488_965,
			leftoverState:   200_000 - 183_600,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tx, sd := testTemporalTxSD(t)
			txNum, _, err := sd.SeekCommitment(t.Context(), tx)
			require.NoError(t, err)
			r, w := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)
			s := state.New(r)
			caller := accounts.InternAddress(common.BytesToAddress([]byte("contract")))
			s.CreateAccount(caller, true)
			s.SetCode(caller, hexutil.MustDecode(callerCode), tracing.CodeChangeUnspecified)
			vmctx := evmtypes.BlockContext{
				CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
				Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
					return nil
				},
			}
			_ = s.CommitBlock(vmctx.Rules(chain.AllProtocolChanges), w)
			vmenv := vm.NewEVM(vmctx, evmtypes.TxContext{}, s, chain.AllProtocolChanges, vm.Config{})
			ret, gas, _, err := vmenv.Call(accounts.ZeroAddress, caller, nil, tt.pool, uint256.Int{}, false /* bailout */)
			require.NoError(t, err, "outer frame must not OOG: NEW_ACCOUNT spill must precede the 63/64 computation")
			require.Len(t, ret, 32)
			require.Equal(t, byte(1), ret[31], "inner CALL must succeed")
			require.Equal(t, tt.leftoverRegular, gas.Regular, "leftover regular gas")
			require.Equal(t, tt.leftoverState, gas.State, "leftover state gas")
			exists, err := vmenv.IntraBlockState().Exist(callee)
			require.NoError(t, err)
			require.True(t, exists, "callee account must have been created")
		})
	}
}

func TestCreate2OntoExistingAccountSkipsNewAccountCharge(t *testing.T) {
	t.Parallel()
	tx, sd := testTemporalTxSD(t)
	txNum, _, err := sd.SeekCommitment(t.Context(), tx)
	require.NoError(t, err)
	r, w := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)
	s := state.New(r)
	initCode := program.New().Push(0).Push(400_000).Op(vm.MSTORE).Return(0, 0).Bytes()
	salt := uint256.NewInt(0)
	factoryAddress := common.HexToAddress("0xfac0")
	factory := accounts.InternAddress(factoryAddress)
	target := accounts.InternAddress(types.CreateAddress2(factoryAddress, salt.Bytes32(), accounts.InternCodeHash(crypto.Keccak256Hash(initCode))))
	factoryCode := program.New().Create2(initCode, salt).Push(0).Op(vm.MSTORE).Return(0, 32).Bytes()
	s.CreateAccount(factory, true)
	s.SetCode(factory, factoryCode, tracing.CodeChangeUnspecified)
	s.CreateAccount(target, false)
	s.AddBalance(target, *uint256.NewInt(1), tracing.BalanceChangeUnspecified)
	vmctx := evmtypes.BlockContext{
		CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
		Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
			return nil
		},
	}
	_ = s.CommitBlock(vmctx.Rules(chain.AllProtocolChanges), w)
	var enteredCreateGas, forwardedCreateGas uint64
	hooks := &tracing.Hooks{
		OnEnter: func(_ int, typ byte, _, _ accounts.Address, _ bool, _ []byte, gas uint64, _ uint256.Int, _ []byte) {
			if vm.OpCode(typ) == vm.CREATE2 {
				enteredCreateGas = gas
			}
		},
		OnGasChange: func(old, new uint64, reason tracing.GasChangeReason) {
			if reason == tracing.GasChangeCallContractCreation2 {
				forwardedCreateGas = old - new
			}
		},
	}
	vmenv := vm.NewEVM(vmctx, evmtypes.TxContext{}, s, chain.AllProtocolChanges, vm.Config{Tracer: hooks})
	ret, _, _, err := vmenv.Call(accounts.ZeroAddress, factory, nil, mdgas.MdGas{Regular: 500_000}, uint256.Int{}, false)
	require.NoError(t, err)
	require.Equal(t, target.Value(), common.BytesToAddress(ret))
	require.Equal(t, forwardedCreateGas, enteredCreateGas)
}

var createGasTests = []struct {
	code    string
	eip3860 bool
	gasUsed uint64
}{
	// create(0, 0, 0xc000)
	{"0x61C00060006000f0", false, 41225},
	// create(0, 0, 0xc000)
	{"0x61C00060006000f0", true, 44297},
	// create2(0, 0, 0xc000, 0)
	{"0x600061C00060006000f5", false, 50444},
	// create2(0, 0, 0xc000, 0)
	{"0x600061C00060006000f5", true, 53516},
}

func TestCreateGas(t *testing.T) {
	t.Parallel()
	db := testutil.TemporalDB(t)
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	for i, tt := range createGasTests {
		address := accounts.InternAddress(common.BytesToAddress([]byte("contract")))

		domains, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
		require.NoError(t, err)

		stateReader := rpchelper.NewLatestStateReader(domains.AsGetter(tx))
		stateWriter := rpchelper.NewLatestStateWriter(tx, domains, (*freezeblocks.BlockReader)(nil), 0)

		s := state.New(stateReader)
		s.CreateAccount(address, true)
		s.SetCode(address, hexutil.MustDecode(tt.code), tracing.CodeChangeUnspecified)

		vmctx := evmtypes.BlockContext{
			CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
			Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
				return nil
			},
		}
		//
		// TODO revis BlockContext and add test for eip8037?
		//
		_ = s.CommitBlock(vmctx.Rules(chain.TestChainBerlinConfig), stateWriter)
		config := vm.Config{}
		if tt.eip3860 {
			config.ExtraEips = []int{3860}
		}

		vmenv := vm.NewEVM(vmctx, evmtypes.TxContext{}, s, chain.TestChainBerlinConfig, config)
		startGas := mdgas.MdGas{
			Regular: math.MaxUint64,
		}
		_, gas, _, err := vmenv.Call(accounts.ZeroAddress, address, nil, startGas, uint256.Int{}, false /* bailout */)
		if err != nil {
			t.Errorf("test %d execution failed: %v", i, err)
		}
		if gasUsed := startGas.Regular - gas.Regular; gasUsed != tt.gasUsed {
			t.Errorf("test %d: gas used mismatch: have %v, want %v", i, gasUsed, tt.gasUsed)
		}
		domains.Close()
	}
	tx.Rollback()
}

// TestSystemCallZeroValueSkipsTransferChecks verifies that zero-value system
// calls do not invoke CanTransfer or Transfer.
func TestSystemCallZeroValueSkipsTransferChecks(t *testing.T) {
	t.Parallel()

	tx, sd := testTemporalTxSD(t)
	txNum, _, err := sd.SeekCommitment(t.Context(), tx)
	require.NoError(t, err)

	r, w := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)
	s := state.New(r)

	address := accounts.InternAddress(common.BytesToAddress([]byte("callee")))

	canTransferCalled := false
	transferCalled := false
	canTransferErr := errors.New("can transfer should not be called")
	transferErr := errors.New("transfer should not be called")

	vmctx := evmtypes.BlockContext{
		CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) {
			canTransferCalled = true
			return false, canTransferErr
		},
		Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
			transferCalled = true
			return transferErr
		},
	}
	_ = s.CommitBlock(vmctx.Rules(chain.TestChainBerlinConfig), w)

	vmenv := vm.NewEVM(vmctx, evmtypes.TxContext{}, s, chain.TestChainBerlinConfig, vm.Config{})
	_, _, _, err = vmenv.Call(params.SystemAddress, address, nil, mdgas.MdGas{Regular: math.MaxUint64}, uint256.Int{}, false /* bailout */)
	require.NoError(t, err)
	require.False(t, canTransferCalled, "CanTransfer should be skipped for zero-value system calls")
	require.False(t, transferCalled, "Transfer should be skipped for zero-value system calls")
}
