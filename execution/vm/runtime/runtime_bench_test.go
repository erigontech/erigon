// Copyright 2015 The go-ethereum Authors
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

package runtime

import (
	"encoding/binary"
	"errors"
	"math/big"
	"strconv"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

func BenchmarkCall(b *testing.B) {
	var definition = `[{"constant":true,"inputs":[],"name":"seller","outputs":[{"name":"","type":"address"}],"type":"function"},{"constant":false,"inputs":[],"name":"abort","outputs":[],"type":"function"},{"constant":true,"inputs":[],"name":"value","outputs":[{"name":"","type":"uint256"}],"type":"function"},{"constant":false,"inputs":[],"name":"refund","outputs":[],"type":"function"},{"constant":true,"inputs":[],"name":"buyer","outputs":[{"name":"","type":"address"}],"type":"function"},{"constant":false,"inputs":[],"name":"confirmReceived","outputs":[],"type":"function"},{"constant":true,"inputs":[],"name":"state","outputs":[{"name":"","type":"uint8"}],"type":"function"},{"constant":false,"inputs":[],"name":"confirmPurchase","outputs":[],"type":"function"},{"inputs":[],"type":"constructor"},{"anonymous":false,"inputs":[],"name":"Aborted","type":"event"},{"anonymous":false,"inputs":[],"name":"PurchaseConfirmed","type":"event"},{"anonymous":false,"inputs":[],"name":"ItemReceived","type":"event"},{"anonymous":false,"inputs":[],"name":"Refunded","type":"event"}]`

	var code = common.Hex2Bytes("6060604052361561006c5760e060020a600035046308551a53811461007457806335a063b4146100865780633fa4f245146100a6578063590e1ae3146100af5780637150d8ae146100cf57806373fac6f0146100e1578063c19d93fb146100fe578063d696069714610112575b610131610002565b610133600154600160a060020a031681565b610131600154600160a060020a0390811633919091161461015057610002565b61014660005481565b610131600154600160a060020a039081163391909116146102d557610002565b610133600254600160a060020a031681565b610131600254600160a060020a0333811691161461023757610002565b61014660025460ff60a060020a9091041681565b61013160025460009060ff60a060020a9091041681146101cc57610002565b005b600160a060020a03166060908152602090f35b6060908152602090f35b60025460009060a060020a900460ff16811461016b57610002565b600154600160a060020a03908116908290301631606082818181858883f150506002805460a060020a60ff02191660a160020a179055506040517f72c874aeff0b183a56e2b79c71b46e1aed4dee5e09862134b8821ba2fddbf8bf9250a150565b80546002023414806101dd57610002565b6002805460a060020a60ff021973ffffffffffffffffffffffffffffffffffffffff1990911633171660a060020a1790557fd5d55c8a68912e9a110618df8d5e2e83b8d83211c57a8ddd1203df92885dc881826060a15050565b60025460019060a060020a900460ff16811461025257610002565b60025460008054600160a060020a0390921691606082818181858883f150508354604051600160a060020a0391821694503090911631915082818181858883f150506002805460a060020a60ff02191660a160020a179055506040517fe89152acd703c9d8c7d28829d443260b411454d45394e7995815140c8cbcbcf79250a150565b60025460019060a060020a900460ff1681146102f057610002565b6002805460008054600160a060020a0390921692909102606082818181858883f150508354604051600160a060020a0391821694503090911631915082818181858883f150506002805460a060020a60ff02191660a160020a179055506040517f8616bbbbad963e4e65b1366f1d75dfb63f9e9704bbbf91fb01bec70849906cf79250a15056")

	abi, err := abi.JSON(strings.NewReader(definition))
	if err != nil {
		b.Fatal(err)
	}

	cpurchase, err := abi.Pack("confirmPurchase")
	if err != nil {
		b.Fatal(err)
	}
	creceived, err := abi.Pack("confirmReceived")
	if err != nil {
		b.Fatal(err)
	}
	refund, err := abi.Pack("refund")
	if err != nil {
		b.Fatal(err)
	}
	cfg := &Config{ChainConfig: &chain.Config{}, BlockNumber: 0, Time: 0, Value: *uint256.MustFromBig(big.NewInt(13377)), Difficulty: uint256.NewInt(0)}
	db := temporaltest.NewTestDB(b, datadir.New(b.TempDir()))
	tx, sd := temporaltest.NewTestTxSD(b, db)
	//cfg.w = state.NewWriter(execctx, nil)
	cfg.State = benchState(b, state.NewReaderV3(sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer cfg.State.Close()
	// cfg carries a non-zero Value, so the origin has to be able to pay it or
	// every call fails the balance check before reaching the interpreter.
	cfg.Origin = accounts.ZeroAddress
	require.NoError(b, cfg.State.SetBalance(cfg.Origin, *uint256.NewInt(1e18), tracing.BalanceChangeUnspecified))
	//cfg.EVMConfig.JumpDestCache = vm.NewJumpDestCache(128)

	tmpdir := b.TempDir()

	// Execute runs the deployed runtime code, so the constructor never initialises
	// seller/value and every entry point hits the contract's pre-REVERT throw.
	// Pinned so the benchmark cannot degrade further without failing; making it
	// execute the purchase flow needs a new fixture.
	inputs := [][]byte{cpurchase, creceived, refund}
	for b.Loop() {
		snap := cfg.State.PushSnapshot()
		for range 400 {
			for _, input := range inputs {
				if _, _, err := Execute(code, input, cfg, tmpdir); !errors.Is(err, vm.ErrInvalidJump) {
					b.Fatalf("expected the contract throw, got %v", err)
				}
			}
		}
		cfg.State.RevertToSnapshot(snap, nil)
		cfg.State.PopSnapshot(snap)
	}
}

func BenchmarkEVM_CREATE_500(bench *testing.B) {
	// initcode size 500K, repeatedly calls CREATE and then modifies the mem contents
	benchmarkEVM_Create(bench, "5b6207a120600080f0600152600056")
}

func BenchmarkEVM_CREATE2_500(bench *testing.B) {
	// initcode size 500K, repeatedly calls CREATE2 and then modifies the mem contents
	benchmarkEVM_Create(bench, "5b586207a120600080f5600152600056")
}

func BenchmarkEVM_CREATE_1200(bench *testing.B) {
	// initcode size 1200K, repeatedly calls CREATE and then modifies the mem contents
	benchmarkEVM_Create(bench, "5b62124f80600080f0600152600056")
}

func BenchmarkEVM_CREATE2_1200(bench *testing.B) {
	// initcode size 1200K, repeatedly calls CREATE2 and then modifies the mem contents
	benchmarkEVM_Create(bench, "5b5862124f80600080f5600152600056")
}

func BenchmarkEVM_RETURN(b *testing.B) {
	// returns a contract that returns a zero-byte slice of len size
	returnContract := func(size uint64) []byte {
		contract := []byte{
			byte(vm.PUSH8), 0, 0, 0, 0, 0, 0, 0, 0, // PUSH8 0xXXXXXXXXXXXXXXXX
			byte(vm.PUSH0),  // PUSH0
			byte(vm.RETURN), // RETURN
		}
		binary.BigEndian.PutUint64(contract[1:], size)
		return contract
	}

	db := temporaltest.NewTestDB(b, datadir.New(b.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(b, db)

	statedb := benchState(b, state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer statedb.Close()
	contractAddr := accounts.InternAddress(common.BytesToAddress([]byte("contract")))

	for _, n := range []uint64{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(strconv.FormatUint(n, 10), func(b *testing.B) {
			b.ReportAllocs()

			contractCode := returnContract(n)
			require.NoError(b, statedb.SetCode(contractAddr, contractCode, tracing.CodeChangeUnspecified))

			cfg := Config{State: statedb}
			setDefaults(&cfg)

			for b.Loop() {
				snap := statedb.PushSnapshot()
				ret, left, err := Call(contractAddr, []byte{}, &cfg)
				mustComplete(b, cfg.GasLimit, left, err)
				if uint64(len(ret)) != n {
					b.Fatalf("expected return size %d, got %d", n, len(ret))
				}
				statedb.RevertToSnapshot(snap, nil)
				statedb.PopSnapshot(snap)
			}
		})
	}
}

// BenchmarkSimpleLoop test a pretty simple loop which loops until OOG
// 55 ms
//
// go test -bench=BenchmarkSimple -run=Benchmark -count 10 ./core/vm/runtime > old.txt
// go test -bench=BenchmarkSimple -run=Benchmark -count 10 ./core/vm/runtime > new.txt
// benchstat old.txt new.txt
func BenchmarkSimpleLoop(b *testing.B) {
	p, lbl := program.New().Jumpdest()
	// Call identity, and pop return value
	staticCallIdentity := p.
		StaticCall(nil, 0x4, 0, 0, 0, 0).
		Op(vm.POP).Jump(lbl).Bytes() // pop return value and jump to label

	p, lbl = program.New().Jumpdest()
	callIdentity := p.
		Call(nil, 0x4, 0, 0, 0, 0, 0).
		Op(vm.POP).Jump(lbl).Bytes() // pop return value and jump to label

	p, lbl = program.New().Jumpdest()
	callInexistant := p.
		Call(nil, 0xff, 0, 0, 0, 0, 0).
		Op(vm.POP).Jump(lbl).Bytes() // pop return value and jump to label

	p, lbl = program.New().Jumpdest()
	// call addr of EOA
	// pop return value and jump to label
	callEOA := p.Call(nil, 0xE0, 0, 0, 0, 0, 0).Op(vm.POP).Jump(lbl).Bytes()

	p, lbl = program.New().Jumpdest()
	// Push as if we were making call, then pop it off again, and loop
	loopingCode := p.Push(0).
		Op(vm.DUP1, vm.DUP1, vm.DUP1).
		Push(0x4).
		Op(vm.GAS, vm.POP, vm.POP, vm.POP, vm.POP, vm.POP, vm.POP).
		Jump(lbl).Bytes()

	p, lbl = program.New().Jumpdest()
	loopingCode2 := p.
		Push(0x01020304).Push(uint64(0x0102030405)).
		Op(vm.POP, vm.POP).
		Op(vm.PUSH6).Append(make([]byte, 6)).Op(vm.JUMP). // Jumpdest zero expressed in 6 bytes
		Jump(lbl).Bytes()

	loopingCode3 := []byte{
		byte(vm.JUMPDEST), //  [ count ]
		// push args for the call
		byte(vm.PUSH4), 1, 2, 3, 4,
		byte(vm.PUSH5), 1, 2, 3, 4, 5,

		byte(vm.POP), byte(vm.POP),
		byte(vm.PUSH6), 0, 0, 0, 0, 0, 0, // jumpdestination
		byte(vm.JUMP),
	}

	p, lbl = program.New().Jumpdest()
	callRevertingContractWithInput := p.
		Call(nil, 0xee, 0, 0, 0x20, 0x0, 0x0).
		Op(vm.POP).Jump(lbl).Bytes() // pop return value and jump to label

	//tracer := logger.NewJSONLogger(nil, os.Stdout)
	//Execute(loopingCode, nil, &Config{
	//	EVMConfig: vm.Config{
	//		Debug:  true,
	//		Tracer: tracer,
	//	}})
	// 100M gas
	benchmarkNonModifyingCode(mdgas.MdGas{Execution: 100_000_000}, staticCallIdentity, "staticcall-identity-100M", "", b)
	benchmarkNonModifyingCode(mdgas.MdGas{Execution: 100_000_000}, callIdentity, "call-identity-100M", "", b)
	benchmarkNonModifyingCode(mdgas.MdGas{Execution: 100_000_000}, loopingCode, "loop-100M", "", b)
	benchmarkNonModifyingCode(mdgas.MdGas{Execution: 100_000_000}, loopingCode2, "loop2-100M", "", b)
	benchmarkNonModifyingCode(mdgas.MdGas{Execution: 100_000_000}, loopingCode3, "loop3-100M", "", b)
	benchmarkNonModifyingCode(mdgas.MdGas{Execution: 100_000_000}, callInexistant, "call-nonexist-100M", "", b)
	benchmarkNonModifyingCode(mdgas.MdGas{Execution: 100_000_000}, callEOA, "call-EOA-100M", "", b)
	benchmarkNonModifyingCode(mdgas.MdGas{Execution: 100_000_000}, callRevertingContractWithInput, "call-reverting-100M", "", b)

	//benchmarkNonModifyingCode(10000000, staticCallIdentity, "staticcall-identity-10M", b)
	//benchmarkNonModifyingCode(10000000, loopingCode, "loop-10M", b)
}

func BenchmarkEVM_SWAP1(b *testing.B) {
	// returns a contract that does n swaps (SWAP1)
	swapContract := func(n uint64) []byte {
		contract := []byte{
			byte(vm.PUSH0), // PUSH0
			byte(vm.PUSH0), // PUSH0
		}
		for range n {
			contract = append(contract, byte(vm.SWAP1))
		}
		return contract
	}

	db := temporaltest.NewTestDB(b, datadir.New(b.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(b, db)
	state := benchState(b, state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer state.Close()
	contractAddr := accounts.InternAddress(common.BytesToAddress([]byte("contract")))

	b.Run("10k", func(b *testing.B) {
		contractCode := swapContract(10_000)
		require.NoError(b, state.SetCode(contractAddr, contractCode, tracing.CodeChangeUnspecified))

		cfg := Config{State: state}
		setDefaults(&cfg)

		for b.Loop() {
			snap := state.PushSnapshot()
			_, left, err := Call(contractAddr, []byte{}, &cfg)
			mustComplete(b, cfg.GasLimit, left, err)
			state.RevertToSnapshot(snap, nil)
			state.PopSnapshot(snap)
		}
	})
}

// benchState builds state on the parallel-execution path, the one staged sync
// runs: the stateObject cache is off and reads resolve from the version map.
func benchState(b *testing.B, reader state.StateReader) *state.IntraBlockState {
	b.Helper()
	statedb := state.NewWithVersionMap(reader, state.NewVersionMap(nil))
	statedb.SetNoMaterialize(true)
	return statedb
}

// mustOOG requires a program that loops until its gas budget is gone to have
// done so. A call reaching an address with no code returns no error and burns
// nothing, so the gas assertion - not the error - catches a broken fixture.
func mustOOG(b *testing.B, left mdgas.MdGas, err error) {
	b.Helper()
	if !errors.Is(err, vm.ErrOutOfGas) || left.Total() != 0 {
		b.Fatalf("expected gas exhaustion, got gasLeft=%d err=%v", left.Total(), err)
	}
}

// mustComplete requires a bounded call to have finished and done work.
func mustComplete(b *testing.B, gasLimit uint64, left mdgas.MdGas, err error) {
	b.Helper()
	if err != nil {
		b.Fatal(err)
	}
	if left.Total() >= gasLimit {
		b.Fatalf("call consumed no gas (gasLeft=%d, limit=%d)", left.Total(), gasLimit)
	}
}

func benchmarkEVM_Create(b *testing.B, code string) {
	db := temporaltest.NewTestDB(b, datadir.New(b.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(b, db)

	err := rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(b, err)

	var (
		statedb  = benchState(b, state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
		sender   = accounts.InternAddress(common.BytesToAddress([]byte("sender")))
		receiver = accounts.InternAddress(common.BytesToAddress([]byte("receiver")))
	)
	defer statedb.Close()

	require.NoError(b, statedb.CreateAccount(sender, true))
	require.NoError(b, statedb.SetCode(receiver, common.FromHex(code), tracing.CodeChangeUnspecified))
	runtimeConfig := Config{
		Origin:      sender,
		State:       statedb,
		GasLimit:    10000000,
		Difficulty:  uint256.NewInt(0x200000),
		Time:        0,
		Coinbase:    accounts.ZeroAddress,
		BlockNumber: 1,
		ChainConfig: &chain.Config{
			ChainID:               uint256.NewInt(1),
			HomesteadBlock:        common.NewUint64(0),
			ByzantiumBlock:        common.NewUint64(0),
			ConstantinopleBlock:   common.NewUint64(0),
			PetersburgBlock:       common.NewUint64(0),
			TangerineWhistleBlock: common.NewUint64(0),
			SpuriousDragonBlock:   common.NewUint64(0),
		},
		EVMConfig: vm.Config{
			//JumpDestCache: vm.NewJumpDestCache(128),
		},
	}
	// Warm up the intpools and stuff
	for b.Loop() {
		snap := statedb.PushSnapshot()
		_, left, err := Call(receiver, []byte{}, &runtimeConfig)
		mustOOG(b, left, err)
		statedb.RevertToSnapshot(snap, nil)
		statedb.PopSnapshot(snap)
	}
	b.StopTimer()
}

// benchmarkNonModifyingCode benchmarks code, but if the code modifies the
// state, this should not be used, since it does not reset the state between runs.
func benchmarkNonModifyingCode(gas mdgas.MdGas, code []byte, name string, tracerCode string, b *testing.B) { //nolint:unparam
	b.Helper()
	cfg := new(Config)
	setDefaults(cfg)
	db := temporaltest.NewTestDB(b, datadir.New(b.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(b, db)

	err := rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(b, err)

	cfg.State = benchState(b, state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer cfg.State.Close()
	cfg.GasLimit = gas.Execution
	//
	// TODO revise
	//
	cfg.Origin = accounts.ZeroAddress
	//if len(tracerCode) > 0 {
	//	tracer, err := tracers.DefaultDirectory.New(tracerCode, new(tracers.Context), nil, cfg.ChainConfig)
	//	if err != nil {
	//		b.Fatal(err)
	//	}
	//	cfg.EVMConfig = vm.Config{
	//		Tracer: tracer.Hooks,
	//	}
	//}

	var (
		destination = accounts.InternAddress(common.BytesToAddress([]byte("contract")))
		vmenv       = NewEnv(cfg)
		sender      = cfg.Origin
	)
	require.NoError(b, cfg.State.CreateAccount(destination, true))
	eoa := accounts.InternAddress(common.HexToAddress("E0"))
	{
		require.NoError(b, cfg.State.CreateAccount(eoa, true))
		require.NoError(b, cfg.State.SetNonce(eoa, 100, tracing.NonceChangeUnspecified))
	}
	reverting := accounts.InternAddress(common.HexToAddress("EE"))
	{
		require.NoError(b, cfg.State.CreateAccount(reverting, true))
		require.NoError(b, cfg.State.SetCode(reverting, []byte{
			byte(vm.PUSH1), 0x00,
			byte(vm.PUSH1), 0x00,
			byte(vm.REVERT),
		}, tracing.CodeChangeUnspecified))
	}

	//cfg.State.CreateAccount(cfg.Origin)
	// set the receiver's (the executing contract) code for execution.
	require.NoError(b, cfg.State.SetCode(destination, code, tracing.CodeChangeUnspecified))
	_, warmLeft, _, warmErr := vmenv.Call(sender, destination, nil, gas, cfg.Value, false /* bailout */)
	mustOOG(b, warmLeft, warmErr)

	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			snap := cfg.State.PushSnapshot()
			_, left, _, err := vmenv.Call(sender, destination, nil, gas, cfg.Value, false /* bailout */)
			mustOOG(b, left, err)
			cfg.State.RevertToSnapshot(snap, nil)
			cfg.State.PopSnapshot(snap)
		}
	})
}
