// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package tests

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"math/big"
	"reflect"
	"testing"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/core/vm"
)

func TestState(t *testing.T) {
	t.Skip()
	t.Parallel()

	st := new(testMatcher)
	// Long tests:
	st.slow(`^stAttackTest/ContractCreationSpam`)
	st.slow(`^stBadOpcode/badOpcodes`)
	st.slow(`^stPreCompiledContracts/modexp`)
	st.slow(`^stQuadraticComplexityTest/`)
	st.slow(`^stStaticCall/static_Call50000`)
	st.slow(`^stStaticCall/static_Return50000`)
	st.slow(`^stStaticCall/static_Call1MB`)
	st.slow(`^stSystemOperationsTest/CallRecursiveBomb`)
	st.slow(`^stTransactionTest/Opcodes_TransactionInit`)
	// Broken tests:
	st.skipLoad(`^stTransactionTest/OverflowGasRequire\.json`) // gasLimit > 256 bits
	st.skipLoad(`^stTransactionTest/zeroSigTransa[^/]*\.json`) // EIP-86 is not supported yet
	// Expected failures:
	st.fails(`(?m)^TestState/stRevertTest/RevertPrecompiledTouch(_storage)?\.json/Byzantium\/0`, "bug in test")
	st.fails(`(?m)^TestState/stRevertTest/RevertPrecompiledTouch(_storage)?\.json/Byzantium\/3`, "bug in test")
	st.fails(`(?m)^TestState/stRevertTest/RevertPrecompiledTouch(_storage)?\.json/Constantinople\/0`, "bug in test")
	st.fails(`(?m)^TestState/stRevertTest/RevertPrecompiledTouch(_storage)?\.json/Constantinople\/3`, "bug in test")
	st.fails(`(?m)^TestState/stRevertTest/RevertPrecompiledTouch(_storage)?\.json/ConstantinopleFix\/0`, "bug in test")
	st.fails(`(?m)^TestState/stRevertTest/RevertPrecompiledTouch(_storage)?\.json/ConstantinopleFix\/3`, "bug in test")

	//st.whitelist(`(?m)^TestState/stSStoreTest/InitCollision\.json`)

	// Work in progress
	st.fails(`(?m)^TestState/stSStoreTest/InitCollision\.json/Constantinople/0`, "work in progress")
	st.fails(`(?m)^TestState/stSStoreTest/InitCollision\.json/Constantinople/1`, "work in progress")
	st.fails(`(?m)^TestState/stSStoreTest/InitCollision\.json/Constantinople/2`, "work in progress")
	st.fails(`(?m)^TestState/stSStoreTest/InitCollision\.json/Constantinople/3`, "work in progress")
	st.fails(`(?m)^TestState/stSStoreTest/InitCollision\.json/ConstantinopleFix/0`, "work in progress")
	st.fails(`(?m)^TestState/stSStoreTest/InitCollision\.json/ConstantinopleFix/1`, "work in progress")
	st.fails(`(?m)^TestState/stSStoreTest/InitCollision\.json/ConstantinopleFix/3`, "work in progress")
	st.fails(`(?m)^TestState/stRevertTest/RevertInCreateInInit\.json/Byzantium/0`, "work in progress")
	st.fails(`(?m)^TestState/stRevertTest/RevertInCreateInInit\.json/Constantinople/0`, "work in progress")
	st.fails(`(?m)^TestState/stRevertTest/RevertInCreateInInit\.json/ConstantinopleFix/0`, "work in progress")
	st.fails(`(?m)^TestState/stCreate2/create2collisionStorage\.json/Constantinople/0`, "work in progress")
	st.fails(`(?m)^TestState/stCreate2/create2collisionStorage\.json/Constantinople/1`, "work in progress")
	st.fails(`(?m)^TestState/stCreate2/create2collisionStorage\.json/Constantinople/1`, "work in progress")
	st.fails(`(?m)^TestState/stCreate2/create2collisionStorage\.json/Constantinople/2`, "work in progress")
	st.fails(`(?m)^TestState/stCreate2/create2collisionStorage\.json/ConstantinopleFix/0`, "work in progress")
	st.fails(`(?m)^TestState/stCreate2/create2collisionStorage\.json/ConstantinopleFix/1`, "work in progress")
	st.fails(`(?m)^TestState/stCreate2/create2collisionStorage\.json/ConstantinopleFix/2`, "work in progress")
	st.fails(`(?m)^TestState/stExtCodeHash/dynamicAccountOverwriteEmpty\.json/Constantinople/0`, "work in progress")
	st.fails(`(?m)^TestState/stExtCodeHash/dynamicAccountOverwriteEmpty\.json/ConstantinopleFix/0`, "work in progress")
	st.fails(`(?m)^TestState/stCreate2/RevertInCreateInInitCreate2\.json/Constantinople/0`, "work in progress")
	st.fails(`(?m)^TestState/stCreate2/RevertInCreateInInitCreate2\.json/ConstantinopleFix/0`, "work in progress")

	st.walk(t, stateTestDir, func(t *testing.T, name string, test *StateTest) {
		for _, subtest := range test.Subtests() {
			subtest := subtest
			key := fmt.Sprintf("%s/%d", subtest.Fork, subtest.Index)
			t.Run(key, func(t *testing.T) {
				withTrace(t, test.gasLimit(subtest), func(vmconfig vm.Config) error {
					config, ok := Forks[subtest.Fork]
					if !ok {
						return UnsupportedForkError{subtest.Fork}
					}
					ctx := config.WithEIPsFlags(context.Background(), big.NewInt(1))
					_, _, _, err := test.Run(ctx, subtest, vmconfig)
					return st.checkFailure(t, err)
				})
			})
		}
	})
}

// Transactions with gasLimit above this value will not get a VM trace on failure.
const traceErrorLimit = 400000

// The VM config for state tests that accepts --vm.* command line arguments.
var testVMConfig = func() vm.Config {
	vmconfig := vm.Config{}
	flag.StringVar(&vmconfig.EVMInterpreter, utils.EVMInterpreterFlag.Name, utils.EVMInterpreterFlag.Value, utils.EVMInterpreterFlag.Usage)
	flag.StringVar(&vmconfig.EWASMInterpreter, utils.EWASMInterpreterFlag.Name, utils.EWASMInterpreterFlag.Value, utils.EWASMInterpreterFlag.Usage)
	flag.Parse()
	return vmconfig
}()

func withTrace(t *testing.T, gasLimit uint64, test func(vm.Config) error) {
	err := test(testVMConfig)
	if err == nil {
		return
	}
	t.Error(err)
	if gasLimit > traceErrorLimit {
		t.Log("gas limit too high for EVM trace")
		return
	}
	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)
	tracer := vm.NewJSONLogger(&vm.LogConfig{DisableMemory: true}, w)
	err2 := test(vm.Config{Debug: true, Tracer: tracer})
	if !reflect.DeepEqual(err, err2) {
		t.Errorf("different error for second run: %v", err2)
	}
	w.Flush()
	if buf.Len() == 0 {
		t.Log("no EVM operation logs generated")
	} else {
		t.Log("EVM operation log:\n" + buf.String())
	}
	//t.Logf("EVM output: 0x%x", tracer.Output())
	//t.Logf("EVM error: %v", tracer.Error())
}
