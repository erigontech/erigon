// Copyright 2024 The Erigon Authors
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

package vm

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"pgregory.net/rapid"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/execution/chain"
)

func TestInterpreterReadonly(t *testing.T) {
	t.Parallel()
	c := NewJumpDestCache(128)
	rapid.Check(t, func(t *rapid.T) {
		env := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.TestChainConfig, Config{})

		isEVMSliceTest := rapid.SliceOfN(rapid.Bool(), 1, -1).Draw(t, "tevm")
		readOnlySliceTest := rapid.SliceOfN(rapid.Bool(), len(isEVMSliceTest), len(isEVMSliceTest)).Draw(t, "readonly")

		isEVMCalled := make([]bool, len(isEVMSliceTest))
		readOnlies := make([]*readOnlyState, len(readOnlySliceTest))
		currentIdx := new(int)
		*currentIdx = -1

		evmInterpreter := &testVM{
			readonlyGetSetter: env.interpreter.(*EVMInterpreter),

			recordedReadOnlies:  &readOnlies,
			recordedIsEVMCalled: &isEVMCalled,

			env:               env,
			isEVMSliceTest:    isEVMSliceTest,
			readOnlySliceTest: readOnlySliceTest,
			currentIdx:        currentIdx,
		}

		env.interpreter = evmInterpreter

		dummyContract := NewContract(
			&dummyContractRef{},
			common.Address{},
			new(uint256.Int),
			0,
			false,
			c,
		)

		newTestSequential(env, currentIdx, readOnlySliceTest, isEVMSliceTest).Run(dummyContract, nil, false)

		var gotReadonly bool
		var firstReadOnly int

		// properties-invariants

		if len(readOnlies) != len(readOnlySliceTest) {
			t.Fatalf("expected static calls the same stack length as generated. got %d, expected %d", len(readOnlies), len(readOnlySliceTest))
		}

		if len(isEVMCalled) != len(isEVMSliceTest) {
			t.Fatalf("expected VM calls the same stack length as generated. got %d, expected %d", len(isEVMCalled), len(isEVMSliceTest))
		}

		if *currentIdx != len(readOnlies) {
			t.Fatalf("expected VM calls the same amount of calls as generated calls. got %d, expected %d", *currentIdx, len(readOnlies))
		}

		for i, readOnly := range readOnlies {

			if readOnly.outer != readOnlySliceTest[i] {
				t.Fatalf("outer readOnly appeared in %d index, got readOnly %t, expected %t",
					i, readOnly.outer, readOnlySliceTest[i])
			}

			if i > 0 {
				if readOnly.before != readOnlies[i-1].in {
					t.Fatalf("before readOnly appeared in %d index, got readOnly %t, expected %t",
						i, readOnly.before, readOnlies[i-1].in)
				}
			}

			if readOnly.in && !gotReadonly {
				gotReadonly = true
				firstReadOnly = i
			}

			if gotReadonly {
				if !readOnly.in {
					t.Fatalf("readOnly appeared in %d index, got non-readOnly in %d: %v",
						firstReadOnly, i, trace(isEVMCalled, readOnlies))
				}

				switch {
				case i < firstReadOnly:
					if readOnly.after != false {
						t.Fatalf("after readOnly appeared in %d index(first readonly %d, case <firstReadOnly), got readOnly %t, expected %t",
							i, firstReadOnly, readOnly.after, false)
					}
				case i == firstReadOnly:
					if readOnly.after != false {
						t.Fatalf("after readOnly appeared in %d index(first readonly %d, case ==firstReadOnly), got readOnly %t, expected %t",
							i, firstReadOnly, readOnly.after, false)
					}
				case i > firstReadOnly:
					if readOnly.after != true {
						t.Fatalf("after readOnly appeared in %d index(first readonly %d, case >firstReadOnly), got readOnly %t, expected %t",
							i, firstReadOnly, readOnly.after, true)
					}
				}
			} else {
				if readOnly.after != false {
					t.Fatalf("after readOnly didn't appear. %d index, got readOnly %t, expected %t",
						i, readOnly.after, false)
				}
			}
		}
	})
}

func TestReadonlyBasicCases(t *testing.T) {
	t.Parallel()
	c := NewJumpDestCache(128)

	cases := []struct {
		testName          string
		readonlySliceTest []bool

		expectedReadonlySlice []readOnlyState
	}{
		{
			"simple non-readonly",
			[]bool{false},

			[]readOnlyState{
				{
					false,
					false,
					false,
					false,
				},
			},
		},
		{
			"simple readonly",
			[]bool{true},

			[]readOnlyState{
				{
					true,
					true,
					true,
					false,
				},
			},
		},

		{
			"2 calls non-readonly",
			[]bool{false, false},

			[]readOnlyState{
				{
					false,
					false,
					false,
					false,
				},
				{
					false,
					false,
					false,
					false,
				},
			},
		},

		{
			"2 calls true,false",
			[]bool{true, false},

			[]readOnlyState{
				{
					true,
					true,
					true,
					false,
				},
				{
					true,
					true,
					true,
					true,
				},
			},
		},
		{
			"2 calls false,true",
			[]bool{false, true},

			[]readOnlyState{
				{
					false,
					false,
					false,
					false,
				},
				{
					true,
					true,
					true,
					false,
				},
			},
		},
		{
			"2 calls readonly",
			[]bool{true, true},

			[]readOnlyState{
				{
					true,
					true,
					true,
					true,
				},
				{
					true,
					true,
					true,
					true,
				},
			},
		},
	}

	type evmsParamsTest struct {
		emvs   []bool
		suffix string
	}

	evmsTest := make([]evmsParamsTest, len(cases))

	// fill all possible isEVM combinations
	for i, testCase := range cases {
		isEVMSliceTest := make([]bool, len(testCase.readonlySliceTest))

		copy(isEVMSliceTest, testCase.readonlySliceTest)
		evmsTest[i].emvs = isEVMSliceTest

		suffix := "-isEVMSliceTest"
		for _, evmParam := range testCase.readonlySliceTest {
			if evmParam {
				suffix += "-true"
			} else {
				suffix += "-false"
			}
		}

		evmsTest[i].suffix = suffix
	}

	for _, testCase := range cases {
		for _, evmsParams := range evmsTest {

			testcase := testCase
			evmsTestcase := evmsParams

			if len(testcase.readonlySliceTest) != len(evmsTestcase.emvs) {
				continue
			}

			t.Run(testcase.testName+evmsTestcase.suffix, func(t *testing.T) {
				t.Parallel()
				readonlySliceTest := testcase.readonlySliceTest

				env := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.TestChainConfig, Config{})

				readonliesGot := make([]*readOnlyState, len(testcase.readonlySliceTest))
				isEVMGot := make([]bool, len(evmsTestcase.emvs))

				currentIdx := new(int)
				*currentIdx = -1

				evmInterpreter := &testVM{
					readonlyGetSetter: env.interpreter.(*EVMInterpreter),

					recordedReadOnlies:  &readonliesGot,
					recordedIsEVMCalled: &isEVMGot,

					env:               env,
					isEVMSliceTest:    evmsTestcase.emvs,
					readOnlySliceTest: testcase.readonlySliceTest,
					currentIdx:        currentIdx,
				}

				env.interpreter = evmInterpreter

				dummyContract := NewContract(
					&dummyContractRef{},
					common.Address{},
					new(uint256.Int),
					0,
					false,
					c,
				)

				newTestSequential(env, currentIdx, readonlySliceTest, evmsTestcase.emvs).Run(dummyContract, nil, false)

				if len(readonliesGot) != len(readonlySliceTest) {
					t.Fatalf("expected static calls the same stack length as generated. got %d, expected %d - %v", len(readonliesGot), len(readonlySliceTest), readonlySliceTest)
				}

				if len(isEVMGot) != len(evmsTestcase.emvs) {
					t.Fatalf("expected VM calls the same stack length as generated. got %d, expected %d - %v, readonly %v", len(isEVMGot), len(evmsTestcase.emvs), evmsTestcase.emvs, readonlySliceTest)
				}

				if *currentIdx != len(readonlySliceTest) {
					t.Fatalf("expected VM calls the same amount of calls as generated calls. got %d, expected %d", *currentIdx, len(readonlySliceTest))
				}

				var gotReadonly bool
				var firstReadOnly int

				for callIndex, readOnly := range readonliesGot {
					if readOnly.outer != readonlySliceTest[callIndex] {
						t.Fatalf("outer readOnly appeared in %d index, got readOnly %t, expected %t. Test EVMs %v; test readonly %v",
							callIndex, readOnly.outer, readonlySliceTest[callIndex], evmsTestcase.emvs, readonlySliceTest)
					}

					if callIndex > 0 {
						if readOnly.before != readonliesGot[callIndex-1].in {
							t.Fatalf("before readOnly appeared in %d index, got readOnly %t, expected %t. Test EVMs %v; test readonly %v",
								callIndex, readOnly.before, readonliesGot[callIndex-1].in, evmsTestcase.emvs, readonlySliceTest)
						}
					}

					if readOnly.in && !gotReadonly {
						gotReadonly = true
						firstReadOnly = callIndex
					}

					if gotReadonly {
						if !readOnly.in {
							t.Fatalf("readOnly appeared in %d index, got non-readOnly in %d: %v. Test EVMs %v; test readonly %v",
								firstReadOnly, callIndex, trace(isEVMGot, readonliesGot), evmsTestcase.emvs, readonlySliceTest)
						}

						switch {
						case callIndex < firstReadOnly:
							if readOnly.after != false {
								t.Fatalf("after readOnly appeared in %d index(first readonly %d, case <firstReadOnly), got readOnly %t, expected %t. Test EVMs %v; test readonly %v",
									callIndex, firstReadOnly, readOnly.after, false, evmsTestcase.emvs, readonlySliceTest)
							}
						case callIndex == firstReadOnly:
							if readOnly.after != false {
								t.Fatalf("after readOnly appeared in %d index(first readonly %d, case ==firstReadOnly), got readOnly %t, expected %t. Test EVMs %v; test readonly %v",
									callIndex, firstReadOnly, readOnly.after, false, evmsTestcase.emvs, readonlySliceTest)
							}
						case callIndex > firstReadOnly:
							if readOnly.after != true {
								t.Fatalf("after readOnly appeared in %d index(first readonly %d, case >firstReadOnly), got readOnly %t, expected %t. Test EVMs %v; test readonly %v",
									callIndex, firstReadOnly, readOnly.after, true, evmsTestcase.emvs, readonlySliceTest)
							}
						}
					} else {
						if readOnly.after != false {
							t.Fatalf("after readOnly didn't appear. %d index, got readOnly %t, expected %t. Test EVMs %v; test readonly %v",
								callIndex, readOnly.after, false, evmsTestcase.emvs, readonlySliceTest)
						}
					}
				}
			})
		}
	}
}

type testSequential struct {
	env         *EVM
	currentIdx  *int
	readOnlys   []bool
	isEVMCalled []bool
}

func newTestSequential(env *EVM, currentIdx *int, readonlies []bool, isEVMCalled []bool) *testSequential {
	return &testSequential{env, currentIdx, readonlies, isEVMCalled}
}

func (st *testSequential) Run(_ *Contract, _ []byte, _ bool) ([]byte, error) {
	*st.currentIdx++
	c := NewJumpDestCache(16)
	nextContract := NewContract(
		&dummyContractRef{},
		common.Address{},
		new(uint256.Int),
		0,
		false,
		c,
	)

	return st.env.interpreter.Run(nextContract, nil, st.readOnlys[*st.currentIdx])
}

func trace(isEVMSlice []bool, readOnlySlice []*readOnlyState) string {
	res := "trace:\n"
	for i := 0; i < len(isEVMSlice); i++ {
		res += fmt.Sprintf("%d: EVM %t, readonly %t\n", i, isEVMSlice[i], readOnlySlice[i].in)
	}
	return res
}
