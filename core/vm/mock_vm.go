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
	"math/big"

	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/core/state"
)

type readonlyGetSetter interface {
	setReadonly(outerReadonly bool) func()
	getReadonly() bool
}

type testVM struct {
	readonlyGetSetter

	recordedReadOnlies  *[]*readOnlyState
	recordedIsEVMCalled *[]bool

	env               *EVM
	isEVMSliceTest    []bool
	readOnlySliceTest []bool
	currentIdx        *int
}

func (evm *testVM) Run(_ *Contract, _ []byte, readOnly bool) (ret []byte, err error) {
	currentReadOnly := new(readOnlyState)

	currentReadOnly.outer = readOnly
	currentReadOnly.before = evm.getReadonly()

	currentIndex := *evm.currentIdx

	callback := evm.setReadonly(readOnly)
	defer func() {
		callback()
		currentReadOnly.after = evm.getReadonly()
	}()

	currentReadOnly.in = evm.getReadonly()

	(*evm.recordedReadOnlies)[currentIndex] = currentReadOnly
	(*evm.recordedIsEVMCalled)[currentIndex] = true

	*evm.currentIdx++

	if *evm.currentIdx < len(evm.readOnlySliceTest) {
		res, err := run(evm.env, NewContract(
			&dummyContractRef{},
			libcommon.Address{},
			new(uint256.Int),
			0,
			false,
			evm.env.JumpDestCache,
		), nil, evm.readOnlySliceTest[*evm.currentIdx])
		return res, err
	}

	return
}

func (evm *testVM) Depth() int {
	return 0
}

type readOnlyState struct {
	outer  bool
	before bool
	in     bool
	after  bool
}

func (r *readOnlyState) String() string {
	return fmt.Sprintf("READONLY Status: outer %t; before %t; in %t; after %t", r.outer, r.before, r.in, r.after)
}

type dummyContractRef struct {
	calledForEach bool
}

func (dummyContractRef) ReturnGas(*big.Int)             {}
func (dummyContractRef) Address() libcommon.Address     { return libcommon.Address{} }
func (dummyContractRef) Value() *big.Int                { return new(big.Int) }
func (dummyContractRef) SetCode(libcommon.Hash, []byte) {}
func (d *dummyContractRef) ForEachStorage(callback func(key, value libcommon.Hash) bool) {
	d.calledForEach = true
}
func (d *dummyContractRef) SubBalance(amount *big.Int) {}
func (d *dummyContractRef) AddBalance(amount *big.Int) {}
func (d *dummyContractRef) SetBalance(*big.Int)        {}
func (d *dummyContractRef) SetNonce(uint64)            {}
func (d *dummyContractRef) Balance() *big.Int          { return new(big.Int) }

type dummyStatedb struct {
	state.IntraBlockState
}

func (*dummyStatedb) GetRefund() uint64 { return 1337 }
