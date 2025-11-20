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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
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

	depth int
}

func (evm *testVM) Run(_ Contract, _ uint64, _ []byte, readOnly bool) (ret []byte, gas uint64, err error) {
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
		res, _, err := evm.env.interpreter.Run(*NewContract(
			common.Address{},
			common.Address{},
			common.Address{},
			uint256.Int{},
			evm.env.config.JumpDestCache,
		), 0, nil, evm.readOnlySliceTest[*evm.currentIdx])
		return res, 0, err
	}

	return
}

func (evm *testVM) Depth() int { return evm.depth }

func (evm *testVM) IncDepth() { evm.depth++ }
func (evm *testVM) DecDepth() { evm.depth-- }

type readOnlyState struct {
	outer  bool
	before bool
	in     bool
	after  bool
}

func (r *readOnlyState) String() string {
	return fmt.Sprintf("READONLY Status: outer %t; before %t; in %t; after %t", r.outer, r.before, r.in, r.after)
}
