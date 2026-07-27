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

//go:build gendispatch

package vm

import (
	"reflect"
	"runtime"
)

// DispatchEntry describes one live jump-table slot for the dispatch generator.
// Function fields carry the runtime symbol name of the value in the table;
// closures keep their compiler-assigned names (containing ".func"), which is
// how the generator tells them apart from directly callable named functions.
type DispatchEntry struct {
	Op          byte
	Name        string
	ConstantGas uint64
	NumPop      int
	MaxStack    int
	Execute     string
	DynamicGas  string
	MemorySize  string
	Undefined   bool
}

// DumpAmsterdamDispatch and DumpCancunDispatch return fully-constructed jump
// tables as data, so the generator does not have to re-implement the fork
// constructor chain (base sets + enableXXXX patches) to learn what each slot
// holds.
func DumpAmsterdamDispatch() []DispatchEntry { return dumpDispatch(&amsterdamInstructionSet) }
func DumpCancunDispatch() []DispatchEntry    { return dumpDispatch(&cancunInstructionSet) }

func dumpDispatch(tbl *JumpTable) []DispatchEntry {
	undefinedName := funcSymbol(opUndefined)
	out := make([]DispatchEntry, 0, 256)
	for i := range 256 {
		op := tbl[i]
		e := DispatchEntry{
			Op:          byte(i),
			Name:        OpCode(i).String(),
			ConstantGas: op.constantGas,
			NumPop:      op.numPop,
			MaxStack:    op.maxStack,
			Execute:     funcSymbol(op.execute),
		}
		if op.dynamicGas != nil {
			e.DynamicGas = funcSymbol(op.dynamicGas)
		}
		if op.memorySize != nil {
			e.MemorySize = funcSymbol(op.memorySize)
		}
		e.Undefined = e.Execute == undefinedName
		out = append(out, e)
	}
	return out
}

func funcSymbol(f any) string {
	v := reflect.ValueOf(f)
	if !v.IsValid() || v.IsNil() {
		return ""
	}
	return runtime.FuncForPC(v.Pointer()).Name()
}
