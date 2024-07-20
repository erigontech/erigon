// Copyright 2020 The go-ethereum Authors
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

package bls

import (
	"bytes"
	"fmt"

	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/core/vm"
)

const (
	blsG1Add      = byte(0x0b)
	blsG1Mul      = byte(0x0c)
	blsG1MultiExp = byte(0x0d)
	blsG2Add      = byte(0x0e)
	blsG2Mul      = byte(0x0f)
	blsG2MultiExp = byte(0x10)
	blsPairing    = byte(0x11)
	blsMapG1      = byte(0x12)
	blsMapG2      = byte(0x13)
)

func FuzzG1Add(data []byte) int      { return fuzz(blsG1Add, data) }
func FuzzG1Mul(data []byte) int      { return fuzz(blsG1Mul, data) }
func FuzzG1MultiExp(data []byte) int { return fuzz(blsG1MultiExp, data) }
func FuzzG2Add(data []byte) int      { return fuzz(blsG2Add, data) }
func FuzzG2Mul(data []byte) int      { return fuzz(blsG2Mul, data) }
func FuzzG2MultiExp(data []byte) int { return fuzz(blsG2MultiExp, data) }
func FuzzPairing(data []byte) int    { return fuzz(blsPairing, data) }
func FuzzMapG1(data []byte) int      { return fuzz(blsMapG1, data) }
func FuzzMapG2(data []byte) int      { return fuzz(blsMapG2, data) }

func checkInput(id byte, inputLen int) bool {
	switch id {
	case blsG1Add:
		return inputLen == 256
	case blsG1Mul:
		return inputLen == 160
	case blsG1MultiExp:
		return inputLen%160 == 0
	case blsG2Add:
		return inputLen == 512
	case blsG2Mul:
		return inputLen == 288
	case blsG2MultiExp:
		return inputLen%288 == 0
	case blsPairing:
		return inputLen%384 == 0
	case blsMapG1:
		return inputLen == 64
	case blsMapG2:
		return inputLen == 128
	}
	panic("programmer error")
}

// The fuzzer functions must return
// 1 if the fuzzer should increase priority of the
//
//	given input during subsequent fuzzing (for example, the input is lexically
//	correct and was parsed successfully);
//
// -1 if the input must not be added to corpus even if gives new coverage; and
// 0  otherwise
// other values are reserved for future use.
func fuzz(id byte, data []byte) int {
	// Even on bad input, it should not crash, so we still test the gas calc
	precompile := vm.PrecompiledContractsPrague[libcommon.BytesToAddress([]byte{id})]
	gas := precompile.RequiredGas(data)
	if !checkInput(id, len(data)) {
		return 0
	}
	// If the gas cost is too large (25M), bail out
	if gas > 25*1000*1000 {
		return 0
	}
	cpy := make([]byte, len(data))
	copy(cpy, data)
	_, err := precompile.Run(cpy)
	if !bytes.Equal(cpy, data) {
		panic(fmt.Sprintf("input data modified, precompile %d: %x %x", id, data, cpy))
	}
	if err != nil {
		return 0
	}
	return 1
}
