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

package testutil

import (
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

// AssertStackWord consumes the top stack word and reverts unless it equals expected.
func AssertStackWord(p *program.Program, expected uint64) *program.Program {
	p.Push(expected).Op(vm.EQ)
	destination := p.Size() + 7
	return p.Op(vm.PUSH2).Append([]byte{byte(destination >> 8), byte(destination)}).Op(vm.JUMPI).
		Push0().Push0().Op(vm.REVERT, vm.JUMPDEST)
}
