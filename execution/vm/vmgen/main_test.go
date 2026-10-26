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

package main

import "testing"

func TestReturnsFindsNestedReturn(t *testing.T) {
	if !returns(mustStmt("if x { return 1, nil, err }")) {
		t.Fatal("a return inside an if was not found")
	}
	if returns(mustStmt("if x { y++ }")) {
		t.Fatal("found a return in return-free code")
	}
}

func TestCallsOutOfLineSetOnlyOnMemory(t *testing.T) {
	if !callsOutOfLine(mustStmt("callContext.Memory.Set(a, b, c)")) {
		t.Fatal("Memory.Set is not inlined by Go and needs the gas and pc save")
	}
	if callsOutOfLine(mustStmt("callContext.Stack.pushRef().Set(chainId)")) {
		t.Fatal("uint256's Set is inlined by Go and needs no save")
	}
}
