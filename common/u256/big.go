// Copyright 2014 The go-ethereum Authors
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

package u256

import (
	"github.com/holiman/uint256"
)

// Common big integers often used
var (
	Num0  = uint256.NewInt().SetUint64(0)
	Num1  = uint256.NewInt().SetUint64(1)
	Num2  = uint256.NewInt().SetUint64(2)
	Num8  = uint256.NewInt().SetUint64(8)
	Num32 = uint256.NewInt().SetUint64(32)
	Num35 = uint256.NewInt().SetUint64(35)
)
