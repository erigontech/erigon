// Copyright 2014 The go-ethereum Authors
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

package u256

import (
	"github.com/holiman/uint256"
)

// Common big integers often used
var (
	Num0  = uint256.NewInt(0)
	Num1  = uint256.NewInt(1)
	Num2  = uint256.NewInt(2)
	Num4  = uint256.NewInt(4)
	Num8  = uint256.NewInt(8)
	Num27 = uint256.NewInt(27)
	Num32 = uint256.NewInt(32)
	Num35 = uint256.NewInt(35)
)
