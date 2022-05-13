/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package u256

import (
	"github.com/holiman/uint256"
)

// Common big integers often used
var (
	N0   = uint256.NewInt(0)
	N1   = uint256.NewInt(1)
	N2   = uint256.NewInt(2)
	N4   = uint256.NewInt(4)
	N8   = uint256.NewInt(8)
	N27  = uint256.NewInt(27)
	N28  = uint256.NewInt(28)
	N32  = uint256.NewInt(32)
	N35  = uint256.NewInt(35)
	N100 = uint256.NewInt(100)
)
