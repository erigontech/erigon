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

package chain

import (
	"embed"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/types"
)

//go:embed allocs
var allocs embed.FS

var chapelChainConfig = readParliaChainSpec("chainspecs/chapel.json")

// ChapelGenesisBlock returns the BSC testnet (Chapel) genesis block.
func ChapelGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     chapelChainConfig,
		Nonce:      0x00,
		Timestamp:  0x5e9da7ce,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000001284214b9b9c85549ab3d2b972df0deef66ac2c9b71b214cb885500844365e95cd9942c7276e7fd8a2959d3f95eae5dc7d70144ce1b73b403b7eb6e0980a75ecd1309ea12fa2ed87a8744fbfc9b863d535552c16704d214347f29fa77f77da6d75d7c752f474cf03cceff28abc65c9cbae594f725c80e12d0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x2625a00,
		Difficulty: uint256.NewInt(0x1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE"),
		Alloc:      chainspec.ReadPrealloc(allocs, "allocs/chapel.json"),
		Number:     0x00,
		GasUsed:    0x00,
	}
}
