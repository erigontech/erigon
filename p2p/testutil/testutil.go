// Copyright 2025 The Erigon Authors
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
	"math/big"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
)

var (
	PoWMainnetChainConfig = &chain.Config{
		ChainID:               big.NewInt(1),
		HomesteadBlock:        big.NewInt(1150000),
		DAOForkBlock:          big.NewInt(1920000),
		TangerineWhistleBlock: big.NewInt(2463000),
		SpuriousDragonBlock:   big.NewInt(2675000),
		ByzantiumBlock:        big.NewInt(4370000),
		ConstantinopleBlock:   big.NewInt(7280000),
		PetersburgBlock:       big.NewInt(7280000),
		IstanbulBlock:         big.NewInt(9069000),
		MuirGlacierBlock:      big.NewInt(9200000),
		BerlinBlock:           big.NewInt(12244000),
		LondonBlock:           big.NewInt(12965000),
		ArrowGlacierBlock:     big.NewInt(13773000),
		GrayGlacierBlock:      big.NewInt(15050000),
		Ethash:                new(chain.EthashConfig),
	}

	MainnetGenesisHash = common.HexToHash("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3")
)
