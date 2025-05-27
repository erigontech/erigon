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

package gaspricecfg

import (
	"math/big"

	"github.com/erigontech/erigon-lib/common"
)

var DefaultIgnorePrice = big.NewInt(2 * common.Wei)

// BorDefaultGpoIgnorePrice defines the minimum gas price below which bor gpo will ignore transactions.
var BorDefaultGpoIgnorePrice = big.NewInt(25 * common.Wei)

var (
	DefaultMaxPrice = big.NewInt(500 * common.GWei)
)

type Config struct {
	Blocks           int
	Percentile       int
	MaxHeaderHistory int
	MaxBlockHistory  int
	Default          *big.Int `toml:",omitempty"`
	MaxPrice         *big.Int `toml:",omitempty"`
	IgnorePrice      *big.Int `toml:",omitempty"`
}
