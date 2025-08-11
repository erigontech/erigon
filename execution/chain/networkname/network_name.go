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

package networkname

import (
	"slices"
	"strings"
)

const (
	Mainnet             = "mainnet"
	Holesky             = "holesky"
	Sepolia             = "sepolia"
	Hoodi               = "hoodi"
	Dev                 = "dev"
	Amoy                = "amoy"
	BorMainnet          = "bor-mainnet"
	BorDevnet           = "bor-devnet"
	Gnosis              = "gnosis"
	BorE2ETestChain2Val = "bor-e2e-test-2Val"
	Chiado              = "chiado"
	Test                = "test"
)

var All = []string{
	Mainnet,
	Holesky,
	Sepolia,
	Hoodi,
	Amoy,
	BorMainnet,
	BorDevnet,
	Gnosis,
	Chiado,
	Test,
}

// Supported checks if the given network name is supported by Erigon.
func Supported(name string) bool { return slices.Contains(All, strings.ToLower(name)) }
