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

const (
	MainnetChainName        = "mainnet"
	HoleskyChainName        = "holesky"
	SepoliaChainName        = "sepolia"
	DevChainName            = "dev"
	AmoyChainName           = "amoy"
	BorMainnetChainName     = "bor-mainnet"
	BorDevnetChainName      = "bor-devnet"
	GnosisChainName         = "gnosis"
	BorE2ETestChain2ValName = "bor-e2e-test-2Val"
	ChiadoChainName         = "chiado"
	Test                    = "test"
)

var All = []string{
	MainnetChainName,
	HoleskyChainName,
	SepoliaChainName,
	AmoyChainName,
	BorMainnetChainName,
	BorDevnetChainName,
	GnosisChainName,
	ChiadoChainName,
	Test,
}
