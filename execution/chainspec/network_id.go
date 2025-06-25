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

package chainspec

const (
	MainnetChainID    = 1
	HoleskyChainID    = 17000
	SepoliaChainID    = 11155111
	HoodiChainID      = 560048
	DevChainName      = 1337
	AmoyChainID       = 80002
	BorMainnetChainID = 137
	BorDevnetChainID  = 1337
	GnosisChainID     = 100
	ChiadoChainID     = 10200
	TestID            = 1337
)

var All = []uint64{
	MainnetChainID,
	HoleskyChainID,
	SepoliaChainID,
	HoodiChainID,
	AmoyChainID,
	BorMainnetChainID,
	BorDevnetChainID,
	GnosisChainID,
	ChiadoChainID,
	TestID,
}

var NetworkNameByID = map[uint64]string{
	MainnetChainID:    "mainnet",
	HoleskyChainID:    "holesky",
	SepoliaChainID:    "sepolia",
	HoodiChainID:      "hoodi",
	AmoyChainID:       "amoy",
	BorMainnetChainID: "bor-mainnet",
	GnosisChainID:     "gnosis",
	ChiadoChainID:     "chiado",
}
