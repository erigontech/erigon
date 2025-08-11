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

package bor

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/polygon/heimdall"
)

// NetworkNameVals is a map of network name to validator set for tests/devnets
var NetworkNameVals = make(map[string][]*heimdall.Validator)

// Validator set for bor e2e test chain with 2 validator configuration
var BorE2ETestChain2Valset = []*heimdall.Validator{
	{
		ID:               1,
		Address:          common.HexToAddress("71562b71999873DB5b286dF957af199Ec94617F7"),
		VotingPower:      1000,
		ProposerPriority: 1,
	},
	{
		ID:               2,
		Address:          common.HexToAddress("9fB29AAc15b9A4B7F17c3385939b007540f4d791"),
		VotingPower:      1000,
		ProposerPriority: 2,
	},
}

// Validator set for bor devnet-chain with 1 validator configuration
var BorDevnetChainVals = []*heimdall.Validator{
	{
		ID:               1,
		Address:          common.HexToAddress("0x67b1d87101671b127f5f8714789C7192f7ad340e"),
		VotingPower:      1000,
		ProposerPriority: 1,
	},
}

func init() {
	NetworkNameVals[networkname.BorE2ETestChain2Val] = BorE2ETestChain2Valset
	NetworkNameVals[networkname.BorDevnet] = BorDevnetChainVals
}
