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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

//go:embed chainspecs
var chainspecs embed.FS

func readParliaChainSpec(filename string) *chain.Config {
	return chainspec.ReadChainConfig(chainspecs, filename)
}

var (
	Chapel = chainspec.Spec{
		Name:        networkname.Chapel,
		GenesisHash: common.HexToHash("0x6d3c66c5357ec91d5c43af47e234a939b22557cbb552dc45bebbceeed90fbe34"),
		Config:      chapelChainConfig,
		Bootnodes:   chapelBootnodes,
		Genesis:     ChapelGenesisBlock(),
	}
)

func init() {
	chainspec.RegisterChainSpec(networkname.Chapel, Chapel)
}
