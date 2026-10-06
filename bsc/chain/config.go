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
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

//go:embed chainspecs
var chainspecs embed.FS

//go:embed upgrades
var upgrades embed.FS

// readParliaChainSpec reads a chainspec and fills its system-contract upgrades
// from upgradesDir, one file per activation block or time.
func readParliaChainSpec(filename, upgradesDir string) *chain.Config {
	config := chainspec.ReadChainConfig(chainspecs, filename)
	config.Parlia.BlockAlloc = readBlockAlloc(upgradesDir)
	return config
}

func readBlockAlloc(dir string) map[string]any {
	entries, err := upgrades.ReadDir(dir)
	if err != nil {
		panic(fmt.Sprintf("could not read system-contract upgrades %s: %v", dir, err))
	}
	blockAlloc := make(map[string]any, len(entries))
	for _, entry := range entries {
		data, err := upgrades.ReadFile(path.Join(dir, entry.Name()))
		if err != nil {
			panic(fmt.Sprintf("could not read system-contract upgrade %s: %v", entry.Name(), err))
		}
		var alloc any
		if err := json.Unmarshal(data, &alloc); err != nil {
			panic(fmt.Sprintf("could not parse system-contract upgrade %s: %v", entry.Name(), err))
		}
		blockAlloc[strings.TrimSuffix(entry.Name(), ".json")] = alloc
	}
	return blockAlloc
}

var (
	Chapel = chainspec.Spec{
		Name:        networkname.Chapel,
		GenesisHash: common.HexToHash("0x6d3c66c5357ec91d5c43af47e234a939b22557cbb552dc45bebbceeed90fbe34"),
		Config:      chapelChainConfig,
		Bootnodes:   chapelPeers,
		StaticPeers: chapelPeers,
		Genesis:     ChapelGenesisBlock(),
	}
)

func init() {
	chainspec.RegisterChainSpec(networkname.Chapel, Chapel)
}
