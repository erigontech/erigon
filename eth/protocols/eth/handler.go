// Copyright 2020 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/core/rawdb"
)

const (
	// softResponseLimit is the target maximum size of replies to data retrievals.
	softResponseLimit = 2 * 1024 * 1024

	// estHeaderSize is the approximate size of an RLP encoded block header.
	estHeaderSize = 500

	// maxHeadersServe is the maximum number of block headers to serve. This number
	// is there to limit the number of disk lookups.
	MaxHeadersServe = 1024

	// maxBodiesServe is the maximum number of block bodies to serve. This number
	// is mostly there to limit the number of disk lookups. With 24KB block sizes
	// nowadays, the practical limit will always be softResponseLimit.
	MaxBodiesServe = 1024

	// maxReceiptsServe is the maximum number of block receipts to serve. This
	// number is mostly there to limit the number of disk lookups. With block
	// containing 200+ transactions nowadays, the practical limit will always
	// be softResponseLimit.
	maxReceiptsServe = 1024
)

// NodeInfo represents a short summary of the `eth` sub-protocol metadata
// known about the host peer.
type NodeInfo struct {
	Network    uint64         `json:"network"`    // Ethereum network ID (1=Frontier, Rinkeby=4, Görli=5)
	Difficulty *big.Int       `json:"difficulty"` // Total difficulty of the host's blockchain
	Genesis    libcommon.Hash `json:"genesis"`    // SHA3 hash of the host's genesis block
	Config     *chain.Config  `json:"config"`     // ChainDB configuration for the fork rules
	Head       libcommon.Hash `json:"head"`       // Hex hash of the host's best owned block
}

// ReadNodeInfo retrieves some `eth` protocol metadata about the running host node.
func ReadNodeInfo(getter kv.Getter, config *chain.Config, genesisHash libcommon.Hash, network uint64) *NodeInfo {
	headHash := rawdb.ReadHeadHeaderHash(getter)
	headNumber := rawdb.ReadHeaderNumber(getter, headHash)
	var td *big.Int
	if headNumber != nil {
		td, _ = rawdb.ReadTd(getter, headHash, *headNumber)
	}
	return &NodeInfo{
		Network:    network,
		Difficulty: td,
		Genesis:    genesisHash,
		Config:     config,
		Head:       headHash,
	}
}
