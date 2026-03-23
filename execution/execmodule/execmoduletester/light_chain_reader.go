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

package execmoduletester

import (
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	rulesif "github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
)

// Compile-time interface check.
var _ rulesif.ChainReader = (*LightChainReader)(nil)

// LightChainReader implements rules.ChainReader with in-memory maps
// for headers and total difficulties, falling back to rawdb reads on
// the provided tx for genesis data.
type LightChainReader struct {
	Config_ *chain.Config
	Headers map[common.Hash]*types.Header // hash -> header
	TDs     map[common.Hash]*big.Int      // hash -> td
	Tx      kv.Tx                         // fallback for genesis reads
}

func (cr *LightChainReader) Config() *chain.Config                 { return cr.Config_ }
func (cr *LightChainReader) CurrentHeader() *types.Header          { return nil }
func (cr *LightChainReader) CurrentFinalizedHeader() *types.Header { return nil }
func (cr *LightChainReader) CurrentSafeHeader() *types.Header      { return nil }
func (cr *LightChainReader) FrozenBlocks() uint64                  { return 0 }
func (cr *LightChainReader) FrozenBorBlocks(bool) uint64           { return 0 }

func (cr *LightChainReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	if h, ok := cr.Headers[hash]; ok {
		return h
	}
	return rawdb.ReadHeader(cr.Tx, hash, number)
}

func (cr *LightChainReader) GetHeaderByNumber(number uint64) *types.Header {
	hash, err := rawdb.ReadCanonicalHash(cr.Tx, number)
	if err != nil || hash == (common.Hash{}) {
		return nil
	}
	return cr.GetHeader(hash, number)
}

func (cr *LightChainReader) GetHeaderByHash(hash common.Hash) *types.Header {
	if h, ok := cr.Headers[hash]; ok {
		return h
	}
	num := rawdb.ReadHeaderNumber(cr.Tx, hash)
	if num == nil {
		return nil
	}
	return rawdb.ReadHeader(cr.Tx, hash, *num)
}

func (cr *LightChainReader) GetTd(hash common.Hash, number uint64) *big.Int {
	if td, ok := cr.TDs[hash]; ok {
		return td
	}
	td, _ := rawdb.ReadTd(cr.Tx, hash, number)
	return td
}

func (cr *LightChainReader) GetBlock(hash common.Hash, number uint64) *types.Block {
	return rawdb.ReadBlock(cr.Tx, hash, number)
}

func (cr *LightChainReader) HasBlock(hash common.Hash, number uint64) bool {
	return rawdb.ReadHeader(cr.Tx, hash, number) != nil
}
