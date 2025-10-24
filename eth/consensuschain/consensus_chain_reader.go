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

package consensuschain

import (
	"context"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/turbo/services"
)

type Reader struct {
	config      *chain.Config
	tx          kv.Tx
	blockReader services.FullBlockReader
	logger      log.Logger
}

func NewReader(config *chain.Config, tx kv.Tx, blockReader services.FullBlockReader, logger log.Logger) *Reader {
	return &Reader{config, tx, blockReader, logger}
}

func (cr Reader) Config() *chain.Config { return cr.config }
func (cr Reader) CurrentHeader() *types.Header {
	hash := rawdb.ReadHeadHeaderHash(cr.tx)
	h, _ := cr.blockReader.HeaderByHash(context.TODO(), cr.tx, hash)
	return h
}
func (cr Reader) CurrentFinalizedHeader() *types.Header {
	hash := rawdb.ReadForkchoiceFinalized(cr.tx)
	h, _ := cr.blockReader.HeaderByHash(context.Background(), cr.tx, hash)
	return h
}
func (cr Reader) CurrentSafeHeader() *types.Header {
	hash := rawdb.ReadForkchoiceSafe(cr.tx)
	h, _ := cr.blockReader.HeaderByHash(context.Background(), cr.tx, hash)
	return h
}
func (cr Reader) GetHeader(hash common.Hash, number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.Header(context.Background(), cr.tx, hash, number)
		return h
	}
	return rawdb.ReadHeader(cr.tx, hash, number)
}
func (cr Reader) GetHeaderByNumber(number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByNumber(context.Background(), cr.tx, number)
		return h
	}
	return rawdb.ReadHeaderByNumber(cr.tx, number)

}
func (cr Reader) GetHeaderByHash(hash common.Hash) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByHash(context.Background(), cr.tx, hash)
		return h
	}
	h, _ := rawdb.ReadHeaderByHash(cr.tx, hash)
	return h
}
func (cr Reader) GetTd(hash common.Hash, number uint64) *big.Int {
	td, err := rawdb.ReadTd(cr.tx, hash, number)
	if err != nil {
		cr.logger.Warn("ReadTd failed", "err", err)
		return nil
	}
	return td
}
func (cr Reader) FrozenBlocks() uint64              { return cr.blockReader.FrozenBlocks() }
func (cr Reader) FrozenBorBlocks(align bool) uint64 { return cr.blockReader.FrozenBorBlocks(align) }
func (cr Reader) GetBlock(hash common.Hash, number uint64) *types.Block {
	b, _, _ := cr.blockReader.BlockWithSenders(context.Background(), cr.tx, hash, number)
	return b
}
func (cr Reader) HasBlock(hash common.Hash, number uint64) bool {
	b, _ := cr.blockReader.BodyRlp(context.Background(), cr.tx, hash, number)
	return b != nil
}
