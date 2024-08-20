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

package stagedsync

import (
	"context"
	"math/big"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/services"
)

// ChainReader implements consensus.ChainReader
type ChainReader struct {
	Cfg         chain.Config
	Db          kv.Getter
	BlockReader services.FullBlockReader
	Logger      log.Logger
}

// Config retrieves the blockchain's chain configuration.
func (cr ChainReader) Config() *chain.Config {
	return &cr.Cfg
}

// CurrentHeader retrieves the current header from the local chain.
func (cr ChainReader) CurrentHeader() *types.Header {
	hash := rawdb.ReadHeadHeaderHash(cr.Db)
	h, _ := cr.BlockReader.HeaderByHash(context.Background(), cr.Db, hash)
	return h
}

// CurrentFinalizedHeader retrieves the current finalized header from the local chain.
func (cr ChainReader) CurrentFinalizedHeader() *types.Header {
	hash := rawdb.ReadForkchoiceFinalized(cr.Db)
	if hash == (libcommon.Hash{}) {
		return nil
	}

	return cr.GetHeaderByHash(hash)
}

func (cr ChainReader) CurrentSafeHeader() *types.Header {
	hash := rawdb.ReadForkchoiceSafe(cr.Db)
	if hash == (libcommon.Hash{}) {
		return nil
	}

	return cr.GetHeaderByHash(hash)
}

// GetHeader retrieves a block header from the database by hash and number.
func (cr ChainReader) GetHeader(hash libcommon.Hash, number uint64) *types.Header {
	h, _ := cr.BlockReader.Header(context.Background(), cr.Db, hash, number)
	return h
}

// GetHeaderByNumber retrieves a block header from the database by number.
func (cr ChainReader) GetHeaderByNumber(number uint64) *types.Header {
	h, _ := cr.BlockReader.HeaderByNumber(context.Background(), cr.Db, number)
	return h
}

// GetHeaderByHash retrieves a block header from the database by its hash.
func (cr ChainReader) GetHeaderByHash(hash libcommon.Hash) *types.Header {
	h, _ := cr.BlockReader.HeaderByHash(context.Background(), cr.Db, hash)
	return h
}

// GetBlock retrieves a block from the database by hash and number.
func (cr ChainReader) GetBlock(hash libcommon.Hash, number uint64) *types.Block {
	b, _, _ := cr.BlockReader.BlockWithSenders(context.Background(), cr.Db, hash, number)
	return b
}

// HasBlock retrieves a block from the database by hash and number.
func (cr ChainReader) HasBlock(hash libcommon.Hash, number uint64) bool {
	b, _ := cr.BlockReader.BodyRlp(context.Background(), cr.Db, hash, number)
	return b != nil
}

// GetTd retrieves the total difficulty from the database by hash and number.
func (cr ChainReader) GetTd(hash libcommon.Hash, number uint64) *big.Int {
	td, err := rawdb.ReadTd(cr.Db, hash, number)
	if err != nil {
		cr.Logger.Error("ReadTd failed", "err", err)
		return nil
	}
	return td
}

func (cr ChainReader) FrozenBlocks() uint64    { return cr.BlockReader.FrozenBlocks() }
func (cr ChainReader) FrozenBorBlocks() uint64 { return cr.BlockReader.FrozenBorBlocks() }

func (cr ChainReader) BorStartEventID(_ libcommon.Hash, _ uint64) uint64 {
	panic("bor events by block not implemented")
}
func (cr ChainReader) BorEventsByBlock(_ libcommon.Hash, _ uint64) []rlp.RawValue {
	panic("bor events by block not implemented")
}

func (cr ChainReader) BorSpan(spanId uint64) []byte {
	span, err := cr.BlockReader.Span(context.Background(), cr.Db, spanId)
	if err != nil {
		cr.Logger.Error("BorSpan failed", "err", err)
		return nil
	}

	return span
}
