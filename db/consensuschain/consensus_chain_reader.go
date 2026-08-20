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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

type Reader struct {
	config      *chain.Config
	tx          kv.Tx
	blockReader dbservices.FullBlockReader
	logger      log.Logger
}

// logger is used only on read-error paths, so a nil one stays invisible until
// something fails. log.Logger is an interface, and calling it then panics.
func NewReader(config *chain.Config, tx kv.Tx, blockReader dbservices.FullBlockReader, logger log.Logger) *Reader {
	if logger == nil {
		logger = log.Root()
	}
	return &Reader{config, tx, blockReader, logger}
}

func (cr Reader) Config() *chain.Config { return cr.config }

func (cr Reader) CurrentHeader() (*types.Header, bool, error) {
	hash, ok, err := rawdb.ReadHeadHeaderHash(cr.tx)
	if err != nil || !ok {
		return nil, false, err
	}
	return cr.blockReader.HeaderByHash(context.TODO(), cr.tx, hash)
}

func (cr Reader) CurrentFinalizedHeader() (*types.Header, bool, error) {
	hash, ok, err := rawdb.ReadForkchoiceFinalized(cr.tx)
	if err != nil || !ok {
		return nil, false, err
	}
	return cr.blockReader.HeaderByHash(context.Background(), cr.tx, hash)
}

func (cr Reader) CurrentSafeHeader() (*types.Header, bool, error) {
	hash, ok, err := rawdb.ReadForkchoiceSafe(cr.tx)
	if err != nil || !ok {
		return nil, false, err
	}
	return cr.blockReader.HeaderByHash(context.Background(), cr.tx, hash)
}

func (cr Reader) GetHeader(hash common.Hash, number uint64) (*types.Header, bool, error) {
	if cr.blockReader != nil {
		return cr.blockReader.Header(context.Background(), cr.tx, hash, number)
	}
	return rawdb.ReadHeader(cr.tx, hash, number)
}

func (cr Reader) GetHeaderByNumber(number uint64) (*types.Header, bool, error) {
	if cr.blockReader != nil {
		return cr.blockReader.HeaderByNumber(context.Background(), cr.tx, number)
	}
	return rawdb.ReadHeaderByNumber(cr.tx, number)

}

func (cr Reader) GetHeaderByHash(hash common.Hash) (*types.Header, bool, error) {
	if cr.blockReader != nil {
		return cr.blockReader.HeaderByHash(context.Background(), cr.tx, hash)
	}
	return rawdb.ReadHeaderByHash(cr.tx, hash)
}

func (cr Reader) GetTd(hash common.Hash, number uint64) (*uint256.Int, bool, error) {
	return rawdb.ReadTd(cr.tx, hash, number)
}

func (cr Reader) FrozenBlocks() uint64 { return cr.blockReader.FrozenBlocks() }

func (cr Reader) FrozenBorBlocks(align bool) uint64 { return cr.blockReader.FrozenBorBlocks(align) }

func (cr Reader) GetBlock(hash common.Hash, number uint64) (*types.Block, bool, error) {
	b, _, ok, err := cr.blockReader.BlockWithSenders(context.Background(), cr.tx, hash, number)
	return b, ok, err
}

func (cr Reader) HasBlock(hash common.Hash, number uint64) (bool, error) {
	_, ok, err := cr.blockReader.BodyRlp(context.Background(), cr.tx, hash, number)
	return ok, err
}
