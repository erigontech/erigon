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

package interfaces

import (
	"context"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/types"
)

type BlockReader interface {
	BlockByNumber(ctx context.Context, db kv.Tx, number uint64) (*types.Block, error)
	BlockByHash(ctx context.Context, db kv.Tx, hash common.Hash) (*types.Block, error)
	CurrentBlock(db kv.Tx) (*types.Block, error)
	BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (block *types.Block, senders []common.Address, err error)
	IterateFrozenBodies(f func(blockNum, baseTxNum, txCount uint64) error) error
}

type HeaderReader interface {
	Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (*types.Header, error)
	HeaderByNumber(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.Header, error)
	HeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (*uint64, error)
	HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (*types.Header, error)
	ReadAncestor(db kv.Getter, hash common.Hash, number, ancestor uint64, maxNonCanonical *uint64) (common.Hash, uint64)

	// HeadersRange - TODO: change it to `stream`
	HeadersRange(ctx context.Context, walker func(header *types.Header) error) error
	Integrity(ctx context.Context) error
}

type BodyReader interface {
	BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (body *types.Body, err error)
	BodyRlp(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (bodyRlp rlp.RawValue, err error)
	Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (body *types.Body, txCount uint32, err error)
	CanonicalBodyForStorage(ctx context.Context, tx kv.Getter, blockNum uint64) (body *types.BodyForStorage, err error)
	HasSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (bool, error)
}

type HeaderAndBodyReader interface {
	BlockReader
	BodyReader
	HeaderReader
}
